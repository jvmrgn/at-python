# Exercício 1
import pandas as pd
import json
import urllib.request
import os
import logging
import re
import sqlite3
import ast
from collections import Counter
import unidecode
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import unicodedata

print("======================================")
print("Iniciando exercício 1")
def carregar_dados():
    arquivo = "data/INFwebNet_Data.json"

    try:
        print(f"Verificando a existência do arquivo: {arquivo}")
        with open(arquivo, 'r') as file:
            print(f"Arquivo {arquivo} encontrado!")

        print(f"Lendo o arquivo: {arquivo}")
        df = pd.read_json(arquivo)
        print("Arquivo carregado com sucesso!")

        colunas_necessarias = [
            "id", "nome", "sobrenome", "email", "idade", "data de nascimento",
            "cidade", "estado", "hobbies", "linguagens de programacao", "jogos"
        ]
        
        for coluna in colunas_necessarias:
            if coluna not in df.columns:
                raise ValueError(f"Coluna obrigatória ausente: {coluna}")
        
        print("Todas as colunas obrigatórias estão presentes!")

        print("Preenchendo valores ausentes com 'Não Informado'...")
        df.fillna("Não Informado", inplace=True)
        
        df.replace(r'^\s*$', 'Não Informado', regex=True, inplace=True)
        print("Valores vazios ou com espaços em branco substituídos por 'Não Informado'!")

        print("Verificando e corrigindo 'linguagens de programacao'...")
        def corrigir_linguagens(x):
            try:
                return json.loads(x.replace("'", '"')) if isinstance(x, str) else x
            except json.JSONDecodeError:
                return "Não Informado"
        
        if df["linguagens de programacao"].dtype == object:
            df["linguagens de programacao"] = df["linguagens de programacao"].apply(corrigir_linguagens)
        print("'linguagens de programacao' corrigidas!")

        print("Verificando e corrigindo 'jogos'...")
        def corrigir_jogos(x):
            try:
                return json.loads(x) if isinstance(x, str) else x
            except json.JSONDecodeError:
                return "Não Informado"
        
        if df["jogos"].dtype == object:
            df["jogos"] = df["jogos"].apply(corrigir_jogos)
        print("'jogos' corrigidos!")

        print("DataFrame após processamento:")
        print(df.head())

        print(f"Salvando os dados processados de volta no arquivo {arquivo}...")
        with open(arquivo, 'w', encoding='utf-8') as file:
            json.dump(df.to_dict(orient="records"), file, ensure_ascii=False, indent=4)
        print("Arquivo salvo com sucesso!")

        return df

    except FileNotFoundError:
        print(f"Erro: O arquivo '{arquivo}' não foi encontrado.")
    except ValueError as e:
        print(f"Erro nos dados: {e}")
    except json.JSONDecodeError:
        print("Erro ao decodificar dados JSON. Verifique o formato do arquivo.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

df_processado = carregar_dados()

# Exercicio 2
print("======================================")
print("Iniciando exercício 2")
def extrair_plataformas(df):
    plataformas = set()
    
    if "jogos" not in df.columns:
        print("Erro: Coluna 'jogos' não encontrada.")
        return
    
    for jogos in df["jogos"]:
        if isinstance(jogos, list):
            for jogo in jogos:
                if isinstance(jogo, list) and len(jogo) > 1:
                    plataforma = jogo[1]
                    plataformas.add(plataforma)
                
    with open("plataformas.txt", "w", encoding="utf-8") as file:
        for plataforma in sorted(plataformas):
            file.write(plataforma + "\n")
    
    print(f"Plataformas extraídas e salvas em 'plataformas.txt'. Total de {len(plataformas)} plataformas.")

df_processado = carregar_dados()
extrair_plataformas(df_processado)

# Exercicio 3
print("======================================")
print("Iniciando exercício 3")
def carregar_plataformas():
    """
    Carrega uma lista de plataformas a partir de um arquivo de texto.

    Esta função tenta abrir o arquivo 'plataformas.txt' (ou um arquivo fornecido pelo usuário) e ler seu conteúdo,
    onde cada linha do arquivo representa uma plataforma. Se o arquivo não for encontrado, o usuário pode 
    fornecer um caminho alternativo. A função continua tentando carregar o arquivo até que seja bem-sucedida 
    ou o usuário opte por sair.

    O arquivo deve conter uma plataforma por linha. Se o arquivo não existir ou ocorrer algum erro ao tentar 
    abri-lo, a função pedirá ao usuário que forneça um novo caminho.

    Retorna:
    list: Uma lista com os nomes das plataformas carregadas a partir do arquivo.

    Exemplo:
    carregar_plataformas()
    ['PlayStation 4', 'Xbox One', 'Nintendo Switch']
    """
    while True:
        try:
            with open("plataformas.txt", "r", encoding="utf-8") as file:
                plataformas = [linha.strip() for linha in file.readlines()]
            print("Plataformas carregadas com sucesso!")
            return plataformas
        except FileNotFoundError:
            print("Erro: O arquivo 'plataformas.txt' não foi encontrado.")
            novo_caminho = input("Digite o caminho correto do arquivo ou 'sair' para encerrar: ")
            
            if novo_caminho.lower() == 'sair':
                print("Encerrando o programa...")
                break
            else:
                try:
                    with open(novo_caminho, "r", encoding="utf-8") as file:
                        plataformas = [linha.strip() for linha in file.readlines()]
                    print("Plataformas carregadas com sucesso!")
                    return plataformas
                except FileNotFoundError:
                    print(f"Erro: O arquivo '{novo_caminho}' não foi encontrado. Tente novamente.")
                    continue
                except Exception as e:
                    print(f"Ocorreu um erro inesperado ao tentar carregar o arquivo: {e}")
                    break
        except Exception as e:
            print(f"Ocorreu um erro inesperado: {e}")
            break

plataformas = carregar_plataformas()
if plataformas:
    print("Plataformas carregadas:", plataformas)

# Exercicio 4
print("======================================")
print("Iniciando exercício 4")
def baixar_paginas_wikipedia(plataformas):
    """
    Baixa páginas da Wikipedia para uma lista de plataformas e salva como arquivos HTML.

    Esta função recebe uma lista de plataformas e para cada uma delas, gera a URL correspondente
    para a Wikipedia e faz o download da página, salvando-a como um arquivo HTML no diretório 'Plataformas'.
    Caso o diretório não exista, ele será criado.

    Parâmetros:
    plataformas (list): Lista de nomes de plataformas para as quais as páginas da Wikipedia serão baixadas.

    Retorna:
    list: Uma lista com os caminhos absolutos dos arquivos HTML salvos.

    Exemplo:
    plataformas = ["PlayStation_4", "Xbox_One"]
    baixar_paginas_wikipedia(plataformas)
    ['C:/caminho/para/Plataformas/plataforma_PlayStation_4.html', 
     'C:/caminho/para/Plataformas/plataforma_Xbox_One.html']
    """
    pasta = "Plataformas"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    
    caminhos_arquivos = []
    
    for plataforma in plataformas:
        nome_plataforma = plataforma.replace(" ", "_")
        url = f"https://pt.wikipedia.org/wiki/Lista_de_jogos_para_{nome_plataforma}"

        nome_arquivo = f"{pasta}/plataforma_{nome_plataforma}.html"
        
        try:
            print(f"Baixando página para a plataforma: {plataforma}...")
            urllib.request.urlretrieve(url, nome_arquivo)
            print(f"Arquivo {nome_arquivo} salvo com sucesso!")
            
            caminhos_arquivos.append(os.path.abspath(nome_arquivo))
            
        except Exception as e:
            print(f"Erro ao tentar baixar a página para {plataforma}: {e}")
    
    return caminhos_arquivos

plataformas = ["PlayStation 4", "Xbox One", "Nintendo Switch"]
caminhos = baixar_paginas_wikipedia(plataformas)
print("Caminhos dos arquivos baixados:", caminhos)

# Exercicio 5
print("======================================")
print("Iniciando exercício 5")
logging.basicConfig(filename='erros_download.txt', level=logging.ERROR,
                    format='%(asctime)s - %(message)s')

def baixar_paginas_wikipedia(plataformas):
    pasta = "Plataformas"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    
    caminhos_arquivos = [] 
    
    for plataforma in plataformas:
        nome_plataforma = plataforma.replace(" ", "_")
        url = f"https://pt.wikipedia.org/wiki/Lista_de_jogos_para_{nome_plataforma}"

        nome_arquivo = f"{pasta}/plataforma_{nome_plataforma}.html"
        
        try:
            print(f"Baixando página para a plataforma: {plataforma}...")
            urllib.request.urlretrieve(url, nome_arquivo)
            print(f"Arquivo {nome_arquivo} salvo com sucesso!")
            
            caminhos_arquivos.append(os.path.abspath(nome_arquivo))
            
        except HTTPError as e:
            mensagem_erro = f"Erro HTTP ao tentar baixar a página para {plataforma}: {e}"
            logging.error(mensagem_erro)
            print(mensagem_erro)
        except URLError as e:
            mensagem_erro = f"Erro de URL ao tentar baixar a página para {plataforma}: {e}"
            logging.error(mensagem_erro)
            print(mensagem_erro)
        except Exception as e:
            mensagem_erro = f"Erro desconhecido ao tentar baixar a página para {plataforma}: {e}"
            logging.error(mensagem_erro)
            print(mensagem_erro)
            
            try:
                if os.path.exists(nome_arquivo):
                    with open(nome_arquivo, 'r', encoding='utf-8') as file:
                        html = file.read()
                        soup = BeautifulSoup(html, 'html.parser')
                        erro_404_link = soup.find('a', href=True, text='procure por')
                        
                        if erro_404_link:
                            novo_link = "https://pt.wikipedia.org" + erro_404_link['href']
                            print(f"Tentando baixar o link alternativo: {novo_link}")
                            urllib.request.urlretrieve(novo_link, nome_arquivo)
                            print(f"Arquivo alternativo {nome_arquivo} salvo com sucesso!")
            except Exception as e:
                logging.error(f"Erro ao tentar processar a página 404 para {plataforma}: {e}")
                print(f"Erro ao tentar processar a página 404 para {plataforma}: {e}")

    return caminhos_arquivos

plataformas = ["PlayStation 4", "Xbox One", "Nintendo Switch"]
caminhos = baixar_paginas_wikipedia(plataformas)
print("Caminhos dos arquivos baixados:", caminhos)

# Exercicio 6
print("======================================")
print("Iniciando exercício 6 e 7")

def limpar_texto(texto):
    """
    Limpa e normaliza o texto para uma versão em minúsculas e sem caracteres especiais.

    Esta função recebe um texto, remove acentos e caracteres especiais, e converte todos os caracteres
    para minúsculas. O objetivo é garantir que o texto seja comparável de forma simples, sem considerar
    variações de maiúsculas, minúsculas ou acentuação.

    Parâmetros:
    texto (str): O texto a ser normalizado.

    Retorna:
    str: O texto normalizado, em minúsculas e sem caracteres especiais.

    Exemplo:
    limpar_texto('Olá, Mundo!')
    'ola mundo'
    """
    texto_normalizado = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower()
    return texto_normalizado


def parsear_paginas(diretorio_plataformas, nome_plataforma):
    """
    Parseia as páginas HTML de um diretório de plataformas e extrai dados de jogos.

    Esta função percorre os arquivos HTML presentes no diretório especificado, verifica se o nome
    da plataforma está presente nos nomes dos arquivos e nas páginas, e extrai informações sobre os jogos
    usando a função `extrair_tabelas_de_jogos`. Caso haja algum erro no título da página ou na extração dos dados,
    ele será registrado em um arquivo de erro.

    Parâmetros:
    diretorio_plataformas (str): O diretório contendo os arquivos HTML das plataformas.
    nome_plataforma (str): O nome da plataforma a ser buscada nas páginas HTML.

    Retorna:
    list: Uma lista de dicionários, cada um contendo a plataforma e os jogos extraídos dessa plataforma.

    Exemplo:
    parsear_paginas('/diretorio/exemplo', 'PlataformaXYZ')
    [{'plataforma': 'PlataformaXYZ', 'jogos': [...]}, {...}]
    """
    dados_plataformas = []

    for filename in os.listdir(diretorio_plataformas):
        if filename.endswith(".html") and nome_plataforma.lower() in limpar_texto(filename).lower():
            caminho_arquivo = os.path.join(diretorio_plataformas, filename)

            try:
                with open(caminho_arquivo, 'r', encoding='utf-8') as file:
                    html = file.read()

                soup = BeautifulSoup(html, 'html.parser')
                titulo_pagina = soup.title.string.strip()

                if limpar_texto(nome_plataforma) not in limpar_texto(titulo_pagina):
                    erro = f"Erro: Título da página '{titulo_pagina}' não corresponde à plataforma '{nome_plataforma}'."
                    with open("erros_parse.txt", "a", encoding="utf-8") as f:
                        f.write(erro + '\n')
                    raise ValueError(erro)

                print(f"Título '{titulo_pagina}' corresponde à plataforma '{nome_plataforma}'")
                
                jogos_extraidos = extrair_tabelas_de_jogos(soup, nome_plataforma)
                if jogos_extraidos:
                    dados_plataformas.append({"plataforma": nome_plataforma, "jogos": jogos_extraidos})

            except Exception as e:
                print(f"Erro ao tentar parsear a página {caminho_arquivo}: {e}")

    return dados_plataformas

def extrair_tabelas_de_jogos(soup, nome_plataforma):
    """
    Extrai dados de jogos de tabelas HTML em uma página de plataforma.

    Esta função encontra todas as tabelas com a classe 'wikitable' na página HTML fornecida, analisa as colunas e
    as linhas de cada tabela e extrai as informações relevantes sobre os jogos, como nome e dados específicos
    para cada jogo. As informações extraídas são retornadas como uma lista de dicionários.

    Parâmetros:
    soup (BeautifulSoup): O objeto BeautifulSoup que contém a página HTML da plataforma.
    nome_plataforma (str): O nome da plataforma que está sendo analisada (para fins de validação).

    Retorna:
    list: Uma lista de dicionários, onde cada dicionário contém o nome de um jogo e seus respectivos dados.

    Exemplo:
    extrair_tabelas_de_jogos(soup, 'PlataformaXYZ')
    [{'nome_jogo': 'Jogo1', 'dados_jogo': {'coluna1': 'valor1', 'coluna2': 'valor2'}}, {...}]
    """
    tabelas = soup.find_all('table', class_='wikitable')
    print(f"Tabelas encontradas: {len(tabelas)}")

    jogos_extraidos = []

    for idx, tabela in enumerate(tabelas):
        print(f"\nAnalisando a tabela {idx+1}...")

        cabecalho = tabela.find_all('th')
        colunas = [col.text.strip() for col in cabecalho]
        print(f"Colunas encontradas: {colunas}")
        linhas = tabela.find_all('tr')[1:]

        for linha_idx, linha in enumerate(linhas):
            colunas_linha = linha.find_all('td')
            print(f"Linha {linha_idx+1} (dados): {[col.text.strip() for col in colunas_linha]}")

            if len(colunas_linha) > 1:
                jogo = {
                    "nome_jogo": colunas_linha[0].text.strip(),
                    "dados_jogo": {}
                }
                for i, col in enumerate(colunas_linha[1:]):
                    if i < len(colunas) - 1:
                        jogo["dados_jogo"][colunas[i + 1]] = col.text.strip()

                jogos_extraidos.append(jogo)

    print(f"\nTotal de jogos extraídos: {len(jogos_extraidos)}")
    return jogos_extraidos

# Exercicio 8
print("======================================")
print("Iniciando exercício 8")

regex_email = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
regex_url = r"((([A-Za-z]{3,9}:(?:\/\/)?)(?:[-;:&=\+\$,\w]+@)?[A-Za-z0-9.-]+|(?:www.|[-;:&=\+\$,\w]+@)[A-Za-z0-9.-]+)((?:\/[\+~%\/.\w_-]*)?\??(?:[-\+=&;%@.\w_]*)#?(?:[.\!\/\\w]*))?)"

def extrair_urls_emails(diretorio):
    conexoes = {
        "urls": [],
        "emails": []
    }

    for filename in os.listdir(diretorio):
        if filename.endswith(".html"):
            caminho_arquivo = os.path.join(diretorio, filename)
            with open(caminho_arquivo, 'r', encoding='utf-8') as file:
                html = file.read()

            soup = BeautifulSoup(html, 'html.parser')

            urls = re.findall(regex_url, html)
            emails = re.findall(regex_email, html)

            conexoes["urls"].extend([url[0] for url in urls])
            conexoes["emails"].extend([email[0] for email in emails])

    with open('conexoes_plataformas.json', 'w', encoding='utf-8') as json_file:
        json.dump(conexoes, json_file, ensure_ascii=False, indent=4)

    print("URLs e e-mails extraídos com sucesso!")

extrair_urls_emails('Plataformas')

# Exercicio 9
print("======================================")
print("Iniciando exercício 9")

def exportar_dados_jogos(dados_jogos):
    with open('dados_jogos_plataformas.json', 'w', encoding='utf-8') as json_file:
        json.dump(dados_jogos, json_file, ensure_ascii=False, indent=4)

    print("Dados dos jogos exportados com sucesso!")

diretorio_plataformas = 'Plataformas'
nome_plataforma = 'Xbox'

dados_jogos_ex7 = parsear_paginas(diretorio_plataformas, nome_plataforma)
exportar_dados_jogos(dados_jogos_ex7)

# Exercicio 10
print("======================================")
print("Iniciando exercício 10")
def carregar_dados_usuarios(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        dados_usuarios = json.load(f)
    
    usuarios_data = {
        'id': [usuario['id'] for usuario in dados_usuarios],
        'nome': [usuario['nome'] for usuario in dados_usuarios],
        'jogos_mencionados': [[jogo[0] for jogo in usuario['jogos']] for usuario in dados_usuarios]
    }
    
    df_usuarios = pd.DataFrame(usuarios_data)
    return df_usuarios

def carregar_dados_jogos_plataformas(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        dados_jogos_plataformas = json.load(f)
    
    jogos_plataformas = {}
    for plataforma in dados_jogos_plataformas:
        for jogo in plataforma['jogos']:
            if 'nome_jogo' in jogo and 'dados_jogo' in jogo:
                nome_jogo = jogo['nome_jogo'].strip().lower()
                plataforma_nome = plataforma['plataforma']
                jogos_plataformas[nome_jogo] = plataforma_nome
            else:
                print(f"Aviso: Chave 'nome_jogo' ou 'dados_jogo' não encontrada no jogo: {jogo}")
    
    return jogos_plataformas

def associar_jogos_usuarios(df_usuarios, jogos_extraidos):
    df_usuarios['jogos_associados'] = [[] for _ in range(len(df_usuarios))]

    for index, row in df_usuarios.iterrows():
        jogos_mencionados = [jogo.strip().lower() for jogo in row['jogos_mencionados']]
        associacoes = []

        for jogo in jogos_mencionados:
            jogo_normalizado = unidecode.unidecode(jogo)
            encontrado = False

            for nome_jogo_extraido, plataforma in jogos_extraidos.items():
                nome_jogo_normalizado = unidecode.unidecode(nome_jogo_extraido).lower()

                if jogo_normalizado in nome_jogo_normalizado:
                    associacoes.append({'jogo': nome_jogo_extraido, 'plataforma': plataforma})
                    encontrado = True
                    break 

            if not encontrado:
                print(f"Jogo não encontrado em jogos_extraidos: {jogo}") 

        df_usuarios.at[index, 'jogos_associados'] = associacoes

    return df_usuarios

dados_jogos_plataformas = carregar_dados_jogos_plataformas('dados_jogos_plataformas.json')
df_usuarios = carregar_dados_usuarios('data/INFwebNet_Data.json')

df_usuarios_atualizado = associar_jogos_usuarios(df_usuarios, dados_jogos_plataformas)

print(df_usuarios_atualizado)

# Exercicio 11
print("======================================")
print("Iniciando exercício 11")
from sqlalchemy import create_engine

banco_dados = "INFwebNET_DB.db"
arquivo_jogos = "dados_jogos_plataformas.json"

def carregar_dados_jogos_plataformas(caminho_arquivo):
    """
    Carrega os dados de jogos e plataformas a partir de um arquivo JSON.
    """
    with open(caminho_arquivo, 'r', encoding='utf-8') as f:
        dados_jogos_plataformas = json.load(f)

    jogos_plataformas = []

    for plataforma in dados_jogos_plataformas:
        for jogo in plataforma['jogos']:
            nome_jogo = jogo['nome_jogo']
            exclusivo = jogo['dados_jogo'].get('Exclusivo', 'Desconhecido') 
            desenvolvedora = jogo['dados_jogo'].get('Desenvolvedora', 'Desconhecido')
            jogos_plataformas.append({'jogo': nome_jogo, 'Exclusivo': exclusivo, 'Desenvolvedora': desenvolvedora, 'plataforma': plataforma['plataforma']})
    
    df_jogos_plataformas = pd.DataFrame(jogos_plataformas)
    return df_jogos_plataformas

def atualizar_banco_dados():
    """
    Atualiza o banco de dados SQLite, adicionando a tabela 'Jogos_Plataformas'.
    """
    try:
        df_jogos_plataformas = carregar_dados_jogos_plataformas(arquivo_jogos)

        engine = create_engine(f"sqlite:///{banco_dados}")
        
        df_jogos_plataformas.to_sql("Jogos_Plataformas", con=engine, if_exists="replace", index=False)

        print("Tabela 'Jogos_Plataformas' criada e dados inseridos com sucesso!")
    
    except Exception as e:
        print(f"Erro ao atualizar o banco de dados: {e}")

atualizar_banco_dados()

# Exercício 12
print("======================================")
print("Iniciando exercício 12")

def consultar_usuarios_por_jogo():
    nome_jogo = input("Digite o nome do jogo: ")

    try:
        conn = sqlite3.connect("INFwebNET_DB.db")
        cursor = conn.cursor()

        query = """
        SELECT nome, jogos
        FROM Consolidado_Atualizado
        """
        cursor.execute(query)
        
        usuarios = cursor.fetchall()

        usuarios_encontrados = []

        for usuario in usuarios:
            nome_usuario = usuario[0]
            jogos_str = usuario[1]

            jogos = ast.literal_eval(jogos_str)

            for jogo, plataforma in jogos:
                if nome_jogo.lower() in jogo.lower():
                    usuarios_encontrados.append(nome_usuario)
                    break
        
        if usuarios_encontrados:
            print(f"Usuários que jogam '{nome_jogo}':")
            for usuario in usuarios_encontrados:
                print(usuario)
        else:
            print(f"Nenhum usuário encontrado para o jogo '{nome_jogo}'.")
        
        conn.close()
    
    except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")

consultar_usuarios_por_jogo()

# Exercício 13
print("======================================")
print("Iniciando exercício 13")
def plataforma_mais_popular():
    try:
        conn = sqlite3.connect("INFwebNET_DB.db")
        cursor = conn.cursor()

        query = """
        SELECT jogos
        FROM Consolidado_Atualizado
        """
        cursor.execute(query)

        plataformas_counter = Counter()

        usuarios = cursor.fetchall()

        for usuario in usuarios:
            jogos_str = usuario[0]

            jogos = ast.literal_eval(jogos_str)

            for jogo, plataforma in jogos:
                plataformas_counter[plataforma] += 1
        
        if plataformas_counter:
            plataforma_mais_popular = plataformas_counter.most_common(1)[0]
            print(f"A plataforma mais popular é '{plataforma_mais_popular[0]}' com {plataforma_mais_popular[1]} usuários.")
        else:
            print("Nenhuma plataforma encontrada.")
        
        conn.close()
    
    except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")

plataforma_mais_popular()

# Exercício 14
print("======================================")
print("Iniciando exercício 14")
def salvar_dados_completos():
    try:
        conn = sqlite3.connect("INFwebNET_DB.db")
        cursor = conn.cursor()

        query = """
        SELECT nome, jogos
        FROM Consolidado_Atualizado
        """
        cursor.execute(query)

        usuarios_completos = []

        usuarios = cursor.fetchall()

        for usuario in usuarios:
            nome_usuario = usuario[0]
            jogos_str = usuario[1]

            jogos = ast.literal_eval(jogos_str)

            dados_usuario = {
                'nome': nome_usuario,
                'jogos': [{'jogo': jogo, 'plataforma': plataforma} for jogo, plataforma in jogos]
            }

            usuarios_completos.append(dados_usuario)

        with open('INFwebNET_Completo.json', 'w', encoding='utf-8') as f:
            json.dump(usuarios_completos, f, ensure_ascii=False, indent=4)

        print("Dados completos salvos com sucesso em 'INFwebNET_Completo.json'.")

        conn.close()
    
    except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")
    except Exception as e:
        print(f"Erro ao salvar os dados: {e}")

salvar_dados_completos()