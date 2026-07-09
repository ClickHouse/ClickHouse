---
description: 'Documentação da interface de cliente de linha de comando do ClickHouse'
sidebar_label: 'ClickHouse Client'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse Client'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

O ClickHouse fornece um cliente nativo de linha de comando para executar consultas SQL diretamente em um servidor ClickHouse.
Ele oferece suporte tanto ao modo interativo (para executar consultas em tempo real) quanto ao modo em lote (para scripts e automação).
Os resultados da consulta podem ser exibidos no terminal ou exportados para um arquivo, com suporte a todos os [formatos](formats.md) de saída do ClickHouse, como Pretty, CSV, JSON e outros.

O cliente fornece feedback em tempo real sobre a execução da consulta, com uma barra de progresso e o número de linhas lidas, bytes processados e tempo de execução da consulta.
Ele oferece suporte tanto a [opções de linha de comando](#command-line-options) quanto a [arquivos de configuração](#configuration_files).

<div id="install">
  ## Instalar
</div>

Para baixar o ClickHouse, execute:

```bash
curl https://clickhouse.com/ | sh
```

Para instalá-lo também, execute:

```bash
sudo ./clickhouse install
```

Consulte [Instalar o ClickHouse](../getting-started/install/install.mdx) para ver mais opções de instalação.

Versões diferentes do cliente e do servidor são compatíveis entre si, mas alguns recursos podem não estar disponíveis em clientes mais antigos. Recomendamos usar a mesma versão para o cliente e o servidor.

<div id="run">
  ## Execute
</div>

:::note
Se você apenas baixou o ClickHouse, mas não o instalou, use `./clickhouse client` em vez de `clickhouse-client`.
:::

Para se conectar ao servidor ClickHouse, execute:

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

Especifique detalhes adicionais da conexão, conforme necessário:

| Option                           | Description                                                                                                                                                                   |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | A porta na qual o servidor ClickHouse aceita conexões. As portas padrão são 9440 (TLS) e 9000 (sem TLS). Observe que o ClickHouse Client usa o protocolo nativo, não HTTP(S). |
| `-s [ --secure ]`                | Define se o TLS deve ser usado (geralmente detectado automaticamente).                                                                                                        |
| `-u [ --user ] <username>`       | O usuário do banco de dados com o qual se conectar. Por padrão, a conexão é feita com o usuário `default`.                                                                    |
| `--password <password>`          | A senha do usuário do banco de dados. Você também pode especificar a senha de uma conexão no arquivo de configuração. Se não especificar a senha, o cliente a solicitará.     |
| `-c [ --config ] <path-to-file>` | O local do arquivo de configuração do ClickHouse Client, caso ele não esteja em um dos locais padrão. Consulte [Arquivos de configuração](#configuration_files).              |
| `--connection <name>`            | O nome de um conjunto de detalhes de conexão pré-configurado no [arquivo de configuração](#connection-credentials).                                                           |

Para ver a lista completa de opções de linha de comando, consulte [Opções de linha de comando](#command-line-options).

<div id="connecting-cloud">
  ### Conectando ao ClickHouse Cloud
</div>

Os detalhes do seu serviço do ClickHouse Cloud estão disponíveis no console do ClickHouse Cloud. Selecione o serviço ao qual você deseja se conectar e clique em **Connect**:

<Image img={cloud_connect_button} size="md" alt="Botão Connect do serviço do ClickHouse Cloud" />

<br />

<br />

Escolha **Native** e os detalhes serão exibidos junto com um exemplo de comando `clickhouse-client`:

<Image img={connection_details_native} size="md" alt="Detalhes da conexão Native TCP do ClickHouse Cloud" />

<div id="connection-credentials">
  ### Armazenando conexões em um arquivo de configuração
</div>

Você pode armazenar os detalhes de conexão de um ou mais servidores ClickHouse em um [arquivo de configuração](#configuration_files).

O formato é o seguinte:

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

Consulte a [seção sobre arquivos de configuração](#configuration_files) para mais informações.

:::note
Para destacar a sintaxe da consulta, os exemplos a seguir omitem os detalhes da conexão (`--host`, `--port` etc.). Lembre-se de adicioná-los ao usar os comandos.
:::

<div id="interactive-mode">
  ## Modo interativo
</div>

<div id="using-interactive-mode">
  ### Usando o modo interativo
</div>

Para executar o ClickHouse em modo interativo, basta usar:

```bash
clickhouse-client
```

Isso abre o loop Read-Eval-Print (REPL), no qual você pode começar a digitar consultas SQL de forma interativa.
Quando a conexão for estabelecida, você verá um prompt no qual poderá inserir consultas:

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

No modo interativo, o formato de saída padrão é `PrettyCompact`.
Você pode alterar o formato na cláusula `FORMAT` da consulta ou especificando a opção de linha de comando `--format`.
Para usar o formato Vertical, você pode usar `--vertical` ou especificar `\G` no final da consulta.
Nesse formato, cada valor é impresso em uma linha separada, o que é conveniente para tabelas com muitas colunas.

No modo interativo, por padrão, tudo o que foi digitado é executado quando você pressiona `Enter`.
Não é necessário usar ponto e vírgula no final da consulta.

Você pode iniciar o cliente com o parâmetro `-m, --multiline`.
Para inserir uma consulta multilinha, digite uma barra invertida `\` antes da quebra de linha.
Depois de pressionar `Enter`, será solicitado que você digite a próxima linha da consulta.
Para executar a consulta, termine-a com ponto e vírgula e pressione `Enter`.

O ClickHouse Client é baseado em `replxx` (semelhante ao `readline`), portanto usa atalhos de teclado conhecidos e mantém um histórico.
Por padrão, o histórico é gravado em `~/.clickhouse-client-history`.

Para sair do cliente, pressione `Ctrl+D` ou digite um dos seguintes em vez de uma consulta:

* `exit` ou `exit;`
* `quit` ou `quit;`
* `q`, `Q` ou `:q`
* `logout` ou `logout;`

<div id="getting-help">
  ### Obtendo ajuda
</div>

Você pode consultar a documentação de qualquer função, engine de tabela, tipo de dado, formato, configuração ou outro componente do sistema sem sair do cliente. Digite `help` seguido do nome (as formas equivalentes `/help`, `man` e `/man` também funcionam):

```text
help domainWithoutWWW
```

A busca não diferencia maiúsculas de minúsculas e consulta a tabela [`system.documentation`](../operations/system-tables/documentation.md). A documentação correspondente é exibida no terminal a partir de Markdown, com texto em negrito/itálico, tabelas e blocos de código com realce de sintaxe. Quando um nome é compartilhado por vários componentes (por exemplo, `file`, que é tanto uma função quanto um engine de tabela), todos eles são exibidos.

Quando não há nenhuma correspondência exata, o cliente lista nomes semelhantes (levando em conta possíveis erros de digitação) e os componentes cuja documentação menciona a palavra:

```text
help maxx_threads
```

Digitar `help` sem nenhum argumento exibe um breve resumo de como usar.

<div id="processing-info">
  ### Informações sobre o processamento de consultas
</div>

Ao processar uma consulta, o cliente mostra:

1. O progresso, que por padrão é atualizado no máximo 10 vezes por segundo.
   Em consultas rápidas, talvez não haja tempo para que o progresso seja exibido.
2. A consulta formatada após o parsing, para depuração.
3. O resultado no formato especificado.
4. O número de linhas no resultado, o tempo decorrido e a velocidade média de processamento da consulta.
   Todas as quantidades de dados se referem a dados não comprimidos.

Você pode cancelar uma consulta longa pressionando `Ctrl+C`.
No entanto, ainda será necessário aguardar um pouco até que o servidor interrompa a solicitação.
Não é possível cancelar uma consulta em determinados estágios.
Se você não esperar e pressionar `Ctrl+C` uma segunda vez, o cliente será encerrado.

O ClickHouse Client permite fornecer dados externos (tabelas temporárias externas) para consultas.
Para mais informações, consulte a seção [Dados externos para o processamento de consultas](../engines/table-engines/special/external-data.md).

<div id="cli_aliases">
  ### Aliases
</div>

Você pode usar os aliases abaixo no REPL:

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - repete a última consulta

<div id="keyboard_shortcuts">
  ### Atalhos de teclado
</div>

* `Alt (Option) + Shift + e` - abre o editor com a consulta atual. É possível especificar qual editor usar com a variável de ambiente `EDITOR`. Por padrão, `vim` é usado.
* `Alt (Option) + #` - comenta a linha.
* `Ctrl + r` - busca difusa no histórico.

A lista completa dos atalhos de teclado disponíveis está em [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
Para configurar corretamente a tecla meta (Option) no MacOS:

iTerm2: vá para Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key e clique em Esc+
:::

<div id="batch-mode">
  ## Modo em lote
</div>

<div id="using-batch-mode">
  ### Usando o modo em lote
</div>

Em vez de usar o ClickHouse Client interativamente, você pode executá-lo no modo em lote.
No modo em lote, o ClickHouse executa uma única consulta e é encerrado imediatamente - não há prompt nem loop interativo.

Você pode especificar uma única consulta assim:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

Você também pode usar a opção de linha de comando `--query`:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

Você pode fornecer uma consulta via `stdin`:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

Supondo a existência de uma tabela `messages`, você também pode inserir dados pela linha de comando:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

Quando `--query` é especificado, qualquer entrada é anexada à requisição após uma quebra de linha.

<div id="cloud-example">
  ### Inserindo um arquivo CSV em um serviço remoto do ClickHouse
</div>

Este exemplo insere um arquivo CSV de um conjunto de dados de exemplo, `cell_towers.csv`, em uma tabela existente, `cell_towers`, no banco de dados `default`:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### Exemplos de como inserir dados pela linha de comando
</div>

Há várias maneiras de inserir dados pela linha de comando.
O exemplo abaixo insere duas linhas de dados CSV em uma tabela do ClickHouse usando o modo em lote:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

No exemplo abaixo, `cat <<_EOF` inicia um heredoc que lerá todo o conteúdo até encontrar `_EOF` novamente e, em seguida, o exibirá:

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

No exemplo abaixo, o conteúdo de file.csv é enviado para stdout usando `cat` e passado por pipe para `clickhouse-client` como entrada:

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

No modo em lote, o [formato](formats.md) de dados padrão é `TabSeparated`.
Você pode definir o formato na cláusula `FORMAT` da consulta, como mostrado no exemplo acima.

<div id="cli-queries-with-parameters">
  ## Consultas com parâmetros
</div>

Você pode especificar parâmetros em uma consulta e passar valores para ela usando opções de linha de comando.
Isso evita a necessidade de formatar a consulta com valores dinâmicos específicos no lado do cliente.
Por exemplo:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

Também é possível definir parâmetros em uma [sessão interativa](#interactive-mode):

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### Sintaxe da consulta
</div>

Na consulta, coloque entre chaves os valores que você deseja preencher com parâmetros de linha de comando, no seguinte formato:

```sql
{<name>:<data type>}
```

| Parâmetro   | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | Identificador de marcador de posição. A opção correspondente da linha de comando é `--param_<name> = value`.                                                                                                                                                                                                                                                                                                                                                                                            |
| `data type` | [Tipo de dado](../sql-reference/data-types/index.md) do parâmetro. <br /><br />Por exemplo, uma estrutura de dados como `(integer, ('string', integer))` pode ter o tipo de dado `Tuple(UInt8, Tuple(String, UInt8))` (você também pode usar outros tipos [inteiros](../sql-reference/data-types/int-uint.md)). <br /><br />Também é possível passar o nome da tabela, o nome do banco de dados e os nomes das colunas como parâmetros; nesse caso, você precisará usar `Identifier` como tipo de dado. |

<div id="cli-queries-with-parameters-examples">
  ### Exemplos
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## Geração de SQL com IA
</div>

O ClickHouse Client inclui assistência de IA integrada para gerar consultas SQL a partir de descrições em linguagem natural. Esse recurso ajuda os usuários a escrever consultas complexas sem precisar de conhecimento aprofundado de SQL.

A assistência de IA funciona imediatamente se você tiver definida a variável de ambiente `OPENAI_API_KEY` ou `ANTHROPIC_API_KEY`. Para configurações mais avançadas, consulte a seção [Configuração](#ai-sql-generation-configuration).

<div id="ai-sql-generation-usage">
  ### Uso
</div>

Para usar a geração de SQL com IA, adicione o prefixo `??` à sua consulta em linguagem natural:

```bash
:) ?? show all users who made purchases in the last 30 days
```

A IA irá:

1. Explorar automaticamente o esquema do seu banco de dados
2. Gerar o SQL adequado com base nas tabelas e colunas identificadas
3. Executar imediatamente a consulta gerada

<div id="ai-sql-generation-example">
  ### Exemplo
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### Configuração
</div>

A geração de SQL com IA exige a configuração de um provedor de IA no arquivo de configuração do seu ClickHouse Client. Você pode usar OpenAI, Anthropic ou qualquer serviço de API compatível com OpenAI.

<div id="ai-sql-generation-fallback">
  #### Fallback por variáveis de ambiente
</div>

Se nenhuma configuração de IA for especificada no arquivo de configuração, o ClickHouse Client tentará automaticamente usar as variáveis de ambiente:

1. Primeiro, verifica a variável de ambiente `OPENAI_API_KEY`
2. Se não a encontrar, verifica a variável de ambiente `ANTHROPIC_API_KEY`
3. Se não encontrar nenhuma das duas, os recursos de IA serão desativados

Isso permite uma configuração rápida sem arquivos de configuração:

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### Arquivo de configuração
</div>

Para ter mais controle sobre as configurações de IA, configure-as no arquivo de configuração do seu ClickHouse Client, localizado em:

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (ou `~/.config/clickhouse/config.xml`, se `XDG_CONFIG_HOME` não estiver definido) (formato XML)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (ou `~/.config/clickhouse/config.yaml`, se `XDG_CONFIG_HOME` não estiver definido) (formato YAML)
* `~/.clickhouse-client/config.xml` (formato XML, local legado)
* `~/.clickhouse-client/config.yaml` (formato YAML, local legado)
* Ou especifique um local personalizado com `--config-file`

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- Obrigatório: sua chave de API (ou defina via variável de ambiente) -->
            <api_key>your-api-key-here</api_key>

            <!-- Obrigatório: tipo de provedor (openai, anthropic) -->
            <provider>openai</provider>

            <!-- Modelo a ser usado (os padrões variam conforme o provedor) -->
            <model>gpt-4o</model>

            <!-- Opcional: endpoint de API personalizado para serviços compatíveis com OpenAI -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- Configurações de exploração de esquema -->
            <enable_schema_access>true</enable_schema_access>

            <!-- Parâmetros de geração -->
            <!-- Opcional: temperature só é enviada ao modelo quando definida aqui.
                 Por padrão, ela é omitida porque alguns modelos rejeitam esse parâmetro. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- Opcional: system prompt personalizado -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # Obrigatório: sua chave de API (ou defina via variável de ambiente)
      api_key: your-api-key-here

      # Obrigatório: tipo de provedor (openai, anthropic)
      provider: openai

      # Modelo a ser usado
      model: gpt-4o

      # Opcional: endpoint de API personalizado para serviços compatíveis com OpenAI
      # base_url: https://openrouter.ai/api

      # Habilita o acesso ao esquema - permite que a IA consulte informações de banco de dados/tabela
      enable_schema_access: true

      # Parâmetros de geração
      # temperature só é enviada ao modelo quando definida aqui; por padrão, é omitida
      # porque alguns modelos rejeitam esse parâmetro.
      # temperature: 0.0    # Controla a aleatoriedade (0.0 = determinístico)
      max_tokens: 1000      # Comprimento máximo da resposta
      timeout_seconds: 30   # Timeout da solicitação
      max_steps: 10         # Número máximo de etapas de exploração de esquema

      # Opcional: system prompt personalizado
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**Usando APIs compatíveis com OpenAI (por exemplo, OpenRouter):**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**Exemplos mínimos de configuração:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### Parâmetros
</div>

<details>
  <summary>Parâmetros obrigatórios</summary>

  * `api_key` - Sua chave de API para o serviço de IA. Pode ser omitida se estiver definida em uma variável de ambiente:
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * Observação: a chave de API no arquivo de configuração tem precedência sobre a variável de ambiente
  * `provider` - O provedor de IA: `openai` ou `anthropic`
    * Se omitido, usa fallback automático com base nas variáveis de ambiente disponíveis
</details>

<details>
  <summary>Configuração do modelo</summary>

  * `model` - O modelo a ser usado (padrão: específico do provedor)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo`, etc.
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`, etc.
    * OpenRouter: use a nomenclatura de modelos deles, como `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>Configurações de conexão</summary>

  * `base_url` - Endpoint de API personalizado para serviços compatíveis com OpenAI (opcional)
  * `timeout_seconds` - Tempo limite da solicitação, em segundos (padrão: `30`)
</details>

<details>
  <summary>Exploração de esquemas</summary>

  * `enable_schema_access` - Permite que a IA explore esquemas de banco de dados (padrão: `true`)
  * `max_steps` - Número máximo de etapas de chamada de ferramenta para explorar esquemas (padrão: `10`)
</details>

<details>
  <summary>Parâmetros de geração</summary>

  * `temperature` - Controla a aleatoriedade; 0.0 = determinístico, 1.0 = criativo. Por padrão, esse parâmetro é omitido e só é enviado ao modelo quando definido explicitamente, porque alguns modelos o rejeitam.
  * `max_tokens` - Comprimento máximo da resposta em tokens (padrão: `1000`)
  * `system_prompt` - Instruções personalizadas para a IA (opcional)
</details>

<div id="ai-sql-generation-how-it-works">
  ### Como funciona
</div>

O gerador de SQL com IA usa um processo em várias etapas:

<VerticalStepper headerLevel="list">
  1. **Descoberta de esquema**

  A IA usa ferramentas integradas para explorar seu banco de dados:

  * Lista os bancos de dados disponíveis
  * Descobre tabelas nos bancos de dados relevantes
  * Analisa a estrutura das tabelas por meio de instruções `CREATE TABLE`

  2. **Geração de consultas**

  Com base no esquema descoberto, a IA gera SQL que:

  * Corresponde à sua intenção em linguagem natural
  * Usa os nomes corretos de tabelas e colunas
  * Aplica junções e agregações adequadas

  3. **Execução**

  O SQL gerado é executado automaticamente, e os resultados são exibidos
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### Limitações
</div>

* Requer uma conexão ativa com a internet
* O uso da API está sujeito a limites de taxa e custos do provedor de IA
* Consultas complexas podem exigir vários refinamentos
* A IA tem acesso somente leitura às informações do esquema, não aos dados reais

<div id="ai-sql-generation-security">
  ### Segurança
</div>

* As chaves de API nunca são enviadas aos servidores ClickHouse
* A IA vê apenas informações do esquema (nomes de tabelas/colunas e tipos), não os dados reais
* Todas as consultas geradas respeitam as permissões atuais do seu banco de dados

<div id="connection_string">
  ## String de conexão
</div>

<div id="ai-sql-generation-usage">
  ### Uso
</div>

Como alternativa, o ClickHouse Client também permite se conectar a um servidor ClickHouse usando uma string de conexão semelhante às do [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) e [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). Ela tem a seguinte sintaxe:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| Componente (todos opcionais) | Descrição                                                                                                                                                                                             | Padrão           |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `user`                       | Nome de usuário do banco de dados.                                                                                                                                                                    | `default`        |
| `password`                   | Senha do usuário do banco de dados. Se `:` for especificado e a senha estiver em branco, o cliente solicitará a senha do usuário.                                                                     | -                |
| `hosts_and_ports`            | Lista de hosts e portas opcionais `host[:port] [, host:[port]], ...`.                                                                                                                                 | `localhost:9000` |
| `database`                   | Nome do banco de dados.                                                                                                                                                                               | `default`        |
| `query_parameters`           | Lista de pares chave-valor `param1=value1[,&param2=value2], ...`. Para alguns parâmetros, não é necessário informar um valor. Os nomes e valores dos parâmetros diferenciam maiúsculas de minúsculas. | -                |

<div id="connection-string-notes">
  ### Observações
</div>

Se o nome de usuário, a senha ou o banco de dados tiverem sido especificados na string de conexão, eles não poderão ser especificados usando `--user`, `--password` ou `--database` (e vice-versa).

O host pode ser um hostname ou um endereço IPv4 ou IPv6.
Os endereços IPv6 devem estar entre `[]`:

```text
clickhouse://[2001:db8::1234]
```

As strings de conexão podem conter vários hosts.
O ClickHouse Client tentará se conectar a esses hosts em sequência (da esquerda para a direita).
Depois que a conexão for estabelecida, não será feita nenhuma tentativa de conexão com os hosts restantes.

A string de conexão deve ser especificada como o primeiro argumento de `clickHouse-client`.
A string de conexão pode ser combinada com qualquer número de outras [opções de linha de comando](#command-line-options), exceto `--host` e `--port`.

As seguintes chaves são permitidas para `query_parameters`:

| Chave             | Descrição                                                                                                                                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `secure` (ou `s`) | Se especificado, o cliente se conectará ao servidor por meio de uma conexão segura (TLS). Consulte `--secure` nas [opções de linha de comando](#command-line-options). |

**Codificação por porcentagem**

Caracteres fora do padrão US-ASCII, espaços e caracteres especiais nos parâmetros a seguir devem ser [codificados por porcentagem](https://en.wikipedia.org/wiki/URL_encoding):

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### Exemplos
</div>

Conecte-se ao `localhost` na porta 9000 e execute a consulta `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Conecte-se a `localhost` como o usuário `john`, com a senha `secret`, host `127.0.0.1` e porta `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Conecte-se ao `localhost` como o usuário `default`, no host com endereço IPv6 `[::1]` e porta `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

Conecte-se ao `localhost` pela porta 9000 no modo multilinha.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Conecte-se a `localhost` pela porta 9000 como o usuário `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Conecte-se ao `localhost` na porta 9000 e use o banco de dados `my_database` como padrão.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Conecte-se a `localhost` na porta 9000, use por padrão o banco de dados `my_database` especificado na string de conexão e use uma conexão segura com o parâmetro abreviado `s`.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Conecte-se ao host padrão usando a porta padrão, o usuário `default` e o banco de dados `default`.

```bash
clickhouse-client clickhouse:
```

Conecte-se ao host padrão usando a porta padrão, com o usuário `my_user` e sem senha.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Conecte-se a `localhost` usando o e-mail como nome de usuário. O símbolo `@` é codificado com percentual como `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Conecte-se a um destes dois hosts: `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## Formato do ID da consulta
</div>

No modo interativo, o ClickHouse Client mostra o ID de cada consulta. Por padrão, o ID é formatado assim:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

Um formato personalizado pode ser especificado em um arquivo de configuração dentro de uma tag `query_id_formats`. O placeholder `{query_id}` na string de formato é substituído pelo ID da consulta. Várias strings de formato são permitidas dentro da tag.
Esse recurso pode ser usado para gerar URLs e facilitar a análise de desempenho das consultas.

**Exemplo**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

Com a configuração acima, o ID de uma consulta é exibido no seguinte formato:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## Arquivos de configuração
</div>

O ClickHouse Client usa o primeiro arquivo existente entre os seguintes:

* Um arquivo definido com o parâmetro `-c [ -C, --config, --config-file ]`.
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (ou `~/.config/clickhouse/config.[xml|yaml|yml]` se `XDG_CONFIG_HOME` não estiver definido)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

Veja um arquivo de configuração de exemplo no repositório do ClickHouse: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## Opções de variáveis de ambiente
</div>

O nome de usuário, a senha e o host podem ser configurados por meio das variáveis de ambiente `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` e `CLICKHOUSE_HOST`.
Os argumentos de linha de comando `--user`, `--password` ou `--host`, ou uma [string de conexão](#connection_string) (se especificada), têm precedência sobre as variáveis de ambiente.

<div id="command-line-options">
  ## Opções de linha de comando
</div>

Todas as opções de linha de comando podem ser especificadas diretamente na linha de comando ou definidas como valores padrão no [arquivo de configuração](#configuration_files).

<div id="command-line-options-general">
  ### Opções gerais
</div>

| Opção                                               | Descrição                                                                                                                                              | Padrão                       |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | O local do arquivo de configuração do cliente, caso ele não esteja em um dos locais padrão. Consulte [Arquivos de configuração](#configuration_files). | -                            |
| `--help`                                            | Imprime o resumo de uso e sai. Combine com `--verbose` para exibir todas as opções possíveis, incluindo as configurações de consulta.                  | -                            |
| `--history_file <path-to-file>`                     | Caminho para um arquivo que contém o histórico de comandos.                                                                                            | -                            |
| `--history_max_entries`                             | Número máximo de entradas no arquivo de histórico.                                                                                                     | `1000000` (1 milhão)         |
| `--prompt <prompt>`                                 | Especifica um prompt personalizado.                                                                                                                    | O `display_name` do servidor |
| `--verbose`                                         | Aumenta o nível de detalhamento da saída.                                                                                                              | -                            |
| `-V [ --version ]`                                  | Imprime a versão e sai.                                                                                                                                | -                            |

<div id="command-line-options-connection">
  ### Opções de conexão
</div>

| Option                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                        | Default                                                                                                                                        |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `--connection <name>`                | O nome dos detalhes de conexão pré-configurados no arquivo de configuração. Consulte [Credenciais de conexão](#connection-credentials).                                                                                                                                                                                                                                                                                                            | -                                                                                                                                              |
| `-d [ --database ] <database>`       | Seleciona o banco de dados padrão para esta conexão.                                                                                                                                                                                                                                                                                                                                                                                               | O banco de dados atual das configurações do servidor (`default` por padrão)                                                                    |
| `-h [ --host ] <host>`               | O nome do host do servidor ClickHouse ao qual se conectar. Pode ser um nome de host, um endereço IPv4 ou um endereço IPv6. Vários hosts podem ser informados com vários argumentos.                                                                                                                                                                                                                                                                | `localhost`                                                                                                                                    |
| `--jwt <value>`                      | Usa JSON Web Token (JWT) para autenticação. <br /><br />A autorização JWT no servidor está disponível apenas no ClickHouse Cloud.                                                                                                                                                                                                                                                                                                                  | -                                                                                                                                              |
| `login`                              | Invoca o fluxo do OAuth de concessão por dispositivo para autenticação por meio de um IdP. <br /><br />Para hosts do ClickHouse Cloud, as variáveis do OAuth são inferidas automaticamente; caso contrário, elas devem ser fornecidas com `--oauth-url`, `--oauth-client-id` e `--oauth-audience`.                                                                                                                                                 | -                                                                                                                                              |
| `--no-warnings`                      | Desativa a exibição de avisos de `system.warnings` quando o cliente se conecta ao servidor.                                                                                                                                                                                                                                                                                                                                                        | -                                                                                                                                              |
| `--no-server-client-version-message` | Suprime a mensagem de incompatibilidade de versão entre servidor e cliente quando o cliente se conecta ao servidor.                                                                                                                                                                                                                                                                                                                                | -                                                                                                                                              |
| `--password <password>`              | A senha do usuário do banco de dados. Você também pode especificar a senha de uma conexão no arquivo de configuração. Se não especificar a senha, o cliente a solicitará.                                                                                                                                                                                                                                                                          | -                                                                                                                                              |
| `--port <port>`                      | A porta em que o servidor aceita conexões. As portas padrão são 9440 (TLS) e 9000 (sem TLS). <br /><br />Observação: o cliente usa o protocolo nativo, não HTTP(S).                                                                                                                                                                                                                                                                                | `9440` se `--secure` for especificado; caso contrário, `9000`. O padrão será sempre `9440` se o nome do host terminar com `.clickhouse.cloud`. |
| `-s [ --secure ]`                    | Define se TLS deve ser usado. <br /><br />É ativado automaticamente ao se conectar à porta 9440 (a porta segura padrão) ou ao ClickHouse Cloud. <br /><br />Talvez seja necessário configurar seus certificados de CA no [arquivo de configuração](#configuration_files). As definições de configuração disponíveis são as mesmas da [configuração de TLS do lado do servidor](../operations/server-configuration-parameters/settings.md#openssl). | Ativado automaticamente ao se conectar à porta 9440 ou ao ClickHouse Cloud                                                                     |
| `--ssh-key-file <path-to-file>`      | Arquivo que contém a chave privada SSH para autenticação no servidor.                                                                                                                                                                                                                                                                                                                                                                              | -                                                                                                                                              |
| `--ssh-key-passphrase <value>`       | Passphrase da chave privada SSH especificada em `--ssh-key-file`.                                                                                                                                                                                                                                                                                                                                                                                  | -                                                                                                                                              |
| `--tls-sni-override <server name>`   | Se estiver usando TLS, o nome do servidor (SNI) a ser enviado no handshake.                                                                                                                                                                                                                                                                                                                                                                        | O host fornecido por `-h` ou `--host`.                                                                                                         |
| `-u [ --user ] <username>`           | O usuário do banco de dados usado na conexão.                                                                                                                                                                                                                                                                                                                                                                                                      | `default`                                                                                                                                      |

:::note
Em vez das opções `--host`, `--port`, `--user` e `--password`, o cliente também oferece suporte a [strings de conexão](#connection_string).
:::

<div id="command-line-options-query">
  ### Opções de consulta
</div>

| Opção                           | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | Valor de substituição para um parâmetro de uma [consulta com parâmetros](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `-q [ --query ] <query>`        | A consulta a ser executada em modo em lote. Pode ser especificada várias vezes (`--query "SELECT 1" --query "SELECT 2"`) ou uma única vez com várias consultas separadas por ponto e vírgula (`--query "SELECT 1; SELECT 2;"`). Neste último caso, consultas `INSERT` com formatos diferentes de `VALUES` devem ser separadas por linhas em branco. <br /><br />Também é possível especificar uma única consulta sem parâmetro: `clickhouse-client "SELECT 1"` <br /><br />Não pode ser usada junto com `--queries-file`.                                                                                                                                                     |
| `--queries-file <path-to-file>` | Caminho para um arquivo que contém consultas. `--queries-file` pode ser especificado várias vezes, por exemplo: `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />Não pode ser usado junto com `--query`.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `-m [ --multiline ]`            | Se especificado, permite consultas em múltiplas linhas (não envia a consulta ao pressionar Enter). As consultas só serão enviadas quando terminarem com ponto e vírgula.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `--inline-insert-data`          | Envia `INSERT ... VALUES` (e outros formatos inline) como estão no texto da consulta, em vez de converter os dados em blocos no formato nativo. O servidor analisa os dados inline por conta própria, evitando a ida e volta necessária para enviar a estrutura da tabela e os valores padrão das colunas de volta ao cliente. Isso pode melhorar o desempenho de muitas inserções pequenas pelo protocolo nativo. Define automaticamente [`send_table_structure_on_insert_with_inline_data`](/pt-BR/operations/settings/settings#send_table_structure_on_insert_with_inline_data) como `0`. Não pode ser combinado com dados inline nem com dados externos (de stdin ou `INFILE`). |

<div id="command-line-options-query-settings">
  ### Configurações da consulta
</div>

As configurações da consulta podem ser definidas como opções de linha de comando no cliente, por exemplo:

```bash
$ clickhouse-client --max_threads 1
```

Consulte [Configurações](../operations/settings/settings.md) para obter uma lista de configurações.

<div id="command-line-options-formatting">
  ### Opções de formatação
</div>

| Opção                             | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Padrão                                                            |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `-f [ --format ] <format>`        | Use o formato especificado para exibir o resultado. <br /><br />Consulte [Formatos para dados de entrada e saída](formats.md) para ver a lista de formatos compatíveis.                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `TabSeparated`                                                    |
| `--pager <command>`               | Direcione toda a saída para este comando. Normalmente `less` (por exemplo, `less -S` para exibir result sets amplos) ou algo semelhante.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | -                                                                 |
| `-E [ --vertical ]`               | Use o [formato Vertical](/pt-BR/interfaces/formats/Vertical) para exibir o resultado. Isso é o mesmo que `–-format Vertical`. Nesse formato, cada valor é impresso em uma linha separada, o que é útil ao exibir wide tables.                                                                                                                                                                                                                                                                                                                                                                                                    | -                                                                 |
| `--echo [ <bool> ]`               | Imprima cada consulta antes da execução. Aceita um valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `true` no modo interativo, `false` no modo não interativo (batch) |
| `--echo-formatted [ <bool> ]`     | Formate as consultas exibidas por echo. Aceita um valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `true` no modo interativo, `false` no modo não interativo (batch) |
| `--echo-query-id [ <bool> ]`      | Imprima o ID da consulta antes da execução. Aceita um valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `true` no modo interativo, `false` no modo não interativo (batch) |
| `--echo-query-separator <string>` | Imprima este separador antes da consulta formatada exibida por echo (requer `--echo-formatted`), facilitando distinguir a consulta digitada da sua versão reformatada exibida por echo.                                                                                                                                                                                                                                                                                                                                                                                                                                    | Vazio (desabilitado)                                              |
| `--highlight [ --hilite ] <bool>` | Ative ou desative o syntax highlighting do prompt de comando e das consultas exibidas por echo.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `true`                                                            |
| `--hints <bool>`                  | Mostre sugestões de preenchimento automático enquanto você digita (texto &quot;fantasma&quot; inline) com a melhor correspondência quando o cursor estiver no fim da entrada. Navegue pelas sugestões com Up/Down (ou Ctrl-Up/Ctrl-Down); aceite a sugestão inline com Tab ou Right; `Enter` aceita uma sugestão somente depois que uma tiver sido explicitamente selecionada e, caso contrário, executa a consulta; `Tab` também abre a lista clássica de preenchimento automático. Requer `--highlight` (as sugestões precisam de cor) e o mecanismo de sugestões (portanto, `--disable_suggestion` também as desativa). | `true`                                                            |

<div id="command-line-options-execution-details">
  ### Detalhes da execução
</div>

| Opção                            | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                   | Padrão                                                         |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `--chime [N]`                    | Escreve o caractere de controle `BEL` em `stderr` quando uma consulta termina (com sucesso ou erro) após ficar em execução por pelo menos `N` segundos. Só é emitido quando `stderr` está conectado a um terminal (TTY); redirecionar `stderr` (por exemplo, `2>err.log`) o suprime, enquanto redirecionar `stdout` (por exemplo, `> result.tsv`) não. Passar `--chime` sem valor usa o limite padrão. Defina `--chime 0` para desabilitar. | `5` segundos                                                   |
| `--enable-progress-table-toggle` | Habilita alternar a tabela de progresso pressionando a tecla de controle (Espaço). Aplicável apenas no modo interativo com a impressão da tabela de progresso habilitada.                                                                                                                                                                                                                                                                   | `habilitado`                                                   |
| `--hardware-utilization`         | Imprime informações de utilização de hardware na barra de progresso.                                                                                                                                                                                                                                                                                                                                                                        | -                                                              |
| `--memory-usage`                 | Se especificado, imprime o uso de memória em `stderr` no modo não interativo. <br /><br />Valores possíveis: <br />• `none` - não imprime o uso de memória <br />• `default` - imprime o número de bytes <br />• `readable` - imprime o uso de memória em formato legível                                                                                                                                                                   | -                                                              |
| `--print-profile-events`         | Imprime pacotes `ProfileEvents`.                                                                                                                                                                                                                                                                                                                                                                                                            | -                                                              |
| `--progress`                     | Imprime o progresso da execução da consulta. <br /><br />Valores possíveis: <br />• `tty\|on\|1\|true\|yes` - envia a saída para o terminal no modo interativo <br />• `err` - envia a saída para `stderr` no modo não interativo <br />• `off\|0\|false\|no` - desabilita a exibição do progresso                                                                                                                                          | `tty` no modo interativo, `off` no modo não interativo (batch) |
| `--progress-table`               | Imprime uma tabela de progresso com métricas que mudam durante a execução da consulta. <br /><br />Valores possíveis: <br />• `tty\|on\|1\|true\|yes` - envia a saída para o terminal no modo interativo <br />• `err` - envia a saída para `stderr` no modo não interativo <br />• `off\|0\|false\|no` - desabilita a tabela de progresso                                                                                                  | `tty` no modo interativo, `off` no modo não interativo (batch) |
| `--stacktrace`                   | Imprime stack traces de exceções.                                                                                                                                                                                                                                                                                                                                                                                                           | -                                                              |
| `-t [ --time ]`                  | Imprime o tempo de execução da consulta em `stderr` no modo não interativo (para benchmarks).                                                                                                                                                                                                                                                                                                                                               | -                                                              |