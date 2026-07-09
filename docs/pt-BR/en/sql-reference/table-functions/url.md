---
description: 'Cria uma tabela a partir da `URL` com o `format` e a `structure` especificados'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # Função de tabela url
</div>

A função `url` cria uma tabela a partir da `URL` com o `format` e a structure especificados.

A função `url` pode ser usada em consultas `SELECT` e `INSERT` sobre dados em tabelas [URL](../../engines/table-engines/special/url.md).

<div id="syntax">
  ## Sintaxe
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## Parâmetros
</div>

| Parâmetro   | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URL`       | Uma URL entre aspas simples cujo esquema seleciona o backend. Uma URL `http`/`https` (ou não reconhecida) é um endereço do servidor que aceita requisições `GET` ou `POST` (para consultas `SELECT` ou `INSERT`, respectivamente); um esquema não HTTP reconhecido (`file://`, `s3://`, `az://`, `hdfs://`, …) é delegado à função de tabela correspondente — consulte [Encaminhamento por esquema de URL](#scheme-dispatch). Tipo: [String](../../sql-reference/data-types/string.md). |
| `format`    | [Formato](/pt-BR/sql-reference/formats) dos dados. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                  |
| `structure` | Estrutura da tabela no formato `'UserID UInt64, Name String'`. Determina os nomes e os tipos das colunas. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                     |
| `headers`   | Cabeçalhos no formato `'headers('key1'='value1', 'key2'='value2')'`. Você pode definir cabeçalhos para a requisição HTTP.                                                                                                                                                                                                                                                                                                                                                               |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com o formato e a structure especificados e com os dados da `URL` especificada.

<div id="examples">
  ## Exemplos
</div>

Obtendo as 3 primeiras linhas de uma tabela com colunas dos tipos `String` e [UInt32](../../sql-reference/data-types/int-uint.md) a partir de um servidor HTTP que responde no formato [CSV](/pt-BR/interfaces/formats/CSV).

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Inserindo dados de uma `URL` em uma tabela:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## Despacho por esquema de URL
</div>

A função `url` atua como um wrapper unificado sobre as outras table functions de arquivo e armazenamento de objetos: ela despacha para o backend correto com base no esquema da URL. Isso permite ler de qualquer local compatível com uma única sintaxe uniforme.

| Scheme                                        | Dispatches to                                      |
| --------------------------------------------- | -------------------------------------------------- |
| `http`, `https` (and any unrecognized scheme) | o próprio engine `URL` (HTTP `GET`/`POST`)         |
| `file`                                        | a função [`file`](file.md)                         |
| `s3`, `gs`, `gcs`, `oss`                      | a função [`s3`](s3.md)                             |
| `az`, `azure`, `abfss`, `abfs`                | a função [`azureBlobStorage`](azureBlobStorage.md) |
| `hdfs`                                        | a função [`hdfs`](hdfs.md)                         |

Somente os esquemas S3 que o mapeador de URI do S3 resolve para um endpoint concreto sem configuração adicional (`s3`, além de `gs`/`gcs`/`oss`) são despachados. Outros esquemas de fornecedores compatíveis com S3 (`cos`, `obs`, `eos`, …) são específicos de cada região e não têm mapeamento de endpoint padrão; por isso, uma URL `cos://…` é tratada como um esquema não reconhecido e gera um erro. Para esses backends, use diretamente a função [`s3`](s3.md) (com `url_scheme_mappers` configurado).

Para `file://`, um caminho relativo (`file://data.csv`) é resolvido dentro do diretório [user&#95;files](/pt-BR/operations/server-configuration-parameters/settings#user_files_path), e um caminho absoluto (`file:///home/user/data.csv`) deve apontar para dentro dele, como de costume.

Os argumentos `format`, `structure` e `compression_method` e a configuração [url&#95;base](#resolving-relative-urls) funcionam da mesma forma, independentemente do destino do despacho.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

O encaminhamento por esquema ainda não foi implementado em [`urlCluster`](urlCluster.md): um esquema diferente de `http(s)` passado para `urlCluster` é rejeitado com um erro. Em vez disso, use a função de cluster correspondente (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …) para esses backends.

<div id="globs-in-url">
  ## Globs em URL
</div>

Padrões em `{ }` são usados para gerar um conjunto de shards ou para especificar endereços de failover. Para os tipos de padrão compatíveis e exemplos, consulte a descrição da função [remote](remote.md#globs-in-addresses).
O caractere `|` dentro dos padrões é usado para especificar endereços de failover. Eles são percorridos na mesma ordem em que aparecem no padrão. O número de endereços gerados é limitado pela configuração [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).
Para a sintaxe de glob no caminho da URL (como `*`, `{a,b}`, `{N..M}` e `**`), consulte [Globs em caminho](file.md#globs-in-path). Observe que `?` inicia a string de consulta em uma URL e não pode ser usado como caractere curinga no componente de caminho.

<div id="wildcards-with-http-index-pages">
  ## Curingas com páginas de índice HTTP
</div>

Para `url` e o mecanismo de tabela `URL`, o ClickHouse pode expandir curingas buscando páginas de índice HTTP (HTML ou texto simples) e extraindo URLs do corpo da resposta. Isso permite padrões como `/**/` quando o servidor expõe listagens de diretórios.

Observações:

* URLs relativas são resolvidas em relação à URL da página de índice.
* Os templates de `URL` são expandidos antes da busca das páginas de índice, incluindo a expansão de shards por vírgulas e intervalos numéricos e opções de failover com `|` fora do componente de caminho.
* Padrões de failover com `|` dentro do componente de caminho não são compatíveis com a expansão de páginas de índice HTTP.
* A correspondência de curingas é aplicada ao componente de caminho da URL.
* Se uma URL listada já contiver uma string de consulta ou fragmento, ela terá precedência sobre os da URL de origem. Caso contrário, a string de consulta e o fragmento da URL de origem serão usados.
* Uma listagem vazia é permitida; erros HTTP (por exemplo, 404) em páginas de índice geram exceções.
* O tamanho máximo da página de índice é limitado por [max&#95;http&#95;index&#95;page&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md#max_http_index_page_size).
* O número máximo de diretórios lidos durante a expansão recursiva é limitado por [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/pt-BR/operations/settings/settings.md#url_wildcard_max_directories_to_read).

Exemplo:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## Colunas Virtuais
</div>

* `_path` — Caminho para a `URL`. Type: `LowCardinality(String)`.
* `_file` — Nome do recurso da `URL`. Type: `LowCardinality(String)`.
* `_size` — Tamanho do recurso em bytes. Type: `Nullable(UInt64)`. Se o tamanho for desconhecido, o valor é `NULL`.
* `_time` — Data e hora da última modificação do arquivo. Type: `Nullable(DateTime)`. Se a data e hora forem desconhecidas, o valor é `NULL`.
* `_headers` - Cabeçalhos da resposta HTTP. Type: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="hive-style-partitioning">
  ## configuração use_hive_partitioning
</div>

Quando a configuração `use_hive_partitioning` é definida como 1, o ClickHouse detecta o particionamento no estilo Hive no caminho (`/name=value/`) e permite usar as colunas de partição como colunas virtuais na consulta. Essas colunas virtuais terão os mesmos nomes do caminho particionado.

**Exemplo**

Usar coluna virtual criada com particionamento no estilo Hive

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## Resolução de URLs relativas
</div>

A configuração [url&#95;base](/pt-BR/operations/settings/settings.md#url_base) permite passar uma URL relativa para a função `url`. Quando `url_base` está definida e o argumento da função é uma referência relativa, ela é resolvida com base na URL base, de acordo com a [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

As regras de resolução são:

* **Relativa ao caminho** (por exemplo, `data.csv`): combinada com o caminho da URL base — tudo após a última `/` do caminho base é substituído. A barra no final faz diferença: `https://example.com/dir/` + `data.csv` resulta em `https://example.com/dir/data.csv`, mas `https://example.com/dir` + `data.csv` resulta em `https://example.com/data.csv`. Os segmentos de ponto (`./` e `../`) são normalizados.
* **Relativa ao host** (por exemplo, `/test/data.csv`): resolvida usando o esquema e o host da URL base.
* **Relativa ao esquema** (por exemplo, `//other.com/test/data.csv`): resolvida usando o esquema da URL base.
* **Somente query** (por exemplo, `?x=1`): anexada ao caminho completo da base, substituindo qualquer query ou fragmento existente.
* **Somente fragmento** (por exemplo, `#frag`): anexado à URL base, preservando a query e substituindo qualquer fragmento existente.
* **Vazia**: retorna a URL base sem fragmento.
* **URL absoluta**: mantida inalterada; `url_base` é ignorada.

**Exemplo**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## Configurações de armazenamento
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/pt-BR/operations/settings/settings.md#engine_url_skip_empty_files) - permite ignorar arquivos vazios durante a leitura. Desabilitado por padrão.
* [enable&#95;url&#95;encoding](/pt-BR/operations/settings/settings.md#enable_url_encoding) - permite habilitar/desabilitar a decodificação/codificação do caminho na URI. Habilitado por padrão.
* [url&#95;base](/pt-BR/operations/settings/settings.md#url_base) - URL base para resolver URLs relativas passadas para a função `url`.

<div id="permissions">
  ## Permissões
</div>

A função `url` exige a permissão `CREATE TEMPORARY TABLE`. Portanto, ela não funcionará para usuários com a configuração [readonly](/pt-BR/operations/settings/permissions-for-queries#readonly) = 1. É necessário, no mínimo, readonly = 2.

<div id="related">
  ## Relacionados
</div>

* [Colunas virtuais](/pt-BR/engines/table-engines/index.md#table_engines-virtual_columns)