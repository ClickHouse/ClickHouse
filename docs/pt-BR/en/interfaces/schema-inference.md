---
description: 'Página que descreve a inferência automática de esquema com base nos dados de entrada no ClickHouse'
sidebar_label: 'Inferência de esquema'
slug: /interfaces/schema-inference
title: 'Inferência automática de esquema com base nos dados de entrada'
doc_type: 'reference'
---

O ClickHouse pode determinar automaticamente a estrutura dos dados de entrada em quase todos os [formatos de entrada](formats.md) compatíveis.
Este documento descreve quando a inferência de esquema é usada, como ela funciona com diferentes formatos de entrada e quais configurações
podem controlá-la.

<div id="usage">
  ## Uso
</div>

A inferência de esquema é usada quando o ClickHouse precisa ler dados em um formato específico, mas a estrutura é desconhecida.

<div id="table-functions-file-s3-url-hdfs-azureblobstorage">
  ## Funções de tabela [file](../sql-reference/table-functions/file.md), [s3](../sql-reference/table-functions/s3.md), [url](../sql-reference/table-functions/url.md), [hdfs](../sql-reference/table-functions/hdfs.md), [azureBlobStorage](../sql-reference/table-functions/azureBlobStorage.md).
</div>

Essas funções de tabela têm o argumento opcional `structure`, que define a estrutura dos dados de entrada. Se esse argumento não for especificado ou estiver definido como `auto`, a estrutura será inferida com base nos dados.

**Exemplo:**

Suponha que temos um arquivo `hobbies.jsonl` no formato JSONEachRow no diretório `user_files` com este conteúdo:

```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

ClickHouse pode ler esses dados sem que você precise especificar sua estrutura:

```sql
SELECT * FROM file('hobbies.jsonl')
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

Observação: o formato `JSONEachRow` foi determinado automaticamente pela extensão de arquivo `.jsonl`.

Você pode ver a estrutura determinada automaticamente usando a consulta `DESCRIBE`:

```sql
DESCRIBE file('hobbies.jsonl')
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="table-engines-file-s3-url-hdfs-azureblobstorage">
  ## Motores de tabela [File](../engines/table-engines/special/file.md), [S3](../engines/table-engines/integrations/s3.md), [URL](../engines/table-engines/special/url.md), [HDFS](../engines/table-engines/integrations/hdfs.md), [azureBlobStorage](../engines/table-engines/integrations/azureBlobStorage.md)
</div>

Se a lista de colunas não for especificada na consulta `CREATE TABLE`, a estrutura da tabela será inferida automaticamente dos dados.

**Exemplo:**

Vamos usar o arquivo `hobbies.jsonl`. Podemos criar uma tabela com o motor `File` usando os dados desse arquivo:

```sql
CREATE TABLE hobbies ENGINE=File(JSONEachRow, 'hobbies.jsonl')
```

```response
Ok.
```

```sql
SELECT * FROM hobbies
```

```response
┌─id─┬─age─┬─name───┬─hobbies──────────────────────────┐
│  1 │  25 │ Josh   │ ['football','cooking','music']   │
│  2 │  19 │ Alan   │ ['tennis','art']                 │
│  3 │  32 │ Lana   │ ['fitness','reading','shopping'] │
│  4 │  47 │ Brayan │ ['movies','skydiving']           │
└────┴─────┴────────┴──────────────────────────────────┘
```

```sql
DESCRIBE TABLE hobbies
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="clickhouse-local">
  ## clickhouse-local
</div>

`clickhouse-local` tem um parâmetro opcional `-S/--structure` para a estrutura dos dados de entrada. Se esse parâmetro não for especificado ou for definido como `auto`, a estrutura será inferida a partir dos dados.

**Exemplo:**

Vamos usar o arquivo `hobbies.jsonl`. Podemos consultar os dados desse arquivo usando `clickhouse-local`:

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='DESCRIBE TABLE hobbies'
```

```response
id    Nullable(Int64)
age    Nullable(Int64)
name    Nullable(String)
hobbies    Array(Nullable(String))
```

```shell
clickhouse-local --file='hobbies.jsonl' --table='hobbies' --query='SELECT * FROM hobbies'
```

```response
1    25    Josh    ['football','cooking','music']
2    19    Alan    ['tennis','art']
3    32    Lana    ['fitness','reading','shopping']
4    47    Brayan    ['movies','skydiving']
```

<div id="using-structure-from-insertion-table">
  ## Usando a estrutura da tabela de inserção
</div>

Quando as funções de tabela `file/s3/url/hdfs` são usadas para inserir dados em uma tabela,
há a opção de usar a estrutura da tabela de inserção em vez de extraí-la dos dados.
Isso pode melhorar o desempenho da inserção, porque a inferência de esquema pode levar algum tempo. Além disso, isso é útil quando a tabela tem um esquema otimizado, de modo que
nenhuma conversão entre tipos seja realizada.

Há uma configuração especial [use&#95;structure&#95;from&#95;insertion&#95;table&#95;in&#95;table&#95;functions](/pt-BR/operations/settings/settings.md/#use_structure_from_insertion_table_in_table_functions)
que controla esse comportamento. Ela tem 3 valores possíveis:

* 0 - a função de tabela extrairá a estrutura dos dados.
* 1 - a função de tabela usará a estrutura da tabela de inserção.
* 2 - o ClickHouse determinará automaticamente se é possível usar a estrutura da tabela de inserção ou recorrer à inferência de esquema. Valor padrão.

**Exemplo 1:**

Vamos criar a tabela `hobbies1` com a seguinte estrutura:

```sql
CREATE TABLE hobbies1
(
    `id` UInt64,
    `age` LowCardinality(UInt8),
    `name` String,
    `hobbies` Array(String)
)
ENGINE = MergeTree
ORDER BY id;
```

E insira dados a partir do arquivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies1 SELECT * FROM file(hobbies.jsonl)
```

Nesse caso, todas as colunas do arquivo são inseridas na tabela sem alterações, então o ClickHouse usará a estrutura da tabela de inserção em vez da inferência de esquema.

**Exemplo 2:**

Vamos criar a tabela `hobbies2` com a seguinte estrutura:

```sql
CREATE TABLE hobbies2
(
  `id` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

E insira dados a partir do arquivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies2 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

Neste caso, todas as colunas da consulta `SELECT` estão presentes na tabela, portanto o ClickHouse usará a estrutura da tabela de inserção.
Observe que isso funcionará apenas para formatos de entrada que oferecem suporte à leitura de um subconjunto de colunas, como JSONEachRow, TSKV, Parquet etc. (portanto, não funcionará, por exemplo, com o formato TSV).

**Exemplo 3:**

Vamos criar a tabela `hobbies3` com a seguinte estrutura:

```sql
CREATE TABLE hobbies3
(
  `identifier` UInt64,
  `age` LowCardinality(UInt8),
  `hobbies` Array(String)
)
  ENGINE = MergeTree
ORDER BY identifier;
```

E insira dados do arquivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies3 SELECT id, age, hobbies FROM file(hobbies.jsonl)
```

Nesse caso, a coluna `id` é usada na consulta `SELECT`, mas a tabela não tem essa coluna (ela tem uma coluna chamada `identifier`),
portanto, o ClickHouse não pode usar a estrutura da tabela de inserção, e a inferência de esquema será usada.

**Exemplo 4:**

Vamos criar a tabela `hobbies4` com a seguinte estrutura:

```sql
CREATE TABLE hobbies4
(
  `id` UInt64,
  `any_hobby` Nullable(String)
)
  ENGINE = MergeTree
ORDER BY id;
```

E insira dados do arquivo `hobbies.jsonl`:

```sql
INSERT INTO hobbies4 SELECT id, empty(hobbies) ? NULL : hobbies[1] FROM file(hobbies.jsonl)
```

Neste caso, como algumas operações são executadas na coluna `hobbies` na consulta `SELECT` antes de inseri-la na tabela, o ClickHouse não pode usar a estrutura da tabela de inserção, e será usada a inferência de esquema.

<div id="schema-inference-cache">
  ## Cache de inferência de esquema
</div>

Na maioria dos formatos de entrada, a inferência de esquema lê alguns dados para determinar sua estrutura, e esse processo pode levar algum tempo.
Para evitar inferir o mesmo esquema sempre que o ClickHouse lê dados do mesmo arquivo, o esquema inferido é armazenado em cache e, ao acessar o mesmo arquivo novamente, o ClickHouse usará o esquema do cache.

Há configurações especiais que controlam esse cache:

* `schema_inference_cache_max_elements_for_{file/s3/hdfs/url/azure}` - o número máximo de esquemas armazenados em cache para a função de tabela correspondente. O valor padrão é `4096`. Essas configurações devem ser definidas na configuração do servidor.
* `schema_inference_use_cache_for_{file,s3,hdfs,url,azure}` - permite ativar/desativar o uso do cache para inferência de esquema. Essas configurações podem ser usadas em consultas.

O esquema do arquivo pode mudar com a modificação dos dados ou com a alteração das configurações de formato.
Por isso, o cache de inferência de esquema identifica o esquema pela origem do arquivo, pelo nome do formato, pelas configurações de formato usadas e pelo horário da última modificação do arquivo.

Nota: alguns arquivos acessados por URL na função de tabela `url` podem não conter informações sobre o horário da última modificação; nesse caso, há uma configuração especial
`schema_inference_cache_require_modification_time_for_url`. Desativar essa configuração permite usar o esquema do cache sem o horário da última modificação para esses arquivos.

Há também uma tabela de sistema [schema&#95;inference&#95;cache](../operations/system-tables/schema_inference_cache.md) com todos os esquemas atualmente no cache e a consulta de sistema `SYSTEM CLEAR SCHEMA CACHE [FOR File/S3/URL/HDFS]`
que permite limpar o cache de esquema de todas as fontes ou de uma fonte específica.

**Exemplos:**

Vamos tentar inferir a estrutura de um conjunto de dados de exemplo no S3, `github-2022.ndjson.gz`, e ver como o cache de inferência de esquema funciona:

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘
5 rows in set. Elapsed: 0.601 sec.
```

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
```

```response
┌─name───────┬─type─────────────────────────────────────────┐
│ type       │ Nullable(String)                             │
│ actor      │ Tuple(                                      ↴│
│            │↳    avatar_url Nullable(String),            ↴│
│            │↳    display_login Nullable(String),         ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    login Nullable(String),                 ↴│
│            │↳    url Nullable(String))                    │
│ repo       │ Tuple(                                      ↴│
│            │↳    id Nullable(Int64),                     ↴│
│            │↳    name Nullable(String),                  ↴│
│            │↳    url Nullable(String))                    │
│ created_at │ Nullable(String)                             │
│ payload    │ Tuple(                                      ↴│
│            │↳    action Nullable(String),                ↴│
│            │↳    distinct_size Nullable(Int64),          ↴│
│            │↳    pull_request Tuple(                     ↴│
│            │↳        author_association Nullable(String),↴│
│            │↳        base Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        head Tuple(                         ↴│
│            │↳            ref Nullable(String),           ↴│
│            │↳            sha Nullable(String)),          ↴│
│            │↳        number Nullable(Int64),             ↴│
│            │↳        state Nullable(String),             ↴│
│            │↳        title Nullable(String),             ↴│
│            │↳        updated_at Nullable(String),        ↴│
│            │↳        user Tuple(                         ↴│
│            │↳            login Nullable(String))),       ↴│
│            │↳    ref Nullable(String),                   ↴│
│            │↳    ref_type Nullable(String),              ↴│
│            │↳    size Nullable(Int64))                    │
└────────────┴──────────────────────────────────────────────┘

5 rows in set. Elapsed: 0.059 sec.
```

Como você pode ver, a segunda consulta teve sucesso quase instantaneamente.

Vamos tentar alterar algumas configurações que podem afetar o esquema inferido:

```sql
DESCRIBE TABLE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/github/github-2022.ndjson.gz')
SETTINGS input_format_json_try_infer_named_tuples_from_objects=0, input_format_json_read_objects_as_strings = 1

┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ type       │ Nullable(String) │              │                    │         │                  │                │
│ actor      │ Nullable(String) │              │                    │         │                  │                │
│ repo       │ Nullable(String) │              │                    │         │                  │                │
│ created_at │ Nullable(String) │              │                    │         │                  │                │
│ payload    │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

5 rows in set. Elapsed: 0.611 sec
```

Como você pode ver, o esquema em cache não foi usado para o mesmo arquivo, porque a configuração que pode afetar o esquema inferido foi alterada.

Vamos verificar o conteúdo da tabela `system.schema_inference_cache`:

```sql
SELECT schema, format, source FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─schema──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─format─┬─source───────────────────────────────────────────────────────────────────────────────────────────────────┐
│ type Nullable(String), actor Tuple(avatar_url Nullable(String), display_login Nullable(String), id Nullable(Int64), login Nullable(String), url Nullable(String)), repo Tuple(id Nullable(Int64), name Nullable(String), url Nullable(String)), created_at Nullable(String), payload Tuple(action Nullable(String), distinct_size Nullable(Int64), pull_request Tuple(author_association Nullable(String), base Tuple(ref Nullable(String), sha Nullable(String)), head Tuple(ref Nullable(String), sha Nullable(String)), number Nullable(Int64), state Nullable(String), title Nullable(String), updated_at Nullable(String), user Tuple(login Nullable(String))), ref Nullable(String), ref_type Nullable(String), size Nullable(Int64)) │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
│ type Nullable(String), actor Nullable(String), repo Nullable(String), created_at Nullable(String), payload Nullable(String)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │ NDJSON │ datasets-documentation.s3.eu-west-3.amazonaws.com443/datasets-documentation/github/github-2022.ndjson.gz │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Como você pode ver, há dois esquemas diferentes para o mesmo arquivo.

Podemos limpar o cache de esquema usando uma consulta SYSTEM:

```sql
SYSTEM CLEAR SCHEMA CACHE FOR S3
```

```response
Ok.
```

```sql
SELECT count() FROM system.schema_inference_cache WHERE storage='S3'
```

```response
┌─count()─┐
│       0 │
└─────────┘
```

<div id="text-formats">
  ## Formatos de texto
</div>

Nos formatos de texto, o ClickHouse lê os dados linha por linha, extrai os valores das colunas de acordo com o formato
e então usa alguns parsers recursivos e heurísticas para determinar o tipo de cada valor. O número máximo de linhas e bytes lidos dos dados durante a inferência de esquema
é controlado pelas configurações `input_format_max_rows_to_read_for_schema_inference` (25000 por padrão) e `input_format_max_bytes_to_read_for_schema_inference` (32Mb por padrão).
Por padrão, todos os tipos inferidos são [Nullable](../sql-reference/data-types/nullable.md), mas você pode alterar isso definindo `schema_inference_make_columns_nullable` (veja exemplos na seção [configurações](#settings-for-text-formats)).

<div id="json-formats">
  ### Formatos JSON
</div>

Nos formatos JSON, o ClickHouse analisa os valores de acordo com a especificação JSON e tenta encontrar o tipo de dado mais adequado para eles.

Veja como funciona, quais tipos podem ser inferidos e quais configurações específicas podem ser usadas nos formatos JSON.

**Exemplos**

Aqui e nas seções seguintes, a função de tabela [format](../sql-reference/table-functions/format.md) será usada nos exemplos.

Integers, Floats, Bool, Strings:

```sql
DESC format(JSONEachRow, '{"int" : 42, "float" : 42.42, "string" : "Hello, World!"}');
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
DESC format(JSONEachRow, '{"date" : "2022-01-01", "datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}')
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date       │ Nullable(Date)          │              │                    │         │                  │                │
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(JSONEachRow, '{"arr" : [1, 2, 3], "nested_arrays" : [[1, 2, 3], [4, 5, 6], []]}')
```

```response
┌─name──────────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr           │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ nested_arrays │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se um array contiver `null`, o ClickHouse usará os tipos dos demais elementos do array:

```sql
DESC format(JSONEachRow, '{"arr" : [null, 42, null]}')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se um array contiver valores de tipos diferentes e a configuração `input_format_json_infer_array_of_dynamic_from_array_of_different_types` estiver habilitada (habilitada por padrão), ele terá o tipo `Array(Dynamic)`:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=1;
DESC format(JSONEachRow, '{"arr" : [42, "hello", [1, 2, 3]]}');
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Dynamic) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas nomeadas:

Quando a configuração `input_format_json_try_infer_named_tuples_from_objects` está habilitada, durante a inferência de esquema o ClickHouse tentará inferir um Tuple nomeado a partir de objetos JSON.
O Tuple nomeado resultante conterá todos os elementos de todos os objetos JSON correspondentes na amostra de dados.

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuples sem nome:

Se a configuração `input_format_json_infer_array_of_dynamic_from_array_of_different_types` estiver desativada, consideramos Arrays com elementos de tipos diferentes como Tuples sem nome em formatos JSON.

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;
DESC format(JSONEachRow, '{"tuple" : [1, "Hello, World!", [1, 2, 3]]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se alguns valores forem `null` ou estiverem vazios, usamos os tipos dos valores correspondentes em outras linhas:

```sql
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
DESC format(JSONEachRow, $$
                              {"tuple" : [1, null, null]}
                              {"tuple" : [null, "Hello, World!", []]}
                              {"tuple" : [null, null, [1, 2, 3]]}
                         $$)
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ tuple │ Tuple(Nullable(Int64), Nullable(String), Array(Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

No JSON, podemos ler objetos com valores do mesmo tipo que o tipo Map.
Nota: isso só funciona quando as configurações `input_format_json_read_objects_as_strings` e `input_format_json_try_infer_named_tuples_from_objects` estão desabilitadas.

```sql
SET input_format_json_read_objects_as_strings = 0, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, '{"map" : {"key1" : 42, "key2" : 24, "key3" : 4}}')
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ map  │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tipos complexos do tipo Nested:

```sql
DESC format(JSONEachRow, '{"value" : [[[42, 24], []], {"key1" : 42, "key2" : 24}]}')
```

```response
┌─name──┬─type─────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Tuple(Array(Array(Nullable(String))), Tuple(key1 Nullable(Int64), key2 Nullable(Int64))) │              │                    │         │                  │                │
└───────┴──────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se o ClickHouse não conseguir determinar o tipo de alguma chave, pois os dados contêm apenas valores nulos/objetos vazios/arrays vazios, o tipo `String` será usado se a configuração `input_format_json_infer_incomplete_types_as_strings` estiver habilitada; caso contrário, será gerada uma exceção:

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 1;
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ arr  │ Array(Nullable(String)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, '{"arr" : [null, null]}') SETTINGS input_format_json_infer_incomplete_types_as_strings = 0;
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'arr' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

<div id="json-settings">
  #### Configurações JSON
</div>

<div id="input_format_json_try_infer_numbers_from_strings">
  ##### input_format_json_try_infer_numbers_from_strings
</div>

Ativar essa configuração permite inferir números de valores de string.

Esta configuração fica desativada por padrão.

**Exemplo:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(JSONEachRow, $$
                              {"value" : "42"}
                              {"value" : "424242424242"}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_try_infer_named_tuples_from_objects">
  ##### input_format_json_try_infer_named_tuples_from_objects
</div>

Habilitar essa configuração permite inferir Tuples nomeadas a partir de objetos JSON. A Tuple nomeada resultante conterá todos os elementos de todos os objetos JSON correspondentes na amostra de dados.
Isso pode ser útil quando os dados JSON não são esparsos, de modo que a amostra de dados contenha todas as chaves de objeto possíveis.

Essa configuração está habilitada por padrão.

**Exemplo**

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

```response title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"array" : [{"a" : 42, "b" : "Hello"}, {}, {"c" : [1,2,3]}, {"d" : "2020-01-01"}]}')
```

```markdown title="Response"
┌─name──┬─type────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ array │ Array(Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Nullable(Date))) │              │                    │         │                  │                │
└───────┴─────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects">
  ##### input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects
</div>

Habilitar essa configuração permite usar o tipo String para paths ambíguos durante a inferência de Tuples nomeados a partir de objetos JSON (quando `input_format_json_try_infer_named_tuples_from_objects` está habilitada), em vez de gerar uma Exception.
Isso permite ler objetos JSON como Tuples nomeados mesmo quando há paths ambíguos.

Desabilitada por padrão.

**Exemplos**

Com a configuração desabilitada:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 0;
DESC format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
Code: 636. DB::Exception: The table structure cannot be extracted from a JSONEachRow format file. Error:
Code: 117. DB::Exception: JSON objects have ambiguous data: in some objects path 'a' has type 'Int64' and in some - 'Tuple(b String)'. You can enable setting input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type for path 'a'. (INCORRECT_DATA) (version 24.3.1.1).
You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
```

Com a configuração habilitada:

```sql title="Query"
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : "a" : 42}, {"obj" : {"a" : {"b" : "Hello"}}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : "Hello"}}}');
```

```response title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(String))     │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
┌─obj─────────────────┐
│ ('42')              │
│ ('{"b" : "Hello"}') │
└─────────────────────┘
```

<div id="input_format_json_read_objects_as_strings">
  ##### input_format_json_read_objects_as_strings
</div>

Ao habilitar essa configuração, é possível ler objetos JSON aninhados como strings.
Essa configuração pode ser usada para ler objetos JSON aninhados sem usar o tipo JSON object.

Essa configuração vem habilitada por padrão.

Nota: habilitar essa configuração só terá efeito se a configuração `input_format_json_try_infer_named_tuples_from_objects` estiver desabilitada.

```sql
SET input_format_json_read_objects_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 0;
DESC format(JSONEachRow, $$
                             {"obj" : {"key1" : 42, "key2" : [1,2,3,4]}}
                             {"obj" : {"key3" : {"nested_key" : 1}}}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_numbers_as_strings">
  ##### input_format_json_read_numbers_as_strings
</div>

Ao habilitar essa configuração, é possível ler valores numéricos como strings.

Essa configuração vem habilitada por padrão.

**Exemplo**

```sql
SET input_format_json_read_numbers_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : 1055}
                                {"value" : "unknown"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_numbers">
  ##### input_format_json_read_bools_as_numbers
</div>

Ao habilitar essa configuração, é possível ler valores Bool como números.

Essa configuração é ativada por padrão.

**Exemplo:**

```sql
SET input_format_json_read_bools_as_numbers = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : 42}
                         $$)
```

```response
┌─name──┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(Int64) │              │                    │         │                  │                │
└───────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_bools_as_strings">
  ##### input_format_json_read_bools_as_strings
</div>

Ao habilitar essa configuração, é possível ler valores Bool como strings.

Essa configuração vem habilitada por padrão.

**Exemplo:**

```sql
SET input_format_json_read_bools_as_strings = 1;
DESC format(JSONEachRow, $$
                                {"value" : true}
                                {"value" : "Hello, World"}
                         $$)
```

```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ value │ Nullable(String) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input_format_json_read_arrays_as_strings">
  ##### input_format_json_read_arrays_as_strings
</div>

Ao ativar esta configuração, é possível ler valores de array JSON como strings.

Esta configuração é habilitada por padrão.

**Exemplo**

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```

```response
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

<div id="input_format_json_infer_incomplete_types_as_strings">
  ##### input_format_json_infer_incomplete_types_as_strings
</div>

Habilitar essa configuração permite usar o tipo String para chaves JSON que contenham apenas `Null`/`{}`/`[]` na amostra de dados durante a inferência de esquema.
Nos formatos JSON, qualquer valor pode ser lido como String se todas as configurações correspondentes estiverem habilitadas (todas vêm habilitadas por padrão), e podemos evitar erros como `Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` durante a inferência de esquema
usando o tipo String para chaves com tipos desconhecidos.

Exemplo:

```sql title="Query"
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

```markdown title="Response"
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

<div id="csv">
  ### CSV
</div>

No formato CSV, o ClickHouse extrai os valores das colunas da linha de acordo com os delimitadores. O ClickHouse espera que todos os tipos, exceto números e strings, estejam entre aspas duplas. Se o valor estiver entre aspas duplas, o ClickHouse tentará analisar
os dados dentro das aspas usando o parser recursivo e, em seguida, tentará encontrar o tipo de dado mais apropriado para ele. Se o valor não estiver entre aspas duplas, o ClickHouse tentará analisá-lo como um número
e, se o valor não for um número, o ClickHouse o tratará como uma string.

Se você não quiser que o ClickHouse tente determinar tipos complexos usando alguns parsers e heurísticas, poderá desabilitar a configuração `input_format_csv_use_best_effort_in_schema_inference`
e o ClickHouse tratará todas as colunas como Strings.

Se a configuração `input_format_csv_detect_header` estiver habilitada, o ClickHouse tentará detectar o cabeçalho com os nomes das colunas (e talvez os tipos) ao inferir o esquema. Essa configuração é habilitada por padrão.

**Exemplos:**

Inteiros, Floats, Bool, Strings:

```sql
DESC format(CSV, '42,42.42,true,"Hello,World!"')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Strings sem aspas:

```sql
DESC format(CSV, 'Hello world!,World hello!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Datas, DateTimes:

```sql
DESC format(CSV, '"2020-01-01","2020-01-01 00:00:00","2022-01-01 00:00:00.000"')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(CSV, '"[1,2,3]","[[1, 2], [], [3, 4]]"')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(CSV, $$"['Hello', 'world']","[['Abc', 'Def'], []]"$$)
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se um array contiver NULL, o ClickHouse usará os tipos dos outros elementos do array:

```sql
DESC format(CSV, '"[NULL, 42, NULL]"')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(CSV, $$"{'key1' : 42, 'key2' : 24}"$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested, Arrays e Maps:

```sql
DESC format(CSV, $$"[{'key1' : [[42, 42], []], 'key2' : [[null], [42]]}]"$$)
```

```response
┌─name─┬─type──────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Array(Nullable(Int64))))) │              │                    │         │                  │                │
└──────┴───────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se o ClickHouse não conseguir determinar o tipo entre aspas porque os dados contêm apenas valores NULL, o ClickHouse vai tratá-lo como String:

```sql
DESC format(CSV, '"[NULL, NULL]"')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Exemplo com a configuração `input_format_csv_use_best_effort_in_schema_inference` desativada:

```sql
SET input_format_csv_use_best_effort_in_schema_inference = 0
DESC format(CSV, '"[1,2,3]",42.42,Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Exemplos de detecção automática do cabeçalho (quando `input_format_csv_detect_header` está habilitado):

Apenas nomes:

```sql
SELECT * FROM format(CSV,
$$"number","string","array"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

Nomes e tipos:

```sql
DESC format(CSV,
$$"number","string","array"
"UInt32","String","Array(UInt16)"
42,"Hello","[1, 2, 3]"
43,"World","[4, 5, 6]"
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Observe que só é possível detectar o cabeçalho se houver pelo menos uma coluna com tipo diferente de String. Se todas as colunas tiverem tipo String, o cabeçalho não será detectado:

```sql
SELECT * FROM format(CSV,
$$"first_column","second_column"
"Hello","World"
"World","Hello"
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="csv-settings">
  #### Configurações do CSV
</div>

<div id="input_format_csv_try_infer_numbers_from_strings">
  ##### input_format_csv_try_infer_numbers_from_strings
</div>

Ao ativar esta configuração, é possível inferir números a partir de valores de string.

Esta configuração vem desabilitada por padrão.

**Exemplo:**

```sql
SET input_format_json_try_infer_numbers_from_strings = 1;
DESC format(CSV, '42,42.42');
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="tsv-tskv">
  ### TSV/TSKV
</div>

Nos formatos TSV/TSKV, o ClickHouse extrai, de cada linha, o valor da coluna com base nos delimitadores tabulares e, em seguida, analisa o valor extraído usando
o parser recursivo para determinar o tipo mais apropriado. Se o tipo não puder ser determinado, o ClickHouse tratará esse valor como String.

Se você não quiser que o ClickHouse tente determinar tipos complexos usando alguns parsers e heurísticas, pode desabilitar a configuração `input_format_tsv_use_best_effort_in_schema_inference`
e o ClickHouse tratará todas as colunas como Strings.

Se a configuração `input_format_tsv_detect_header` estiver habilitada, o ClickHouse tentará detectar o cabeçalho com nomes de colunas (e possivelmente tipos) ao inferir o esquema. Essa configuração vem habilitada por padrão.

**Exemplos:**

Inteiros, Floats, Bool, Strings:

```sql
DESC format(TSV, '42    42.42    true    Hello,World!')
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSKV, 'int=42    float=42.42    bool=true    string=Hello,World!\n')
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ int    │ Nullable(Int64)   │              │                    │         │                  │                │
│ float  │ Nullable(Float64) │              │                    │         │                  │                │
│ bool   │ Nullable(Bool)    │              │                    │         │                  │                │
│ string │ Nullable(String)  │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Datas, DateTimes:

```sql
DESC format(TSV, '2020-01-01    2020-01-01 00:00:00    2022-01-01 00:00:00.000')
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(TSV, '[1,2,3]    [[1, 2], [], [3, 4]]')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(TSV, '[''Hello'', ''world'']    [[''Abc'', ''Def''], []]')
```

```response
┌─name─┬─type───────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(String))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se um array contiver NULL, o ClickHouse usará os tipos dos outros elementos do array:

```sql
DESC format(TSV, '[NULL, 42, NULL]')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas:

```sql
DESC format(TSV, $$(42, 'Hello, world!')$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(TSV, $${'key1' : 42, 'key2' : 24}$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Nested Arrays, Tuples e Maps:

```sql
DESC format(TSV, $$[{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}]$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se o ClickHouse não conseguir determinar o tipo, pois os dados contêm apenas nulls, o ClickHouse os tratará como String:

```sql
DESC format(TSV, '[NULL, NULL]')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Exemplo com a configuração `input_format_tsv_use_best_effort_in_schema_inference` desativada:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Exemplos de detecção automática de cabeçalho (quando `input_format_tsv_detect_header` está ativado):

Apenas nomes:

```sql
SELECT * FROM format(TSV,
$$number    string    array
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$);
```

```response
┌─number─┬─string─┬─array───┐
│     42 │ Hello  │ [1,2,3] │
│     43 │ World  │ [4,5,6] │
└────────┴────────┴─────────┘
```

Nomes e tipos:

```sql
DESC format(TSV,
$$number    string    array
UInt32    String    Array(UInt16)
42    Hello    [1, 2, 3]
43    World    [4, 5, 6]
$$)
```

```response
┌─name───┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ UInt32        │              │                    │         │                  │                │
│ string │ String        │              │                    │         │                  │                │
│ array  │ Array(UInt16) │              │                    │         │                  │                │
└────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Note que o cabeçalho só é detectado se houver pelo menos uma coluna com um tipo diferente de String. Se todas as colunas tiverem tipo String, o cabeçalho não será detectado:

```sql
SELECT * FROM format(TSV,
$$first_column    second_column
Hello    World
World    Hello
$$)
```

```response
┌─c1───────────┬─c2────────────┐
│ first_column │ second_column │
│ Hello        │ World         │
│ World        │ Hello         │
└──────────────┴───────────────┘
```

<div id="values">
  ### Valores
</div>

No formato Values, o ClickHouse extrai o valor da coluna da linha e o analisa usando
o parser recursivo, de forma semelhante à análise de literais.

**Exemplos:**

Integers, Floats, Bool, Strings:

```sql
DESC format(Values, $$(42, 42.42, true, 'Hello,World!')$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)   │              │                    │         │                  │                │
│ c2   │ Nullable(Float64) │              │                    │         │                  │                │
│ c3   │ Nullable(Bool)    │              │                    │         │                  │                │
│ c4   │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Dates, DateTimes:

```sql
 DESC format(Values, $$('2020-01-01', '2020-01-01 00:00:00', '2022-01-01 00:00:00.000')$$)
```

```response
┌─name─┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Date)          │              │                    │         │                  │                │
│ c2   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ c3   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└──────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays:

```sql
DESC format(Values, '([1,2,3], [[1, 2], [], [3, 4]])')
```

```response
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64))        │              │                    │         │                  │                │
│ c2   │ Array(Array(Nullable(Int64))) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se um array contiver null, o ClickHouse usará os tipos dos demais elementos do array:

```sql
DESC format(Values, '([NULL, 42, NULL])')
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Tuplas:

```sql
DESC format(Values, $$((42, 'Hello, world!'))$$)
```

```response
┌─name─┬─type─────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Tuple(Nullable(Int64), Nullable(String)) │              │                    │         │                  │                │
└──────┴──────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Maps:

```sql
DESC format(Values, $$({'key1' : 42, 'key2' : 24})$$)
```

```response
┌─name─┬─type─────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Map(String, Nullable(Int64)) │              │                    │         │                  │                │
└──────┴──────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Arrays, Tuples e Maps aninhados:

```sql
DESC format(Values, $$([{'key1' : [(42, 'Hello'), (24, NULL)], 'key2' : [(NULL, ','), (42, 'world!')]}])$$)
```

```response
┌─name─┬─type────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Array(Map(String, Array(Tuple(Nullable(Int64), Nullable(String))))) │              │                    │         │                  │                │
└──────┴─────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Se o ClickHouse não conseguir determinar o tipo porque os dados contêm apenas valores nulos, uma exceção será lançada:

```sql
DESC format(Values, '([NULL, NULL])')
```

```response
Code: 652. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot determine type for column 'c1' by first 1 rows of data,
most likely this column contains only Nulls or empty Arrays/Maps.
...
```

Exemplo com a configuração `input_format_tsv_use_best_effort_in_schema_inference` desativada:

```sql
SET input_format_tsv_use_best_effort_in_schema_inference = 0
DESC format(TSV, '[1,2,3]    42.42    Hello World!')
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(String) │              │                    │         │                  │                │
│ c3   │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="custom-separated">
  ### CustomSeparated
</div>

No formato CustomSeparated, o ClickHouse primeiro extrai todos os valores de todas as colunas da linha de acordo com os delimitadores especificados e, em seguida, tenta inferir
o tipo de dado de cada valor de acordo com a regra de escape.

Se a configuração `input_format_custom_detect_header` estiver habilitada, o ClickHouse tentará detectar o cabeçalho com nomes de coluna (e possivelmente tipos) ao inferir
o esquema. Essa configuração vem habilitada por padrão.

**Exemplo**

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64)      │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Exemplo de detecção automática de cabeçalho (quando `input_format_custom_detect_header` está ativado):

```sql
SET format_custom_row_before_delimiter = '<row_before_delimiter>',
       format_custom_row_after_delimiter = '<row_after_delimiter>\n',
       format_custom_row_between_delimiter = '<row_between_delimiter>\n',
       format_custom_result_before_delimiter = '<result_before_delimiter>\n',
       format_custom_result_after_delimiter = '<result_after_delimiter>\n',
       format_custom_field_delimiter = '<field_delimiter>',
       format_custom_escaping_rule = 'Quoted'

DESC format(CustomSeparated, $$<result_before_delimiter>
<row_before_delimiter>'number'<field_delimiter>'string'<field_delimiter>'array'<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[1, NULL, 3]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[1, 2, NULL]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─number─┬─string────────┬─array──────┐
│  42.42 │ Some string 1 │ [1,NULL,3] │
│   ᴺᵁᴸᴸ │ Some string 3 │ [1,2,NULL] │
└────────┴───────────────┴────────────┘
```

<div id="template">
  ### Template
</div>

No formato Template, o ClickHouse primeiro extrai todos os valores das colunas da linha de acordo com o template especificado e, em seguida, tenta inferir o tipo de dados de cada valor de acordo com sua regra de escape.

**Exemplo**

Digamos que temos um arquivo `resultset` com o seguinte conteúdo:

```bash
<result_before_delimiter>
${data}<result_after_delimiter>
```

E um arquivo `row_format` com o seguinte conteúdo:

```text
<row_before_delimiter>${column_1:CSV}<field_delimiter_1>${column_2:Quoted}<field_delimiter_2>${column_3:JSON}<row_after_delimiter>
```

Em seguida, podemos executar as seguintes consultas:

```sql
SET format_template_rows_between_delimiter = '<row_between_delimiter>\n',
       format_template_row = 'row_format',
       format_template_resultset = 'resultset_format'

DESC format(Template, $$<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>'Some string 1'<field_delimiter_2>[1, null, 2]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter_1>'Some string 3'<field_delimiter_2>[1, 2, null]<row_after_delimiter>
<result_after_delimiter>
$$)
```

```response
┌─name─────┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ column_1 │ Nullable(Float64)      │              │                    │         │                  │                │
│ column_2 │ Nullable(String)       │              │                    │         │                  │                │
│ column_3 │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="regexp">
  ### Regexp
</div>

Semelhante ao Template, no formato Regexp o ClickHouse primeiro extrai todos os valores das colunas na linha de acordo com a expressão regular especificada e, em seguida, tenta inferir
o tipo de dado de cada valor de acordo com a regra de escape especificada.

**Exemplo**

```sql
SET format_regexp = '^Line: value_1=(.+?), value_2=(.+?), value_3=(.+?)',
       format_regexp_escaping_rule = 'CSV'

DESC format(Regexp, $$Line: value_1=42, value_2="Some string 1", value_3="[1, NULL, 3]"
Line: value_1=2, value_2="Some string 2", value_3="[4, 5, NULL]"$$)
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Int64)        │              │                    │         │                  │                │
│ c2   │ Nullable(String)       │              │                    │         │                  │                │
│ c3   │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="settings-for-text-formats">
  ### Configurações dos formatos de texto
</div>

<div id="input-format-max-rows-to-read-for-schema-inference">
  #### input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference
</div>

Essas configurações controlam a quantidade de dados lida durante a inferência de esquema.
Quanto mais linhas/bytes forem lidos, mais tempo será gasto na inferência de esquema, mas maior será a chance de
determinar corretamente os tipos (especialmente quando os dados contêm muitos valores NULL).

Valores padrão:

* `25000` para `input_format_max_rows_to_read_for_schema_inference`.
* `33554432` (32 Mb) para `input_format_max_bytes_to_read_for_schema_inference`.

<div id="column-names-for-schema-inference">
  #### column_names_for_schema_inference
</div>

A lista de nomes de colunas a serem usados na inferência de esquema para formatos sem nomes de colunas explícitos. Os nomes especificados serão usados em vez do padrão `c1,c2,c3,...`. Formato: `column1,column2,column3,...`.

**Exemplo**

```sql
DESC format(TSV, 'Hello, World!    42    [1, 2, 3]') settings column_names_for_schema_inference = 'str,int,arr'
```

```response
┌─name─┬─type───────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ str  │ Nullable(String)       │              │                    │         │                  │                │
│ int  │ Nullable(Int64)        │              │                    │         │                  │                │
│ arr  │ Array(Nullable(Int64)) │              │                    │         │                  │                │
└──────┴────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-hints">
  #### schema_inference_hints
</div>

A lista de nomes e tipos de colunas a serem usados na inferência de esquema em vez dos tipos determinados automaticamente. Formato: &#39;column&#95;name1 column&#95;type1, column&#95;name2 column&#95;type2, ...&#39;.
Essa configuração pode ser usada para especificar os tipos de colunas que não puderam ser determinados automaticamente ou para otimizar o esquema.

**Exemplo**

```sql
DESC format(JSONEachRow, '{"id" : 1, "age" : 25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}') SETTINGS schema_inference_hints = 'age LowCardinality(UInt8), status Nullable(String)', allow_suspicious_low_cardinality_types=1
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ LowCardinality(UInt8)   │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-make-columns-nullable">
  #### schema_inference_make_columns_nullable $
</div>

Controla a criação de tipos inferidos como `Nullable` na inferência de esquema para formatos sem informações sobre nulabilidade. Valores possíveis:

* 0 - o tipo inferido nunca será `Nullable`,
* 1 - todos os tipos inferidos serão `Nullable`,
* 2 ou &#39;auto&#39; - para formatos de texto, o tipo inferido será `Nullable` somente se a coluna contiver `NULL` em uma amostra processada durante a inferência de esquema; para formatos fortemente tipados (Parquet, ORC, Arrow), a informação de nulabilidade é obtida dos metadados do arquivo,
* 3 - para formatos de texto, use `Nullable`; para formatos fortemente tipados, use os metadados do arquivo.

Padrão: 3.

**Exemplos**

```sql
SET schema_inference_make_columns_nullable = 1;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Int64)         │              │                    │         │                  │                │
│ age     │ Nullable(Int64)         │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ status  │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 'auto';
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response
┌─name────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64            │              │                    │         │                  │                │
│ age     │ Int64            │              │                    │         │                  │                │
│ name    │ String           │              │                    │         │                  │                │
│ status  │ Nullable(String) │              │                    │         │                  │                │
│ hobbies │ Array(String)    │              │                    │         │                  │                │
└─────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET schema_inference_make_columns_nullable = 0;
DESC format(JSONEachRow, $$
                                {"id" :  1, "age" :  25, "name" : "Josh", "status" : null, "hobbies" : ["football", "cooking"]}
                                {"id" :  2, "age" :  19, "name" :  "Alan", "status" : "married", "hobbies" :  ["tennis", "art"]}
                         $$)
```

```response

┌─name────┬─type──────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Int64         │              │                    │         │                  │                │
│ age     │ Int64         │              │                    │         │                  │                │
│ name    │ String        │              │                    │         │                  │                │
│ status  │ String        │              │                    │         │                  │                │
│ hobbies │ Array(String) │              │                    │         │                  │                │
└─────────┴───────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-integers">
  #### input_format_try_infer_integers
</div>

:::note
Esta configuração não se aplica ao tipo de dado `JSON`.
:::

Se habilitada, o ClickHouse tentará inferir inteiros em vez de números de ponto flutuante na inferência de esquema para formatos de texto.
Se todos os números na coluna dos dados de amostra forem inteiros, o tipo resultante será `Int64`; se pelo menos um número for de ponto flutuante, o tipo resultante será `Float64`.
Se os dados de amostra contiverem apenas inteiros e pelo menos um inteiro for positivo e exceder o limite de `Int64`, o ClickHouse inferirá `UInt64`.

Habilitada por padrão.

**Exemplos**

```sql
SET input_format_try_infer_integers = 0
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_integers = 1
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2}
                         $$)
```

```response
┌─name───┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Int64) │              │                    │         │                  │                │
└────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 18446744073709551615}
                         $$)
```

```response
┌─name───┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(UInt64) │              │                    │         │                  │                │
└────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"number" : 1}
                                {"number" : 2.2}
                         $$)
```

```response
┌─name───┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ number │ Nullable(Float64) │              │                    │         │                  │                │
└────────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes">
  #### input_format_try_infer_datetimes
</div>

Se estiver habilitado, o ClickHouse tentará inferir o tipo `DateTime` ou `DateTime64` a partir de campos do tipo string na inferência de esquema para formatos de texto.
Se todos os campos de uma coluna nos dados de amostra forem convertidos com sucesso em datetimes, o tipo resultante será `DateTime` ou `DateTime64(9)` (se algum datetime tiver parte fracionária),
se pelo menos um campo não for convertido como datetime, o tipo resultante será `String`.

Habilitado por padrão.

**Exemplos**

```sql
SET input_format_try_infer_datetimes = 0;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_datetimes = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime)      │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "unknown", "datetime64" : "unknown"}
                         $$)
```

```response
┌─name───────┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(String) │              │                    │         │                  │                │
│ datetime64 │ Nullable(String) │              │                    │         │                  │                │
└────────────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-datetimes-only-datetime64">
  #### input_format_try_infer_datetimes_only_datetime64
</div>

Se ativado, o ClickHouse sempre inferirá `DateTime64(9)` quando `input_format_try_infer_datetimes` estiver habilitado, mesmo que os valores de data e hora não contenham parte fracionária.

Desativado por padrão.

**Exemplos**

```sql
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
DESC format(JSONEachRow, $$
                                {"datetime" : "2021-01-01 00:00:00", "datetime64" : "2021-01-01 00:00:00.000"}
                                {"datetime" : "2022-01-01 00:00:00", "datetime64" : "2022-01-01 00:00:00.000"}
                         $$)
```

```response
┌─name───────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ datetime   │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
│ datetime64 │ Nullable(DateTime64(9)) │              │                    │         │                  │                │
└────────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Observação: a análise de valores de data e hora durante a inferência de esquema respeita a configuração [date&#95;time&#95;input&#95;format](/pt-BR/operations/settings/settings-formats.md#date_time_input_format)

<div id="input-format-try-infer-dates">
  #### input_format_try_infer_dates
</div>

Se estiver habilitado, o ClickHouse tentará inferir o tipo `Date` a partir de campos do tipo string na inferência de esquema para formatos de texto.
Se todos os campos de uma coluna na amostra de dados forem convertidos com sucesso em datas, o tipo resultante será `Date`,
se pelo menos um campo não for convertido como data, o tipo resultante será `String`.

Habilitado por padrão.

**Exemplos**

```sql
SET input_format_try_infer_datetimes = 0, input_format_try_infer_dates = 0
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SET input_format_try_infer_dates = 1
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "2022-01-01"}
                         $$)
```

```response
┌─name─┬─type───────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(Date) │              │                    │         │                  │                │
└──────┴────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
DESC format(JSONEachRow, $$
                                {"date" : "2021-01-01"}
                                {"date" : "unknown"}
                         $$)
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ date │ Nullable(String) │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="input-format-try-infer-exponent-floats">
  #### input_format_try_infer_exponent_floats
</div>

Se estiver habilitado, o ClickHouse tentará inferir números de ponto flutuante em notação exponencial nos formatos de texto (exceto no JSON, em que números em notação exponencial são sempre inferidos).

Desabilitado por padrão.

**Exemplo**

```sql
SET input_format_try_infer_exponent_floats = 1;
DESC format(CSV,
$$1.1E10
2.3e-12
42E00
$$)
```

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(Float64) │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="self-describing-formats">
  ## Formatos autodescritivos
</div>

Formatos autodescritivos contêm informações sobre a estrutura dos dados nos próprios dados;
isso pode ser algum cabeçalho com uma descrição, uma árvore binária de tipos ou algum tipo de tabela.
Para inferir automaticamente um esquema a partir de arquivos nesses formatos, o ClickHouse lê uma parte dos dados que contém
informações sobre os tipos e a converte em um esquema da tabela do ClickHouse.

<div id="formats-with-names-and-types">
  ### Formatos com o sufixo -WithNamesAndTypes
</div>

O ClickHouse oferece suporte a alguns formatos de texto com o sufixo -WithNamesAndTypes. Esse sufixo significa que os dados contêm duas linhas adicionais com os nomes e tipos das colunas antes dos dados reais.
Durante a inferência de esquema desses formatos, o ClickHouse lê as duas primeiras linhas e extrai os nomes e tipos das colunas.

**Exemplo**

```sql
DESC format(TSVWithNamesAndTypes,
$$num    str    arr
UInt8    String    Array(UInt8)
42    Hello, World!    [1,2,3]
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-with-metadata">
  ### Formatos JSON com metadados
</div>

Alguns formatos de entrada JSON ([JSON](/pt-BR/interfaces/formats/JSON), [JSONCompact](/pt-BR/interfaces/formats/JSONCompact), [JSONColumnsWithMetadata](/pt-BR/interfaces/formats/JSONColumnsWithMetadata)) contêm metadados com nomes e tipos das colunas.
Na inferência de esquema desses formatos, o ClickHouse lê esses metadados.

**Exemplo**

```sql
DESC format(JSON, $$
{
    "meta":
    [
        {
            "name": "num",
            "type": "UInt8"
        },
        {
            "name": "str",
            "type": "String"
        },
        {
            "name": "arr",
            "type": "Array(UInt8)"
        }
    ],

    "data":
    [
        {
            "num": 42,
            "str": "Hello, World",
            "arr": [1,2,3]
        }
    ],

    "rows": 1,

    "statistics":
    {
        "elapsed": 0.005723915,
        "rows_read": 1,
        "bytes_read": 1
    }
}
$$)
```

```response
┌─name─┬─type─────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ num  │ UInt8        │              │                    │         │                  │                │
│ str  │ String       │              │                    │         │                  │                │
│ arr  │ Array(UInt8) │              │                    │         │                  │                │
└──────┴──────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="avro">
  ### Avro
</div>

No formato Avro, o ClickHouse lê o esquema a partir dos dados e o converte para o esquema do ClickHouse usando as seguintes correspondências de tipos:

| Tipo de dados Avro                 | Tipo de dados ClickHouse                                                       |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `boolean`                          | [Bool](../sql-reference/data-types/boolean.md)                                 |
| `int`                              | [Int32](../sql-reference/data-types/int-uint.md)                               |
| `int (date)` *                     | [Date32](../sql-reference/data-types/date32.md)                                |
| `long`                             | [Int64](../sql-reference/data-types/int-uint.md)                               |
| `float`                            | [Float32](../sql-reference/data-types/float.md)                                |
| `double`                           | [Float64](../sql-reference/data-types/float.md)                                |
| `bytes`, `string`                  | [String](../sql-reference/data-types/string.md)                                |
| `fixed`                            | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                   |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)                                    |
| `array(T)`                         | [Array(T)](../sql-reference/data-types/array.md)                               |
| `union(null, T)`, `union(T, null)` | [Nullable(T)](../sql-reference/data-types/date.md)                             |
| `null`                             | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md) |
| `string (uuid)` *                  | [UUID](../sql-reference/data-types/uuid.md)                                    |
| `binary (decimal)` *               | [Decimal(P, S)](../sql-reference/data-types/decimal.md)                        |

* [Tipos lógicos do Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Outros tipos de Avro não são suportados.

<div id="parquet">
  ### Parquet
</div>

No formato Parquet, o ClickHouse lê o esquema a partir dos dados e o converte para o esquema do ClickHouse usando as seguintes correspondências de tipos:

| Tipo de dado Parquet         | Tipo de dado ClickHouse                                 |
| ---------------------------- | ------------------------------------------------------- |
| `BOOL`                       | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                      | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`                      | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)         |
| `DATE`                       | [Date32](../sql-reference/data-types/date32.md)         |
| `TIME (ms)`                  | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME (us, ns)` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`           | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                       | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                     | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                        | [Map](../sql-reference/data-types/map.md)               |

Outros tipos Parquet não são suportados.

<div id="arrow">
  ### Arrow
</div>

No formato Arrow, o ClickHouse lê o esquema a partir dos dados e o converte para o esquema do ClickHouse com base nas seguintes correspondências de tipos:

| Tipo de dado Arrow              | Tipo de dado ClickHouse                                 |
| ------------------------------- | ------------------------------------------------------- |
| `BOOL`                          | [Bool](../sql-reference/data-types/boolean.md)          |
| `UINT8`                         | [UInt8](../sql-reference/data-types/int-uint.md)        |
| `INT8`                          | [Int8](../sql-reference/data-types/int-uint.md)         |
| `UINT16`                        | [UInt16](../sql-reference/data-types/int-uint.md)       |
| `INT16`                         | [Int16](../sql-reference/data-types/int-uint.md)        |
| `UINT32`                        | [UInt32](../sql-reference/data-types/int-uint.md)       |
| `INT32`                         | [Int32](../sql-reference/data-types/int-uint.md)        |
| `UINT64`                        | [UInt64](../sql-reference/data-types/int-uint.md)       |
| `INT64`                         | [Int64](../sql-reference/data-types/int-uint.md)        |
| `FLOAT`, `HALF_FLOAT`           | [Float32](../sql-reference/data-types/float.md)         |
| `DOUBLE`                        | [Float64](../sql-reference/data-types/float.md)         |
| `DATE32`                        | [Date32](../sql-reference/data-types/date32.md)         |
| `DATE64`                        | [DateTime](../sql-reference/data-types/datetime.md)     |
| `TIMESTAMP`, `TIME32`, `TIME64` | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `STRING`, `BINARY`              | [String](../sql-reference/data-types/string.md)         |
| `DECIMAL128`, `DECIMAL256`      | [Decimal](../sql-reference/data-types/decimal.md)       |
| `LIST`                          | [Array](../sql-reference/data-types/array.md)           |
| `STRUCT`                        | [Tuple](../sql-reference/data-types/tuple.md)           |
| `MAP`                           | [Map](../sql-reference/data-types/map.md)               |

Outros tipos do Arrow não são suportados.

<div id="orc">
  ### ORC
</div>

No formato ORC, o ClickHouse lê o esquema dos dados e o converte para o esquema do ClickHouse usando as seguintes correspondências de tipos:

| tipo de dado ORC                     | tipo de dado ClickHouse                                 |
| ------------------------------------ | ------------------------------------------------------- |
| `Boolean`                            | [Bool](../sql-reference/data-types/boolean.md)          |
| `Tinyint`                            | [Int8](../sql-reference/data-types/int-uint.md)         |
| `Smallint`                           | [Int16](../sql-reference/data-types/int-uint.md)        |
| `Int`                                | [Int32](../sql-reference/data-types/int-uint.md)        |
| `Bigint`                             | [Int64](../sql-reference/data-types/int-uint.md)        |
| `Float`                              | [Float32](../sql-reference/data-types/float.md)         |
| `Double`                             | [Float64](../sql-reference/data-types/float.md)         |
| `Date`                               | [Date32](../sql-reference/data-types/date32.md)         |
| `Timestamp`                          | [DateTime64](../sql-reference/data-types/datetime64.md) |
| `String`, `Char`, `Varchar`,`BINARY` | [String](../sql-reference/data-types/string.md)         |
| `Decimal`                            | [Decimal](../sql-reference/data-types/decimal.md)       |
| `List`                               | [Array](../sql-reference/data-types/array.md)           |
| `Struct`                             | [Tuple](../sql-reference/data-types/tuple.md)           |
| `Map`                                | [Map](../sql-reference/data-types/map.md)               |

Outros tipos ORC não são compatíveis.

<div id="native">
  ### Native
</div>

O formato Native é usado no ClickHouse e contém o esquema nos próprios dados.
Na inferência de esquema, o ClickHouse lê o esquema diretamente dos dados, sem nenhuma transformação.

<div id="formats-with-external-schema">
  ## Formatos com esquema externo
</div>

Esses formatos exigem um esquema que descreva os dados em um arquivo separado, em uma linguagem de esquema específica.
Para inferir automaticamente um esquema a partir de arquivos nesses formatos, o ClickHouse lê o esquema externo de um arquivo separado e o converte em um esquema de tabela do ClickHouse.

<div id="protobuf">
  ### Protobuf
</div>

Na inferência de esquema do formato Protobuf, o ClickHouse usa as seguintes correspondências de tipos:

| Tipo de dados do Protobuf     | Tipo de dados do ClickHouse                       |
| ----------------------------- | ------------------------------------------------- |
| `bool`                        | [UInt8](../sql-reference/data-types/int-uint.md)  |
| `float`                       | [Float32](../sql-reference/data-types/float.md)   |
| `double`                      | [Float64](../sql-reference/data-types/float.md)   |
| `int32`, `sint32`, `sfixed32` | [Int32](../sql-reference/data-types/int-uint.md)  |
| `int64`, `sint64`, `sfixed64` | [Int64](../sql-reference/data-types/int-uint.md)  |
| `uint32`, `fixed32`           | [UInt32](../sql-reference/data-types/int-uint.md) |
| `uint64`, `fixed64`           | [UInt64](../sql-reference/data-types/int-uint.md) |
| `string`, `bytes`             | [String](../sql-reference/data-types/string.md)   |
| `enum`                        | [Enum](../sql-reference/data-types/enum.md)       |
| `repeated T`                  | [Array(T)](../sql-reference/data-types/array.md)  |
| `message`, `group`            | [Tuple](../sql-reference/data-types/tuple.md)     |

<div id="capnproto">
  ### CapnProto
</div>

Na inferência de esquema do formato CapnProto, o ClickHouse usa as seguintes correspondências de tipos:

| Tipo de dado CapnProto             | Tipo de dado ClickHouse                                |
| ---------------------------------- | ------------------------------------------------------ |
| `Bool`                             | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int8`                             | [Int8](../sql-reference/data-types/int-uint.md)        |
| `UInt8`                            | [UInt8](../sql-reference/data-types/int-uint.md)       |
| `Int16`                            | [Int16](../sql-reference/data-types/int-uint.md)       |
| `UInt16`                           | [UInt16](../sql-reference/data-types/int-uint.md)      |
| `Int32`                            | [Int32](../sql-reference/data-types/int-uint.md)       |
| `UInt32`                           | [UInt32](../sql-reference/data-types/int-uint.md)      |
| `Int64`                            | [Int64](../sql-reference/data-types/int-uint.md)       |
| `UInt64`                           | [UInt64](../sql-reference/data-types/int-uint.md)      |
| `Float32`                          | [Float32](../sql-reference/data-types/float.md)        |
| `Float64`                          | [Float64](../sql-reference/data-types/float.md)        |
| `Text`, `Data`                     | [String](../sql-reference/data-types/string.md)        |
| `enum`                             | [Enum](../sql-reference/data-types/enum.md)            |
| `List`                             | [Array](../sql-reference/data-types/array.md)          |
| `struct`                           | [Tuple](../sql-reference/data-types/tuple.md)          |
| `union(T, Void)`, `union(Void, T)` | [Nullable(T)](../sql-reference/data-types/nullable.md) |

<div id="strong-typed-binary-formats">
  ## Formatos binários com tipagem forte
</div>

Nesses formatos, cada valor serializado contém informações sobre seu tipo (e possivelmente sobre seu nome), mas não há informações sobre toda a tabela.
Na inferência de esquema desses formatos, o ClickHouse lê os dados linha por linha (até `input_format_max_rows_to_read_for_schema_inference` linhas ou `input_format_max_bytes_to_read_for_schema_inference` bytes) e extrai
o tipo (e possivelmente o nome) de cada valor a partir dos dados e, em seguida, converte esses tipos em tipos do ClickHouse.

<div id="msgpack">
  ### MsgPack
</div>

No formato MsgPack, não há delimitador entre linhas; para usar a inferência de esquema nesse formato, você deve especificar o número de colunas na tabela
usando a configuração `input_format_msgpack_number_of_columns`. O ClickHouse usa as seguintes correspondências de tipos:

| Tipo de dado do MessagePack (`INSERT`)                             | Tipo de dado do ClickHouse                            |
| ------------------------------------------------------------------ | ----------------------------------------------------- |
| `int N`, `uint N`, `negative fixint`, `positive fixint`            | [Int64](../sql-reference/data-types/int-uint.md)      |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)       |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)       |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)       |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)           |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)   |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md) |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)         |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)             |

Por padrão, todos os tipos inferidos são encapsulados em `Nullable`, mas isso pode ser alterado usando a configuração `schema_inference_make_columns_nullable`.

<div id="bsoneachrow">
  ### BSONEachRow
</div>

No BSONEachRow, cada linha de dados é apresentada como um documento BSON. Na inferência de esquema, o ClickHouse lê os documentos BSON um a um e extrai
valores, nomes e tipos dos dados, transformando esses tipos em tipos do ClickHouse usando as seguintes correspondências de tipos:

| Tipo BSON                                                                                         | Tipo do ClickHouse                                                                                                                   |
| ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `\x08` boolean                                                                                    | [Bool](../sql-reference/data-types/boolean.md)                                                                                       |
| `\x10` int32                                                                                      | [Int32](../sql-reference/data-types/int-uint.md)                                                                                     |
| `\x12` int64                                                                                      | [Int64](../sql-reference/data-types/int-uint.md)                                                                                     |
| `\x01` double                                                                                     | [Float64](../sql-reference/data-types/float.md)                                                                                      |
| `\x09` datetime                                                                                   | [DateTime64](../sql-reference/data-types/datetime64.md)                                                                              |
| `\x05` binário com subtipo binário `\x00`, `\x02` string, `\x0E` symbol, `\x0D` código JavaScript | [String](../sql-reference/data-types/string.md)                                                                                      |
| `\x07` ObjectId,                                                                                  | [FixedString(12)](../sql-reference/data-types/fixedstring.md)                                                                        |
| `\x05` binário com subtipo uuid `\x04`, tamanho = 16                                              | [UUID](../sql-reference/data-types/uuid.md)                                                                                          |
| `\x04` array                                                                                      | [Array](../sql-reference/data-types/array.md)/[Tuple](../sql-reference/data-types/tuple.md) (se os tipos aninhados forem diferentes) |
| `\x03` documento                                                                                  | [Named Tuple](../sql-reference/data-types/tuple.md)/[Map](../sql-reference/data-types/map.md) (com chaves String)                    |

Por padrão, todos os tipos inferidos ficam dentro de `Nullable`, mas isso pode ser alterado com a configuração `schema_inference_make_columns_nullable`.

<div id="formats-with-constant-schema">
  ## Formatos com esquema constante
</div>

Os dados nesses formatos sempre têm o mesmo esquema.

<div id="line-as-string">
  ### LineAsString
</div>

Nesse formato, o ClickHouse lê a linha inteira de dados em uma única coluna com o tipo de dado `String`. O tipo inferido para esse formato é sempre `String`, e o nome da coluna é `line`.

**Exemplo**

```sql
DESC format(LineAsString, 'Hello\nworld!')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-string">
  ### JSONAsString
</div>

Nesse formato, o ClickHouse lê o objeto JSON inteiro dos dados em uma única coluna com o tipo de dado `String`. O tipo inferido para esse formato é sempre `String`, e o nome da coluna é `json`.

**Exemplo**

```sql
DESC format(JSONAsString, '{"x" : 42, "y" : "Hello, World!"}')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="json-as-object">
  ### JSONAsObject
</div>

Nesse formato, o ClickHouse lê o objeto JSON inteiro dos dados em uma única coluna do tipo `JSON`. O tipo inferido para esse formato é sempre `JSON`, e o nome da coluna é `json`.

**Exemplo**

```sql
DESC format(JSONAsObject, '{"x" : 42, "y" : "Hello, World!"}');
```

```response
┌─name─┬─type─┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ json │ JSON │              │                    │         │                  │                │
└──────┴──────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

<div id="schema-inference-modes">
  ## Modos de inferência de esquema
</div>

A inferência de esquema a partir do conjunto de arquivos de dados pode operar em 2 modos diferentes: `default` e `union`.
O modo é controlado pela configuração `schema_inference_mode`.

<div id="default-schema-inference-mode">
  ### Modo padrão
</div>

No modo padrão, o ClickHouse parte do pressuposto de que todos os arquivos têm o mesmo esquema e tenta inferi-lo lendo os arquivos um a um até conseguir.

Exemplo:

Vamos supor que temos 3 arquivos `data1.jsonl`, `data2.jsonl` e `data3.jsonl` com o seguinte conteúdo:

`data1.jsonl`:

```json
{"field1" :  1, "field2" :  null}
{"field1" :  2, "field2" :  null}
{"field1" :  3, "field2" :  null}
```

`data2.jsonl`:

```json
{"field1" :  4, "field2" :  "Data4"}
{"field1" :  5, "field2" :  "Data5"}
{"field1" :  6, "field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field1" :  7, "field2" :  "Data7", "field3" :  [1, 2, 3]}
{"field1" :  8, "field2" :  "Data8", "field3" :  [4, 5, 6]}
{"field1" :  9, "field2" :  "Data9", "field3" :  [7, 8, 9]}
```

Vamos tentar usar a inferência de esquema nesses 3 arquivos:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='default'
```

```response title="Response"
┌─name───┬─type─────────────┐
│ field1 │ Nullable(Int64)  │
│ field2 │ Nullable(String) │
└────────┴──────────────────┘
```

Como podemos ver, não temos `field3` do arquivo `data3.jsonl`.
Isso acontece porque o ClickHouse primeiro tentou inferir o esquema do arquivo `data1.jsonl`, mas falhou porque havia apenas nulls no campo `field2`,
e depois tentou inferir o esquema de `data2.jsonl` e teve sucesso, então os dados do arquivo `data3.jsonl` não foram lidos.

<div id="default-schema-inference-mode-1">
  ### Modo union
</div>

No modo union, o ClickHouse assume que os arquivos podem ter esquemas diferentes, então ele infere os esquemas de todos os arquivos e depois os combina em um esquema comum.

Digamos que temos 3 arquivos `data1.jsonl`, `data2.jsonl` e `data3.jsonl` com o conteúdo a seguir:

`data1.jsonl`:

```json
{"field1" :  1}
{"field1" :  2}
{"field1" :  3}
```

`data2.jsonl`:

```json
{"field2" :  "Data4"}
{"field2" :  "Data5"}
{"field2" :  "Data5"}
```

`data3.jsonl`:

```json
{"field3" :  [1, 2, 3]}
{"field3" :  [4, 5, 6]}
{"field3" :  [7, 8, 9]}
```

Vamos tentar usar a inferência de esquema nestes 3 arquivos:

```sql title="Query"
:) DESCRIBE file('data{1,2,3}.jsonl') SETTINGS schema_inference_mode='union'
```

```response title="Response"
┌─name───┬─type───────────────────┐
│ field1 │ Nullable(Int64)        │
│ field2 │ Nullable(String)       │
│ field3 │ Array(Nullable(Int64)) │
└────────┴────────────────────────┘
```

Como podemos ver, temos todos os campos de todos os arquivos.

Observação:

* Como alguns arquivos podem não conter algumas colunas do esquema resultante, o modo union é compatível apenas com formatos que oferecem suporte à leitura de subconjuntos de colunas (como JSONEachRow, Parquet, TSVWithNames etc.) e não funcionará com outros formatos (como CSV, TSV, JSONCompactEachRow etc.).
* Se o ClickHouse não conseguir inferir o esquema de um dos arquivos, uma exceção será lançada.
* Se você tiver muitos arquivos, ler o esquema de todos eles pode levar muito tempo.

<div id="automatic-format-detection">
  ## Detecção automática de formato
</div>

Se o formato dos dados não for especificado e não puder ser determinado pela extensão do arquivo, o ClickHouse tentará detectar o formato do arquivo a partir do seu conteúdo.

**Exemplos:**

Digamos que temos `data` com o seguinte conteúdo:

```csv
"a","b"
1,"Data1"
2,"Data2"
3,"Data3"
```

Podemos inspecionar e consultar este arquivo sem especificar o formato nem a estrutura:

```sql
:) desc file(data);
```

```response
┌─name─┬─type─────────────┐
│ a    │ Nullable(Int64)  │
│ b    │ Nullable(String) │
└──────┴──────────────────┘
```

```sql
:) select * from file(data);
```

```response
┌─a─┬─b─────┐
│ 1 │ Data1 │
│ 2 │ Data2 │
│ 3 │ Data3 │
└───┴───────┘
```

:::note
O ClickHouse consegue detectar apenas um subconjunto dos formatos, e essa detecção leva algum tempo; por isso, é sempre melhor especificar o formato explicitamente.
:::