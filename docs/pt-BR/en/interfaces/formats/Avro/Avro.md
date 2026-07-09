---
alias: []
description: 'Documentação sobre o formato Avro'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

[Apache Avro](https://avro.apache.org/) é um formato de serialização orientado a linhas que usa codificação binária para um processamento eficiente de dados. O formato `Avro` oferece suporte à leitura e à gravação de [arquivos de dados Avro](https://avro.apache.org/docs/current/specification/#object-container-files). Esse formato requer mensagens autodescritivas com um esquema embutido. Se você estiver usando Avro com um registro de esquemas, consulte o formato [`AvroConfluent`](./AvroConfluent.md).

<div id="data-type-mapping">
  ## Mapeamento de tipos de dados
</div>

<DataTypeMapping />

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                               | Descrição                                                                                                                                                                             | Padrão  |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `input_format_avro_allow_missing_fields`   | Se deve usar um valor padrão em vez de gerar um erro quando um campo não é encontrado no esquema.                                                                                     | `0`     |
| `input_format_avro_null_as_default`        | Se deve usar um valor padrão em vez de gerar um erro ao inserir um valor `null` em uma coluna que não aceita nulos.                                                                   | `0`     |
| `output_format_avro_codec`                 | Algoritmo de compressão para arquivos Avro de saída. Valores possíveis: `null`, `deflate`, `snappy`, `zstd`.                                                                          |         |
| `output_format_avro_sync_interval`         | Frequência do marcador de sincronização em arquivos Avro (em bytes).                                                                                                                  | `16384` |
| `output_format_avro_string_column_pattern` | Expressão regular para identificar colunas `String` para o mapeamento do tipo string do Avro. Por padrão, as colunas `String` do ClickHouse são gravadas como o tipo `bytes` do Avro. |         |
| `output_format_avro_rows_in_file`          | Número máximo de linhas por arquivo Avro de saída. Quando esse limite é atingido, um novo arquivo é criado (se o sistema de armazenamento oferecer suporte à divisão de arquivos).    | `1`     |

<div id="examples">
  ## Exemplos
</div>

<div id="reading-avro-data">
  ### Lendo dados em Avro
</div>

Para ler dados de um arquivo Avro para uma tabela do ClickHouse:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

O esquema raiz do arquivo Avro ingerido deve ser do tipo `record`.

Para encontrar a correspondência entre as colunas da tabela e os campos do esquema Avro, o ClickHouse compara seus nomes.
Essa comparação diferencia maiúsculas de minúsculas, e os campos não utilizados são ignorados.

Os tipos de dados das colunas da tabela do ClickHouse podem diferir dos campos correspondentes dos dados Avro inseridos. Ao inserir dados, o ClickHouse interpreta os tipos de dados de acordo com a tabela acima e depois [converte](/pt-BR/sql-reference/functions/type-conversion-functions#CAST) os dados para o tipo de coluna correspondente.

Ao importar dados, quando um campo não é encontrado no esquema e a configuração [`input_format_avro_allow_missing_fields`](/pt-BR/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) está habilitada, o valor padrão será usado em vez de gerar um erro.

<div id="writing-avro-data">
  ### Gravando dados em Avro
</div>

Para gravar dados de uma tabela do ClickHouse em um arquivo Avro:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Os nomes de colunas devem:

* Começar com `[A-Za-z_]`
* Ser seguidos apenas por `[A-Za-z0-9_]`

A compressão de saída e o intervalo de sincronização de arquivos Avro podem ser configurados, respectivamente, com as configurações [`output_format_avro_codec`](/pt-BR/operations/settings/settings-formats.md/#output_format_avro_codec) e [`output_format_avro_sync_interval`](/pt-BR/operations/settings/settings-formats.md/#output_format_avro_sync_interval).

<div id="inferring-the-avro-schema">
  ### Inferindo o esquema do Avro
</div>

Com a função [`DESCRIBE`](/pt-BR/sql-reference/statements/describe-table) do ClickHouse, você pode visualizar rapidamente o formato inferido de um arquivo Avro, como no exemplo a seguir.
Este exemplo inclui a URL de um arquivo Avro acessível publicamente no bucket S3 público do ClickHouse:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```