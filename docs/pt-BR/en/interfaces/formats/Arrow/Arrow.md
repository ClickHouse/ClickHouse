---
alias: []
description: 'Documentação sobre o formato Arrow'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O [Apache Arrow](https://arrow.apache.org/) inclui dois formatos integrados de armazenamento colunar.
O ClickHouse oferece suporte a operações de leitura e escrita nesses formatos.
`Arrow` é o formato de &quot;modo de arquivo&quot; do Apache Arrow, projetado para acesso aleatório na memória.

<div id="data-types-matching">
  ## Correspondência entre tipos de dados
</div>

A tabela abaixo mostra os tipos de dados suportados e como eles correspondem aos [tipos de dados](/pt-BR/sql-reference/data-types/index.md) do ClickHouse em consultas `INSERT` e `SELECT`.

| Tipo de dados do Arrow (`INSERT`)       | Tipo de dados do ClickHouse                                                                                      | Tipo de dados do Arrow (`SELECT`) |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `BOOL`                                  | [Bool](/pt-BR/sql-reference/data-types/boolean.md)                                                                     | `BOOL`                            |
| `UINT8`, `BOOL`                         | [UInt8](/pt-BR/sql-reference/data-types/int-uint.md)                                                                   | `UINT8`                           |
| `INT8`                                  | [Int8](/pt-BR/sql-reference/data-types/int-uint.md)/[Enum8](/pt-BR/sql-reference/data-types/enum.md)                         | `INT8`                            |
| `UINT16`                                | [UInt16](/pt-BR/sql-reference/data-types/int-uint.md)                                                                  | `UINT16`                          |
| `INT16`                                 | [Int16](/pt-BR/sql-reference/data-types/int-uint.md)/[Enum16](/pt-BR/sql-reference/data-types/enum.md)                       | `INT16`                           |
| `UINT32`                                | [UInt32](/pt-BR/sql-reference/data-types/int-uint.md)                                                                  | `UINT32`                          |
| `INT32`                                 | [Int32](/pt-BR/sql-reference/data-types/int-uint.md)                                                                   | `INT32`                           |
| `UINT64`                                | [UInt64](/pt-BR/sql-reference/data-types/int-uint.md)                                                                  | `UINT64`                          |
| `INT64`                                 | [Int64](/pt-BR/sql-reference/data-types/int-uint.md)                                                                   | `INT64`                           |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/pt-BR/sql-reference/data-types/float.md)                                                                    | `FLOAT32`                         |
| `DOUBLE`                                | [Float64](/pt-BR/sql-reference/data-types/float.md)                                                                    | `FLOAT64`                         |
| `DATE32`                                | [Date32](/pt-BR/sql-reference/data-types/date32.md)                                                                    | `UINT16`                          |
| `DATE64`                                | [DateTime](/pt-BR/sql-reference/data-types/datetime.md)                                                                | `UINT32`                          |
| `TIMESTAMP`                             | [DateTime64](/pt-BR/sql-reference/data-types/datetime64.md)                                                            | `TIMESTAMP`                       |
| `TIME32`, `TIME64`                      | [Time64](/pt-BR/sql-reference/data-types/time64.md)                                                                    | `TIME32`, `TIME64`                |
| `STRING`, `BINARY`                      | [String](/pt-BR/sql-reference/data-types/string.md)                                                                    | `BINARY`                          |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                          | `FIXED_SIZE_BINARY`               |
| `DECIMAL`                               | [Decimal](/pt-BR/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL`                         |
| `DECIMAL256`                            | [Decimal256](/pt-BR/sql-reference/data-types/decimal.md)                                                               | `DECIMAL256`                      |
| `LIST`                                  | [Array](/pt-BR/sql-reference/data-types/array.md)                                                                      | `LIST`                            |
| `STRUCT`                                | [Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                                      | `STRUCT`                          |
| `MAP`                                   | [Map](/pt-BR/sql-reference/data-types/map.md)                                                                          | `MAP`                             |
| `UINT32`                                | [IPv4](/pt-BR/sql-reference/data-types/ipv4.md)                                                                        | `UINT32`                          |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/pt-BR/sql-reference/data-types/ipv6.md)                                                                        | `FIXED_SIZE_BINARY`               |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/pt-BR/sql-reference/data-types/int-uint.md)                                           | `FIXED_SIZE_BINARY`               |
| `DURATION`                              | [Interval](/pt-BR/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`                        |
| `INT64`                                 | [Interval](/pt-BR/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year)   | `INT64`                           |

Arrays podem ser aninhados e podem ter um valor do tipo `Nullable` como argumento. Os tipos `Tuple` e `Map` também podem ser aninhados.

O tipo `DICTIONARY` é compatível com consultas `INSERT`, e, para consultas `SELECT`, há uma configuração [`output_format_arrow_low_cardinality_as_dictionary`](/pt-BR/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) que permite gerar o tipo [LowCardinality](/pt-BR/sql-reference/data-types/lowcardinality.md) como um tipo `DICTIONARY`. Observe que pode haver valores não utilizados no dicionário `LowCardinality`, o que pode resultar em valores não utilizados no `DICTIONARY` do Arrow na saída.

Tipos de dados Arrow não compatíveis:

* `JSON`
* `ENUM`.

Os tipos de dados das colunas da tabela ClickHouse não precisam corresponder aos respectivos campos de dados do Arrow. Ao inserir dados, o ClickHouse interpreta os tipos de dados de acordo com a tabela acima e então [converte](/pt-BR/sql-reference/functions/type-conversion-functions#CAST) os dados para o tipo de dados definido para a coluna da tabela ClickHouse.

<div id="example-usage">
  ## Exemplo de uso
</div>

No exemplo abaixo, usamos o conjunto de dados `forex` disponível no
[playground SQL do ClickHouse](https://sql.clickhouse.com).

<div id="selecting-data">
  ### Selecionando dados
</div>

Selecionamos um dia de taxas de câmbio de `EUR/USD` no playground e salvamos
em um arquivo local `forex_eurusd.arrow`. Fazemos a consulta ao playground pela interface
HTTP, em que o host é `sql-clickhouse.clickhouse.com` e o usuário é
`demo` (sem senha):

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### Lendo o arquivo novamente
</div>

Agora podemos ler novamente o arquivo local em Arrow com
[`clickhouse-local`](/pt-BR/operations/utilities/clickhouse-local), usando a
função de tabela [`file`](/pt-BR/sql-reference/table-functions/file). O arquivo é
autodescritivo, portanto o formato `Arrow` infere o esquema automaticamente:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### Inserindo dados
</div>

Para carregar um arquivo Arrow em uma tabela do ClickHouse, envie-o por pipe para `clickhouse-client`
com `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                 | Descrição                                                                                                                                                                                                                                | Padrão      |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Permite colunas ausentes ao ler formatos de entrada Arrow                                                                                                                                                                                | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignora maiúsculas e minúsculas ao corresponder colunas Arrow a colunas CH.                                                                                                                                                               | `0`         |
| `input_format_arrow_import_nested`                                           | Configuração obsoleta, não faz nada.                                                                                                                                                                                                     | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Ignora colunas com tipos não suportados durante a inferência de esquema do formato Arrow                                                                                                                                                 | `0`         |
| `input_format_arrow_use_native_reader`                                       | Usa o leitor nativo do ClickHouse para os formatos `Arrow` e `ArrowStream` em vez da biblioteca Apache Arrow. Defina `0` para usar o leitor da biblioteca Apache Arrow.                                                                  | `1`         |
| `output_format_arrow_compression_method`                                     | Método de compressão para o formato de saída Arrow. Codecs compatíveis: lz4&#95;frame, zstd, none (não comprimido)                                                                                                                       | `lz4_frame` |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Usa o tipo Arrow FIXED&#95;SIZE&#95;BINARY em vez de Binary para colunas FixedString.                                                                                                                                                    | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Habilita a saída do tipo LowCardinality como tipo Dicionário do Arrow                                                                                                                                                                    | `0`         |
| `output_format_arrow_string_as_string`                                       | Usa o tipo Arrow String em vez de Binary para colunas String                                                                                                                                                                             | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | Gera como dados binários brutos um tipo sem equivalente em Arrow (por exemplo, `BFloat16`, `AggregateFunction`). Se false, esse tipo gera uma exceção. Aplica-se tanto ao gravador nativo quanto ao gravador da biblioteca Apache Arrow. | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Sempre usa inteiros de 64 bits para índices de dicionário no formato Arrow                                                                                                                                                               | `0`         |
| `output_format_arrow_use_native_writer`                                      | Usa o gravador nativo do ClickHouse para os formatos `Arrow` e `ArrowStream` em vez da biblioteca Apache Arrow. Defina `0` para usar o gravador da biblioteca Apache Arrow.                                                              | `1`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Usa inteiros com sinal para índices de dicionário no formato Arrow                                                                                                                                                                       | `1`         |