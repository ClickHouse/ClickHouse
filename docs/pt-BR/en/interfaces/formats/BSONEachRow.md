---
alias: []
description: 'Documentação do formato BSONEachRow'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'referência'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

O formato `BSONEachRow` analisa os dados como uma sequência de documentos Binary JSON (BSON), sem nenhum separador entre eles.
Cada linha é formatada como um único documento, e cada coluna como um único campo de documento BSON, com o nome da coluna como chave.

<div id="data-types-matching">
  ## Correspondência entre tipos de dados
</div>

Para a saída, é usada a seguinte correspondência entre os tipos do ClickHouse e os tipos BSON:

| Tipo do ClickHouse                                                                                    | Tipo BSON                                                                                                                                         |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Bool](/pt-BR/sql-reference/data-types/boolean.md)                                                          | `\x08` boolean                                                                                                                                    |
| [Int8/UInt8](/pt-BR/sql-reference/data-types/int-uint.md)/[Enum8](/pt-BR/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                                      |
| [Int16/UInt16](/pt-BR/sql-reference/data-types/int-uint.md)/[Enum16](/pt-BR/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                                      |
| [Int32](/pt-BR/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                                      |
| [UInt32](/pt-BR/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                                      |
| [Int64/UInt64](/pt-BR/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                                      |
| [Float32/Float64](/pt-BR/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                                     |
| [Date](/pt-BR/sql-reference/data-types/date.md)/[Date32](/pt-BR/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                                      |
| [DateTime](/pt-BR/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                                      |
| [DateTime64](/pt-BR/sql-reference/data-types/datetime64.md)                                                 | `\x09` datetime                                                                                                                                   |
| [Decimal32](/pt-BR/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                                      |
| [Decimal64](/pt-BR/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                                      |
| [Decimal128](/pt-BR/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` subtipo binário, tamanho = 16                                                                                               |
| [Decimal256](/pt-BR/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` subtipo binário, tamanho = 32                                                                                               |
| [Int128/UInt128](/pt-BR/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` subtipo binário, tamanho = 16                                                                                               |
| [Int256/UInt256](/pt-BR/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` subtipo binário, tamanho = 32                                                                                               |
| [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` subtipo binário ou \x02 string se a configuração output&#95;format&#95;bson&#95;string&#95;as&#95;string estiver habilitada |
| [UUID](/pt-BR/sql-reference/data-types/uuid.md)                                                             | `\x05` binary, `\x04` subtipo uuid, tamanho = 16                                                                                                  |
| [Array](/pt-BR/sql-reference/data-types/array.md)                                                           | `\x04` array                                                                                                                                      |
| [Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                           | `\x04` array                                                                                                                                      |
| [Named Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                     | `\x03` documento                                                                                                                                  |
| [Map](/pt-BR/sql-reference/data-types/map.md)                                                               | `\x03` documento                                                                                                                                  |
| [IPv4](/pt-BR/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                                      |
| [IPv6](/pt-BR/sql-reference/data-types/ipv6.md)                                                             | `\x05` binary, `\x00` subtipo binário                                                                                                             |

Para a entrada, é usada a seguinte correspondência entre os tipos BSON e os tipos do ClickHouse:

| Tipo BSON                                    | Tipo ClickHouse                                                                                                                                                                                     |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                                | [Float32/Float64](/pt-BR/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` string                                | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` documento                             | [Map](/pt-BR/sql-reference/data-types/map.md)/[Named Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` array                                 | [Array](/pt-BR/sql-reference/data-types/array.md)/[Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` binary, `\x00` subtipo binário        | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)/[IPv6](/pt-BR/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` binary, `\x02` subtipo binário antigo | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` binary, `\x03` subtipo uuid antigo    | [UUID](/pt-BR/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` binary, `\x04` subtipo uuid           | [UUID](/pt-BR/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                              | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` boolean                               | [Bool](/pt-BR/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` datetime                              | [DateTime64](/pt-BR/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` valor NULL                            | [NULL](/pt-BR/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` código JavaScript                     | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` símbolo                               | [String](/pt-BR/sql-reference/data-types/string.md)/[FixedString](/pt-BR/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                                 | [Int32/UInt32](/pt-BR/sql-reference/data-types/int-uint.md)/[Decimal32](/pt-BR/sql-reference/data-types/decimal.md)/[IPv4](/pt-BR/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/pt-BR/sql-reference/data-types/enum.md) |
| `\x12` int64                                 | [Int64/UInt64](/pt-BR/sql-reference/data-types/int-uint.md)/[Decimal64](/pt-BR/sql-reference/data-types/decimal.md)/[DateTime64](/pt-BR/sql-reference/data-types/datetime64.md)                                       |

Outros tipos BSON não são suportados. Além disso, o formato realiza conversão entre diferentes tipos inteiros.
Por exemplo, é possível inserir um valor BSON `int32` no ClickHouse como [`UInt8`](../../sql-reference/data-types/int-uint.md).

Inteiros grandes e decimais, como `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256`, podem ser analisados a partir de um valor BSON Binary com o subtipo binário `\x00`.
Nesse caso, o formato validará se o tamanho dos dados binários é igual ao tamanho do valor esperado.

:::note
Este formato não funciona corretamente em plataformas big-endian.
:::

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="inserting-data">
  ### Inserindo dados
</div>

Use um arquivo BSON com os dados a seguir, chamado `football.bson`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insira os dados:

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### Leitura de dados
</div>

Leia os dados usando o formato `BSONEachRow`:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON é um formato binário que não é exibido de forma legível no terminal. Use `INTO OUTFILE` para gerar arquivos BSON.
:::

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                                                                                                                                          | Descrição                                                                                                | Padrão  |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Usa o tipo BSON String em vez de Binary para colunas do tipo String.                                     | `false` |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Permite ignorar colunas com tipos não suportados durante a inferência de esquema do formato BSONEachRow. | `false` |