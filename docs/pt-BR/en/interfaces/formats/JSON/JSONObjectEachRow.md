---
alias: []
description: 'Documentação do formato JSONObjectEachRow'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Neste formato, todos os dados são representados como um único objeto JSON, com cada linha representada como um campo separado desse objeto, de forma semelhante ao formato [`JSONEachRow`](./JSONEachRow.md).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="basic-example">
  ### Exemplo básico
</div>

Dado o seguinte JSON:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

Para usar um nome de objeto como valor de uma coluna, você pode usar a configuração especial [`format_json_object_each_row_column_for_object_name`](/pt-BR/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name).
O valor dessa configuração é definido como o nome de uma coluna, que é usada como chave JSON de uma linha no objeto resultante.

<div id="output">
  #### Saída
</div>

Digamos que temos a tabela `test` com duas colunas:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Vamos gerar a saída no formato `JSONObjectEachRow` e usar a configuração `format_json_object_each_row_column_for_object_name`:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### Entrada
</div>

Digamos que armazenamos a saída do exemplo anterior em um arquivo chamado `data.json`:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Também funciona para inferência de esquema:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### Inserção de dados
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

O ClickHouse permite:

* Os pares chave-valor em qualquer ordem no objeto.
* Omitir alguns valores.

O ClickHouse ignora espaços entre os elementos e vírgulas após os objetos. Você pode passar todos os objetos em uma única linha. Não é necessário separá-los com quebras de linha.

<div id="omitted-values-processing">
  #### Processamento de valores omitidos
</div>

O ClickHouse substitui os valores omitidos pelos valores padrão dos [tipos de dados](/pt-BR/sql-reference/data-types/index.md) correspondentes.

Se `DEFAULT expr` for especificado, o ClickHouse usará regras de substituição diferentes, dependendo da configuração [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/pt-BR/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields).

Considere a tabela a seguir:

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* Se `input_format_defaults_for_omitted_fields = 0`, então o valor padrão de `x` e `a` é `0` (assim como o valor padrão do tipo de dados `UInt32`).
* Se `input_format_defaults_for_omitted_fields = 1`, então o valor padrão de `x` é `0`, mas o valor padrão de `a` é `x * 2`.

:::note
Ao inserir dados com `input_format_defaults_for_omitted_fields = 1`, o ClickHouse consome mais recursos computacionais do que na inserção com `input_format_defaults_for_omitted_fields = 0`.
:::

<div id="json-selecting-data">
  ### Selecionando dados
</div>

Considere a tabela `UserActivity` como exemplo:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

A consulta `SELECT * FROM UserActivity FORMAT JSONEachRow` retorna:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Ao contrário do formato [JSON](/pt-BR/interfaces/formats/JSON), não há substituição de sequências UTF-8 inválidas. Os valores são escapados da mesma forma que no `JSON`.

:::info
As strings podem conter qualquer sequência de bytes. Use o formato [`JSONEachRow`](./JSONEachRow.md) se tiver certeza de que os dados na tabela podem ser formatados como JSON sem perda de informação.
:::

<div id="jsoneachrow-nested">
  ### Uso de estruturas do tipo Nested
</div>

Se você tiver uma tabela com colunas do tipo de dado [`Nested`](/pt-BR/sql-reference/data-types/nested-data-structures/index.md), poderá inserir dados JSON com a mesma estrutura. Habilite esse recurso com a configuração [input&#95;format&#95;import&#95;nested&#95;json](/pt-BR/operations/settings/settings-formats.md/#input_format_import_nested_json).

Por exemplo, considere a seguinte tabela:

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Como você pode ver na descrição do tipo de dado `Nested`, o ClickHouse trata cada componente da estrutura aninhada como uma coluna separada (`n.s` e `n.i` na nossa tabela). Você pode inserir os dados da seguinte forma:

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Para inserir dados como um objeto JSON hierárquico, defina [`input_format_import_nested_json=1`](/pt-BR/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Sem essa configuração, o ClickHouse lança uma exceção.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                                                                                                                 | Descrição                                                                                                                                                                        | Padrão   | Notas                                                                                                                                                                                                |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`input_format_import_nested_json`](/pt-BR/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | mapeia dados JSON aninhados para tabelas aninhadas (funciona para o formato JSONEachRow).                                                                                        | `false`  |                                                                                                                                                                                                      |
| [`input_format_json_read_bools_as_numbers`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | permite interpretar Bool como números em formatos de entrada JSON.                                                                                                               | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_read_bools_as_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | permite interpretar valores Bool como Strings em formatos de entrada JSON.                                                                                                       | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_read_numbers_as_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | permite interpretar números como Strings em formatos de entrada JSON.                                                                                                            | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_read_arrays_as_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | permite interpretar arrays JSON como Strings em formatos de entrada JSON.                                                                                                        | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_read_objects_as_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | permite interpretar objetos JSON como strings em formatos de entrada JSON.                                                                                                       | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_named_tuples_as_objects`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | interpreta colunas de tupla nomeada como objetos JSON.                                                                                                                           | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_try_infer_numbers_from_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | tenta inferir números a partir de campo do tipo string durante a inferência de esquema.                                                                                          | `false`  |                                                                                                                                                                                                      |
| [`input_format_json_try_infer_named_tuples_from_objects`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | tentar inferir uma tupla nomeada a partir de objetos JSON durante a inferência de esquema.                                                                                       | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_infer_incomplete_types_as_strings`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | usar o tipo String para chaves que contêm apenas NULLs ou objetos/arrays vazios durante a inferência de esquema em formatos de entrada JSON.                                     | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | inserir valores padrão para elementos ausentes em um objeto JSON durante a análise de uma tupla nomeada.                                                                         | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | ignorar chaves desconhecidas em objeto JSON para Tuples nomeadas.                                                                                                                | `false`  |                                                                                                                                                                                                      |
| [`input_format_json_compact_allow_variable_number_of_columns`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | permitir número variável de colunas no formato JSONCompact/JSONCompactEachRow, ignorar colunas extras e usar valores padrão nas colunas ausentes.                                | `false`  |                                                                                                                                                                                                      |
| [`input_format_json_throw_on_bad_escape_sequence`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | lançar uma exceção se a string JSON contiver uma sequência de escape inválida. Se desabilitado, as sequências de escape inválidas permanecerão inalteradas nos dados.            | `true`   |                                                                                                                                                                                                      |
| [`input_format_json_empty_as_default`](/pt-BR/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | trata campos vazios na entrada JSON como valores padrão.                                                                                                                         | `false`. | Para expressões `default` complexas, também é necessário habilitar [`input_format_defaults_for_omitted_fields`](/pt-BR/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [`output_format_json_quote_64bit_integers`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | controla o uso de aspas em inteiros de 64 bits no formato de saída JSON.                                                                                                         | `true`   |                                                                                                                                                                                                      |
| [`output_format_json_quote_64bit_floats`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | controla o uso de aspas em números de ponto flutuante de 64 bits no formato de saída JSON.                                                                                       | `false`  |                                                                                                                                                                                                      |
| [`output_format_json_quote_denormals`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | habilita saídas &#39;+nan&#39;, &#39;-nan&#39;, &#39;+inf&#39;, &#39;-inf&#39; no formato de saída JSON.                                                                         | `false`  |                                                                                                                                                                                                      |
| [`output_format_json_quote_decimals`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | controla o uso de aspas em decimais no formato de saída JSON.                                                                                                                    | `false`  |                                                                                                                                                                                                      |
| [`output_format_json_escape_forward_slashes`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | controla o escaping de barras no sentido direto em saídas de string no formato de saída JSON.                                                                                    | `true`   |                                                                                                                                                                                                      |
| [`output_format_json_named_tuples_as_objects`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | serializa colunas do tipo tupla nomeada como objetos JSON.                                                                                                                       | `true`   |                                                                                                                                                                                                      |
| [`output_format_json_array_of_rows`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | gera um array JSON com todas as linhas no formato JSONEachRow(Compact).                                                                                                          | `false`  |                                                                                                                                                                                                      |
| [`output_format_json_validate_utf8`](/pt-BR/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | habilita a validação de sequências UTF-8 nos formatos de saída JSON (observe que isso não afeta os formatos JSON/JSONCompact/JSONColumnsWithMetadata, que sempre validam UTF-8). | `false`  |                                                                                                                                                                                                      |