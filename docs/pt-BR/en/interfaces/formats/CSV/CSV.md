---
alias: []
description: 'Documentação sobre o formato CSV'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'reference'
---

<div id="description">
  ## Descrição
</div>

Formato de valores separados por vírgulas ([RFC](https://tools.ietf.org/html/rfc4180)).
Na formatação, as linhas são colocadas entre aspas duplas. Uma aspa dupla dentro de uma string é gerada como duas aspas duplas consecutivas.
Não há outras regras de escape de caracteres.

* Date e date-time são colocados entre aspas duplas.
* Números são gerados sem aspas.
* Os valores são separados por um caractere delimitador, que é `,` por padrão. O caractere delimitador é definido na configuração [format&#95;csv&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_csv_delimiter).
* As linhas são separadas usando a quebra de linha Unix (LF).
* Arrays são serializados em CSV da seguinte forma:
  * primeiro, o array é serializado como uma string, como no formato TabSeparated
  * A string resultante é gerada em CSV entre aspas duplas.
* Tuples no formato CSV são serializados como colunas separadas (ou seja, seu aninhamento na tuple é perdido).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
Por padrão, o delimitador é `,`
Consulte a configuração [format&#95;csv&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_csv_delimiter) para mais informações.
:::

Ao fazer o parsing, todos os valores podem ser interpretados com ou sem aspas. Há suporte a aspas simples e duplas.

As linhas também podem ser dispostas sem aspas. Nesse caso, elas são interpretadas até o caractere delimitador ou a quebra de linha (CR ou LF).
No entanto, em desacordo com a RFC, ao fazer o parsing de linhas sem aspas, os espaços e tabulações no início e no fim são ignorados.
Há suporte aos seguintes tipos de quebra de linha: Unix (LF), Windows (CR LF) e Mac OS Classic (CR LF).

`NULL` é formatado de acordo com a configuração [format&#95;csv&#95;null&#95;representation](/pt-BR/operations/settings/settings-formats.md/#format_csv_null_representation) (o valor padrão é `\N`).

Nos dados de entrada, os valores `ENUM` podem ser representados como nomes ou como IDs.
Primeiro, tentamos corresponder o valor de entrada ao nome do `ENUM`.
Se isso falhar e o valor de entrada for um número, tentamos corresponder esse número ao ID do `ENUM`.
Se os dados de entrada contiverem apenas IDs de `ENUM`, é recomendável habilitar a configuração [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) para otimizar o parsing de `ENUM`.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

| Configuração                                                                                                                                                                             | Descrição                                                                                                                                         | Padrão  | Notas                                                                                                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [format&#95;csv&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | o caractere considerado como delimitador em dados CSV.                                                                                            | `,`     |                                                                                                                                                                                                                    |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/pt-BR/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | permitir strings entre aspas simples.                                                                                                             | `true`  |                                                                                                                                                                                                                    |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/pt-BR/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | permitir strings entre aspas duplas.                                                                                                              | `true`  |                                                                                                                                                                                                                    |
| [format&#95;csv&#95;null&#95;representation](/pt-BR/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | representação personalizada de NULL no formato CSV.                                                                                               | `\N`    |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | tratar campos vazios na entrada CSV como valores padrão.                                                                                          | `true`  | Para expressões padrão complexas, [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/pt-BR/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) também deve estar habilitada. |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | tratar valores de enum inseridos em formatos CSV como índices de enum.                                                                            | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | usar alguns ajustes e heurísticas para fazer inferência de esquema no formato CSV. Se desabilitado, todos os campos serão inferidos como Strings. | `true`  |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | ao ler Array de CSV, esperar que seus elementos tenham sido serializados como CSV aninhado e então armazenados como string.                       | `false` |                                                                                                                                                                                                                    |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/pt-BR/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | se estiver definido como true, o fim de linha no formato de saída CSV será `\r\n` em vez de `\n`.                                                 | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | pular o número especificado de linhas no início dos dados.                                                                                        | `0`     |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;detect&#95;header](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | detectar automaticamente o cabeçalho com nomes e tipos no formato CSV.                                                                            | `true`  |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | pular linhas vazias no final dos dados.                                                                                                           | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | remover espaços e tabulações em strings CSV sem aspas.                                                                                            | `true`  |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | permitir o uso de espaço em branco ou tabulação como delimitador de campo em strings CSV.                                                         | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | permitir um número variável de colunas no formato CSV, ignorar colunas extras e usar valores padrão para colunas ausentes.                        | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | permitir definir um valor padrão para a coluna quando a desserialização do campo CSV falhar devido a um valor inválido.                           | `false` |                                                                                                                                                                                                                    |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/pt-BR/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | tentar inferir números a partir de campos do tipo string durante a inferência de esquema.                                                         | `false` |                                                                                                                                                                                                                    |