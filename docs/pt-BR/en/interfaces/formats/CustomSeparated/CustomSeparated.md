---
alias: []
description: 'Documentação sobre o formato CustomSeparated'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
doc_type: 'referência'
---

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Semelhante a [Template](../Template/Template.md), mas imprime ou lê todos os nomes e tipos das colunas e usa a regra de escape da configuração [format&#95;custom&#95;escaping&#95;rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule), além dos delimitadores das seguintes configurações:

* [format&#95;custom&#95;field&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_field_delimiter)
* [format&#95;custom&#95;row&#95;before&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
* [format&#95;custom&#95;row&#95;after&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
* [format&#95;custom&#95;row&#95;between&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
* [format&#95;custom&#95;result&#95;before&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
* [format&#95;custom&#95;result&#95;after&#95;delimiter](/pt-BR/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)

:::note
Não usa as configurações de regras de escape nem os delimitadores das strings de formato.
:::

Também existe o formato [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md), que é semelhante ao [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="inserting-data">
  ### Inserindo dados
</div>

Usando o arquivo txt a seguir, chamado `football.txt`:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

Configure a configuração personalizada de delimitador:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insira os dados:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

<div id="reading-data">
  ### Leitura de dados
</div>

Configure a configuração personalizada de delimitador:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Leia dados usando o formato `CustomSeparated`:

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

A saída estará no formato personalizado configurado:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## Configurações de formato
</div>

Configurações adicionais:

| Configuração                                                                                                                                                                               | Descrição                                                                                                                                 | Padrão  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [input&#95;format&#95;custom&#95;detect&#95;header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                                   | ativa a detecção automática de cabeçalhos com nomes e tipos, se houver.                                                                   | `true`  |
| [input&#95;format&#95;custom&#95;skip&#95;trailing&#95;empty&#95;lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)                   | ignora linhas vazias no final do arquivo.                                                                                                 | `false` |
| [input&#95;format&#95;custom&#95;allow&#95;variable&#95;number&#95;of&#95;columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | permite um número variável de colunas no formato CustomSeparated, ignorando colunas extras e usando valores padrão para colunas ausentes. | `false` |