---
alias: []
description: 'Documentação sobre o formato RowBinaryWithNamesAndTypes'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Semelhante ao formato [RowBinary](./RowBinary.md), mas com o cabeçalho adicional:

* número de colunas (N) codificado em [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* N `String`s que especificam os nomes das colunas.
* N `String`s que especificam os tipos das colunas.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<RowBinaryFormatSettings />

:::note
Se a configuração [`input_format_with_names_use_header`](/pt-BR/operations/settings/settings-formats.md/#input_format_with_names_use_header) estiver definida como 1,
as colunas dos dados de entrada serão mapeadas para as colunas da tabela pelos respectivos nomes, e as colunas com nomes desconhecidos serão ignoradas se a configuração [input&#95;format&#95;skip&#95;unknown&#95;fields](/pt-BR/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) estiver definida como 1.
Caso contrário, a primeira linha será ignorada.
Se a configuração [`input_format_with_types_use_header`](/pt-BR/operations/settings/settings-formats.md/#input_format_with_types_use_header) estiver definida como `1`,
os tipos dos dados de entrada serão comparados com os tipos das colunas correspondentes da tabela. Caso contrário, a segunda linha será ignorada.
:::