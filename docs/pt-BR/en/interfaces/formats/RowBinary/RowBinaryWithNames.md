---
description: 'Documentação do formato RowBinaryWithNames'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✔       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Semelhante ao formato [`RowBinary`](./RowBinary.md), mas com um cabeçalho adicional:

* Número de colunas (N) codificado em [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* N `String`s que especificam os nomes das colunas.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<RowBinaryFormatSettings />

:::note

* Se a configuração [`input_format_with_names_use_header`](/pt-BR/operations/settings/settings-formats.md/#input_format_with_names_use_header) estiver definida como `1`, as colunas dos dados de entrada serão mapeadas para as colunas da tabela pelos respectivos nomes; colunas com nomes desconhecidos serão ignoradas.
* Se a configuração [`input_format_skip_unknown_fields`](/pt-BR/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) estiver definida como `1`.
  Caso contrário, a primeira linha será ignorada.
  :::