---
alias: []
description: 'Documentação sobre o formato PrettyCompact'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere do formato [`Pretty`](./Pretty.md) por exibir a tabela com uma grade entre as linhas.
Por isso, o resultado é mais compacto.

:::note
Esse formato é usado por padrão no cliente de linha de comando no modo interativo.
:::

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />