---
alias: []
description: 'Documentação sobre o formato PrettySpaceMonoBlock'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere do formato [`PrettySpace`](./PrettySpace.md) porque até `10,000` linhas são armazenadas em buffer
e depois exibidas como uma única tabela, e não por [blocos](/pt-BR/development/architecture#block).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />