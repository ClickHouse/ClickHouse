---
alias: []
description: 'Documentação do formato PrettyCompactMonoBlock'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere do formato [`PrettyCompact`](./PrettyCompact.md) porque até `10,000` linhas são mantidas em buffer
e então exibidas como uma única tabela, e não por [blocos](/pt-BR/development/architecture#block).

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />