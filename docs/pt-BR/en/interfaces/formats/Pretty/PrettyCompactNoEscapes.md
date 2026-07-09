---
alias: []
description: 'Documentação do formato PrettyCompactNoEscapes'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere do formato [`PrettyCompact`](./PrettyCompact.md) por não usar [sequências de escape ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
Isso é necessário para exibir o formato em um navegador, bem como para usar o utilitário de linha de comando `watch`.

<div id="example-usage">
  ## Exemplo de uso
</div>

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />