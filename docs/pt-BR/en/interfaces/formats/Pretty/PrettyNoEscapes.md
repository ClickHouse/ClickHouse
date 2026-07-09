---
alias: []
description: 'Documentação do formato PrettyNoEscapes'
input_format: false
keywords: ['PrettyNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyNoEscapes
title: 'PrettyNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Saída | Alias |
| ------- | ----- | ----- |
| ✗       | ✔     |       |

<div id="description">
  ## Descrição
</div>

Difere de [Pretty](/pt-BR/interfaces/formats/Pretty) por não usar [sequências de escape ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
Isso é necessário para exibir o formato em um navegador, bem como para usar o utilitário de linha de comando &#39;watch&#39;.

<div id="example-usage">
  ## Exemplo de uso
</div>

Exemplo:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
A [interface HTTP](/pt-BR/interfaces/http) pode ser usada para exibir esse formato no navegador.
:::

<div id="format-settings">
  ## Configurações de formato
</div>

<PrettyFormatSettings />