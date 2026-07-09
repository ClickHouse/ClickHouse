---
alias: []
description: 'Documentación sobre el formato PrettyCompactNoEscapes'
input_format: false
keywords: ['PrettyCompactNoEscapes']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapes
title: 'PrettyCompactNoEscapes'
doc_type: 'referencia'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettyCompact`](./PrettyCompact.md) en que no utiliza [secuencias de escape ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
Esto es necesario para mostrar el formato en un navegador, así como para utilizar la utilidad de línea de comandos &#39;watch&#39;.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Ajustes de formato
</div>

<PrettyFormatSettings />