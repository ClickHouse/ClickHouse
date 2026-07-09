---
alias: []
description: 'Documentación sobre el formato PrettySpaceNoEscapes'
input_format: false
keywords: ['PrettySpaceNoEscapes']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapes
title: 'PrettySpaceNoEscapes'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettySpace`](./PrettySpace.md) en que no utiliza [secuencias de escape ANSI](http://en.wikipedia.org/wiki/ANSI_escape_code).
Esto es necesario para mostrar este formato en un navegador, así como para utilizar la utilidad de línea de comandos &#39;watch&#39;.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración de formato
</div>

<PrettyFormatSettings />