---
alias: []
description: 'Documentación sobre el formato PrettySpaceMonoBlock'
input_format: false
keywords: ['PrettySpaceMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceMonoBlock
title: 'PrettySpaceMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettySpace`](./PrettySpace.md) en que se almacenan en un búfer hasta `10,000` filas
y luego se muestran en una sola tabla, en lugar de por [bloques](/es/development/architecture#block).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración de formato
</div>

<PrettyFormatSettings />