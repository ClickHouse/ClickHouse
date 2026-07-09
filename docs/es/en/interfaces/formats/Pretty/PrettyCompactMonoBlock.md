---
alias: []
description: 'Documentación del formato PrettyCompactMonoBlock'
input_format: false
keywords: ['PrettyCompactMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactMonoBlock
title: 'PrettyCompactMonoBlock'
doc_type: 'referencia'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettyCompact`](./PrettyCompact.md) en que se almacenan en un búfer hasta `10,000` filas
y luego se muestran como una sola tabla, y no por [bloques](/es/development/architecture#block).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración del formato
</div>

<PrettyFormatSettings />