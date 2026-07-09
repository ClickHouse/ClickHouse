---
alias: []
description: 'Documentación sobre el formato PrettyCompactNoEscapesMonoBlock'
input_format: false
keywords: ['PrettyCompactNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyCompactNoEscapesMonoBlock
title: 'PrettyCompactNoEscapesMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) en que se almacenan en búfer hasta `10,000` filas
y luego se muestran como una sola tabla, en lugar de por [bloques](/es/development/architecture#block).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Ajustes de formato
</div>

<PrettyFormatSettings />