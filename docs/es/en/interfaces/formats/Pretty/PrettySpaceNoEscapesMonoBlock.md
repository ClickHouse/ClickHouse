---
alias: []
description: 'Documentación sobre el formato PrettySpaceNoEscapesMonoBlock'
input_format: false
keywords: ['PrettySpaceNoEscapesMonoBlock']
output_format: true
slug: /interfaces/formats/PrettySpaceNoEscapesMonoBlock
title: 'PrettySpaceNoEscapesMonoBlock'
doc_type: 'referencia'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md) en que se almacenan en búfer hasta `10,000` filas,
que luego se muestran como una sola tabla, en lugar de por [bloques](/es/development/architecture#block).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración de formatos
</div>

<PrettyFormatSettings />