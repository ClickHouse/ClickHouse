---
alias: []
description: 'Documentación sobre el formato PrettyMonoBlock'
input_format: false
keywords: ['PrettyMonoBlock']
output_format: true
slug: /interfaces/formats/PrettyMonoBlock
title: 'PrettyMonoBlock'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`Pretty`](/es/interfaces/formats/Pretty) en que almacena en búfer hasta `10,000` filas
y luego las muestra como una sola tabla, en lugar de por [bloques](/es/development/architecture#block).

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración de formato
</div>

<PrettyFormatSettings />