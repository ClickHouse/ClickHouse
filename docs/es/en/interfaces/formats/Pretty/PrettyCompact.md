---
alias: []
description: 'Documentación sobre el formato PrettyCompact'
input_format: false
keywords: ['PrettyCompact']
output_format: true
slug: /interfaces/formats/PrettyCompact
title: 'PrettyCompact'
doc_type: 'referencia'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Se diferencia del formato [`Pretty`](./Pretty.md) en que la tabla se muestra con una cuadrícula entre las filas.
Por eso, el resultado es más compacto.

:::note
Este formato se usa de forma predeterminada en el cliente de línea de comandos en modo interactivo.
:::

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración del formato
</div>

<PrettyFormatSettings />