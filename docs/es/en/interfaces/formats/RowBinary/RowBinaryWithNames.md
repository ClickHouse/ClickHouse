---
description: 'Documentación del formato RowBinaryWithNames'
input_format: true
keywords: ['RowBinaryWithNames']
output_format: true
slug: /interfaces/formats/RowBinaryWithNames
title: 'RowBinaryWithNames'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Similar al formato [`RowBinary`](./RowBinary.md), pero con un encabezado adicional:

* Número de columnas (N) codificado en [`LEB128`](https://en.wikipedia.org/wiki/LEB128).
* N `String` que especifican los nombres de las columnas.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="format-settings">
  ## Configuración del formato
</div>

<RowBinaryFormatSettings />

:::note

* Si la configuración [`input_format_with_names_use_header`](/es/operations/settings/settings-formats.md/#input_format_with_names_use_header) está establecida en `1`,
  las columnas de los datos de entrada se asignarán a las columnas de la tabla según sus nombres; las columnas con nombres desconocidos se omitirán.
* Si la configuración [`input_format_skip_unknown_fields`](/es/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) está establecida en `1`.
  De lo contrario, se omitirá la primera fila.
  :::