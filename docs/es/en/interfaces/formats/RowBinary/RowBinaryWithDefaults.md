---
alias: []
description: 'Documentación sobre el formato RowBinaryWithDefaults'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

Similar al formato [`RowBinary`](./RowBinary.md), pero con un byte adicional antes de cada columna que indica si debe usarse el valor predeterminado.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Ejemplos:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```

```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

* Para la columna `x` solo hay un byte `01` que indica que se debe usar el valor predeterminado y que no se proporciona ningún otro dato después de ese byte.
* Para la columna `y`, los datos comienzan con el byte `00`, que indica que la columna tiene un valor real que debe leerse de los datos siguientes `01000000`.

<div id="format-settings">
  ## Ajustes de formato
</div>

<RowBinaryFormatSettings />