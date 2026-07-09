---
alias: []
description: 'Documentación sobre el formato One'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

El formato `One` es un formato de entrada especial que no lee ningún dato de un archivo y devuelve una sola fila con una columna de tipo [`UInt8`](../../sql-reference/data-types/int-uint.md), llamada `dummy` y con el valor `0` (como la tabla `system.one`).
Puede usarse con las columnas virtuales `_file/_path` para enumerar todos los archivos sin leer los datos reales.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Ejemplo:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## Ajustes de formato
</div>
