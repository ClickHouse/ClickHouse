---
alias: []
description: 'Documentación sobre el formato Npy'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `Npy` está diseñado para cargar un array de NumPy desde un archivo `.npy` en ClickHouse.
El formato de archivo de NumPy es un formato binario que se utiliza para almacenar arrays de datos numéricos de forma eficiente.
Durante la importación, ClickHouse trata la dimensión de nivel superior como un array de filas con una sola columna.

La siguiente tabla muestra los tipos de datos Npy compatibles y su tipo correspondiente en ClickHouse:

<div id="data_types-matching">
  ## Equivalencia entre tipos de datos
</div>

| Tipo de dato de Npy (`INSERT`) | Tipo de dato de ClickHouse                              | Tipo de dato de Npy (`SELECT`) |
| ------------------------------ | ------------------------------------------------------- | ------------------------------ |
| `i1`                           | [Int8](/es/sql-reference/data-types/int-uint.md)           | `i1`                           |
| `i2`                           | [Int16](/es/sql-reference/data-types/int-uint.md)          | `i2`                           |
| `i4`                           | [Int32](/es/sql-reference/data-types/int-uint.md)          | `i4`                           |
| `i8`                           | [Int64](/es/sql-reference/data-types/int-uint.md)          | `i8`                           |
| `u1`, `b1`                     | [UInt8](/es/sql-reference/data-types/int-uint.md)          | `u1`                           |
| `u2`                           | [UInt16](/es/sql-reference/data-types/int-uint.md)         | `u2`                           |
| `u4`                           | [UInt32](/es/sql-reference/data-types/int-uint.md)         | `u4`                           |
| `u8`                           | [UInt64](/es/sql-reference/data-types/int-uint.md)         | `u8`                           |
| `f2`, `f4`                     | [Float32](/es/sql-reference/data-types/float.md)           | `f4`                           |
| `f8`                           | [Float64](/es/sql-reference/data-types/float.md)           | `f8`                           |
| `S`, `U`                       | [String](/es/sql-reference/data-types/string.md)           | `S`                            |
|                                | [FixedString](/es/sql-reference/data-types/fixedstring.md) | `S`                            |

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### Guardar un array en formato .npy con Python
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### Leer un archivo NumPy en ClickHouse
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### Selección de datos
</div>

Puede seleccionar datos de una tabla de ClickHouse y guardarlos en un archivo en formato Npy mediante el siguiente comando con clickhouse-client:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## Configuración de formato
</div>
