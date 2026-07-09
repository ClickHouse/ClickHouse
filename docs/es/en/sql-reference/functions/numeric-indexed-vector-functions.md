---
description: 'Documentación de NumericIndexedVector y sus funciones'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'Funciones de NumericIndexedVector'
doc_type: 'reference'
---

NumericIndexedVector es una estructura de datos abstracta que encapsula un vector e implementa operaciones de agregación de vectores y operaciones punto a punto. Bit-Sliced Index es su método de almacenamiento. Para conocer los fundamentos teóricos y los casos de uso, consulte el artículo [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

<div id="bit-sliced-index">
  ## BSI
</div>

En el método de almacenamiento BSI (Bit-Sliced Index), los datos se almacenan en [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) y luego se comprimen con [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap). Las operaciones de agregación y las operaciones punto a punto se realizan directamente sobre los datos comprimidos, lo que puede mejorar significativamente la eficiencia del almacenamiento y de la consulta.

Un vector contiene índices y sus valores correspondientes. A continuación, se muestran algunas características y restricciones de esta estructura de datos en el modo de almacenamiento BSI:

* El tipo de índice puede ser `UInt8`, `UInt16` o `UInt32`. **Nota:** Teniendo en cuenta el rendimiento de la implementación de 64 bits de Roaring Bitmap, el formato BSI no admite `UInt64`/`Int64`.
* El tipo de valor puede ser `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32` o `Float64`. **Nota:** El tipo de valor no se amplía automáticamente. Por ejemplo, si usas `UInt8` como tipo de valor, cualquier suma que supere la capacidad de `UInt8` producirá un desbordamiento en lugar de promoverse a un tipo superior; de forma similar, las operaciones con enteros producirán resultados enteros (por ejemplo, la división no se convertirá automáticamente en un resultado de coma flotante). Por lo tanto, es importante planificar y diseñar el tipo de valor con antelación. En escenarios reales, se suelen usar tipos de coma flotante (`Float32`/`Float64`).
* Solo pueden realizar operaciones dos vectores con el mismo tipo de índice y el mismo tipo de valor.
* El almacenamiento subyacente usa Bit-Sliced Index, y bitmap almacena los índices. Roaring Bitmap se utiliza como implementación específica de bitmap. Una práctica recomendada es concentrar el índice en varios contenedores de Roaring Bitmap en la mayor medida posible para maximizar la compresión y el rendimiento de la consulta.
* El mecanismo Bit-Sliced Index convierte el valor en binario. Para los tipos de coma flotante, la conversión usa una representación de punto fijo, lo que puede provocar pérdida de precisión. La precisión puede ajustarse personalizando el número de bits usados para la parte fraccionaria; el valor predeterminado es 24 bits, lo cual es suficiente para la mayoría de los casos. Puedes personalizar el número de bits enteros y fraccionarios al construir NumericIndexedVector usando la función de agregación groupNumericIndexedVector con `-State`.
* Hay tres casos para los índices: valor distinto de cero, valor cero e inexistente. En NumericIndexedVector, solo se almacenan los valores distintos de cero y los valores cero. Además, en las operaciones punto a punto entre dos NumericIndexedVectors, el valor de un índice inexistente se tratará como 0. En el caso de la división, el resultado es cero cuando el divisor es cero.

<div id="create-numeric-indexed-vector-object">
  ## Crear un objeto numericIndexedVector
</div>

Hay dos formas de crear esta estructura: una es usar la función de agregación `groupNumericIndexedVector` con `-State`.
Puede añadir el sufijo `-if` para que acepte una condición adicional.
La función de agregación solo procesará las filas que cumplan la condición.
La otra es construirlo a partir de un map usando `numericIndexedVectorBuild`.
La función `groupNumericIndexedVectorState` permite personalizar la cantidad de bits enteros y fraccionarios mediante parámetros, mientras que `numericIndexedVectorBuild` no lo permite.

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

Construye un NumericIndexedVector a partir de dos columnas de datos y devuelve la suma de todos los valores de tipo `Float64`. Si se añade el sufijo `State`, devuelve un objeto NumericIndexedVector.

**Sintaxis**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Parámetros**

* `type`: String, opcional. Especifica el formato de almacenamiento. Actualmente, solo se admite `'BSI'`.
* `integer_bit_num`: `UInt32`, opcional. Solo tiene efecto con el formato de almacenamiento `'BSI'`; este parámetro indica la cantidad de bits usada para la parte entera. Cuando el tipo de índice es un tipo entero, el valor predeterminado corresponde a la cantidad de bits usada para almacenar el índice. Por ejemplo, si el tipo de índice es UInt16, el valor predeterminado de `integer_bit_num` es 16. Para los tipos de índice Float32 y Float64, el valor predeterminado de integer&#95;bit&#95;num es 40, por lo que la parte entera de los datos que puede representarse está en el rango `[-2^39, 2^39 - 1]`. El rango válido es `[0, 64]`.
* `fraction_bit_num`: `UInt32`, opcional. Solo tiene efecto con el formato de almacenamiento `'BSI'`; este parámetro indica la cantidad de bits usada para la parte fraccionaria. Cuando el tipo de valor es entero, el valor predeterminado es 0; cuando el tipo de valor es Float32 o Float64, el valor predeterminado es 24. El rango válido es `[0, 24]`.
* También existe la restricción de que el rango válido de integer&#95;bit&#95;num + fraction&#95;bit&#95;num es [0, 64].
* `col1`: La columna de índice. Tipos admitidos: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
* `col2`: La columna de valor. Tipos admitidos: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**Valor de retorno**

Un valor `Float64` que representa la suma de todos los valores.

**Ejemplo**

Datos de prueba:

```text
UserID  PlayTime
1       10
2       20
3       30
```

Consulta &amp; Resultado:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
La siguiente documentación se genera a partir de la tabla del sistema `system.functions`.
:::

{/* 
  las etiquetas que aparecen a continuación se usan para generar la documentación a partir de las tablas del sistema y no deben eliminarse.
  Para más detalles, consulta https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }