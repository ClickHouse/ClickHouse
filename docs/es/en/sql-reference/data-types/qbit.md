---
description: 'Documentación del tipo de dato QBit en ClickHouse, que permite una cuantización de grano fino para la búsqueda vectorial aproximada'
keywords: ['qbit', 'tipo de dato']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'Tipo de dato QBit'
doc_type: 'reference'
---

El tipo de dato `QBit` reorganiza el almacenamiento de vectores para acelerar las búsquedas aproximadas. En lugar de almacenar juntos los elementos de cada vector, agrupa las mismas posiciones de bits en todos los vectores.
Esto almacena los vectores con precisión completa y, al mismo tiempo, te permite elegir el nivel de cuantización de grano fino en el momento de la búsqueda: leer menos bits para reducir la E/S y acelerar los cálculos, o más bits para obtener una mayor exactitud. Obtienes las ventajas de velocidad que aporta la cuantización al reducir la transferencia de datos y el cálculo, pero todos los datos originales siguen estando disponibles cuando se necesitan.

Para declarar una columna de tipo `QBit`, utiliza la siguiente sintaxis:

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – el tipo de cada elemento del vector. Los tipos permitidos son `Int8`, `BFloat16`, `Float32` y `Float64`
* `dimension` – el número de elementos de cada vector
* `stride` – opcional. El número de dimensiones almacenadas juntas en un grupo de flujos. Si se omite, el valor predeterminado es `dimension` (un solo grupo). Si se especifica, `dimension` debe ser un múltiplo de `stride` y, cuando `stride` es menor que `dimension`, `stride` debe ser un múltiplo de 8. Consulta [Strides](#strides).

<div id="creating-qbit">
  ## Crear QBit
</div>

Uso del tipo `QBit` en la definición de una columna de tabla:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## Conversión de arrays a QBit
</div>

Los arrays se convierten en `QBit` cuando la longitud del array coincide con la dimensión de `QBit`. No es necesario que el tipo de elemento del array coincida con el tipo de elemento de `QBit`. Cualquier tipo de elemento numérico se convierte automáticamente. Esto le permite mover una columna existente de embeddings directamente a una columna `QBit`:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

La conversión también puede hacerse explícitamente con `CAST`, por ejemplo `CAST(embedding AS QBit(Float32, 8))`.

<div id="converting-qbit-to-arrays">
  ## Conversión de QBit a arrays
</div>

La conversión inversa reconstruye el vector original a partir de la representación transpuesta por bits, de modo que al convertir un `QBit` en un `Array` se devuelven los valores almacenados. Esto es lo contrario de [convertir arrays a `QBit`](#converting-arrays-to-qbit):

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

La array reconstruida usa el `tipo de elemento` de `QBit`, y sus elementos se convierten después al tipo de elemento de array solicitado. Por lo tanto, también funciona una conversión de tipo que cambie además el tipo de elemento, como de `QBit(Float32, N)` a `Array(Float64)`.

Un recorrido de ida y vuelta `Array` -&gt; `QBit` -&gt; `Array` no pierde información para `Int8`, `Float32` y `Float64`. En el caso de `BFloat16`, coincide con una conversión directa a `BFloat16`: la única precisión que se pierde es la del propio `BFloat16`.

Cuando la `dimensión` no es múltiplo de 8, los elementos finales de `relleno` presentes en la representación interna se descartan, por lo que el resultado siempre tiene exactamente `dimensión` elementos.

<div id="qbit-subcolumns">
  ## Subcolumnas de QBit
</div>

`QBit` implementa un patrón de acceso a subcolumnas que permite acceder a planos de bits individuales de los vectores almacenados. Se puede acceder a cada posición de bit mediante la sintaxis `.N`, donde `N` es la posición del bit:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

El número de subcolumnas accesibles depende del tipo de elemento (y, cuando se usan `strides`, del número de grupos de stride; consulta [Strides](#strides)):

* `Int8`: 8 subcolumnas por grupo de stride (1-8)
* `BFloat16`: 16 subcolumnas por grupo de stride (1-16)
* `Float32`: 32 subcolumnas por grupo de stride (1-32)
* `Float64`: 64 subcolumnas por grupo de stride (1-64)

<div id="strides">
  ## Strides
</div>

De forma predeterminada, un `QBit` almacena cada plano de bits como un único flujo que abarca todas las dimensiones de `dimension`, por lo que una búsqueda siempre lee planos de bits completos de todo el vector. El parámetro opcional `stride` divide las dimensiones de `dimension` en `dimension / stride` grupos contiguos y almacena los planos de bits de cada grupo en flujos separados. Esto permite que una búsqueda sobre solo las primeras `D` dimensiones (donde `D` es un múltiplo de `stride`) lea únicamente los flujos de los grupos que abarcan esas dimensiones, lo que resulta útil para los [embeddings Matryoshka](https://arxiv.org/abs/2205.13147), donde las dimensiones iniciales forman un embedding utilizable de menor dimensionalidad.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

Aquí, las 4096 dimensiones se dividen en 4 grupos de 1024. Las subcolumnas siguen un orden por grupos: con `BFloat16` (16 bit planes), `vec.1` … `vec.16` son los 16 bit planes del primer grupo de stride (dimensiones 1–1024), `vec.17` … `vec.32` pertenecen al segundo grupo (dimensiones 1025–2048), y así sucesivamente. En general, `vec.N` corresponde al bit plane `(N-1) % element_size` del grupo de stride `(N-1) / element_size`.

Para ejecutar una búsqueda con dimensiones reducidas, pase el número de dimensiones que se deben leer como cuarto argumento de las funciones de distancia transpuestas (consulte más abajo). El vector de referencia debe tener exactamente esa cantidad de elementos, y el valor debe ser un múltiplo de `stride`.

<div id="vector-search-functions">
  ## Funciones de búsqueda vectorial
</div>

Estas son las funciones de distancia para la búsqueda de similitud vectorial que usan el tipo de datos `QBit`:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

Para un `QBit` con stride, estas funciones aceptan un cuarto argumento opcional, `used_dims` —el número de dimensiones iniciales que se deben leer—, que solo lee los grupos de stride que abarcan esas dimensiones:

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```