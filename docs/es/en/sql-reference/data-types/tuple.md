---
description: 'Documentación del tipo de dato Tuple en ClickHouse'
sidebar_label: 'Tuple(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: 'Tuple(T1, T2, ...)'
doc_type: 'reference'
---

Una tupla de elementos, cada uno con un [tipo](/es/sql-reference/data-types) individual. Tuple debe contener al menos un elemento.

Las tuplas se usan para el agrupamiento temporal de columnas. Las columnas se pueden agrupar cuando se usa una expresión IN en una consulta, y para especificar ciertos parámetros formales de las funciones lambda. Para obtener más información, consulte las secciones [operadores IN](../../sql-reference/operators/in.md) y [Funciones de orden superior](/es/sql-reference/functions/overview#higher-order-functions).

Las tuplas pueden ser el resultado de una consulta. En este caso, en los formatos de texto distintos de JSON, los valores se separan por comas dentro de `()`. En los formatos JSON, las tuplas se generan como arrays (en `[]`).

<div id="creating-tuples">
  ## Creación de tuplas
</div>

Puede usar una función para crear una tupla:

```sql
tuple(T1, T2, ...)
```

Ejemplo de cómo crear una tupla:

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

El tipo Tuple puede contener un solo elemento

Ejemplo:

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

La sintaxis `(tuple_element1, tuple_element2)` puede utilizarse para crear una tupla con varios elementos sin llamar a la función `tuple()`.

Ejemplo:

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## Detección de tipos de datos
</div>

Al crear tuplas sobre la marcha, ClickHouse infiere el tipo de los argumentos de la tupla como los tipos más pequeños capaces de contener el valor del argumento proporcionado. Si el valor es [NULL](/es/operations/settings/formats#input_format_null_as_default), el tipo inferido es [Nullable](../../sql-reference/data-types/nullable.md).

Ejemplo de detección automática de tipos de datos:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## Referencia a los elementos de Tuple
</div>

Se puede hacer referencia a los elementos de Tuple por nombre o por índice:

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## Operaciones de comparación con Tuple
</div>

Dos tuplas se comparan evaluando sus elementos secuencialmente, de izquierda a derecha. Si el elemento de la primera tupla es mayor (o menor) que el elemento correspondiente de la segunda, entonces la primera tupla es mayor (o menor) que la segunda; en caso contrario (si ambos elementos son iguales), se compara el siguiente elemento.

Ejemplo:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

Ejemplos reales:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(Tuple(T1, T2, ...))
</div>

:::note Funcionalidad Beta
Requiere `SET enable_nullable_tuple_type = 1`
Esta funcionalidad está en Beta.
:::

Permite que la tupla completa sea `NULL`, a diferencia de `Tuple(Nullable(T1), Nullable(T2), ...)`, donde solo los elementos individuales pueden ser `NULL`.

| Tipo                                       | La tupla puede ser NULL | Los elementos pueden ser NULL |
| ------------------------------------------ | ----------------------- | ----------------------------- |
| `Nullable(Tuple(String, Int64))`           | ✅                       | ❌                             |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌                       | ✅                             |

Ejemplo:

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```