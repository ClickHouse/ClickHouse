---
description: 'crea un almacenamiento temporal con columnas rellenas de valores.'
keywords: ['valores', 'función de tabla']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

La función de tabla `Values` le permite crear un almacenamiento temporal con
columnas rellenas de valores. Es útil para pruebas rápidas o para generar datos de ejemplo.

:::note
Values es una función que no distingue entre mayúsculas y minúsculas. Es decir, tanto `VALUES` como `values` son válidos.
:::

<div id="syntax">
  ## Sintaxis
</div>

La sintaxis básica de la función de tabla `VALUES` es:

```sql
VALUES([structure,] values...)
```

Suele usarse como:

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## Argumentos
</div>

* `column1_name Type1, ...` (opcional). [String](/es/sql-reference/data-types/string)
  que especifica los nombres y tipos de las columnas. Si se omite este argumento, las columnas
  se llamarán `c1`, `c2`, etc.
* `(value1_row1, value2_row1)`. [Tuples](/es/sql-reference/data-types/tuple)
  que contienen valores de cualquier tipo.

:::note
Las tuplas separadas por comas también se pueden sustituir por valores individuales. En este caso,
cada valor se considera una nueva fila. Consulta la sección de [ejemplos](#examples) para obtener
más información.
:::

<div id="returned-value">
  ## Valor devuelto
</div>

* Devuelve una tabla temporal que contiene los valores proporcionados.

<div id="examples">
  ## Ejemplos
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` también puede usarse con valores individuales en lugar de con tuplas. Por ejemplo:

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

O bien, sin proporcionar una especificación de fila (`'column1_name Type1, column2_name Type2, ...'`
en la [sintaxis](#syntax)), en cuyo caso las columnas reciben nombre automáticamente.

Por ejemplo:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## Cláusula VALUES estándar de SQL
</div>

A partir de la versión 26.3, ClickHouse también admite la cláusula `VALUES` estándar de SQL como expresión de tabla
en `FROM`, tal como se usa en PostgreSQL, MySQL, DuckDB y SQL Server. Esta sintaxis se
reescribe internamente para usar la función de tabla `values` descrita anteriormente.

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

Se puede utilizar en las CTE:

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

Y en los JOINs:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
Los alias de columna después de `AS t(col1, col2, ...)` siguen la sintaxis estándar de SQL para
nombrar las columnas de las tablas derivadas. Si se omiten, las columnas se llaman `c1`, `c2`, etc.
:::

<div id="see-also">
  ## Véase también
</div>

* [Formato Values](/es/interfaces/formats/Values)