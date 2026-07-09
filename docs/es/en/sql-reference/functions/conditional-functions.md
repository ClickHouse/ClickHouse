---
description: 'Documentación de las funciones condicionales'
sidebar_label: 'Condicional'
slug: /sql-reference/functions/conditional-functions
title: 'Funciones condicionales'
doc_type: 'reference'
---

<div id="overview">
  ## Descripción general
</div>

<div id="using-conditional-results-directly">
  ### Uso directo de los resultados condicionales
</div>

Las expresiones condicionales siempre dan como resultado `0`, `1` o `NULL`. Por lo tanto, puedes usar directamente los resultados condicionales así:

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

<div id="null-values-in-conditionals">
  ### Valores `NULL` en expresiones condicionales
</div>

Cuando intervienen valores `NULL` en expresiones condicionales, el resultado también será `NULL`.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Por lo tanto, debe construir sus consultas con cuidado si los tipos son `Nullable`.

El siguiente ejemplo demuestra esto porque no añade la condición equals a `multiIf`.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

<div id="case-statement">
  ### Instrucción CASE
</div>

La expresión CASE en ClickHouse proporciona una lógica condicional similar al operador CASE de SQL. Evalúa condiciones y devuelve valores según la primera condición que coincida.

ClickHouse admite dos formas de CASE:

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   Esta forma ofrece total flexibilidad y se implementa internamente mediante la función [multiIf](/es/sql-reference/functions/conditional-functions#multiIf). Cada condición se evalúa de forma independiente, y las expresiones pueden incluir valores no constantes.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   Esta forma más compacta está optimizada para comparar valores constantes y utiliza internamente `caseWithExpression()`.

Por ejemplo, lo siguiente es válido:

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

Esta forma tampoco requiere que las expresiones de retorno sean constantes.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### Consideraciones
</div>

ClickHouse determina el tipo de resultado de una expresión CASE (o su equivalente interno, como `multiIf`) antes de evaluar cualquier condición. Esto es importante cuando las expresiones devueltas difieren en el tipo, por ejemplo, al usar distintas zonas horarias o tipos numéricos.

* El tipo de resultado se selecciona según el tipo compatible de mayor tamaño entre todas las ramas.
* Una vez seleccionado ese tipo, todas las demás ramas se convierten implícitamente a él, aunque su lógica nunca llegue a ejecutarse en tiempo de ejecución.
* En tipos como DateTime64, donde la zona horaria forma parte de la firma del tipo, esto puede dar lugar a un comportamiento inesperado: la primera zona horaria encontrada puede aplicarse a todas las ramas, incluso cuando otras ramas especifican zonas horarias distintas.

Por ejemplo, en el siguiente caso, todas las filas devuelven el timestamp en la zona horaria de la primera rama coincidente, es decir, `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

Aquí, ClickHouse detecta varios tipos de retorno `DateTime64(3, <timezone>)`. Infiere que el tipo común es `DateTime64(3, 'Asia/Kolkata'` por ser el primero que encuentra, convirtiendo implícitamente las demás ramas a este tipo.

Esto puede resolverse convirtiendo el valor en una cadena para preservar el formato de zona horaria deseado:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  El contenido interno de las etiquetas siguientes se reemplaza durante la compilación del framework de documentación con 
  documentación generada a partir de system.functions. No modifique ni elimine las etiquetas.
  Ver: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }