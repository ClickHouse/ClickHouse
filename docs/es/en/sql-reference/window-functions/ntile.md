---
description: 'Documentación de la función de ventana ntile'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

Divide las filas ordenadas dentro de una partición en un número determinado de buckets (grupos) de tamaño lo más uniforme posible y devuelve el número del bucket al que pertenece la fila actual. Los buckets se numeran a partir de 1. En cada partición, las filas se asignan a los buckets en orden: si el número de filas no es divisible por el número de buckets, los primeros buckets reciben una fila más que los últimos.

**Sintaxis**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

El argumento `buckets` debe ser un entero positivo constante.

Se requiere una cláusula `ORDER BY`. El marco de ventana debe abarcar toda la partición (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`), que además es el marco predeterminado cuando no se especifica ninguno explícitamente.

Para obtener más detalles sobre la sintaxis de funciones de ventana, consulte: [Funciones de ventana - Sintaxis](./index.md/#syntax).

**Valor devuelto**

* El número de bucket de la fila actual dentro de su partición. [UInt64](../data-types/int-uint.md).

**Ejemplo**

El siguiente ejemplo divide a los jugadores en cuatro buckets ordenados por salario descendente.

```sql title="Query"
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

Aquí hay siete filas y cuatro buckets, por lo que los tres primeros buckets contienen dos filas cada uno y el último bucket contiene una sola fila.