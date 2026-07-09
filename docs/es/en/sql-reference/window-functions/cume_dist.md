---
description: 'Documentación de la función de ventana cume_dist'
sidebar_label: 'cume_dist'
sidebar_position: 11
slug: /sql-reference/window-functions/cume_dist
title: 'cume_dist'
doc_type: 'reference'
---

Calcula la distribución acumulada de un valor dentro de un grupo de valores; es decir, el porcentaje de filas con valores menores o iguales que el valor de la fila actual. Puede utilizarse para determinar la posición relativa de un valor dentro de una partición.

**Sintaxis**

```sql
cume_dist ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

La definición predeterminada y obligatoria del marco de la ventana es `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Para más detalles sobre la sintaxis de las funciones de ventana, consulte: [Funciones de ventana - Sintaxis](./index.md/#syntax).

**Valor devuelto**

* El rango relativo de la fila actual. El tipo de retorno es Float64 en el intervalo [0, 1]. [Float64](../data-types/float.md).

**Ejemplo**

El siguiente ejemplo calcula la distribución acumulativa de los salarios dentro de un equipo:

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
       cume_dist() OVER (ORDER BY salary DESC) AS cume_dist
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬───────────cume_dist─┐
1. │ Robert George   │ 195000 │  0.2857142857142857 │
2. │ Gary Chen       │ 195000 │  0.2857142857142857 │
3. │ Charles Juarez  │ 190000 │ 0.42857142857142855 │
4. │ Douglas Benson  │ 150000 │  0.8571428571428571 │
5. │ Michael Stanley │ 150000 │  0.8571428571428571 │
6. │ Scott Harrison  │ 150000 │  0.8571428571428571 │
7. │ James Henderson │ 140000 │                   1 │
   └─────────────────┴────────┴─────────────────────┘
```

**Detalles de implementación**

La función `cume_dist()` calcula la posición relativa mediante la siguiente fórmula:

```text
cume_dist = (number of rows ≤ current row value) / (total number of rows in partition)
```

Las filas con valores iguales (peers) reciben el mismo valor de distribución acumulada, que corresponde a la posición más alta del grupo de filas iguales.