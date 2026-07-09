---
description: 'Documentación de la función de ventana first_value'
sidebar_label: 'first_value'
sidebar_position: 3
slug: /sql-reference/window-functions/first_value
title: 'first_value'
doc_type: 'reference'
---

Devuelve el primer valor evaluado dentro de su marco ordenado. De forma predeterminada, se omiten los argumentos NULL; sin embargo, el modificador `RESPECT NULLS` puede utilizarse para cambiar este comportamiento.

**Sintaxis**

```sql
first_value (column_name) [[RESPECT NULLS] | [IGNORE NULLS]]
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Alias: `any`.

:::note
El uso del modificador opcional `RESPECT NULLS` después de `first_value(column_name)` garantiza que no se omitan los argumentos `NULL`.
Consulte [procesamiento de NULL](../aggregate-functions/index.md/#null-processing) para obtener más información.

Alias: `firstValueRespectNulls`
:::

Para más información sobre la sintaxis de las funciones de ventana, consulte: [Funciones de ventana - Sintaxis](./index.md/#syntax).

**Valor devuelto**

* El primer valor evaluado dentro de su marco ordenado.

**Ejemplo**

En este ejemplo, la función `first_value` se utiliza para encontrar al futbolista mejor pagado en un conjunto de datos ficticio con los salarios de jugadores de la Premier League.

```sql title="Query"
DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT VALUES
    ('Port Elizabeth Barbarians', 'Gary Chen', 196000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 100000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 180000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary, 
       first_value(player) OVER (ORDER BY salary DESC) AS highest_paid_player
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─highest_paid_player─┐
1. │ Gary Chen       │ 196000 │ Gary Chen           │
2. │ Robert George   │ 195000 │ Gary Chen           │
3. │ Charles Juarez  │ 190000 │ Gary Chen           │
4. │ Scott Harrison  │ 180000 │ Gary Chen           │
5. │ Douglas Benson  │ 150000 │ Gary Chen           │
6. │ James Henderson │ 140000 │ Gary Chen           │
7. │ Michael Stanley │ 100000 │ Gary Chen           │
   └─────────────────┴────────┴─────────────────────┘
```