---
description: 'Documentación de la función de ventana lagInFrame'
sidebar_label: 'lagInFrame'
sidebar_position: 9
slug: /sql-reference/window-functions/lagInFrame
title: 'lagInFrame'
doc_type: 'reference'
---

Devuelve el valor evaluado en la fila situada un desplazamiento físico especificado antes de la fila actual dentro del marco ordenado.

:::warning
El comportamiento de `lagInFrame` difiere del de la función de ventana estándar de SQL `lag`.
La función de ventana `lagInFrame` de ClickHouse respeta el marco de la ventana.
Para obtener un comportamiento idéntico a `lag`, use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**Sintaxis**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Para obtener más información sobre la sintaxis de las funciones de ventana, consulte: [Funciones de ventana - Sintaxis](./index.md/#syntax).

**Parámetros**

* `x` — Nombre de la columna.
* `offset` — Desplazamiento que se va a aplicar. [(U)Int*](../data-types/int-uint.md). (Opcional: `1` de forma predeterminada).
* `default` — Valor que se devolverá si la fila calculada supera los límites del marco de la ventana. (Opcional: valor predeterminado del tipo de columna si se omite).

**Valor devuelto**

* Valor evaluado en la fila ubicada en un desplazamiento físico especificado antes de la fila actual dentro del marco ordenado.

**Ejemplo**

En este ejemplo, se analizan datos históricos de una acción concreta y se usa la función `lagInFrame` para calcular la variación diaria y el cambio porcentual en el precio de cierre de la acción.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     ) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```