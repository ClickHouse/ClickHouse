---
description: 'Documentación para la función de ventana nonNegativeDerivative'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

Calcula la derivada no negativa de `metric_column` con respecto a `timestamp_column`.
Esta es una función de ventana específica de ClickHouse y no forma parte del SQL estándar.

Para cada fila, la derivada se calcula con respecto a la *fila anterior en el orden de evaluación de la ventana*, que está determinado por la cláusula `ORDER BY` de la ventana, no por `timestamp_column`.
El argumento `timestamp_column` se lee únicamente para medir el tiempo transcurrido entre la fila actual y la fila anterior; no ordena las filas por sí solo.

:::warning
`nonNegativeDerivative` no ordena las filas por `timestamp_column`; eso lo hace el `ORDER BY` de la ventana.
Para que la fórmula que se muestra a continuación sea aplicable, `timestamp_column` debe ser estrictamente creciente en el orden de evaluación de la ventana, por lo que normalmente se debe ordenar la ventana por `timestamp_column` de forma ascendente (por ejemplo, `... OVER (ORDER BY ts ASC)` junto con `nonNegativeDerivative(metric, ts)`).
Cuando el tiempo transcurrido entre la fila actual y la fila anterior no es positivo —lo que ocurre con `ORDER BY timestamp_column DESC` o con timestamps duplicados (iguales)— la función devuelve `0` para esa fila en lugar de aplicar la fórmula.
:::

El resultado es la tasa de cambio de la métrica por `INTERVAL`, con cualquier valor negativo truncado a `0`.
Esto es útil para métricas que aumentan de forma monótona, como los contadores, donde una disminución generalmente indica un reinicio en lugar de una tasa negativa real.

**Sintaxis**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS or RANGE expression_to_bound_rows_within_the_group])
```

Para más detalles sobre la sintaxis de las funciones de ventana, consulte: [Window Functions - Syntax](./index.md/#syntax).

**Argumentos**

- `metric_column` — La columna cuya derivada se calcula. [(U)Int*](../data-types/int-uint.md) o [Float*](../data-types/float.md).
- `timestamp_column` — La columna utilizada para medir el tiempo transcurrido entre la fila actual y la fila anterior en el orden de la ventana. No ordena las filas; eso lo hace el `ORDER BY` de la ventana, que normalmente debería usar esta misma columna. [DateTime](../data-types/datetime.md) o [DateTime64](../data-types/datetime64.md).
- `INTERVAL X UNITS` — Opcional. La unidad de tiempo a la que se escala el resultado. El valor predeterminado es `INTERVAL 1 SECOND`. Solo se admiten unidades de longitud fija (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`); las unidades de longitud variable (`MONTH`, `QUARTER`, `YEAR`) generan una excepción.

**Valor devuelto**

Para cada fila, el valor se calcula de la siguiente manera:

- `0` para la primera fila;
- `0` para cualquier fila cuyo tiempo transcurrido desde la fila anterior no sea positivo (es decir, $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$, como ocurre con el orden descendente o timestamps duplicados); y
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$ en caso contrario.

Si el valor calculado resultara negativo, se trunca a `0`. El tipo de retorno es [Float64](../data-types/float.md).

**Ejemplo**

El siguiente ejemplo calcula la tasa de cambio por segundo de una lectura de sensor.
Nótese que la tercera fila desciende de `110` a `105`, por lo que su derivada se trunca a `0`.

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```