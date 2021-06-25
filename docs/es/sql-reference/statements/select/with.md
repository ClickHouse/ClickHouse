---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# CON Cláusula {#with-clause}

Esta sección proporciona soporte para expresiones de tabla común ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), por lo que los resultados de `WITH` cláusula se puede utilizar en el interior `SELECT` clausula.

## Limitacion {#limitations}

1.  No se admiten consultas recursivas.
2.  Cuando se usa subconsulta dentro de la sección WITH , su resultado debe ser escalar con exactamente una fila.
3.  Los resultados de la expresión no están disponibles en las subconsultas.

## Ejemplos {#examples}

**Ejemplo 1:** Usando expresión constante como “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**Ejemplo 2:** Evictar el resultado de la expresión de sum (bytes) de la lista de columnas de la cláusula SELECT

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

**Ejemplo 3:** Uso de los resultados de la subconsulta escalar

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

**Ejemplo 4:** Reutilización de la expresión en subconsulta

Como solución alternativa para la limitación actual para el uso de expresiones en subconsultas, puede duplicarla.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```
