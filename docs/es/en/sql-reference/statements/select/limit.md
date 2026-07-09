---
description: 'Documentación de la cláusula LIMIT'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'Cláusula LIMIT'
doc_type: 'reference'
---

La cláusula `LIMIT` controla cuántas filas se devuelven en el resultado de la consulta.

<div id="basic-syntax">
  ## Sintaxis básica
</div>

**Seleccionar las primeras filas:**

```sql
LIMIT m
```

Devuelve las primeras `m` filas del resultado, o todos los registros si hay menos de `m`.

**Sintaxis alternativa de TOP (compatible con MS SQL Server):**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

Esto equivale a `LIMIT m` y puede usarse por compatibilidad con consultas de Microsoft SQL Server.

**Select con desplazamiento:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

Omite las primeras `n` filas y devuelve las siguientes `m`.

En ambas formas, `n` y `m` deben ser enteros no negativos.

<div id="negative-limits">
  ## Límites negativos
</div>

Usa valores negativos para seleccionar filas del *final* del conjunto de resultados:

| Sintaxis             | Resultado                                                  |
| -------------------- | ---------------------------------------------------------- |
| `LIMIT -m`           | Últimas `m` filas                                          |
| `LIMIT -m OFFSET -n` | Últimas `m` filas después de omitir las últimas `n` filas  |
| `LIMIT m OFFSET -n`  | Primeras `m` filas después de omitir las últimas `n` filas |
| `LIMIT -m OFFSET n`  | Últimas `m` filas después de omitir las primeras `n` filas |

La sintaxis `LIMIT -n, -m` equivale a `LIMIT -m OFFSET -n`.

<div id="fractional-limits">
  ## Límites fraccionarios
</div>

Utilice valores decimales entre 0 y 1 para seleccionar un porcentaje de filas:

| Sintaxis                | Resultado                                                     |
| ----------------------- | ------------------------------------------------------------- |
| `LIMIT 0.1`             | El primer 10 % de las filas                                   |
| `LIMIT 1 OFFSET 0.5`    | La fila mediana                                               |
| `LIMIT 0.25 OFFSET 0.5` | Tercer cuartil (25 % de las filas tras omitir el primer 50 %) |

:::note

* Las fracciones deben ser valores [Float64](../../data-types/float.md) mayores que 0 y menores que 1.
* Las cantidades fraccionarias de filas se redondean al siguiente número entero.
  :::

<div id="combining-limit-types">
  ## Combinar tipos de límite
</div>

Puedes combinar enteros estándar con desplazamientos fraccionarios o negativos:

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

El modificador `WITH TIES` incluye filas adicionales que tienen los mismos valores de `ORDER BY` que la última fila dentro del límite.

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

Con `WITH TIES`, se incluyen todas las filas que comparten el último valor:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

La fila 6 se incluye porque tiene el mismo valor (`2`) que la fila 5.

Lo mismo ocurre cuando el desplazamiento se especifica con la palabra clave `OFFSET`:

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Si se omiten las primeras 2 filas y se toman 3, normalmente se devolverían `1, 1, 2`, pero se incluye el segundo `2` porque empata con la última fila.

`WITH TIES` también funciona con límites y desplazamientos negativos. Incluye filas adicionales que tienen los mismos valores de `ORDER BY` que la primera fila seleccionada:

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

Sin `WITH TIES`, el resultado sería `1, 1, 2, 2`. Con `WITH TIES`, se incluyen tres filas adicionales con el valor `1` porque empatan con la primera fila seleccionada.

Este modificador puede combinarse con el modificador [`ORDER BY ... WITH FILL`](/es/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier).

<div id="considerations">
  ## Consideraciones
</div>

**Resultados no deterministas:** Sin una cláusula [`ORDER BY`](../../../sql-reference/statements/select/order-by.md), las filas devueltas pueden ser arbitrarias y variar entre distintas ejecuciones de la consulta.

**Límite del lado del servidor:** La cantidad de filas devueltas también puede verse afectada por la configuración [limit](../../../operations/settings/settings.md#limit).

<div id="see-also">
  ## Véase también
</div>

* [LIMIT BY](/es/sql-reference/statements/select/limit-by) — Limita las filas por cada grupo de valores; es útil para obtener los N resultados principales dentro de cada categoría.