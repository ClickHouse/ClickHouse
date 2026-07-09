---
description: 'Documentación de la cláusula DISTINCT'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'Cláusula DISTINCT'
doc_type: 'reference'
---

Si se especifica `SELECT DISTINCT`, en el resultado de una consulta solo permanecerán las filas únicas. Por lo tanto, de cada conjunto de filas completamente coincidentes en el resultado, solo se conservará una.

Puede especificar la lista de columnas que deben tener valores únicos: `SELECT DISTINCT ON (column1, column2,...)`. Si no se especifican las columnas, se tendrán en cuenta todas.

Considere la tabla:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Uso de `DISTINCT` sin especificar columnas:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Uso de `DISTINCT` con columnas específicas:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT y ORDER BY
</div>

ClickHouse admite el uso de las cláusulas `DISTINCT` y `ORDER BY` en columnas diferentes dentro de una misma consulta. La cláusula `DISTINCT` se ejecuta antes que la cláusula `ORDER BY`.

Considere la tabla:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Selección de datos:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Seleccionar datos con distintas direcciones de ordenación:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

La fila `2, 4` se truncó antes de ordenar.

Tenga en cuenta esta particularidad de la implementación al programar consultas.

<div id="null-processing">
  ## Procesamiento de NULL
</div>

`DISTINCT` funciona con [NULL](/es/sql-reference/syntax#null) como si `NULL` fuera un valor específico y `NULL==NULL`. En otras palabras, en los resultados de `DISTINCT`, las distintas combinaciones con `NULL` aparecen una sola vez. Esto difiere del procesamiento de `NULL` en la mayoría de los otros contextos.

<div id="alternatives">
  ## Alternativas
</div>

Es posible obtener el mismo resultado aplicando [GROUP BY](/es/sql-reference/statements/select/group-by) al mismo conjunto de valores especificado en la cláusula `SELECT`, sin usar funciones de agregación. Sin embargo, hay algunas diferencias con respecto al enfoque con `GROUP BY`:

* `DISTINCT` se puede aplicar junto con `GROUP BY`.
* Cuando se omite [ORDER BY](../../../sql-reference/statements/select/order-by.md) y se define [LIMIT](../../../sql-reference/statements/select/limit.md), la consulta deja de ejecutarse inmediatamente después de leer el número requerido de filas distintas.
* Los bloques de datos se muestran a medida que se procesan, sin esperar a que toda la consulta termine de ejecutarse.