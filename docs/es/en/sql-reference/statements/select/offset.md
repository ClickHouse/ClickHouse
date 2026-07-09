---
description: 'Documentación de OFFSET'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'Cláusula OFFSET FETCH'
doc_type: 'reference'
---

`OFFSET` y `FETCH` permiten recuperar datos en porciones. Especifican un bloque de filas que se desea obtener con una sola consulta.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

El valor de `offset_row_count` o `fetch_row_count` puede ser un número o una constante literal. Puede omitir `fetch_row_count`; de forma predeterminada, su valor es 1.

`OFFSET` especifica el número de filas que se omiten antes de empezar a devolver filas del conjunto de resultados de la consulta. `OFFSET n` omite las primeras `n` filas del resultado.

Se admite `OFFSET` negativo: `OFFSET -n` omite las últimas `n` filas del resultado.

También se admite `OFFSET` fraccional: `OFFSET n`: si 0 &lt; n &lt; 1, se omite el primer n * 100% del resultado.

Ejemplo:
• `OFFSET 0.1`: omite el primer 10% del resultado.

> **Nota**
> • La fracción debe ser un número [Float64](../../data-types/float.md) menor que 1 y mayor que cero.
> • Si del cálculo resulta un número fraccional de filas, se redondea hacia arriba al siguiente número entero.

`FETCH` especifica el número máximo de filas que puede haber en el resultado de una consulta.

La opción `ONLY` se usa para devolver las filas que siguen inmediatamente a las omitidas por `OFFSET`. En este caso, `FETCH` es una alternativa a la cláusula [LIMIT](../../../sql-reference/statements/select/limit.md). Por ejemplo, la siguiente consulta

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

es idéntica a la consulta

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

La opción `WITH TIES` se utiliza para devolver las filas adicionales que empatan en la última posición del conjunto de resultados según la cláusula `ORDER BY`. Por ejemplo, si `fetch_row_count` está establecido en 5, pero dos filas adicionales coinciden con los valores de las columnas de `ORDER BY` de la quinta fila, el conjunto de resultados contendrá siete filas.

:::note
Según el estándar, la cláusula `OFFSET` debe ir antes de la cláusula `FETCH` si ambas están presentes.
:::

:::note
El desplazamiento real también puede depender del ajuste [offset](../../../operations/settings/settings.md#offset).
:::

<div id="examples">
  ## Ejemplos
</div>

Tabla de entrada:

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Uso de la opción `ONLY`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Uso de la opción `WITH TIES`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```