---
description: 'Documentación de la cláusula LIMIT BY'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'Cláusula LIMIT BY'
doc_type: 'reference'
---

Una consulta con la cláusula `LIMIT n BY expressions` selecciona las primeras `n` filas para cada valor distinto de `expressions`. La clave de `LIMIT BY` puede contener cualquier número de [expresiones](/es/sql-reference/syntax#expressions).

ClickHouse admite las siguientes variantes de sintaxis:

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

Durante el procesamiento de la consulta, ClickHouse selecciona los datos ordenados por la clave de ordenación. La clave de ordenación se establece explícitamente mediante una cláusula [ORDER BY](/es/sql-reference/statements/select/order-by) o implícitamente como una propiedad del motor de tabla (el orden de las filas solo se garantiza cuando se usa [ORDER BY](/es/sql-reference/statements/select/order-by); de lo contrario, los bloques de filas no estarán ordenados debido a la ejecución multihilo). A continuación, ClickHouse aplica `LIMIT n BY expressions` y devuelve las primeras `n` filas para cada combinación distinta de `expressions`. Si se especifica `OFFSET`, entonces para cada bloque de datos que pertenece a una combinación distinta de `expressions`, ClickHouse omite `offset_value` filas desde el inicio del bloque y devuelve como resultado un máximo de `n` filas. Si `offset_value` es mayor que el número de filas del bloque de datos, ClickHouse no devuelve ninguna fila de ese bloque.

:::note
`LIMIT BY` no está relacionado con [LIMIT](../../../sql-reference/statements/select/limit.md). Ambos pueden usarse en la misma consulta.
:::

Si desea usar números de columna en lugar de nombres de columna en la cláusula `LIMIT BY`, habilite la configuración [enable&#95;positional&#95;arguments](/es/operations/settings/settings#enable_positional_arguments).

<div id="examples">
  ## Ejemplos
</div>

Tabla de ejemplo:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consultas:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

La consulta `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` devuelve el mismo resultado.

La siguiente consulta devuelve los 5 principales referentes por cada par `domain, device_type`, con un máximo de 100 filas en total (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` también funciona con límites y desplazamientos negativos. Al igual que en la [cláusula LIMIT con valores negativos](/es/sql-reference/statements/select/limit#negative-limits), puedes usar valores negativos con `LIMIT BY` para seleccionar filas desde el *final* de cada grupo.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

Devuelve las 2 últimas filas de cada `id`. Para `id = 1`, obtenemos las filas `11` y `12`; para `id = 2`, se devuelven ambas filas porque el grupo solo tiene 2 filas.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

Devuelve la penúltima fila de cada `id`: el `OFFSET -1` final descarta la última fila de cada grupo, y el `-1` inicial conserva la última fila de lo que queda.

También se pueden mezclar `LIMIT` y `OFFSET` de signo distinto. Por ejemplo, para descartar la primera fila de cada grupo y luego conservar las 2 últimas de lo que queda:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Para `id = 1`, se omite la primera fila (`10`); se devuelven las 2 últimas de `11, 12`. Para `id = 2`, se omite la primera fila (`20`), por lo que solo queda `21`.

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

`LIMIT BY ALL` equivale a enumerar todas las expresiones de `SELECT` que no son funciones de agregación.

Por ejemplo:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

es igual que

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

En el caso especial de que haya una función que tenga tanto funciones de agregación como otros campos entre sus argumentos, las claves de `LIMIT BY` contendrán la mayor cantidad posible de campos no agregados que podamos extraer de ella.

Por ejemplo:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

es lo mismo que

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## Ejemplos
</div>

Tabla de ejemplo:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consultas:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

La consulta `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` devuelve el mismo resultado.

Uso de `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

Esto equivale a:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```