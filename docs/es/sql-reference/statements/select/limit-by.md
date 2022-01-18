---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# LIMITAR POR Cláusula {#limit-by-clause}

Una consulta con el `LIMIT n BY expressions` cláusula selecciona la primera `n` para cada valor distinto de `expressions`. La clave para `LIMIT BY` puede contener cualquier número de [expresiones](../../syntax.md#syntax-expressions).

ClickHouse admite las siguientes variantes de sintaxis:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Durante el procesamiento de consultas, ClickHouse selecciona los datos ordenados por clave de ordenación. La clave de ordenación se establece explícitamente utilizando un [ORDER BY](order-by.md) cláusula o implícitamente como una propiedad del motor de tablas. Entonces se aplica ClickHouse `LIMIT n BY expressions` y devuelve la primera `n` filas para cada combinación distinta de `expressions`. Si `OFFSET` se especifica, a continuación, para cada bloque de datos que pertenece a una combinación distinta de `expressions`, ClickHouse salta `offset_value` número de filas desde el principio del bloque y devuelve un máximo de `n` filas como resultado. Si `offset_value` es mayor que el número de filas en el bloque de datos, ClickHouse devuelve cero filas del bloque.

!!! note "Nota"
    `LIMIT BY` no está relacionado con [LIMIT](limit.md). Ambos se pueden usar en la misma consulta.

## Ejemplos {#examples}

Tabla de muestra:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consulta:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

El `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query devuelve el mismo resultado.

La siguiente consulta devuelve las 5 referencias principales para cada `domain, device_type` par con un máximo de 100 filas en total (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```
