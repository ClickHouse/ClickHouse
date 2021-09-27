---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: Fusionar
---

# Fusionar {#merge}

El `Merge` motor (no debe confundirse con `MergeTree`) no almacena datos en sí, pero permite leer de cualquier número de otras tablas simultáneamente.
La lectura se paralela automáticamente. No se admite la escritura en una tabla. Al leer, se usan los índices de las tablas que realmente se están leyendo, si existen.
El `Merge` engine acepta parámetros: el nombre de la base de datos y una expresión regular para las tablas.

Ejemplo:

``` sql
Merge(hits, '^WatchLog')
```

Los datos se leerán de las tablas en el `hits` base de datos que tienen nombres que coinciden con la expresión regular ‘`^WatchLog`’.

En lugar del nombre de la base de datos, puede usar una expresión constante que devuelva una cadena. Por ejemplo, `currentDatabase()`.

Regular expressions — [Re2](https://github.com/google/re2) (soporta un subconjunto de PCRE), sensible a mayúsculas y minúsculas.
Vea las notas sobre los símbolos de escape en expresiones regulares en el “match” apartado.

Al seleccionar tablas para leer, el `Merge` no se seleccionará la tabla en sí, incluso si coincide con la expresión regular. Esto es para evitar bucles.
Es posible crear dos `Merge` tablas que intentarán interminablemente leer los datos de los demás, pero esta no es una buena idea.

La forma típica de usar el `Merge` para trabajar con un gran número de `TinyLog` tablas como si con una sola tabla.

Ejemplo 2:

Digamos que tiene una tabla antigua (WatchLog_old) y decidió cambiar la partición sin mover datos a una nueva tabla (WatchLog_new) y necesita ver datos de ambas tablas.

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## Virtual Columnas {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [Cadena](../../../sql-reference/data-types/string.md).

    Puede establecer las condiciones constantes en `_table` en el `WHERE/PREWHERE` cláusula (por ejemplo, `WHERE _table='xyz'`). En este caso, la operación de lectura se realiza sólo para las tablas donde la condición en `_table` está satisfecho, por lo que el `_table` columna actúa como un índice.

**Ver también**

-   [Virtual columnas](index.md#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->
