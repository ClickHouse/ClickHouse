---
description: 'Documentación de DETACH'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'Sentencia DETACH'
doc_type: 'reference'
---

Hace que el servidor &quot;olvide&quot; que existe una tabla, una vista materializada, un diccionario o una base de datos.

**Sintaxis**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

Desasociar no elimina los datos ni los metadatos de una tabla, una vista materializada, un diccionario o una base de datos. Si una entidad no se desasoció `PERMANENTLY`, en el siguiente inicio del servidor, este leerá los metadatos y volverá a cargar la tabla/vista/diccionario/base de datos. Si una entidad se desasoció `PERMANENTLY`, no se volverá a cargar automáticamente.

Tanto si una tabla, un diccionario o una base de datos se desasoció permanentemente como si no, en ambos casos puede volver a adjuntarlos mediante la consulta [ATTACH](../../sql-reference/statements/attach.md).
Las tablas de log del sistema también pueden volver a adjuntarse (por ejemplo, `query_log`, `text_log`, etc.). Otras tablas del sistema no pueden volver a adjuntarse. En el siguiente inicio del servidor, este volverá a cargar esas tablas.

`ATTACH MATERIALIZED VIEW` no funciona con la sintaxis corta (sin `SELECT`), pero puede adjuntarse mediante la consulta `ATTACH TABLE`.

Tenga en cuenta que no puede desasociar permanentemente una tabla que ya está desasociada (temporalmente). Pero puede volver a adjuntarla y después desasociarla permanentemente de nuevo.

Además, no puede hacer [DROP](../../sql-reference/statements/drop.md#drop-table) de una tabla desasociada, ni ejecutar [CREATE TABLE](../../sql-reference/statements/create/table.md) con el mismo nombre que una desasociada permanentemente, ni reemplazarla por otra tabla con la consulta [RENAME TABLE](../../sql-reference/statements/rename.md).

El modificador `SYNC` ejecuta la acción sin demora.

**Ejemplo**

Crear una tabla:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Desvincular la tabla:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
En ClickHouse Cloud, los usuarios deben usar la cláusula `PERMANENTLY`, por ejemplo, `DETACH TABLE <table> PERMANENTLY`. Si no se usa esta cláusula, las tablas volverán a adjuntarse al reiniciar el clúster, por ejemplo, durante las actualizaciones.
:::

**Ver también**

* [Vista materializada](/es/sql-reference/statements/create/view#materialized-view)
* [Diccionarios](./create/dictionary/overview.md)