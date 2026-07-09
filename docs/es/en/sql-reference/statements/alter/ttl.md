---
description: 'Documentación sobre las modificaciones del TTL de la tabla'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'Modificaciones del TTL de la tabla'
doc_type: 'reference'
---

:::note
Si buscas más información sobre cómo usar TTL para gestionar datos antiguos, consulta la guía de usuario [Gestionar datos con TTL](/es/guides/developer/ttl.md). La documentación siguiente muestra cómo modificar o eliminar una regla TTL existente.
:::

<div id="modify-ttl">
  ## MODIFICAR TTL
</div>

Puede cambiar el [TTL de la tabla](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) con una consulta de la siguiente forma:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## REMOVE TTL
</div>

La propiedad TTL puede eliminarse de la tabla con la siguiente consulta:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Ejemplo**

Considere la tabla con el `TTL` de la tabla:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Ejecute `OPTIMIZE` para forzar la limpieza del `TTL`:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

Se eliminó la segunda fila de la tabla.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Ahora quite el `TTL` de la tabla con la siguiente consulta:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Vuelve a insertar la fila eliminada y fuerza de nuevo la limpieza de `TTL` con `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

El `TTL` ya no existe, por lo que la segunda fila no se elimina:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**Véase también**

* Más información sobre la [expresión TTL](../../../sql-reference/statements/create/table.md#ttl-expression).
* Modificar una columna [con TTL](/es/sql-reference/statements/alter/ttl).