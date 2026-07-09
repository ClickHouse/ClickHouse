---
description: 'Permite realizar consultas sobre datos almacenados en una base de datos SQLite.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

Permite realizar consultas sobre datos almacenados en una base de datos [SQLite](../../engines/database-engines/sqlite.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## Argumentos
</div>

* `db_path` — Ruta de acceso a un archivo con una base de datos SQLite. [String](../../sql-reference/data-types/string.md).
* `table_name` — Nombre de una tabla de la base de datos SQLite, o una consulta que se pasa a SQLite tal cual (consulte [Pasar una consulta en lugar de un nombre de tabla](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor devuelto
</div>

* Un objeto de tipo tabla con las mismas columnas que la tabla `SQLite` original.

<div id="passing-a-query">
  ## Pasar una consulta en lugar de un nombre de tabla
</div>

En lugar de un nombre de tabla, el segundo argumento puede ser una consulta `SELECT` que se pasa a SQLite tal cual. La estructura de la tabla resultante se infiere del resultado de la consulta. La consulta puede escribirse como una subconsulta o envolverse en la función `query`:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Dicha tabla es de solo lectura: no se permite `INSERT` en ella. El motor de tabla [`SQLite`](/es/engines/table-engines/integrations/sqlite) admite la misma sintaxis.

:::note
La forma de subconsulta `(SELECT ...)` la analiza ClickHouse y la vuelve a serializar antes de enviarla a SQLite. Por lo tanto, debe ser válida en ClickHouse SQL. Para pasar sintaxis específica de SQLite que ClickHouse no analiza, use la forma `query('...')`, cuyo texto se envía literalmente a SQLite.

Cualquier `WHERE`, `LIMIT`, agregación, etc. externo de la consulta de ClickHouse circundante **no** se hace pushdown en la consulta pasada; se aplica en ClickHouse después de recuperar el resultado completo de la consulta. Para restringir los datos leídos desde SQLite, coloque el filtro dentro de la consulta pasada. Con [`external_table_strict_query = 1`](/es/operations/settings/settings#external_table_strict_query), un filtro externo sobre el que no se puede hacer pushdown se rechaza con una excepción, en lugar de aplicarse localmente.
:::

<div id="example">
  ## Ejemplo
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## Relacionados
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md), motor de tabla
* [Motor de bases de datos SQLite](../../engines/database-engines/sqlite.md) — sección de compatibilidad con tipos de datos