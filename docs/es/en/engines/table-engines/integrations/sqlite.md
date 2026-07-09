---
description: 'El motor permite importar y exportar datos desde y hacia SQLite, y admite
  consultas a tablas de SQLite directamente desde ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'Motor de tabla SQLite'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # Motor de tabla SQLite
</div>

<CloudNotSupportedBadge />

Este motor permite importar y exportar datos a SQLite, y admite consultas directas a tablas de SQLite desde ClickHouse.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Parámetros del motor**

* `db_path` — Ruta al archivo de SQLite con una base de datos.
* `table` — Nombre de una tabla en la base de datos de SQLite, o una consulta que se pasa a SQLite tal cual (consulta [Pasar una consulta en lugar de un nombre de tabla](#passing-a-query)).

<div id="passing-a-query">
  ## Pasar una consulta en lugar del nombre de una tabla
</div>

En lugar del nombre de una tabla, el argumento `table` puede ser una consulta `SELECT` que se pasa a SQLite tal cual. La estructura de la tabla se infiere a partir del resultado de la consulta. La consulta puede escribirse como una subconsulta o envolverse en la función `query`:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Dicha tabla es de solo lectura: no se permite `INSERT` en ella. La función de tabla [`sqlite`](/es/sql-reference/table-functions/sqlite) también admite la misma sintaxis.

:::note
ClickHouse analiza la forma de subconsulta `(SELECT ...)` y la vuelve a serializar antes de enviarla a SQLite. Por lo tanto, debe ser ClickHouse SQL válido. Para pasar sintaxis específica de SQLite que ClickHouse no analiza, use la forma `query('...')`, cuyo texto se envía a SQLite literalmente.

Cualquier `WHERE`, `LIMIT`, agregación, etc. externo de la consulta de ClickHouse circundante **no** se hace pushdown en la consulta proporcionada; se aplica en ClickHouse después de recuperar el resultado completo de la consulta. Para limitar los datos leídos desde SQLite, coloque el filtro dentro de la consulta proporcionada. Con [`external_table_strict_query = 1`](/es/operations/settings/settings#external_table_strict_query), un filtro externo sobre el que no se puede hacer pushdown se rechaza con una excepción en lugar de aplicarse localmente.
:::

<div id="data-types-support">
  ## Compatibilidad con tipos de datos
</div>

Cuando se especifican explícitamente los tipos de columna de ClickHouse en la definición de la tabla, los siguientes tipos de ClickHouse se pueden interpretar a partir de columnas TEXT de SQLite:

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* Todos los tipos enteros ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

Consulte el [motor de base de datos SQLite](../../../engines/database-engines/sqlite.md#data_types-support) para ver la correspondencia de tipos predeterminada.

<div id="usage-example">
  ## Ejemplo de uso
</div>

Muestra una consulta que crea la tabla de SQLite:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Devuelve los datos de la tabla:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**Véase también**

* motor [SQLite](../../../engines/database-engines/sqlite.md)
* función de tabla [sqlite](../../../sql-reference/table-functions/sqlite.md)