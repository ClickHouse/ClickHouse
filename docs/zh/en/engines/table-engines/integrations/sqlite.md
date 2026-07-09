---
description: '该引擎支持将数据导入到 SQLite 以及从 SQLite 导出数据，并支持直接从 ClickHouse 查询
  SQLite 表。'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'SQLite 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # SQLite 表引擎
</div>

<CloudNotSupportedBadge />

该引擎支持将数据导入到 SQLite，也支持将数据从 SQLite 导出，并且可以直接从 ClickHouse 查询 SQLite 表。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**引擎参数**

* `db_path` — 指向包含数据库的 SQLite 文件的路径。
* `table` — SQLite 数据库中的表名，或按原样传递给 SQLite 的查询 (请参阅[传递查询而不是表名](#passing-a-query)) 。

<div id="passing-a-query">
  ## 传入查询而不是表名
</div>

`table` 参数除了可以是表名，也可以是原样传递给 SQLite 的 `SELECT` 查询。表的结构会根据查询结果自动推断。该查询既可以写成子查询，也可以包装在 `query` 函数中：

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

这样的表是只读的：不允许对其执行 `INSERT`。[`sqlite`](/zh/sql-reference/table-functions/sqlite) 表函数也支持相同的语法。

:::note
子查询形式 `(SELECT ...)` 会先由 ClickHouse 解析并重新序列化，然后再发送给 SQLite。因此，它必须是合法的 ClickHouse SQL。若要传递 ClickHouse 无法解析的 SQLite 专用语法，请使用 `query('...')` 形式，其文本会原样发送给 SQLite。

外围 ClickHouse 查询中的任何外层 `WHERE`、`LIMIT`、聚合等，**都不会**下推到传入的查询中——而是会在完整的查询结果被拉取后，由 ClickHouse 再执行。要限制从 SQLite 读取的数据，请将过滤器写在传入的查询内部。启用 [`external_table_strict_query = 1`](/zh/operations/settings/settings#external_table_strict_query) 时，无法下推的外层过滤器会被直接拒绝并抛出异常，而不是在本地执行。
:::

<div id="data-types-support">
  ## 数据类型支持
</div>

当在表定义中显式指定 ClickHouse 列类型时，可将 SQLite 的 TEXT 列解析为以下 ClickHouse 类型：

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* 所有整数类型 ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

默认类型映射请参见 [SQLite 数据库引擎](../../../engines/database-engines/sqlite.md#data_types-support)。

<div id="usage-example">
  ## 使用示例
</div>

下面的查询用于创建 SQLite 表：

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

返回该表中的数据：

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

**另请参阅**

* [SQLite](../../../engines/database-engines/sqlite.md) 引擎
* [sqlite](../../../sql-reference/table-functions/sqlite.md) 表函数