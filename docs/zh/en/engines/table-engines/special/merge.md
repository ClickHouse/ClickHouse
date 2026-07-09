---
description: '`Merge` 引擎（不要与 `MergeTree` 混淆）本身不存储数据，而是允许同时从任意数量的其他表中读取数据。'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Merge 表引擎'
doc_type: 'reference'
---

`Merge` 引擎 (不要与 `MergeTree` 混淆) 本身不存储数据，而是允许同时从任意数量的其他表中读取数据。

读取会自动并行执行。不支持向该表写入数据。读取时，如果相关表存在索引，则会使用实际读取的那些表的索引。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## 引擎参数
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — 可能的值：

* 数据库名称，
  * 返回数据库名称字符串的常量表达式，例如 `currentDatabase()`，
  * `REGEXP(expression)`，其中 `expression` 是用于匹配数据库名称的正则表达式。

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — 用于匹配指定 DB 或多个 DB 中表名的正则表达式。

正则表达式 — [re2](https://github.com/google/re2) (支持 PCRE 的部分特性) ，区分大小写。
请参阅“match”部分中关于正则表达式符号转义的说明。

<div id="usage">
  ## 用法
</div>

在选择要读取的表时，即使 `Merge` 表本身匹配正则表达式，也不会选中它，以避免出现循环。
可以创建两个 `Merge` 表，让它们无休止地尝试读取对方的数据，但这并不是个好主意。

`Merge` 引擎 的典型用法，是将大量 `TinyLog` 表作为一张表来使用。

<div id="examples">
  ## 示例
</div>

**示例 1**

假设有两个数据库 `ABC_corporate_site` 和 `ABC_store`。`all_visitors` 表将包含这两个数据库中 `visitors` 表里的 ID。

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**示例 2**

假设你有一个旧表 `WatchLog_old`，并决定在不将数据迁移到新表 `WatchLog_new` 的情况下调整分区方式，同时还需要查看这两个表中的数据。

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_table` — 读取数据的来源表名称。类型：[String](../../../sql-reference/data-types/string.md)。

  如果对 `_table` 进行过滤 (例如 `WHERE _table='xyz'`) ，则只会读取满足过滤条件的表。

* `_database` — 包含读取数据的来源数据库名称。类型：[String](../../../sql-reference/data-types/string.md)。

**另请参阅**

* [虚拟列](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [merge](../../../sql-reference/table-functions/merge.md) 表函数