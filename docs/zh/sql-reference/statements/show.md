---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 38
toc_title: SHOW
---

# 显示查询 {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

返回单 `String`-类型 ‘statement’ column, which contains a single value – the `CREATE` 用于创建指定对象的查询。

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

打印所有数据库的列表。
这个查询是相同的 `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

输出的内容 [系统。流程](../../operations/system-tables.md#system_tables-processes) 表，包含目前正在处理的查询列表，除了 `SHOW PROCESSLIST` 查询。

该 `SELECT * FROM system.processes` 查询返回有关所有当前查询的数据。

提示（在控制台中执行):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

显示表的列表。

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果 `FROM` 如果未指定子句，则查询返回当前数据库中的表列表。

你可以得到相同的结果 `SHOW TABLES` 通过以下方式进行查询:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**示例**

下面的查询从表的列表中选择前两行 `system` 数据库，其名称包含 `co`.

``` sql
SHOW TABLES FROM system LIKE '%co%' LIMIT 2
```

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ collations                     │
└────────────────────────────────┘
```

## SHOW DICTIONARIES {#show-dictionaries}

显示列表 [外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果 `FROM` 如果未指定子句，则查询从当前数据库返回字典列表。

你可以得到相同的结果 `SHOW DICTIONARIES` 通过以下方式进行查询:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**示例**

下面的查询从表的列表中选择前两行 `system` 数据库，其名称包含 `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

[原始文章](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
