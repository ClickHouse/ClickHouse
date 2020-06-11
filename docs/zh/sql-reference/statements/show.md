---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
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

## SHOW GRANTS {#show-grants-statement}

显示用户的权限。

### 语法 {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

如果未指定user，则查询返回当前用户的权限。

## SHOW CREATE USER {#show-create-user-statement}

显示了在使用的参数 [用户创建](create.md#create-user-statement).

`SHOW CREATE USER` 不输出用户密码。

### 语法 {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

显示了在使用的参数 [角色创建](create.md#create-role-statement)

### 语法 {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

显示了在使用的参数 [创建行策略](create.md#create-row-policy-statement)

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

显示了在使用的参数 [创建配额](create.md#create-quota-statement)

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

显示了在使用的参数 [设置配置文件创建](create.md#create-settings-profile-statement)

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[原始文章](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
