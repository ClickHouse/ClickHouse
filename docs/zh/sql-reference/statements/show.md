---
toc_priority: 38
toc_title: SHOW
---

# SHOW 查询 {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```
返回单个字符串类型的 ‘statement’列，其中只包含了一个值 - 用来创建指定对象的 `CREATE` 语句。


## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

打印所有的数据库列表，该查询等同于 `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

输出 [system.processes](../../operations/system-tables/processes.md#system_tables-processes)表的内容，包含有当前正在处理的请求列表，除了 `SHOW PROCESSLIST`查询。


 `SELECT * FROM system.processes` 查询返回和当前请求相关的所有数据


提示 (在控制台执行):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

显示表的清单

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果未使用 `FROM` 字句，该查询返回当前数据库的所有表清单

可以用下面的方式获得和 `SHOW TABLES`一样的结果：

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**示例**

下列查询获取最前面的2个位于`system`库中且表名包含 `co`的表。

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

以列表形式显示 [外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果 `FROM`字句没有指定，返回当前数据库的字典列表

可以通过下面的查询获取和 `SHOW DICTIONARIES`相同的结果：

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**示例**

下列查询获取最前面的2个位于 `system`库中且名称包含 `reg`的字典表。

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

显示用户的权限

### 语法 {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

如果未指定用户，输出当前用户的权限

## SHOW CREATE USER {#show-create-user-statement}

显示  [user creation](../../sql-reference/statements/create.md#create-user-statement)用到的参数。

`SHOW CREATE USER` 不会输出用户的密码信息

### 语法 {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

显示 [role creation](../../sql-reference/statements/create.md#create-role-statement) 中用到的参数。

### 语法 {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

显示 [row policy creation](../../sql-reference/statements/create.md#create-row-policy-statement)中用到的参数

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

显示 [quota creation](../../sql-reference/statements/create.md#create-quota-statement)中用到的参数

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

显示 [settings profile creation](../../sql-reference/statements/create.md#create-settings-profile-statement)中用到的参数

### 语法 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[原始文档](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
