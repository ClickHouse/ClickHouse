---
description: 'SHOW 文档'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'SHOW 语句'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)` 会隐藏敏感信息，除非启用以下设置：

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (服务器设置)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (格式设置)

此外，用户还应具有 [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect) 权限。
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

这些语句会返回一个仅含一列且类型为 String 的结果，
其中包含用于创建指定对象的 `CREATE` 查询。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
如果你使用此语句获取系统表的 `CREATE` 查询，
得到的将是一个*伪*查询：它只声明了表结构，
但不能用于创建表。
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

该语句会显示所有数据库的列表。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

这与以下查询完全相同：

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### 示例
</div>

在此示例中，我们使用 `SHOW` 获取名称中包含符号序列 &#39;de&#39; 的数据库名：

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

我们也可以用不区分大小写的方式来实现：

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

或者，获取名称中不包含 &#39;de&#39; 的数据库名称：

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

最后，我们可以仅获取前两个数据库的名称：

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### 另请参阅
</div>

* [`CREATE DATABASE`](/zh/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

`SHOW TABLES` 语句用于显示表列表。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果未指定 `FROM` 子句，则该查询会返回当前数据库中的表列表。

此语句与以下查询完全相同：

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### 示例
</div>

在此示例中，我们使用 `SHOW TABLES` 语句查找名称中包含 &#39;user&#39; 的所有表：

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

我们也可以用不区分大小写的方式来执行此操作：

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

或者，查找名称中不含字母 &#39;s&#39; 的表：

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

最后，我们只获取前两个表的名称：

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also-1">
  ### 另请参见
</div>

* [`Create Tables`](/zh/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

`SHOW COLUMNS` 语句用于显示列列表。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

数据库名和表名可以使用缩写形式 `<db>.<table>` 来指定，
这意味着 `FROM tab FROM db` 与 `FROM db.tab` 是等价的。
如果未指定数据库，则查询会返回当前数据库中的列列表。

还有两个可选关键字：`EXTENDED` 和 `FULL`。`EXTENDED` 关键字当前没有任何作用，
其存在是为了兼容 MySQL。`FULL` 关键字会使输出包含 `collation`、`comment` 和 `privilege` 列。

`SHOW COLUMNS` 语句会生成一个具有以下结构的结果表：

| Column      | Description                                                          | Type               |
| ----------- | -------------------------------------------------------------------- | ------------------ |
| `field`     | 列名                                                                   | `String`           |
| `type`      | 列的数据类型。如果查询是通过 MySQL wire 协议发出的，则会显示 MySQL 中对应的类型名称。                 | `String`           |
| `null`      | 如果列的数据类型为 Nullable，则为 `YES`，否则为 `NO`                                 | `String`           |
| `key`       | 如果该列是 primary key 的一部分，则为 `PRI`；如果该列是 sorting key 的一部分，则为 `SOR`；否则为空 | `String`           |
| `default`   | 如果该列的类型为 `ALIAS`、`DEFAULT` 或 `MATERIALIZED`，则为该列的默认表达式；否则为 `NULL`。   | `Nullable(String)` |
| `extra`     | 附加信息，当前未使用                                                           | `String`           |
| `collation` |  (仅当指定了 `FULL` 关键字时) 列的排序规则，始终为 `NULL`，因为 ClickHouse 不支持按列设置排序规则     | `Nullable(String)` |
| `comment`   |  (仅当指定了 `FULL` 关键字时) 列的注释                                            | `String`           |
| `privilege` |  (仅当指定了 `FULL` 关键字时) 你对此列拥有的权限，当前不可用                                 | `String`           |

<div id="examples">
  ### 示例
</div>

在此示例中，我们将使用 `SHOW COLUMNS` 语句获取表 &#39;orders&#39; 中所有以 &#39;delivery&#95;&#39; 开头的列信息：

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### 另请参阅
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

`SHOW DICTIONARIES` 语句用于显示[字典](./create/dictionary/overview.md)列表。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

如果未指定 `FROM` 子句，该查询将返回当前数据库中的字典列表。

你也可以通过以下方式获得与 `SHOW DICTIONARIES` 查询相同的结果：

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### 示例
</div>

以下查询从 `system` 数据库的表列表中选出名称包含 `reg` 的前两行记录。

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

显示表的主键和数据跳过索引列表。

该语句主要是为了兼容 MySQL 而存在。系统表 [`system.tables`](../../operations/system-tables/tables.md) (用于
主键) 和 [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (用于数据跳过索引)
提供了等效的信息，但其呈现方式更符合 ClickHouse 的原生风格。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

数据库名和表名可以用缩写形式 `<db>.<table>` 指定，即 `FROM tab FROM db` 与 `FROM db.tab` 等价。如果未指定数据库，则查询默认使用当前数据库。

可选关键字 `EXTENDED` 目前没有任何作用，仅为兼容 MySQL 而保留。

该语句会生成一个具有以下结构的结果表：

| 列               | 描述                                                               | 类型                 |
| --------------- | ---------------------------------------------------------------- | ------------------ |
| `table`         | 表名。                                                              | `String`           |
| `non_unique`    | 始终为 `1`，因为 ClickHouse 不支持唯一性约束。                                  | `UInt8`            |
| `key_name`      | 索引名称；如果该索引是主键索引，则为 `PRIMARY`。                                    | `String`           |
| `seq_in_index`  | 对于主键索引，表示列的位置，从 `1` 开始。对于数据跳过索引：始终为 `1`。                         | `UInt8`            |
| `column_name`   | 对于主键索引，表示列名。对于数据跳过索引：为 `''` (空字符串) ，参见字段 &quot;expression&quot;。 | `String`           |
| `collation`     | 列在索引中的排序方式：升序时为 `A`，降序时为 `D`，未排序时为 `NULL`。                       | `Nullable(String)` |
| `cardinality`   | 索引基数的估计值 (索引中唯一值的数量) 。当前始终为 0。                                   | `UInt64`           |
| `sub_part`      | 始终为 `NULL`，因为 ClickHouse 不支持像 MySQL 那样的索引前缀。                     | `Nullable(String)` |
| `packed`        | 始终为 `NULL`，因为 ClickHouse 不支持 packed 索引 (类似 MySQL) 。              | `Nullable(String)` |
| `null`          | 当前未使用                                                            |                    |
| `index_type`    | 索引类型，例如 `PRIMARY`、`MINMAX`、`BLOOM_FILTER` 等。                     | `String`           |
| `comment`       | 索引的附加信息，当前始终为 `''` (空字符串) 。                                      | `String`           |
| `index_comment` | `''` (空字符串) ，因为 ClickHouse 中的索引不能有 `COMMENT` 字段 (类似 MySQL) 。     | `String`           |
| `visible`       | 如果索引对优化器可见，则始终为 `YES`。                                           | `String`           |
| `expression`    | 对于数据跳过索引，表示索引表达式。对于主键索引：为 `''` (空字符串) 。                          | `String`           |

<div id="examples">
  ### 示例
</div>

在此示例中，我们使用 `SHOW INDEX` 语句获取表 &#39;tbl&#39; 中所有索引的信息。

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also">
  ### 另请参阅
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

输出 [`system.processes`](/zh/operations/system-tables/processes) 表的内容，其中包含当前正在处理的查询列表，但不包括 `SHOW PROCESSLIST` 查询。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

`SELECT * FROM system.processes` 查询会返回当前所有查询的信息。

:::tip
在控制台中执行：

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

`SHOW GRANTS` 语句用于显示用户的特权。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

如果未指定用户，则查询返回当前用户的特权。

`WITH IMPLICIT` 修饰符可显示隐式授权 (例如 `GRANT SELECT ON system.one`) 。

`FINAL` 修饰符会合并该用户及其已授予角色的所有授权 (包括继承的授权) 。

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

`SHOW CREATE USER` 语句用于显示[创建用户](../../sql-reference/statements/create/user.md)时所使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

`SHOW CREATE ROLE` 语句用于显示[创建角色](../../sql-reference/statements/create/role.md)时所使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

`SHOW CREATE ROW POLICY` 语句用于显示在[创建 ROW POLICY](../../sql-reference/statements/create/row-policy.md)时使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

`SHOW CREATE QUOTA` 语句会显示[创建 QUOTA](../../sql-reference/statements/create/quota.md)时使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

`SHOW CREATE SETTINGS PROFILE` 语句会显示[创建 settings profile](../../sql-reference/statements/create/settings-profile.md)时所使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

`SHOW USERS` 语句返回[用户账户](../../guides/sre/user-management/index.md#user-account-management)名称列表。
要查看用户账户的参数，请参见系统表 [`system.users`](/zh/operations/system-tables/users)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

`SHOW ROLES` 语句返回 [角色](../../guides/sre/user-management/index.md#role-management) 列表。
要查看其他参数，
请参阅系统表 [`system.roles`](/zh/operations/system-tables/roles) 和 [`system.role_grants`](/zh/operations/system-tables/role_grants)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

`SHOW PROFILES` 语句返回 [profile](../../guides/sre/user-management/index.md#settings-profiles-management) 列表。
要查看用户账户的参数，请参见系统表 [`settings_profiles`](/zh/operations/system-tables/settings_profiles)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

`SHOW POLICIES` 语句返回指定表的[行策略](../../guides/sre/user-management/index.md#row-policy-management)列表。
要查看用户账户的参数，请参见系统表 [`system.row_policies`](/zh/operations/system-tables/row_policies)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

`SHOW QUOTAS` 语句返回[配额](../../guides/sre/user-management/index.md#quotas-management)列表。
要查看配额参数，请参见系统表 [`system.quotas`](/zh/operations/system-tables/quotas)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

`SHOW QUOTA` 语句返回所有用户或当前用户的 [配额](../../operations/quotas.md) 使用情况。
要查看其他参数，请参见系统表 [`system.quotas_usage`](/zh/operations/system-tables/quotas_usage) 和 [`system.quota_usage`](/zh/operations/system-tables/quota_usage)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

`SHOW ACCESS` 语句会显示所有[用户](../../guides/sre/user-management/index.md#user-account-management)、[角色](../../guides/sre/user-management/index.md#role-management)、[profiles](../../guides/sre/user-management/index.md#settings-profiles-management) 等，以及它们各自拥有的所有[授权](../../sql-reference/statements/grant.md#privileges)。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

`SHOW CLUSTER(S)` 语句会返回集群列表。
所有可用集群均列在 [`system.clusters`](../../operations/system-tables/clusters.md) 表中。

:::note
`SHOW CLUSTER name` 查询会显示 `system.clusters` 表中指定集群名称对应的 `cluster`、`shard_num`、`replica_num`、`host_name`、`host_address` 和 `port`。
:::

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### 示例
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

`SHOW SETTINGS` 语句返回系统设置及其值的列表。
它从 [`system.settings`](../../operations/system-tables/settings.md) 表中查询数据。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### 子句
</div>

`LIKE|ILIKE` 可用于为设置名称指定匹配模式。该模式可包含 `%` 或 `_` 等通配符。`LIKE` 子句区分大小写，`ILIKE` 子句则不区分大小写。

使用 `CHANGED` 子句时，查询仅返回与默认值相比已发生更改的设置。

<div id="examples">
  ### 示例
</div>

使用 `LIKE` 子句的查询：

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

使用 `ILIKE` 子句的查询：

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

带有 `CHANGED` 子句的查询：

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

`SHOW SETTING` 语句会返回指定设置名称对应的设置值。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also">
  ### 另请参阅
</div>

* [`system.settings`](../../operations/system-tables/settings.md) 表

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### 示例
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also">
  ### 另请参阅
</div>

* [`system.settings`](../../operations/system-tables/settings.md) 表

<div id="show-engines">
  ## SHOW ENGINES
</div>

`SHOW ENGINES` 语句会输出 [`system.table_engines`](../../operations/system-tables/table_engines.md) 表的内容，
其中包含服务器支持的表引擎说明及其功能支持信息。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### 另请参阅
</div>

* [system.table&#95;engines](../../operations/system-tables/table_engines.md) 表

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

`SHOW FUNCTIONS` 语句会输出 [`system.functions`](../../operations/system-tables/functions.md) 表中的内容。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

如果指定了 `LIKE` 或 `ILIKE` 子句，则查询将返回名称与所提供的 `<pattern>` 匹配的系统函数列表。

<div id="see-also">
  ### 另请参阅
</div>

* [`system.functions`](../../operations/system-tables/functions.md) 表

<div id="show-merges">
  ## SHOW MERGES
</div>

`SHOW MERGES` 语句返回合并操作列表。
所有合并操作都列在 [`system.merges`](../../operations/system-tables/merges.md) 表中：

| 列                   | 描述                         |
| ------------------- | -------------------------- |
| `table`             | 表名。                        |
| `database`          | 该表所在数据库的名称。                |
| `estimate_complete` | 预计完成时间 (以秒为单位) 。           |
| `elapsed`           | 自合并开始以来已过去的时间 (以秒为单位) 。    |
| `progress`          | 已完成工作的百分比 (0–100%) 。       |
| `is_mutation`       | 如果此进程是 part mutation，则为 1。 |
| `size_compressed`   | 已合并 parts 的压缩数据总大小。        |
| `memory_usage`      | 合并过程的内存消耗。                 |

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### 示例
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

`SHOW CREATE MASKING POLICY` 语句会显示[创建数据脱敏策略](../../sql-reference/statements/create/masking-policy.md)时所使用的参数。

<div id="syntax">
  ### 语法
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```