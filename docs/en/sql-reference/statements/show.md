---
description: 'Documentation for Show'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'SHOW Statements'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)` hides secrets unless the following settings are turned on:

- [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (server setting)
- [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (format setting)  

Additionally, the user should have the [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect) privilege.
:::

## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE {#show-create-table--dictionary--view--database}

These statements return a single column of type String, 
containing the `CREATE` query used for creating the specified object.

### Syntax {#syntax}

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
If you use this statement to get the `CREATE` query of system tables,
you will get a *fake* query, which only declares the table structure,
but cannot be used to create a table.
:::

## SHOW DATABASES {#show-databases}

This statement prints a list of all databases.

### Syntax {#syntax-1}

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

It is identical to the query:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

### Examples {#examples}

In this example we use `SHOW` to obtain database names containing the symbol sequence 'de' in their names:

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

We can also do so in a case-insensitive manner:

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

Or get database names which do not contain 'de' in their names:

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

Finally, we can get the names of only the first two databases:

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

### See also {#see-also}

- [`CREATE DATABASE`](/sql-reference/statements/create/database)

## SHOW TABLES {#show-tables}

The `SHOW TABLES` statement displays a list of tables.

### Syntax {#syntax-2}

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns a list of tables from the current database.

This statement is identical to the query:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

### Examples {#examples-1}

In this example we use the `SHOW TABLES` statement to find all tables containing 'user' in their names:

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

We can also do so in a case-insensitive manner:

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Or to find tables which don't contain the letter 's' in their names:

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

Finally, we can get the names of only the first two tables:

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

### See also {#see-also-1}

- [`Create Tables`](/sql-reference/statements/create/table)
- [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

## SHOW COLUMNS {#show-columns}

The `SHOW COLUMNS` statement displays a list of columns.

### Syntax {#syntax-3}

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

The database and table name can be specified in abbreviated form as `<db>.<table>`, 
meaning that `FROM tab FROM db` and `FROM db.tab` are equivalent. 
If no database is specified, the query returns the list of columns from the current database.

There are also two optional keywords: `EXTENDED` and `FULL`. The `EXTENDED` keyword currently has no effect,
and exists for MySQL compatibility. The `FULL` keyword causes the output to include the collation, comment and privilege columns.

The `SHOW COLUMNS` statement produces a result table with the following structure:

| Column      | Description                                                                                                                   | Type               |
|-------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------|
| `field`     | The name of the column                                                                                                        | `String`           |
| `type`      | The column data type. If the query was made through the MySQL wire protocol, then the equivalent type name in MySQL is shown. | `String`           |
| `null`      | `YES` if the column data type is Nullable, `NO` otherwise                                                                     | `String`           |
| `key`       | `PRI` if the column is part of the primary key, `SOR` if the column is part of the sorting key, empty otherwise               | `String`           |
| `default`   | Default expression of the column if it is of type `ALIAS`, `DEFAULT`, or `MATERIALIZED`, otherwise `NULL`.                    | `Nullable(String)` |
| `extra`     | Additional information, currently unused                                                                                      | `String`           |
| `collation` | (only if `FULL` keyword was specified) Collation of the column, always `NULL` because ClickHouse has no per-column collations | `Nullable(String)` |
| `comment`   | (only if `FULL` keyword was specified) Comment on the column                                                                  | `String`           |
| `privilege` | (only if `FULL` keyword was specified) The privilege you have on this column, currently not available                         | `String`           |

### Examples {#examples-2}

In this example we'll use the `SHOW COLUMNS` statement to get information about all columns in table 'orders',
starting from 'delivery_':

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

### See also {#see-also-2}

- [`system.columns`](../../operations/system-tables/columns.md)

## SHOW DICTIONARIES {#show-dictionaries}

The `SHOW DICTIONARIES` statement displays a list of [Dictionaries](./create/dictionary/overview.md).

### Syntax {#syntax-4}

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of dictionaries from the current database.

You can get the same results as the `SHOW DICTIONARIES` query in the following way:

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

### Examples {#examples-3}

The following query selects the first two rows from the list of tables in the `system` database, whose names contain `reg`.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW INDEX {#show-index}

Displays a list of primary and data skipping indexes of a table.

This statement mostly exists for compatibility with MySQL. System tables [`system.tables`](../../operations/system-tables/tables.md) (for
primary keys) and [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (for data skipping indices)
provide equivalent information but in a fashion more native to ClickHouse.

### Syntax {#syntax-5}

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

The database and table name can be specified in abbreviated form as `<db>.<table>`, i.e. `FROM tab FROM db` and `FROM db.tab` are
equivalent. If no database is specified, the query assumes the current database as database.

The optional keyword `EXTENDED` currently has no effect, and exists for MySQL compatibility.

The statement produces a result table with the following structure:

| Column          | Description                                                                                                              | Type               |
|-----------------|--------------------------------------------------------------------------------------------------------------------------|--------------------|
| `table`         | The name of the table.                                                                                                   | `String`           |
| `non_unique`    | Always `1` as ClickHouse does not support uniqueness constraints.                                                        | `UInt8`            |
| `key_name`      | The name of the index, `PRIMARY` if the index is a primary key index.                                                    | `String`           |
| `seq_in_index`  | For a primary key index, the position of the column starting from `1`. For a data skipping index: always `1`.            | `UInt8`            |
| `column_name`   | For a primary key index, the name of the column. For a data skipping index: `''` (empty string), see field "expression". | `String`           |
| `collation`     | The sorting of the column in the index: `A` if ascending, `D` if descending, `NULL` if unsorted.                         | `Nullable(String)` |
| `cardinality`   | An estimation of the index cardinality (number of unique values in the index). Currently always 0.                       | `UInt64`           |
| `sub_part`      | Always `NULL` because ClickHouse does not support index prefixes like MySQL.                                             | `Nullable(String)` |
| `packed`        | Always `NULL` because ClickHouse does not support packed indexes (like MySQL).                                           | `Nullable(String)` |
| `null`          | Currently unused                                                                                                         |                    |
| `index_type`    | The index type, e.g. `PRIMARY`, `MINMAX`, `BLOOM_FILTER` etc.                                                            | `String`           |
| `comment`       | Additional information about the index, currently always `''` (empty string).                                            | `String`           |
| `index_comment` | `''` (empty string) because indexes in ClickHouse cannot have a `COMMENT` field (like in MySQL).                         | `String`           |
| `visible`       | If the index is visible to the optimizer, always `YES`.                                                                  | `String`           |
| `expression`    | For a data skipping index, the index expression. For a primary key index: `''` (empty string).                           | `String`           |

### Examples {#examples-4}

In this example we use the `SHOW INDEX` statement to get information about all indexes in table 'tbl'

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

### See also {#see-also-3}

- [`system.tables`](../../operations/system-tables/tables.md)
- [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

## SHOW PROCESSLIST {#show-processlist}

Outputs the content of the [`system.processes`](/operations/system-tables/processes) table, that contains a list of queries that are being processed at the moment, excluding `SHOW PROCESSLIST` queries.

### Syntax {#syntax-6}

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

The `SELECT * FROM system.processes` query returns data about all the current queries.

:::tip
Execute in the console:

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```
:::

## SHOW GRANTS {#show-grants}

The `SHOW GRANTS` statement shows privileges for a user.

### Syntax {#syntax-7}

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

If the user is not specified, the query returns privileges for the current user.

The `WITH IMPLICIT` modifier allows showing the implicit grants (e.g., `GRANT SELECT ON system.one`)

The `FINAL` modifier merges all grants from the user and its granted roles (with inheritance)

## SHOW CREATE USER {#show-create-user}

The `SHOW CREATE USER` statement shows parameters which were used at [user creation](../../sql-reference/statements/create/user.md).

### Syntax {#syntax-8}

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role}

The `SHOW CREATE ROLE` statement shows parameters which were used at [role creation](../../sql-reference/statements/create/role.md).

### Syntax {#syntax-9}

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

## SHOW CREATE ROW POLICY {#show-create-row-policy}

The `SHOW CREATE ROW POLICY` statement shows parameters which were used at [row policy creation](../../sql-reference/statements/create/row-policy.md).

### Syntax {#syntax-10}

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

## SHOW CREATE QUOTA {#show-create-quota}

The `SHOW CREATE QUOTA` statement shows parameters which were used at [quota creation](../../sql-reference/statements/create/quota.md).

### Syntax {#syntax-11}

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile}

The `SHOW CREATE SETTINGS PROFILE` statement shows parameters which were used at [settings profile creation](../../sql-reference/statements/create/settings-profile.md).

### Syntax {#syntax-12}

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

## SHOW USERS {#show-users}

The `SHOW USERS` statement returns a list of [user account](../../guides/sre/user-management/index.md#user-account-management) names. 
To view user accounts parameters, see the system table [`system.users`](/operations/system-tables/users).

### Syntax {#syntax-13}

```sql title="Syntax"
SHOW USERS
```

## SHOW ROLES {#show-roles}

The `SHOW ROLES` statement returns a list of [roles](../../guides/sre/user-management/index.md#role-management). 
To view other parameters, 
see system tables [`system.roles`](/operations/system-tables/roles) and [`system.role_grants`](/operations/system-tables/role_grants).

### Syntax {#syntax-14}

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```
## SHOW PROFILES {#show-profiles}

The `SHOW PROFILES` statement returns a list of [setting profiles](../../guides/sre/user-management/index.md#settings-profiles-management). 
To view user accounts parameters, see system table [`settings_profiles`](/operations/system-tables/settings_profiles).

### Syntax {#syntax-15}

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

## SHOW POLICIES {#show-policies}

The `SHOW POLICIES` statement returns a list of [row policies](../../guides/sre/user-management/index.md#row-policy-management) for the specified table. 
To view user accounts parameters, see system table [`system.row_policies`](/operations/system-tables/row_policies).

### Syntax {#syntax-16}

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

## SHOW QUOTAS {#show-quotas}

The `SHOW QUOTAS` statement returns a list of [quotas](../../guides/sre/user-management/index.md#quotas-management). 
To view quotas parameters, see the system table [`system.quotas`](/operations/system-tables/quotas).

### Syntax {#syntax-17}

```sql title="Syntax"
SHOW QUOTAS
```

## SHOW QUOTA {#show-quota}

The `SHOW QUOTA` statement returns a [quota](../../operations/quotas.md) consumption for all users or for current user. 
To view other parameters, see system tables [`system.quotas_usage`](/operations/system-tables/quotas_usage) and [`system.quota_usage`](/operations/system-tables/quota_usage).

### Syntax {#syntax-18}

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```
## SHOW ACCESS {#show-access}

The `SHOW ACCESS` statement shows all [users](../../guides/sre/user-management/index.md#user-account-management), [roles](../../guides/sre/user-management/index.md#role-management), [profiles](../../guides/sre/user-management/index.md#settings-profiles-management), etc. and all their [grants](../../sql-reference/statements/grant.md#privileges).

### Syntax {#syntax-19}

```sql title="Syntax"
SHOW ACCESS
```

## SHOW CLUSTER(S) {#show-clusters}

The `SHOW CLUSTER(S)` statement returns a list of clusters. 
All available clusters are listed in the [`system.clusters`](../../operations/system-tables/clusters.md) table.

:::note
The `SHOW CLUSTER name` query displays `cluster`, `shard_num`, `replica_num`, `host_name`, `host_address`, and `port` of the `system.clusters` table for the specified cluster name.
:::

### Syntax {#syntax-20}

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

### Examples {#examples-5}

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

## SHOW SETTINGS {#show-settings}

The `SHOW SETTINGS` statement returns a list of system settings and their values. 
It selects data from the [`system.settings`](../../operations/system-tables/settings.md) table.

### Syntax {#syntax-21}

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

### Clauses {#clauses}

`LIKE|ILIKE` allow to specify a matching pattern for the setting name. It can contain globs such as `%` or `_`. `LIKE` clause is case-sensitive, `ILIKE` — case insensitive.

When the `CHANGED` clause is used, the query returns only settings changed from their default values.

### Examples {#examples-6}

Query with the `LIKE` clause:

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Query with the `ILIKE` clause:

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

Query with the `CHANGED` clause:

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

## SHOW SETTING {#show-setting}

The `SHOW SETTING` statement outputs setting value for specified setting name.

### Syntax {#syntax-22}

```sql title="Syntax"
SHOW SETTING <name>
```

### See also {#see-also-4}

- [`system.settings`](../../operations/system-tables/settings.md) table

## SHOW FILESYSTEM CACHES {#show-filesystem-caches}

### Examples {#examples-7}

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

### See also {#see-also-5}

- [`system.settings`](../../operations/system-tables/settings.md) table

## SHOW ENGINES {#show-engines}

The `SHOW ENGINES` statement outputs the content of the [`system.table_engines`](../../operations/system-tables/table_engines.md) table, 
that contains description of table engines supported by server and their feature support information.

### Syntax {#syntax-23}

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

### See also {#see-also-6}

- [system.table_engines](../../operations/system-tables/table_engines.md) table

## SHOW FUNCTIONS {#show-functions}

The `SHOW FUNCTIONS` statement outputs the content of the [`system.functions`](../../operations/system-tables/functions.md) table.

### Syntax {#syntax-24}

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

If either `LIKE` or `ILIKE` clause is specified, the query returns a list of system functions whose names match the provided `<pattern>`.

### See Also {#see-also-7}

- [`system.functions`](../../operations/system-tables/functions.md) table

## SHOW MERGES {#show-merges}

The `SHOW MERGES` statement returns a list of merges. 
All merges are listed in the [`system.merges`](../../operations/system-tables/merges.md) table:

| Column              | Description                                                |
|---------------------|------------------------------------------------------------|
| `table`             | Table name.                                                |
| `database`          | The name of the database the table is in.                  |
| `estimate_complete` | The estimated time to complete (in seconds).               |
| `elapsed`           | The time elapsed (in seconds) since the merge started.     |
| `progress`          | The percentage of completed work (0-100 percent).          |
| `is_mutation`       | 1 if this process is a part mutation.                      |
| `size_compressed`   | The total size of the compressed data of the merged parts. |
| `memory_usage`      | Memory consumption of the merge process.                   |

### Syntax {#syntax-25}

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

### Examples {#examples-8}

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

## SHOW CREATE MASKING POLICY {#show-create-masking-policy}

The `SHOW CREATE MASKING POLICY` statement shows parameters which were used at [masking policy creation](../../sql-reference/statements/create/masking-policy.md).

### Syntax {#syntax-26}

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```
