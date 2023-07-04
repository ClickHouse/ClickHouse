---
slug: /en/sql-reference/statements/show
sidebar_position: 37
sidebar_label: SHOW
---

# SHOW Statements

N.B. `SHOW CREATE (TABLE|DATABASE|USER)` hides secrets unless
[`display_secrets_in_show_and_select` server setting](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
is turned on,
[`format_display_secrets_in_show_and_select` format setting](../../operations/settings/formats#format_display_secrets_in_show_and_select)
is turned on and user has
[`displaySecretsInShowAndSelect`](grant.md#grant-display-secrets) privilege.

## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE

``` sql
SHOW [CREATE] [TEMPORARY] TABLE|DICTIONARY|VIEW|DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

Returns a single column of type String containing the CREATE query used for creating the specified object.

`SHOW TABLE t` and `SHOW DATABASE db` have the same meaning as `SHOW CREATE TABLE|DATABASE t|db`, but `SHOW t` and `SHOW db` are not supported.

Note that if you use this statement to get `CREATE` query of system tables, you will get a *fake* query, which only declares table structure, but cannot be used to create table.

## SHOW DATABASES

Prints a list of all databases.

```sql
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

This statement is identical to the query:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

**Examples**

Getting database names, containing the symbols sequence 'de' in their names:

``` sql
SHOW DATABASES LIKE '%de%'
```

Result:

``` text
┌─name────┐
│ default │
└─────────┘
```

Getting database names, containing symbols sequence 'de' in their names, in the case insensitive manner:

``` sql
SHOW DATABASES ILIKE '%DE%'
```

Result:

``` text
┌─name────┐
│ default │
└─────────┘
```

Getting database names, not containing the symbols sequence 'de' in their names:

``` sql
SHOW DATABASES NOT LIKE '%de%'
```

Result:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

Getting the first two rows from database names:

``` sql
SHOW DATABASES LIMIT 2
```

Result:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

**See also**

- [CREATE DATABASE](https://clickhouse.com/docs/en/sql-reference/statements/create/database/#query-language-create-database)

## SHOW TABLES

Displays a list of tables.

```sql
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of tables from the current database.

This statement is identical to the query:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Examples**

Getting table names, containing the symbols sequence 'user' in their names:

``` sql
SHOW TABLES FROM system LIKE '%user%'
```

Result:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Getting table names, containing sequence 'user' in their names, in the case insensitive manner:

``` sql
SHOW TABLES FROM system ILIKE '%USER%'
```

Result:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Getting table names, not containing the symbol sequence 's' in their names:

``` sql
SHOW TABLES FROM system NOT LIKE '%s%'
```

Result:

``` text
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

Getting the first two rows from table names:

``` sql
SHOW TABLES FROM system LIMIT 2
```

Result:

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

**See also**

- [Create Tables](https://clickhouse.com/docs/en/getting-started/tutorial/#create-tables)
- [SHOW CREATE TABLE](https://clickhouse.com/docs/en/sql-reference/statements/show/#show-create-table)

## SHOW COLUMNS

Displays a list of columns

```sql
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

The database and table name can be specified in abbreviated form as `<db>.<table>`, i.e. `FROM tab FROM db` and `FROM db.tab` are
equivalent. If no database is specified, the query returns the list of columns from the current database.

The optional keyword `EXTENDED` currently has no effect, it only exists for MySQL compatibility.

The optional keyword `FULL` causes the output to include the collation, comment and privilege columns.

`SHOW COLUMNS` produces a result table with the following structure:
- field - The name of the column (String)
- type - The column data type (String)
- null - If the column data type is Nullable (UInt8)
- key - `PRI` if the column is part of the primary key, `SOR` if the column is part of the sorting key, empty otherwise (String)
- default - Default expression of the column if it is of type `ALIAS`, `DEFAULT`, or `MATERIALIZED`, otherwise `NULL`. (Nullable(String))
- extra - Additional information, currently unused (String)
- collation - (only if `FULL` keyword was specified) Collation of the column, always `NULL` because ClickHouse has no per-column collations (Nullable(String))
- comment - (only if `FULL` keyword was specified) Comment on the column (String)
- privilege - (only if `FULL` keyword was specified) The privilege you have on this column, currently not available (String)

**Examples**

Getting information about all columns in table 'order' starting with 'delivery_':

```sql
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

Result:

``` text
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

**See also**

- [system.columns](https://clickhouse.com/docs/en/operations/system-tables/columns)

## SHOW DICTIONARIES

Displays a list of [Dictionaries](../../sql-reference/dictionaries/index.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of dictionaries from the current database.

You can get the same results as the `SHOW DICTIONARIES` query in the following way:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Examples**

The following query selects the first two rows from the list of tables in the `system` database, whose names contain `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW INDEX

Displays a list of primary and data skipping indexes of a table.

```sql
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

The database and table name can be specified in abbreviated form as `<db>.<table>`, i.e. `FROM tab FROM db` and `FROM db.tab` are
equivalent. If no database is specified, the query assumes the current database as database.

The optional keyword `EXTENDED` currently has no effect, it only exists for MySQL compatibility.

`SHOW INDEX` produces a result table with the following structure:
- table - The name of the table (String)
- non_unique - 0 if the index cannot contain duplicates, 1 otherwise (UInt8)
- key_name - The name of the index, `PRIMARY` if the index is a primary key index (String)
- seq_in_index - Currently unused
- column_name - Currently unused
- collation - The sorting of the column in the index, `A` if ascending, `D` if descending, `NULL` if unsorted (Nullable(String))
- cardinality - Currently unused
- sub_part - Currently unused
- packed - Currently unused
- null - Currently unused
- index_type - The index type, e.g. `primary`, `minmax`, `bloom_filter` etc. (String)
- comment - Currently unused
- index_comment - Currently unused
- visible - If the index is visible to the optimizer, always `YES` (String)
- expression - The index expression (String)

**Examples**

Getting information about all indexes in table 'tbl'

```sql
SHOW INDEX FROM 'tbl'
```

Result:

``` text
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          0 │ blf_idx  │ ᴺᵁᴸᴸ         │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ bloom_filter │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ          │ YES     │ d, b       │
│ tbl   │          0 │ mm1_idx  │ ᴺᵁᴸᴸ         │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ minmax       │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ          │ YES     │ a, c, d    │
│ tbl   │          0 │ mm2_idx  │ ᴺᵁᴸᴸ         │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ minmax       │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ          │ YES     │ c, d, e    │
│ tbl   │          0 │ PRIMARY  │ ᴺᵁᴸᴸ         │ ᴺᵁᴸᴸ        │ A         │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ primary      │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ          │ YES     │ c, a       │
│ tbl   │          0 │ set_idx  │ ᴺᵁᴸᴸ         │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ        │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ set          │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ          │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

**See also**

- [system.tables](../../operations/system-tables/tables.md)
- [system.data_skipping_indices](../../operations/system-tables/data_skipping_indices.md)

## SHOW PROCESSLIST

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Outputs the content of the [system.processes](../../operations/system-tables/processes.md#system_tables-processes) table, that contains a list of queries that is being processed at the moment, excepting `SHOW PROCESSLIST` queries.

The `SELECT * FROM system.processes` query returns data about all the current queries.

Tip (execute in the console):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW GRANTS

Shows privileges for a user.

**Syntax**

``` sql
SHOW GRANTS [FOR user1 [, user2 ...]]
```

If user is not specified, the query returns privileges for the current user.

## SHOW CREATE USER

Shows parameters that were used at a [user creation](../../sql-reference/statements/create/user.md).

**Syntax**

``` sql
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

## SHOW CREATE ROLE

Shows parameters that were used at a [role creation](../../sql-reference/statements/create/role.md).

**Syntax**

``` sql
SHOW CREATE ROLE name1 [, name2 ...]
```

## SHOW CREATE ROW POLICY

Shows parameters that were used at a [row policy creation](../../sql-reference/statements/create/row-policy.md).

**Syntax**

``` sql
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

## SHOW CREATE QUOTA

Shows parameters that were used at a [quota creation](../../sql-reference/statements/create/quota.md).

**Syntax**

``` sql
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE

Shows parameters that were used at a [settings profile creation](../../sql-reference/statements/create/settings-profile.md).

**Syntax**

``` sql
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

## SHOW USERS

Returns a list of [user account](../../guides/sre/user-management/index.md#user-account-management) names. To view user accounts parameters, see the system table [system.users](../../operations/system-tables/users.md#system_tables-users).

**Syntax**

``` sql
SHOW USERS
```

## SHOW ROLES

Returns a list of [roles](../../guides/sre/user-management/index.md#role-management). To view another parameters, see system tables [system.roles](../../operations/system-tables/roles.md#system_tables-roles) and [system.role_grants](../../operations/system-tables/role-grants.md#system_tables-role_grants).

**Syntax**

``` sql
SHOW [CURRENT|ENABLED] ROLES
```
## SHOW PROFILES

Returns a list of [setting profiles](../../guides/sre/user-management/index.md#settings-profiles-management). To view user accounts parameters, see the system table [settings_profiles](../../operations/system-tables/settings_profiles.md#system_tables-settings_profiles).

**Syntax**

``` sql
SHOW [SETTINGS] PROFILES
```

## SHOW POLICIES

Returns a list of [row policies](../../guides/sre/user-management/index.md#row-policy-management) for the specified table. To view user accounts parameters, see the system table [system.row_policies](../../operations/system-tables/row_policies.md#system_tables-row_policies).

**Syntax**

``` sql
SHOW [ROW] POLICIES [ON [db.]table]
```

## SHOW QUOTAS

Returns a list of [quotas](../../guides/sre/user-management/index.md#quotas-management). To view quotas parameters, see the system table [system.quotas](../../operations/system-tables/quotas.md#system_tables-quotas).

**Syntax**

``` sql
SHOW QUOTAS
```

## SHOW QUOTA

Returns a [quota](../../operations/quotas.md) consumption for all users or for current user. To view another parameters, see system tables [system.quotas_usage](../../operations/system-tables/quotas_usage.md#system_tables-quotas_usage) and [system.quota_usage](../../operations/system-tables/quota_usage.md#system_tables-quota_usage).

**Syntax**

``` sql
SHOW [CURRENT] QUOTA
```
## SHOW ACCESS

Shows all [users](../../guides/sre/user-management/index.md#user-account-management), [roles](../../guides/sre/user-management/index.md#role-management), [profiles](../../guides/sre/user-management/index.md#settings-profiles-management), etc. and all their [grants](../../sql-reference/statements/grant.md#grant-privileges).

**Syntax**

``` sql
SHOW ACCESS
```
## SHOW CLUSTER(s)

Returns a list of clusters. All available clusters are listed in the [system.clusters](../../operations/system-tables/clusters.md) table.

:::note
`SHOW CLUSTER name` query displays the contents of system.clusters table for this cluster.
:::

**Syntax**

``` sql
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

**Examples**

Query:

``` sql
SHOW CLUSTERS;
```

Result:

```text
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

Query:

``` sql
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

Result:

```text
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

Query:

``` sql
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

Result:

```text
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
estimated_recovery_time: 0
```

## SHOW SETTINGS

Returns a list of system settings and their values. Selects data from the [system.settings](../../operations/system-tables/settings.md) table.

**Syntax**

```sql
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

**Clauses**

`LIKE|ILIKE` allow to specify a matching pattern for the setting name. It can contain globs such as `%` or `_`. `LIKE` clause is case-sensitive, `ILIKE` — case insensitive.

When the `CHANGED` clause is used, the query returns only settings changed from their default values.

**Examples**

Query with the `LIKE` clause:

```sql
SHOW SETTINGS LIKE 'send_timeout';
```
Result:

```text
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Query with the `ILIKE` clause:

```sql
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

Result:

```text
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

Query with the `CHANGED` clause:

```sql
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

Result:

```text
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

## SHOW FILESYSTEM CACHES

```sql
SHOW FILESYSTEM CACHES
```

Result:

``` text
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

**See Also**

- [system.settings](../../operations/system-tables/settings.md) table

## SHOW ENGINES

``` sql
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

Outputs the content of the [system.table_engines](../../operations/system-tables/table_engines.md) table, that contains description of table engines supported by server and their feature support information.

**See Also**

- [system.table_engines](../../operations/system-tables/table_engines.md) table
