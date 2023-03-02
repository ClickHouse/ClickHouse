---
toc_priority: 37
toc_title: SHOW
---

# SHOW Statements {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY|VIEW] [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `String`-type ‘statement’ column, which contains a single value – the `CREATE` query used for creating the specified object.

Note that if you use this statement to get `CREATE` query of system tables, you will get a *fake* query, which only declares table structure, but cannot be used to create table.

## SHOW DATABASES {#show-databases}

Prints a list of all databases.

```sql
SHOW DATABASES [LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

This statement is identical to the query:

```sql
SELECT name FROM system.databases [WHERE name LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

### Examples {#examples}

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

### See Also {#see-also}

-   [CREATE DATABASE](https://clickhouse.com/docs/en/sql-reference/statements/create/database/#query-language-create-database)

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Outputs the content of the [system.processes](../../operations/system-tables/processes.md#system_tables-processes) table, that contains a list of queries that is being processed at the moment, excepting `SHOW PROCESSLIST` queries.

The `SELECT * FROM system.processes` query returns data about all the current queries.

Tip (execute in the console):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Displays a list of tables.

```sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of tables from the current database.

This statement is identical to the query:

```sql
SELECT name FROM system.tables [WHERE name LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

### Examples {#examples}

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

### See Also {#see-also}

-   [Create Tables](https://clickhouse.com/docs/en/getting-started/tutorial/#create-tables)
-   [SHOW CREATE TABLE](https://clickhouse.com/docs/en/sql-reference/statements/show/#show-create-table)

## SHOW DICTIONARIES {#show-dictionaries}

Displays a list of [external dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of dictionaries from the current database.

You can get the same results as the `SHOW DICTIONARIES` query in the following way:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Example**

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

## SHOW GRANTS {#show-grants-statement}

Shows privileges for a user.

### Syntax {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user1 [, user2 ...]]
```

If user is not specified, the query returns privileges for the current user.

## SHOW CREATE USER {#show-create-user-statement}

Shows parameters that were used at a [user creation](../../sql-reference/statements/create/user.md).

`SHOW CREATE USER` does not output user passwords.

### Syntax {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Shows parameters that were used at a [role creation](../../sql-reference/statements/create/role.md).

### Syntax {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name1 [, name2 ...]
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Shows parameters that were used at a [row policy creation](../../sql-reference/statements/create/row-policy.md).

### Syntax {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Shows parameters that were used at a [quota creation](../../sql-reference/statements/create/quota.md).

### Syntax {#show-create-quota-syntax}

``` sql
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Shows parameters that were used at a [settings profile creation](../../sql-reference/statements/create/settings-profile.md).

### Syntax {#show-create-settings-profile-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

## SHOW USERS {#show-users-statement}

Returns a list of [user account](../../operations/access-rights.md#user-account-management) names. To view user accounts parameters, see the system table [system.users](../../operations/system-tables/users.md#system_tables-users).

### Syntax {#show-users-syntax}

``` sql
SHOW USERS
```

## SHOW ROLES {#show-roles-statement}

Returns a list of [roles](../../operations/access-rights.md#role-management). To view another parameters, see system tables [system.roles](../../operations/system-tables/roles.md#system_tables-roles) and [system.role-grants](../../operations/system-tables/role-grants.md#system_tables-role_grants).

### Syntax {#show-roles-syntax}

``` sql
SHOW [CURRENT|ENABLED] ROLES
```
## SHOW PROFILES {#show-profiles-statement}

Returns a list of [setting profiles](../../operations/access-rights.md#settings-profiles-management). To view user accounts parameters, see the system table [settings_profiles](../../operations/system-tables/settings_profiles.md#system_tables-settings_profiles).

### Syntax {#show-profiles-syntax}

``` sql
SHOW [SETTINGS] PROFILES
```

## SHOW POLICIES {#show-policies-statement}

Returns a list of [row policies](../../operations/access-rights.md#row-policy-management) for the specified table. To view user accounts parameters, see the system table [system.row_policies](../../operations/system-tables/row_policies.md#system_tables-row_policies).

### Syntax {#show-policies-syntax}

``` sql
SHOW [ROW] POLICIES [ON [db.]table]
```

## SHOW QUOTAS {#show-quotas-statement}

Returns a list of [quotas](../../operations/access-rights.md#quotas-management). To view quotas parameters, see the system table [system.quotas](../../operations/system-tables/quotas.md#system_tables-quotas).

### Syntax {#show-quotas-syntax}

``` sql
SHOW QUOTAS
```

## SHOW QUOTA {#show-quota-statement}

Returns a [quota](../../operations/quotas.md) consumption for all users or for current user. To view another parameters, see system tables [system.quotas_usage](../../operations/system-tables/quotas_usage.md#system_tables-quotas_usage) and [system.quota_usage](../../operations/system-tables/quota_usage.md#system_tables-quota_usage).

### Syntax {#show-quota-syntax}

``` sql
SHOW [CURRENT] QUOTA
```
## SHOW ACCESS {#show-access-statement}

Shows all [users](../../operations/access-rights.md#user-account-management), [roles](../../operations/access-rights.md#role-management), [profiles](../../operations/access-rights.md#settings-profiles-management), etc. and all their [grants](../../sql-reference/statements/grant.md#grant-privileges).

### Syntax {#show-access-syntax}

``` sql
SHOW ACCESS
```
## SHOW CLUSTER(s) {#show-cluster-statement}

Returns a list of clusters. All available clusters are listed in the [system.clusters](../../operations/system-tables/clusters.md) table.

!!! info "Note"
    `SHOW CLUSTER name` query displays the contents of system.clusters table for this cluster.

### Syntax {#show-cluster-syntax}

``` sql
SHOW CLUSTER '<name>'
SHOW CLUSTERS [LIKE|NOT LIKE '<pattern>'] [LIMIT <N>]
```
### Examples {#show-cluster-examples}

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

## SHOW SETTINGS {#show-settings}

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

**See Also**

-   [system.settings](../../operations/system-tables/settings.md) table

[Original article](https://clickhouse.com/docs/en/sql-reference/statements/show/) <!--hide-->
