---
toc_priority: 37
toc_title: SHOW
---

# SHOW Queries {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY|VIEW] [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

Возвращает один столбец типа `String` с именем statement, содержащий одно значение — запрос `CREATE`, с помощью которого был создан указанный объект.

## SHOW DATABASES {#show-databases}

Выводит список всех баз данных.

```sql
SHOW DATABASES [LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

Этот запрос идентичен запросу:

```sql
SELECT name FROM system.databases [WHERE name LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

### Примеры {#examples}

Получение списка баз данных, имена которых содержат последовательность символов 'de':

``` sql
SHOW DATABASES LIKE '%de%'
```

Результат:

``` text
┌─name────┐
│ default │
└─────────┘
```

Получение списка баз данных, имена которых содержат последовательность символов 'de' независимо от регистра:

``` sql
SHOW DATABASES ILIKE '%DE%'
```

Результат:

``` text
┌─name────┐
│ default │
└─────────┘
```

Получение списка баз данных, имена которых не содержат последовательность символов 'de':

``` sql
SHOW DATABASES NOT LIKE '%de%'
```

Результат:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

Получение первых двух строк из списка имен баз данных:

``` sql
SHOW DATABASES LIMIT 2
```

Результат:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

### Смотрите также {#see-also}

-   [CREATE DATABASE](https://clickhouse.com/docs/ru/sql-reference/statements/create/database/#query-language-create-database)

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Выводит содержимое таблицы [system.processes](../../operations/system-tables/processes.md#system_tables-processes), которая содержит список запросов, выполняющихся в данный момент времени, кроме самих запросов `SHOW PROCESSLIST`.

Запрос `SELECT * FROM system.processes` возвращает данные обо всех текущих запросах.

Полезный совет (выполните в консоли):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Выводит список таблиц.

```sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Если условие `FROM` не указано, запрос возвращает список таблиц из текущей базы данных.

Этот запрос идентичен запросу:

```sql
SELECT name FROM system.tables [WHERE name LIKE | ILIKE | NOT LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

### Примеры {#examples}

Получение списка таблиц, имена которых содержат последовательность символов 'user':

``` sql
SHOW TABLES FROM system LIKE '%user%'
```

Результат:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Получение списка таблиц, имена которых содержат последовательность символов 'user' без учета регистра:

``` sql
SHOW TABLES FROM system ILIKE '%USER%'
```

Результат:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Получение списка таблиц, имена которых не содержат символ 's':

``` sql
SHOW TABLES FROM system NOT LIKE '%s%'
```

Результат:

``` text
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

Получение первых двух строк из списка таблиц:

``` sql
SHOW TABLES FROM system LIMIT 2
```

Результат:

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

### Смотрите также {#see-also}

-   [Create Tables](https://clickhouse.com/docs/ru/getting-started/tutorial/#create-tables)
-   [SHOW CREATE TABLE](https://clickhouse.com/docs/ru/sql-reference/statements/show/#show-create-table)

## SHOW DICTIONARIES {#show-dictionaries}

Выводит список [внешних словарей](../../sql-reference/statements/show.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Если секция `FROM` не указана, запрос возвращает список словарей из текущей базы данных.

Аналогичный результат можно получить следующим запросом:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Example**

Запрос выводит первые две стоки из списка таблиц в базе данных `system`, имена которых содержат `reg`.

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

Выводит привилегии пользователя.

### Синтаксис {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

Если пользователь не задан, запрос возвращает привилегии текущего пользователя.



## SHOW CREATE USER {#show-create-user-statement}

Выводит параметры, использованные при [создании пользователя](create/user.md#create-user-statement).

`SHOW CREATE USER` не возвращает пароль пользователя.

### Синтаксис {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Выводит параметры, использованные при [создании роли](create/role.md#create-role-statement).

### Синтаксис {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name1 [, name2 ...]
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Выводит параметры, использованные при [создании политики доступа к строкам](create/row-policy.md#create-row-policy-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Выводит параметры, использованные при [создании квоты](create/quota.md#create-quota-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Выводит параметры, использованные при [создании профиля настроек](create/settings-profile.md#create-settings-profile-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

## SHOW USERS {#show-users-statement}

Выводит список [пользовательских аккаунтов](../../operations/access-rights.md#user-account-management). Для просмотра параметров пользовательских аккаунтов, см. системную таблицу [system.users](../../operations/system-tables/users.md#system_tables-users).

### Синтаксис {#show-users-syntax}

``` sql
SHOW USERS
```

## SHOW ROLES {#show-roles-statement}

Выводит список [ролей](../../operations/access-rights.md#role-management). Для просмотра параметров ролей, см. системные таблицы [system.roles](../../operations/system-tables/roles.md#system_tables-roles) и [system.role-grants](../../operations/system-tables/role-grants.md#system_tables-role_grants).

### Синтаксис {#show-roles-syntax}

``` sql
SHOW [CURRENT|ENABLED] ROLES
```

## SHOW PROFILES {#show-profiles-statement}

Выводит список [профилей настроек](../../operations/access-rights.md#settings-profiles-management). Для просмотра других параметров профилей настроек, см. системную таблицу [settings_profiles](../../operations/system-tables/settings_profiles.md#system_tables-settings_profiles).

### Синтаксис {#show-profiles-syntax}

``` sql
SHOW [SETTINGS] PROFILES
```

## SHOW POLICIES {#show-policies-statement}

Выводит список [политик доступа к строкам](../../operations/access-rights.md#row-policy-management) для указанной таблицы. Для просмотра других параметров, см. системную таблицу [system.row_policies](../../operations/system-tables/row_policies.md#system_tables-row_policies).

### Синтаксис {#show-policies-syntax}

``` sql
SHOW [ROW] POLICIES [ON [db.]table]
```

## SHOW QUOTAS {#show-quotas-statement}

Выводит список [квот](../../operations/access-rights.md#quotas-management). Для просмотра параметров квот, см. системную таблицу [system.quotas](../../operations/system-tables/quotas.md#system_tables-quotas).

### Синтаксис {#show-quotas-syntax}

``` sql
SHOW QUOTAS
```

## SHOW QUOTA {#show-quota-statement}

Выводит потребление [квоты](../../operations/quotas.md) для всех пользователей или только для текущего пользователя. Для просмотра других параметров, см. системные таблицы [system.quotas_usage](../../operations/system-tables/quotas_usage.md#system_tables-quotas_usage) и [system.quota_usage](../../operations/system-tables/quota_usage.md#system_tables-quota_usage).

### Синтаксис {#show-quota-syntax}

``` sql
SHOW [CURRENT] QUOTA
```

## SHOW ACCESS {#show-access-statement}

Выводит список всех [пользователей](../../operations/access-rights.md#user-account-management), [ролей](../../operations/access-rights.md#role-management), [профилей](../../operations/access-rights.md#settings-profiles-management) и пр., а также все [привилегии](../../sql-reference/statements/grant.md#grant-privileges).

### Синтаксис {#show-access-syntax}

``` sql
SHOW ACCESS
```

## SHOW CLUSTER(s) {#show-cluster-statement}

Возвращает список кластеров. Все доступные кластеры перечислены в таблице [system.clusters](../../operations/system-tables/clusters.md).

!!! info "Note"
    По запросу `SHOW CLUSTER name` вы получите содержимое таблицы system.clusters для этого кластера.

### Синтаксис {#show-cluster-syntax}

``` sql
SHOW CLUSTER '<name>'
SHOW CLUSTERS [LIKE|NOT LIKE '<pattern>'] [LIMIT <N>]
```
### Примеры {#show-cluster-examples}

Запрос:

``` sql
SHOW CLUSTERS;
```

Результат:

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

Запрос:

``` sql
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

Результат:

```text
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

Запрос:

``` sql
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

Результат:

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

Возвращает список системных настроек и их значений. Использует данные из таблицы [system.settings](../../operations/system-tables/settings.md).

**Синтаксис**

```sql
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

**Секции**

При использовании `LIKE|ILIKE` можно задавать шаблон для имени настройки. Этот шаблон может содержать символы подстановки, такие как `%` или `_`. При использовании `LIKE` шаблон чувствителен к регистру, а при использовании `ILIKE` — не чувствителен.

Если используется `CHANGED`, запрос вернет только те настройки, значения которых были изменены, т.е. отличны от значений по умолчанию.

**Примеры**

Запрос с использованием `LIKE`:

```sql
SHOW SETTINGS LIKE 'send_timeout';
```
Результат:

```text
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Запрос с использованием `ILIKE`:

```sql
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

Результат:

```text
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

Запрос с использованием `CHANGED`:

```sql
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

Результат:

```text
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

**См. также**

-   Таблица [system.settings](../../operations/system-tables/settings.md)
