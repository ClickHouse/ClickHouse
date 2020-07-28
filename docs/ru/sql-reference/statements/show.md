# SHOW Queries {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Возвращает один столбец типа `String` с именем statement, содержащий одно значение — запрос `CREATE TABLE`, с помощью которого был создан указанный объект.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Выводит список всех баз данных.
Запрос полностью аналогичен запросу `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Выводит содержимое таблицы [system.processes](../../operations/system-tables.md#system_tables-processes), которая содержит список запросов, выполняющихся в данный момент времени, кроме самих запросов `SHOW PROCESSLIST`.

Запрос `SELECT * FROM system.processes` возвращает данные обо всех текущих запросах.

Полезный совет (выполните в консоли):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Выводит список таблиц.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Если секция `FROM` не используется, то запрос возвращает список таблиц из текущей базы данных.

Результат, идентичный тому, что выдаёт запрос `SHOW TABLES` можно получить также запросом следующего вида:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Пример**

Следующий запрос выбирает первые две строки из списка таблиц в базе данных `system`, чьи имена содержат `co`.

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

Выводит параметры, использованные при [создании пользователя](create.md#create-user-statement).

`SHOW CREATE USER` не возвращает пароль пользователя.

### Синтаксис {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```



## SHOW CREATE ROLE {#show-create-role-statement}

Выводит параметры, использованные при [создании роли](create.md#create-role-statement).

### Синтаксис {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```



## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Выводит параметры, использованные при [создании политики доступа к строкам](create.md#create-row-policy-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```


## SHOW CREATE QUOTA {#show-create-quota-statement}

Выводит параметры, использованные при [создании квоты](create.md#create-quota-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE QUOTA [name | CURRENT]
```


## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Выводит параметры, использованные при [создании профиля настроек](create.md#create-settings-profile-statement).

### Синтаксис {#show-create-row-policy-syntax}

```sql
SHOW CREATE [SETTINGS] PROFILE name
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/show/) <!--hide-->
