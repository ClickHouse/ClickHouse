---
toc_priority: 38
toc_title: GRANT
---

# GRANT

- Присваивает [привилегии](#grant-privileges) пользователям или ролям ClickHouse.
- Назначает роли пользователям или другим ролям.

Отозвать привилегию можно с помощью выражения [REVOKE](revoke.md). Чтобы вывести список присвоенных привилегий, воспользуйтесь выражением [SHOW GRANTS](show.md#show-grants-statement).

## Синтаксис присвоения привилегий {#grant-privigele-syntax}

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

- `privilege` — Тип привилегии
- `role` — Роль пользователя ClickHouse.
- `user` — Пользователь ClickHouse.

`WITH GRANT OPTION` разрешает пользователю или роли выполнять запрос `GRANT`. Пользователь может выдавать только те привилегии, которые есть у него, той же или меньшей области действий.


## Синтаксис назначения ролей {#assign-role-syntax}

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

- `role` — Роль пользователя ClickHouse.
- `user` — Пользователь ClickHouse.

`WITH ADMIN OPTION` присваивает привилегию [ADMIN OPTION](#admin-option-privilege) пользователю или роли.

## Использование {#grant-usage}

Для использования `GRANT` пользователь должен иметь привилегию `GRANT OPTION`. Пользователь может выдавать привилегии только внутри области действий назначенных ему самому привилегий.

Например, администратор выдал привилегию пользователю `john`:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Это означает, что пользователю `john` разрешено выполнять:

- `SELECT x,y FROM db.table`.
- `SELECT x FROM db.table`.
- `SELECT y FROM db.table`.

`john` не может выполнить `SELECT z FROM db.table` или `SELECT * FROM db.table`. После обработки данных запросов ClickHouse ничего не вернет — даже `x` или `y`. Единственное исключение — если таблица содержит только столбцы `x` и `y`. В таком случае ClickHouse вернет все данные.

Также у `john` есть привилегия `GRANT OPTION`. `john` может выдать другим пользователям привилегии той же или меньшей области действий из тех, которые есть у него.

При присвоении привилегий допускается использовать астериск (`*`) вместо имени таблицы или базы данных. Например, запрос `GRANT SELECT ON db.* TO john` позволит пользователю `john` выполнять `SELECT` над всеми таблицам в базе данных `db`. Также вы можете опускать имя базы данных. В таком случае привилегии позволят совершать операции над текущей базой данных. Например, запрос `GRANT SELECT ON * TO john` выдаст привилегию на выполнение `SELECT` над всеми таблицами в текущей базе данных; `GRANT SELECT ON mytable TO john` — только над таблицей `mytable` в текущей базе данных.

Доступ к базе данных `system` разрешен всегда (данная база данных используется при обработке запросов).

Вы можете присвоить несколько привилегий нескольким пользователям в одном запросе. Запрос `GRANT SELECT, INSERT ON *.* TO john, robin` позволит пользователям `john` и `robin` выполнять `INSERT` и `SELECT` над всеми таблицами всех баз данных на сервере.


## Привилегии {#grant-privileges}

Привилегия — это разрешение на выполнение определенного типа запросов.

Привилегии имеют иерархическую структуру. Набор разрешенных запросов зависит от области действия привилегии.

Иерархия привилегий:

- [SELECT](#grant-select)
- [INSERT](#grant-insert)
- [ALTER](#grant-alter)
    - `ALTER TABLE`
        - `ALTER UPDATE`
        - `ALTER DELETE`
        - `ALTER COLUMN`
            - `ALTER ADD COLUMN`
            - `ALTER DROP COLUMN`
            - `ALTER MODIFY COLUMN`
            - `ALTER COMMENT COLUMN`
            - `ALTER CLEAR COLUMN`
            - `ALTER RENAME COLUMN`
        - `ALTER INDEX`
            - `ALTER ORDER BY`
            - `ALTER SAMPLE BY`			
            - `ALTER ADD INDEX`
            - `ALTER DROP INDEX`
            - `ALTER MATERIALIZE INDEX`
            - `ALTER CLEAR INDEX`
        - `ALTER CONSTRAINT`
            - `ALTER ADD CONSTRAINT`
            - `ALTER DROP CONSTRAINT`
        - `ALTER TTL`
            - `ALTER MATERIALIZE TTL`
        - `ALTER SETTINGS`
        - `ALTER MOVE PARTITION`
        - `ALTER FETCH PARTITION`
        - `ALTER FREEZE PARTITION`
    - `ALTER VIEW`
        - `ALTER VIEW REFRESH `
        - `ALTER VIEW MODIFY QUERY`
- [CREATE](#grant-create)
    - `CREATE DATABASE`
    - `CREATE TABLE`
        - `CREATE TEMPORARY TABLE`
    - `CREATE VIEW`
    - `CREATE DICTIONARY`
- [DROP](#grant-drop)
    - `DROP DATABASE`
    - `DROP TABLE`
    - `DROP VIEW`
    - `DROP DICTIONARY`
- [TRUNCATE](#grant-truncate)
- [OPTIMIZE](#grant-optimize)
- [SHOW](#grant-show)
    - `SHOW DATABASES`
    - `SHOW TABLES`
    - `SHOW COLUMNS`
    - `SHOW DICTIONARIES`
- [KILL QUERY](#grant-kill-query)
- [ACCESS MANAGEMENT](#grant-access-management)
    - `CREATE USER`
    - `ALTER USER`
    - `DROP USER`
    - `CREATE ROLE`
    - `ALTER ROLE`
    - `DROP ROLE`
    - `CREATE ROW POLICY`
    - `ALTER ROW POLICY`
    - `DROP ROW POLICY`
    - `CREATE QUOTA`
    - `ALTER QUOTA`
    - `DROP QUOTA`
    - `CREATE SETTINGS PROFILE`
    - `ALTER SETTINGS PROFILE`
    - `DROP SETTINGS PROFILE`
    - `SHOW ACCESS`
        - `SHOW_USERS`
        - `SHOW_ROLES`
        - `SHOW_ROW_POLICIES`
        - `SHOW_QUOTAS`
        - `SHOW_SETTINGS_PROFILES`
    - `ROLE ADMIN`
- [SYSTEM](#grant-system)
    - `SYSTEM SHUTDOWN`
    - `SYSTEM DROP CACHE`
        - `SYSTEM DROP DNS CACHE`
        - `SYSTEM DROP MARK CACHE`
        - `SYSTEM DROP UNCOMPRESSED CACHE`
    - `SYSTEM RELOAD`
        - `SYSTEM RELOAD CONFIG`
        - `SYSTEM RELOAD DICTIONARY`
            - `SYSTEM RELOAD EMBEDDED DICTIONARIES`
    - `SYSTEM MERGES`
    - `SYSTEM TTL MERGES`
    - `SYSTEM FETCHES`
    - `SYSTEM MOVES`
    - `SYSTEM SENDS`
        - `SYSTEM DISTRIBUTED SENDS`
        - `SYSTEM REPLICATED SENDS`
    - `SYSTEM REPLICATION QUEUES`
    - `SYSTEM SYNC REPLICA`
    - `SYSTEM RESTART REPLICA`
    - `SYSTEM FLUSH`
        - `SYSTEM FLUSH DISTRIBUTED`
        - `SYSTEM FLUSH LOGS`
- [INTROSPECTION](#grant-introspection)
    - `addressToLine`
    - `addressToSymbol`
    - `demangle`
- [SOURCES](#grant-sources)
    - `FILE`
    - `URL`
    - `REMOTE`
    - `MYSQL`
    - `ODBC`
    - `JDBC`
    - `HDFS`
    - `S3`
- [dictGet](#grant-dictget)

Примеры того, как трактуется данная иерархия:

- Привилегия `ALTER` включает все остальные `ALTER*` привилегии. 
- `ALTER CONSTRAINT` включает `ALTER ADD CONSTRAINT` и `ALTER DROP CONSTRAINT`.

Привилегии применяются на разных уровнях. Уровень определяет синтаксис присваивания привилегии.

Уровни (от низшего к высшему):

- `COLUMN` — Привилегия присваивается для столбца, таблицы, базы данных или глобально.
- `TABLE` — Привилегия присваивается для таблицы, базы данных или глобально.
- `VIEW` — Привилегия присваивается для представления, базы данных или глобально.
- `DICTIONARY` — Привилегия присваивается для словаря, базы данных или глобально.
- `DATABASE` — Привилегия присваивается для базы данных или глобально.
- `GLOBAL` — Привилегия присваивается только глобально.
- `GROUP` — Группирует привилегии разных уровней. При присвоении привилегии уровня `GROUP` присваиваются только привилегии из группы в соответствии с используемым синтаксисом.

Примеры допустимого синтаксиса:

- `GRANT SELECT(x) ON db.table TO user`
- `GRANT SELECT ON db.* TO user`

Примеры недопустимого синтаксиса:

- `GRANT CREATE USER(x) ON db.table TO user`
- `GRANT CREATE USER ON db.* TO user`

Специальная привилегия [ALL](#grant-all) присваивает все привилегии пользователю или роли.

По умолчанию пользователь или роль не имеют привилегий.

Отсутствие привилегий у пользователя или роли отображается как привилегия [NONE](#grant-none).

Выполнение некоторых запросов требует определенного набора привилегий. Например, чтобы выполнить запрос [RENAME](misc.md#misc_operations-rename), нужны следующие привилегии: `SELECT`, `CREATE TABLE`, `INSERT` и `DROP TABLE`.


### SELECT {#grant-select}

Разрешает выполнять запросы [SELECT](select/index.md).

Уровень: `COLUMN`.

**Описание**

Пользователь с данной привилегией может выполнять запросы `SELECT` над определенными столбцами из определенной таблицы и базы данных. При включении в запрос других столбцов запрос ничего не вернет.

Рассмотрим следующую привилегию:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

Данная привилегия позволяет пользователю `john` выполнять выборку данных из столбцов `x` и/или `y` в `db.table`, например, `SELECT x FROM db.table`. `john` не может выполнить `SELECT z FROM db.table` или `SELECT * FROM db.table`. После обработки данных запросов ClickHouse ничего не вернет — даже `x` или `y`. Единственное исключение — если таблица содержит только столбцы `x` и `y`. В таком случае ClickHouse вернет все данные.

### INSERT {#grant-insert}

Разрешает выполнять запросы [INSERT](insert-into.md).

Уровень: `COLUMN`.

**Описание**

Пользователь с данной привилегией может выполнять запросы `INSERT` над определенными столбцами из определенной таблицы и базы данных. При включении в запрос других столбцов запрос не добавит никаких данных.

**Пример**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

Присвоенная привилегия позволит пользователю `john` вставить данные в столбцы `x` и/или `y` в `db.table`.

### ALTER {#grant-alter}

Разрешает выполнять запросы [ALTER](alter/index.md) в соответствии со следующей иерархией привилегий:

- `ALTER`. Уровень: `COLUMN`. 
    - `ALTER TABLE`. Уровень: `GROUP`
        - `ALTER UPDATE`. Уровень: `COLUMN`.  Алиасы: `UPDATE`
        - `ALTER DELETE`. Уровень: `COLUMN`. Алиасы: `DELETE`
        - `ALTER COLUMN`. Уровень: `GROUP`
            - `ALTER ADD COLUMN`. Уровень: `COLUMN`. Алиасы: `ADD COLUMN`
            - `ALTER DROP COLUMN`. Уровень: `COLUMN`. Алиасы: `DROP COLUMN`
            - `ALTER MODIFY COLUMN`. Уровень: `COLUMN`. Алиасы: `MODIFY COLUMN`
            - `ALTER COMMENT COLUMN`. Уровень: `COLUMN`. Алиасы: `COMMENT COLUMN`
            - `ALTER CLEAR COLUMN`. Уровень: `COLUMN`. Алиасы: `CLEAR COLUMN`
            - `ALTER RENAME COLUMN`. Уровень: `COLUMN`. Алиасы: `RENAME COLUMN`
        - `ALTER INDEX`. Уровень: `GROUP`. Алиасы: `INDEX`
            - `ALTER ORDER BY`. Уровень: `TABLE`. Алиасы: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            - `ALTER SAMPLE BY`. Уровень: `TABLE`. Алиасы: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`			
            - `ALTER ADD INDEX`. Уровень: `TABLE`. Алиасы: `ADD INDEX`
            - `ALTER DROP INDEX`. Уровень: `TABLE`. Алиасы: `DROP INDEX`
            - `ALTER MATERIALIZE INDEX`. Уровень: `TABLE`. Алиасы: `MATERIALIZE INDEX`
            - `ALTER CLEAR INDEX`. Уровень: `TABLE`. Алиасы: `CLEAR INDEX`
        - `ALTER CONSTRAINT`. Уровень: `GROUP`. Алиасы: `CONSTRAINT`
            - `ALTER ADD CONSTRAINT`. Уровень: `TABLE`. Алиасы: `ADD CONSTRAINT`
            - `ALTER DROP CONSTRAINT`. Уровень: `TABLE`. Алиасы: `DROP CONSTRAINT`
        - `ALTER TTL`. Уровень: `TABLE`. Алиасы: `ALTER MODIFY TTL`, `MODIFY TTL`
            - `ALTER MATERIALIZE TTL`. Уровень: `TABLE`. Алиасы: `MATERIALIZE TTL`
        - `ALTER SETTINGS`. Уровень: `TABLE`. Алиасы: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        - `ALTER MOVE PARTITION`. Уровень: `TABLE`. Алиасы: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        - `ALTER FETCH PARTITION`. Уровень: `TABLE`. Алиасы: `FETCH PARTITION`
        - `ALTER FREEZE PARTITION`. Уровень: `TABLE`. Алиасы: `FREEZE PARTITION`
    - `ALTER VIEW` Уровень: `GROUP`
        - `ALTER VIEW REFRESH `. Уровень: `VIEW`. Алиасы: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        - `ALTER VIEW MODIFY QUERY`. Уровень: `VIEW`. Алиасы: `ALTER TABLE MODIFY QUERY`

Примеры того, как трактуется данная иерархия:

- Привилегия `ALTER` включает все остальные `ALTER*` привилегии. 
- `ALTER CONSTRAINT` включает `ALTER ADD CONSTRAINT` и `ALTER DROP CONSTRAINT`.

**Дополнительно**

- Привилегия `MODIFY SETTING` позволяет изменять настройки движков таблиц. Не влияет на настройки или конфигурационные параметры сервера.
- Операция `ATTACH` требует наличие привилегии [CREATE](#grant-create).
- Операция `DETACH` требует наличие привилегии [DROP](#grant-drop).
- Для остановки мутации с помощью [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation), необходима привилегия на выполнение данной мутации. Например, чтобы остановить запрос `ALTER UPDATE`, необходима одна из привилегий: `ALTER UPDATE`, `ALTER TABLE` или `ALTER`.

### CREATE {#grant-create}

Разрешает выполнять DDL-запросы [CREATE](../../sql-reference/statements/create/index.md) и [ATTACH](misc.md#attach) в соответствии со следующей иерархией привилегий:

- `CREATE`. Уровень: `GROUP`
    - `CREATE DATABASE`. Уровень: `DATABASE`
    - `CREATE TABLE`. Уровень: `TABLE`
        - `CREATE TEMPORARY TABLE`. Уровень: `GLOBAL`
    - `CREATE VIEW`. Уровень: `VIEW`
    - `CREATE DICTIONARY`. Уровень: `DICTIONARY`

**Дополнительно**

- Для удаления созданной таблицы пользователю необходима привилегия [DROP](#grant-drop).

### DROP {#grant-drop}

Разрешает выполнять запросы [DROP](misc.md#drop) и [DETACH](misc.md#detach-statement) в соответствии со следующей иерархией привилегий:

- `DROP`. Уровень: `GROUP`
    - `DROP DATABASE`. Уровень: `DATABASE`
    - `DROP TABLE`. Уровень: `TABLE`
    - `DROP VIEW`. Уровень: `VIEW`
    - `DROP DICTIONARY`. Уровень: `DICTIONARY`

### TRUNCATE {#grant-truncate}

Разрешает выполнять запросы [TRUNCATE](../../sql-reference/statements/truncate.md).

Уровень: `TABLE`.

### OPTIMIZE {#grant-optimize}

Разрешает выполнять запросы [OPTIMIZE TABLE](misc.md#misc_operations-optimize).

Уровень: `TABLE`.

### SHOW {#grant-show}

Разрешает выполнять запросы `SHOW`, `DESCRIBE`, `USE` и `EXISTS` в соответствии со следующей иерархией привилегий:

- `SHOW`. Уровень: `GROUP`
    - `SHOW DATABASES`. Уровень: `DATABASE`. Разрешает выполнять запросы `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>`.
    - `SHOW TABLES`. Уровень: `TABLE`. Разрешает выполнять запросы `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>`.
    - `SHOW COLUMNS`. Уровень: `COLUMN`. Разрешает выполнять запросы `SHOW CREATE TABLE`, `DESCRIBE`.
    - `SHOW DICTIONARIES`. Уровень: `DICTIONARY`. Разрешает выполнять запросы `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>`.

**Дополнительно**

У пользователя есть привилегия `SHOW`, если ему присвоена любая другая привилегия по отношению к определенной таблице, словарю или базе данных.


### KILL QUERY {#grant-kill-query}

Разрешает выполнять запросы [KILL](../../sql-reference/statements/kill.md#kill-query) в соответствии со следующей иерархией привилегий:

Уровень: `GLOBAL`.

**Дополнительно**

`KILL QUERY` позволяет пользователю останавливать запросы других пользователей.


### ACCESS MANAGEMENT {#grant-access-management}

Разрешает пользователю выполнять запросы на управление пользователями, ролями и политиками доступа к строкам.

- `ACCESS MANAGEMENT`. Уровень: `GROUP`
    - `CREATE USER`. Уровень: `GLOBAL`
    - `ALTER USER`. Уровень: `GLOBAL`
    - `DROP USER`. Уровень: `GLOBAL`
    - `CREATE ROLE`. Уровень: `GLOBAL`
    - `ALTER ROLE`. Уровень: `GLOBAL`
    - `DROP ROLE`. Уровень: `GLOBAL`
    - `ROLE ADMIN`. Уровень: `GLOBAL`
    - `CREATE ROW POLICY`. Уровень: `GLOBAL`. Алиасы: `CREATE POLICY`
    - `ALTER ROW POLICY`. Уровень: `GLOBAL`. Алиасы: `ALTER POLICY`
    - `DROP ROW POLICY`. Уровень: `GLOBAL`. Алиасы: `DROP POLICY`
    - `CREATE QUOTA`. Уровень: `GLOBAL`
    - `ALTER QUOTA`. Уровень: `GLOBAL`
    - `DROP QUOTA`. Уровень: `GLOBAL`
    - `CREATE SETTINGS PROFILE`. Уровень: `GLOBAL`. Алиасы: `CREATE PROFILE`
    - `ALTER SETTINGS PROFILE`. Уровень: `GLOBAL`. Алиасы: `ALTER PROFILE`
    - `DROP SETTINGS PROFILE`. Уровень: `GLOBAL`. Алиасы: `DROP PROFILE`
    - `SHOW ACCESS`. Уровень: `GROUP`
        - `SHOW_USERS`. Уровень: `GLOBAL`. Алиасы: `SHOW CREATE USER`
        - `SHOW_ROLES`. Уровень: `GLOBAL`. Алиасы: `SHOW CREATE ROLE`
        - `SHOW_ROW_POLICIES`. Уровень: `GLOBAL`. Алиасы: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        - `SHOW_QUOTAS`. Уровень: `GLOBAL`. Алиасы: `SHOW CREATE QUOTA`
        - `SHOW_SETTINGS_PROFILES`. Уровень: `GLOBAL`. Алиасы: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

Привилегия `ROLE ADMIN` разрешает пользователю назначать и отзывать любые роли, включая те, которые не назначены пользователю с опцией администратора.

### SYSTEM {#grant-system}

Разрешает выполнять запросы [SYSTEM](system.md) в соответствии со следующей иерархией привилегий:

- `SYSTEM`. Уровень: `GROUP`
    - `SYSTEM SHUTDOWN`. Уровень: `GLOBAL`. Алиасы: `SYSTEM KILL`, `SHUTDOWN`
    - `SYSTEM DROP CACHE`. Алиасы: `DROP CACHE`
        - `SYSTEM DROP DNS CACHE`. Уровень: `GLOBAL`. Алиасы: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        - `SYSTEM DROP MARK CACHE`. Уровень: `GLOBAL`. Алиасы: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        - `SYSTEM DROP UNCOMPRESSED CACHE`. Уровень: `GLOBAL`. Алиасы: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    - `SYSTEM RELOAD`. Уровень: `GROUP`
        - `SYSTEM RELOAD CONFIG`. Уровень: `GLOBAL`. Алиасы: `RELOAD CONFIG`
        - `SYSTEM RELOAD DICTIONARY`. Уровень: `GLOBAL`. Алиасы: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
            - `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Уровень: `GLOBAL`. Алиасы: `RELOAD EMBEDDED DICTIONARIES`
    - `SYSTEM MERGES`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    - `SYSTEM TTL MERGES`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    - `SYSTEM FETCHES`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    - `SYSTEM MOVES`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    - `SYSTEM SENDS`. Уровень: `GROUP`. Алиасы: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        - `SYSTEM DISTRIBUTED SENDS`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        - `SYSTEM REPLICATED SENDS`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    - `SYSTEM REPLICATION QUEUES`. Уровень: `TABLE`. Алиасы: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    - `SYSTEM SYNC REPLICA`. Уровень: `TABLE`. Алиасы: `SYNC REPLICA`
    - `SYSTEM RESTART REPLICA`. Уровень: `TABLE`. Алиасы: `RESTART REPLICA`
    - `SYSTEM FLUSH`. Уровень: `GROUP`
        - `SYSTEM FLUSH DISTRIBUTED`. Уровень: `TABLE`. Алиасы: `FLUSH DISTRIBUTED`
        - `SYSTEM FLUSH LOGS`. Уровень: `GLOBAL`. Алиасы: `FLUSH LOGS`

Привилегия `SYSTEM RELOAD EMBEDDED DICTIONARIES` имплицитно присваивается привилегией `SYSTEM RELOAD DICTIONARY ON *.*`.


### INTROSPECTION {#grant-introspection}

Разрешает использовать функции [интроспекции](../../operations/optimizing-performance/sampling-query-profiler.md).

- `INTROSPECTION`. Уровень: `GROUP`. Алиасы: `INTROSPECTION FUNCTIONS`
    - `addressToLine`. Уровень: `GLOBAL`
    - `addressToSymbol`. Уровень: `GLOBAL`
    - `demangle`. Уровень: `GLOBAL`


### SOURCES {#grant-sources}

Разрешает использовать внешние источники данных. Применяется к [движкам таблиц](../../engines/table-engines/index.md) и [табличным функциям](../table-functions/index.md#table-functions).

- `SOURCES`. Уровень: `GROUP`
    - `FILE`. Уровень: `GLOBAL`
    - `URL`. Уровень: `GLOBAL`
    - `REMOTE`. Уровень: `GLOBAL`
    - `YSQL`. Уровень: `GLOBAL`
    - `ODBC`. Уровень: `GLOBAL`
    - `JDBC`. Уровень: `GLOBAL`
    - `HDFS`. Уровень: `GLOBAL`
    - `S3`. Уровень: `GLOBAL`

Привилегия `SOURCES` разрешает использование всех источников. Также вы можете присвоить привилегию для каждого источника отдельно. Для использования источников необходимы дополнительные привилегии.

Примеры:

- Чтобы создать таблицу с [движком MySQL](../../engines/table-engines/integrations/mysql.md), необходимы привилегии `CREATE TABLE (ON db.table_name)` и `MYSQL`.
- Чтобы использовать [табличную функцию mysql](../table-functions/mysql.md), необходимы привилегии `CREATE TEMPORARY TABLE` и `MYSQL`.

### dictGet {#grant-dictget}

- `dictGet`. Алиасы: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Разрешает вызывать функции [dictGet](../functions/ext-dict-functions.md#dictget), [dictHas](../functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../functions/ext-dict-functions.md#dictgethierarchy), [dictIsIn](../functions/ext-dict-functions.md#dictisin).

Уровень: `DICTIONARY`.

**Примеры**

- `GRANT dictGet ON mydb.mydictionary TO john`
- `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

Присваивает пользователю или роли все привилегии на объект с регулируемым доступом.


### NONE {#grant-none}

Не присваивает никаких привилегий.


### ADMIN OPTION {#admin-option-privilege}

Привилегия `ADMIN OPTION` разрешает пользователю назначать свои роли другому пользователю.

