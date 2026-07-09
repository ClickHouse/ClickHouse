---
description: 'Документация по оператору GRANT'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'Оператор GRANT'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # Оператор GRANT
</div>

* Выдаёт [привилегии](#privileges) учётным записям пользователей ClickHouse или ролям.
* Назначает роли учётным записям пользователей или другим ролям.

Чтобы отозвать привилегии, используйте оператор [REVOKE](../../sql-reference/statements/revoke.md). Список выданных привилегий также можно посмотреть с помощью оператора [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants).

<div id="granting-privilege-syntax">
  ## Синтаксис предоставления привилегии
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — тип привилегии.
* `role` — роль пользователя ClickHouse.
* `user` — учетная запись пользователя ClickHouse.

Клауза `WITH GRANT OPTION` предоставляет `user` или `role` право выполнять запрос `GRANT`. Пользователи могут предоставлять привилегии того же или более узкого уровня действия, чем есть у них.
Предложение `WITH REPLACE OPTION` заменяет старые привилегии новыми для `user` или `role`; если она не указана, привилегии добавляются.

<div id="assigning-role-syntax">
  ## Синтаксис назначения роли
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — роль пользователя ClickHouse.
* `user` — учётная запись пользователя ClickHouse.

Предложение `WITH ADMIN OPTION` предоставляет привилегию [ADMIN OPTION](#admin-option) объекту `user` или `role`.
Предложение `WITH REPLACE OPTION` заменяет старые роли новыми для `user` или `role`; если оно не указано, роли добавляются.

<div id="grant-current-grants-syntax">
  ## Синтаксис GRANT CURRENT GRANTS
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — Тип привилегии.
* `role` — роль пользователя ClickHouse.
* `user` — учётная запись пользователя ClickHouse.

Использование оператора `CURRENT GRANTS` позволяет выдать все указанные привилегии указанному пользователю или роли.
Если ни одна привилегия не указана, указанный пользователь или роль получит все доступные привилегии для `CURRENT_USER`.

<div id="usage">
  ## Использование
</div>

Чтобы использовать `GRANT`, у вашего аккаунта должна быть привилегия `GRANT OPTION`. Вы можете выдавать привилегии только в рамках привилегий, имеющихся у вашего аккаунта.

Например, администратор выдал привилегии аккаунту `john` с помощью запроса:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Это означает, что `john` имеет разрешение на выполнение:

* `SELECT x,y FROM db.table`.
* `SELECT x FROM db.table`.
* `SELECT y FROM db.table`.

`john` не может выполнить `SELECT z FROM db.table`. `SELECT * FROM db.table` также недоступен. При обработке этого запроса ClickHouse не возвращает никаких данных, даже `x` и `y`. Единственное исключение — если таблица содержит только столбцы `x` и `y`. В этом случае ClickHouse возвращает все данные.

Кроме того, у `john` есть привилегия `GRANT OPTION`, поэтому он может выдавать другим пользователям привилегии того же или меньшего уровня.

Доступ к базе данных `system` всегда разрешён (поскольку эта база данных используется для обработки запросов).

:::note
Хотя по умолчанию новые пользователи могут получать доступ ко многим системным таблицам, без привилегий им могут быть недоступны некоторые из них.
Кроме того, доступ к некоторым системным таблицам, таким как `system.zookeeper`, ограничен для пользователей Cloud из соображений безопасности.
:::

Вы можете выдать несколько привилегий нескольким аккаунтам в одном запросе. Запрос `GRANT SELECT, INSERT ON *.* TO john, robin` позволяет аккаунтам `john` и `robin` выполнять запросы `INSERT` и `SELECT` для всех таблиц во всех базах данных на сервере.

<div id="wildcard-grants">
  ## Привилегии с символами подстановки
</div>

При указании привилегий можно использовать звёздочку (`*`) вместо имени таблицы или базы данных. Например, запрос `GRANT SELECT ON db.* TO john` позволяет `john` выполнять запрос `SELECT` для всех таблиц в базе данных `db`.
Также можно опустить имя базы данных. В этом случае привилегии выдаются для текущей базы данных.
Например, `GRANT SELECT ON * TO john` выдаёт привилегию на все таблицы в текущей базе данных, а `GRANT SELECT ON mytable TO john` выдаёт привилегию на таблицу `mytable` в текущей базе данных.

:::note
Описанная ниже возможность доступна начиная с версии ClickHouse 24.10.
:::

Также можно ставить звёздочки в конце имени таблицы или базы данных. Эта возможность позволяет выдавать привилегии на абстрактный префикс пути таблицы.
Пример: `GRANT SELECT ON db.my_tables* TO john`. Этот запрос позволяет `john` выполнять запрос `SELECT` для всех таблиц в базе данных `db` с префиксом `my_tables*`.

Другие примеры:

`GRANT SELECT ON db.my_tables* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted

SELECT * FROM db.other_table -- not_granted
SELECT * FROM db2.my_tables -- not_granted
```

`GRANT SELECT ON db*.* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted
SELECT * FROM db.other_table -- granted
SELECT * FROM db2.my_tables -- granted
```

Все вновь создаваемые таблицы в пределах путей, на которые выданы привилегии, автоматически унаследуют все привилегии от своих родительских путей.
Например, если вы выполните запрос `GRANT SELECT ON db.* TO john`, а затем создадите новую таблицу `db.new_table`, пользователь `john` сможет выполнить запрос `SELECT * FROM db.new_table`.

Вы можете указать звёздочку **только** для префиксов:

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## Привилегии
</div>

Привилегия — это разрешение, предоставляемое пользователю для выполнения определённых видов запросов.

Привилегии имеют иерархическую структуру, и набор разрешённых запросов зависит от области действия привилегии.

Иерархия привилегий в ClickHouse показана ниже:

* [`ALL`](#all)
  * [`УПРАВЛЕНИЕ ДОСТУПОМ`](#access-management)
    * `ALLOW SQL SECURITY NONE`
    * `ALTER QUOTA`
    * `ALTER ROLE`
    * `ALTER ROW POLICY`
    * `ALTER SETTINGS PROFILE`
    * `ALTER USER`
    * `CREATE QUOTA`
    * `CREATE ROLE`
    * `CREATE ROW POLICY`
    * `CREATE SETTINGS PROFILE`
    * `CREATE USER`
    * `DROP QUOTA`
    * `DROP ROLE`
    * `DROP ROW POLICY`
    * `DROP SETTINGS PROFILE`
    * `DROP USER`
    * `ROLE ADMIN`
    * `SHOW ACCESS`
      * `SHOW QUOTAS`
      * `SHOW ROLES`
      * `SHOW ROW POLICIES`
      * `SHOW SETTINGS PROFILES`
      * `SHOW USERS`
  * [`ALTER`](#alter)
    * `ALTER DATABASE`
      * `ALTER DATABASE SETTINGS`
    * `ALTER TABLE`
      * `ALTER COLUMN`
        * `ALTER ADD COLUMN`
        * `ALTER CLEAR COLUMN`
        * `ALTER COMMENT COLUMN`
        * `ALTER DROP COLUMN`
        * `ALTER MATERIALIZE COLUMN`
        * `ALTER MODIFY COLUMN`
        * `ALTER RENAME COLUMN`
      * `ALTER CONSTRAINT`
        * `ALTER ADD CONSTRAINT`
        * `ALTER DROP CONSTRAINT`
        * `ALTER MODIFY CONSTRAINT`
      * `ALTER DELETE`
      * `ALTER FETCH PARTITION`
      * `ALTER FREEZE PARTITION`
      * `ALTER INDEX`
        * `ALTER ADD INDEX`
        * `ALTER CLEAR INDEX`
        * `ALTER DROP INDEX`
        * `ALTER MATERIALIZE INDEX`
        * `ALTER ORDER BY`
        * `ALTER SAMPLE BY`
      * `ALTER MATERIALIZE TTL`
      * `ALTER MODIFY COMMENT`
      * `ALTER MOVE PARTITION`
      * `ALTER PROJECTION`
      * `ALTER SETTINGS`
      * `ALTER STATISTICS`
        * `ALTER ADD STATISTICS`
        * `ALTER DROP STATISTICS`
        * `ALTER MATERIALIZE STATISTICS`
        * `ALTER MODIFY STATISTICS`
      * `ALTER TTL`
      * `ALTER UPDATE`
      * `ALTER TABLE EXECUTE`
    * `ALTER VIEW`
      * `ALTER VIEW MODIFY QUERY`
      * `ALTER VIEW REFRESH`
      * `ALTER VIEW MODIFY SQL SECURITY`
  * [`BACKUP`](#backup)
  * [`CLUSTER`](#cluster)
  * [`CREATE`](#create)
    * `CREATE ARBITRARY TEMPORARY TABLE`
      * `CREATE TEMPORARY TABLE`
    * `CREATE DATABASE`
    * `CREATE DICTIONARY`
    * `CREATE FUNCTION`
    * `CREATE RESOURCE`
    * `CREATE TABLE`
    * `CREATE VIEW`
    * `CREATE WORKLOAD`
  * [`dictGet`](#dictget)
  * [`displaySecretsInShowAndSelect`](#displaysecretsinshowandselect)
  * [`DROP`](#drop)
    * `DROP DATABASE`
    * `DROP DICTIONARY`
    * `DROP FUNCTION`
    * `DROP RESOURCE`
    * `DROP TABLE`
    * `DROP VIEW`
    * `DROP WORKLOAD`
  * [`INSERT`](#insert)
  * [`INTROSPECTION`](#introspection)
    * `addressToLine`
    * `addressToLineWithInlines`
    * `addressToSymbol`
    * `demangle`
  * `KILL QUERY`
  * `KILL TRANSACTION`
  * `MOVE PARTITION BETWEEN SHARDS`
  * [`NAMED COLLECTION ADMIN`](#named-collection-admin)
    * `ALTER NAMED COLLECTION`
    * `CREATE NAMED COLLECTION`
    * `DROP NAMED COLLECTION`
    * `NAMED COLLECTION`
    * `SHOW NAMED COLLECTIONS`
    * `SHOW NAMED COLLECTIONS SECRETS`
  * [`OPTIMIZE`](#optimize)
  * [`SELECT`](#select)
  * [`SET DEFINER`](/ru/sql-reference/statements/create/view#sql_security)
  * [`SHOW`](#show)
    * `SHOW COLUMNS`
    * `SHOW DATABASES`
    * `SHOW DICTIONARIES`
    * `SHOW TABLES`
  * `SHOW FILESYSTEM CACHES`
  * [`SOURCES`](#sources)
    * `AZURE`
    * `FILE`
    * `HDFS`
    * `HIVE`
    * `JDBC`
    * `KAFKA`
    * `MONGO`
    * `MYSQL`
    * `NATS`
    * `ODBC`
    * `POSTGRES`
    * `RABBITMQ`
    * `REDIS`
    * `REMOTE`
    * `S3`
    * `SQLITE`
    * `URL`
  * [`SYSTEM`](#system)
    * `SYSTEM CLEANUP`
    * `SYSTEM DROP CACHE`
      * `SYSTEM DROP COMPILED EXPRESSION CACHE`
      * `SYSTEM DROP CONNECTIONS CACHE`
      * `SYSTEM DROP DISTRIBUTED CACHE`
      * `SYSTEM DROP DNS CACHE`
      * `SYSTEM DROP FILESYSTEM CACHE`
      * `SYSTEM DROP FORMAT SCHEMA CACHE`
      * `SYSTEM DROP MARK CACHE`
      * `SYSTEM DROP MMAP CACHE`
      * `SYSTEM DROP PAGE CACHE`
      * `SYSTEM DROP PRIMARY INDEX CACHE`
      * `SYSTEM DROP QUERY CACHE`
      * `SYSTEM DROP S3 CLIENT CACHE`
      * `SYSTEM DROP SCHEMA CACHE`
      * `SYSTEM DROP UNCOMPRESSED CACHE`
    * `SYSTEM DROP PRIMARY INDEX CACHE`
    * `SYSTEM DROP REPLICA`
    * `SYSTEM FAILPOINT`
    * `SYSTEM FETCHES`
    * `SYSTEM FLUSH`
      * `SYSTEM FLUSH ASYNC INSERT QUEUE`
      * `SYSTEM FLUSH LOGS`
    * `SYSTEM JEMALLOC`
    * `SYSTEM KILL QUERY`
    * `SYSTEM KILL TRANSACTION`
    * `SYSTEM LISTEN`
    * `SYSTEM LOAD PRIMARY KEY`
    * `SYSTEM MERGES`
    * `SYSTEM MOVES`
    * `SYSTEM PULLING REPLICATION LOG`
    * `SYSTEM REDUCE BLOCKING PARTS`
    * `SYSTEM REPLICATION QUEUES`
    * `SYSTEM REPLICA READINESS`
    * `SYSTEM RESET DDL WORKER`
    * `SYSTEM RESTART DISK`
    * `SYSTEM RESTART REPLICA`
    * `SYSTEM RESTORE REPLICA`
    * `SYSTEM RELOAD`
      * `SYSTEM RELOAD ASYNCHRONOUS METRICS`
      * `SYSTEM RELOAD CONFIG`
        * `SYSTEM RELOAD DICTIONARY`
        * `SYSTEM RELOAD EMBEDDED DICTIONARIES`
        * `SYSTEM RELOAD FUNCTION`
        * `SYSTEM RELOAD MODEL`
        * `SYSTEM RELOAD USERS`
    * `SYSTEM SENDS`
      * `SYSTEM DISTRIBUTED SENDS`
      * `SYSTEM REPLICATED SENDS`
    * `SYSTEM SHUTDOWN`
    * `SYSTEM SYNC DATABASE REPLICA`
    * `SYSTEM SYNC FILE CACHE`
    * `SYSTEM SYNC FILESYSTEM CACHE`
    * `SYSTEM SYNC REPLICA`
    * `SYSTEM SYNC TRANSACTION LOG`
    * `SYSTEM THREAD FUZZER`
    * `SYSTEM TTL MERGES`
    * `SYSTEM UNFREEZE`
    * `SYSTEM UNLOAD PRIMARY KEY`
    * `SYSTEM VIEWS`
    * `SYSTEM VIRTUAL PARTS UPDATE`
    * `SYSTEM WAIT LOADING PARTS`
  * [`TABLE ENGINE`](#table-engine)
  * [`TRUNCATE`](#truncate)
  * `UNDROP TABLE`
* [`NONE`](#none)

Примеры того, как трактуется эта иерархия:

* Привилегия `ALTER` включает все остальные привилегии `ALTER*`.
* `ALTER CONSTRAINT` включает привилегии `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` и `ALTER MODIFY CONSTRAINT`.

Привилегии применяются на разных уровнях. Уровень определяет, какой синтаксис доступен для привилегии.

Уровни (от низшего к высшему):

* `COLUMN` — Привилегия может быть выдана для столбца, таблицы, базы данных или глобально.
* `TABLE` — Привилегия может быть выдана для таблицы, базы данных или глобально.
* `VIEW` — Привилегия может быть выдана для представления, базы данных или глобально.
* `DICTIONARY` — Привилегия может быть выдана для словаря, базы данных или глобально.
* `DATABASE` — Привилегия может быть выдана для базы данных или глобально.
* `GLOBAL` — Привилегия может быть выдана только глобально.
* `GROUP` — Группирует привилегии разных уровней. Когда выдается привилегия уровня `GROUP`, из группы выдаются только те привилегии, которые соответствуют используемому синтаксису.

Примеры допустимого синтаксиса:

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

Примеры недопустимого синтаксиса:

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

Специальная привилегия [ALL](#all) предоставляет все привилегии учётной записи пользователя или роли.

По умолчанию у учётной записи пользователя или роли нет привилегий.

Если у пользователя или роли нет привилегий, это отображается как привилегия [NONE](#none).

Некоторые запросы в силу своей реализации требуют набора привилегий. Например, чтобы выполнить запрос [RENAME](../../sql-reference/statements/optimize.md), вам нужны следующие привилегии: `SELECT`, `CREATE TABLE`, `INSERT` и `DROP TABLE`.

<div id="select">
  ### SELECT
</div>

Позволяет выполнять запросы [SELECT](../../sql-reference/statements/select/index.md).

Уровень привилегии: `COLUMN`.

**Описание**

Пользователь, которому выдана эта привилегия, может выполнять запросы `SELECT` к указанному списку столбцов в указанной таблице и базе данных. Если пользователь включает другие столбцы, помимо указанных, запрос не возвращает данных.

Рассмотрим следующую привилегию:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

Эта привилегия позволяет `john` выполнять любой запрос `SELECT`, который обращается к данным из столбцов `x` и/или `y` в `db.table`, например `SELECT x FROM db.table`. `john` не может выполнить `SELECT z FROM db.table`. Запрос `SELECT * FROM db.table` также недоступен. При обработке этого запроса ClickHouse не возвращает никаких данных, даже `x` и `y`. Единственное исключение — если таблица содержит только столбцы `x` и `y`, в этом случае ClickHouse возвращает все данные.

<div id="insert">
  ### INSERT
</div>

Разрешает выполнять запросы [INSERT](../../sql-reference/statements/insert-into.md).

Уровень привилегии: `COLUMN`.

**Описание**

Пользователь с этой привилегией может выполнять запросы `INSERT` для указанного списка столбцов в указанной таблице и базе данных. Если пользователь указывает столбцы, отличные от разрешённых, запрос не вставляет данные.

**Пример**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

Предоставленная привилегия позволяет `john` вставлять данные в столбцы `x` и/или `y` таблицы `db.table`.

<div id="alter">
  ### ALTER
</div>

Разрешает выполнять запросы [ALTER](../../sql-reference/statements/alter/index.md) в соответствии со следующей иерархией привилегий:

* `ALTER`. Уровень: `COLUMN`.
  * `ALTER TABLE`. Уровень: `GROUP`
  * `ALTER UPDATE`. Уровень: `COLUMN`. Псевдонимы: `UPDATE`
  * `ALTER DELETE`. Уровень: `COLUMN`. Псевдонимы: `DELETE`
  * `ALTER COLUMN`. Уровень: `GROUP`
  * `ALTER ADD COLUMN`. Уровень: `COLUMN`. Псевдонимы: `ADD COLUMN`
  * `ALTER DROP COLUMN`. Уровень: `COLUMN`. Псевдонимы: `DROP COLUMN`
  * `ALTER MODIFY COLUMN`. Уровень: `COLUMN`. Псевдонимы: `MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`. Уровень: `COLUMN`. Псевдонимы: `COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`. Уровень: `COLUMN`. Псевдонимы: `CLEAR COLUMN`
  * `ALTER RENAME COLUMN`. Уровень: `COLUMN`. Псевдонимы: `RENAME COLUMN`
  * `ALTER INDEX`. Уровень: `GROUP`. Псевдонимы: `INDEX`
  * `ALTER ORDER BY`. Уровень: `TABLE`. Псевдонимы: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
  * `ALTER SAMPLE BY`. Уровень: `TABLE`. Псевдонимы: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`. Уровень: `TABLE`. Псевдонимы: `ADD INDEX`
  * `ALTER DROP INDEX`. Уровень: `TABLE`. Псевдонимы: `DROP INDEX`
  * `ALTER MATERIALIZE INDEX`. Уровень: `TABLE`. Псевдонимы: `MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`. Уровень: `TABLE`. Псевдонимы: `CLEAR INDEX`
  * `ALTER CONSTRAINT`. Уровень: `GROUP`. Псевдонимы: `CONSTRAINT`
  * `ALTER ADD CONSTRAINT`. Уровень: `TABLE`. Псевдонимы: `ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`. Уровень: `TABLE`. Псевдонимы: `DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`. Уровень: `TABLE`. Псевдонимы: `MODIFY CONSTRAINT`
  * `ALTER TTL`. Уровень: `TABLE`. Псевдонимы: `ALTER MODIFY TTL`, `MODIFY TTL`
  * `ALTER MATERIALIZE TTL`. Уровень: `TABLE`. Псевдонимы: `MATERIALIZE TTL`
  * `ALTER SETTINGS`. Уровень: `TABLE`. Псевдонимы: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
  * `ALTER MOVE PARTITION`. Уровень: `TABLE`. Псевдонимы: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
  * `ALTER FETCH PARTITION`. Уровень: `TABLE`. Псевдонимы: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
  * `ALTER FREEZE PARTITION`. Уровень: `TABLE`. Псевдонимы: `FREEZE PARTITION`
  * `ALTER EXECUTE`. Уровень: `TABLE`. Псевдонимы: `ALTER TABLE EXECUTE`
  * `ALTER VIEW`. Уровень: `GROUP`
  * `ALTER VIEW REFRESH`. Уровень: `VIEW`. Псевдонимы: `REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`. Уровень: `VIEW`. Псевдонимы: `ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`. Уровень: `VIEW`. Псевдонимы: `ALTER TABLE MODIFY SQL SECURITY`

Примеры того, как работает эта иерархия:

* Привилегия `ALTER` включает все остальные привилегии `ALTER*`.
* `ALTER CONSTRAINT` включает привилегии `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` и `ALTER MODIFY CONSTRAINT`.

**Примечания**

* Привилегия `MODIFY SETTING` позволяет изменять настройки движка таблицы. Она не влияет на настройки или параметры конфигурации сервера.
* Для операции `ATTACH` требуется привилегия [CREATE](#create).
* Для операции `DETACH` требуется привилегия [DROP](#drop).
* Чтобы остановить мутацию с помощью запроса [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation), необходимо иметь привилегию, позволяющую запустить эту мутацию. Например, если вы хотите остановить запрос `ALTER UPDATE`, вам нужна привилегия `ALTER UPDATE`, `ALTER TABLE` или `ALTER`.

<div id="backup">
  ### BACKUP
</div>

Разрешает выполнять [`BACKUP`] в запросах. Подробнее о резервном копировании см. в разделе [&quot;Резервное копирование и восстановление&quot;](/ru/operations/backup/overview).

<div id="create">
  ### CREATE
</div>

Разрешает выполнять DDL-запросы [CREATE](../../sql-reference/statements/create/index.md) и [ATTACH](../../sql-reference/statements/attach.md) согласно следующей иерархии привилегий:

* `CREATE`. Уровень: `GROUP`
  * `CREATE DATABASE`. Уровень: `DATABASE`
  * `CREATE TABLE`. Уровень: `TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`. Уровень: `GLOBAL`
      * `CREATE TEMPORARY TABLE`. Уровень: `GLOBAL`
  * `CREATE VIEW`. Уровень: `VIEW`
  * `CREATE DICTIONARY`. Уровень: `DICTIONARY`

**Примечания**

* Чтобы удалить созданную таблицу, пользователю нужна привилегия [DROP](#drop).

<div id="cluster">
  ### CLUSTER
</div>

Позволяет выполнять запросы с предложением `ON CLUSTER`.

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

По умолчанию для запросов с `ON CLUSTER` у пользователя должен быть grant `CLUSTER`.
Если вы попытаетесь использовать `ON CLUSTER` в запросе, предварительно не выдав привилегию `CLUSTER`, вы получите следующую ошибку:

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

Поведение по умолчанию можно изменить, установив для параметра `on_cluster_queries_require_cluster_grant`, расположенного в разделе `access_control_improvements` файла `config.xml` (см. ниже), значение `false`.

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

Разрешает выполнять запросы [DROP](../../sql-reference/statements/drop.md) и [DETACH](../../sql-reference/statements/detach.md) в соответствии со следующей иерархией привилегий:

* `DROP`. Уровень: `GROUP`
  * `DROP DATABASE`. Уровень: `DATABASE`
  * `DROP TABLE`. Уровень: `TABLE`
  * `DROP VIEW`. Уровень: `VIEW`
  * `DROP DICTIONARY`. Уровень: `DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

Позволяет выполнять запросы [TRUNCATE](../../sql-reference/statements/truncate.md).

Уровень привилегии: `TABLE`.

<div id="optimize">
  ### OPTIMIZE
</div>

Позволяет выполнять команды [OPTIMIZE TABLE](../../sql-reference/statements/optimize.md).

Уровень привилегии: `TABLE`.

<div id="show">
  ### SHOW
</div>

Разрешает выполнять запросы `SHOW`, `DESCRIBE`, `USE` и `EXISTS` в соответствии со следующей иерархией привилегий:

* `SHOW`. Уровень: `GROUP`
  * `SHOW DATABASES`. Уровень: `DATABASE`. Разрешает выполнять запросы `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>`.
  * `SHOW TABLES`. Уровень: `TABLE`. Разрешает выполнять запросы `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>`.
  * `SHOW COLUMNS`. Уровень: `COLUMN`. Разрешает выполнять запросы `SHOW CREATE TABLE`, `DESCRIBE`.
  * `SHOW DICTIONARIES`. Уровень: `DICTIONARY`. Разрешает выполнять запросы `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>`.

**Примечания**

Пользователь имеет привилегию `SHOW`, если у него есть любая другая привилегия в отношении указанной таблицы, словаря или базы данных.

<div id="kill-query">
  ### KILL QUERY
</div>

Разрешает выполнять запросы [KILL](../../sql-reference/statements/kill.md#kill-query) согласно следующей иерархии привилегий:

Уровень привилегии: `GLOBAL`.

**Примечания**

Привилегия `KILL QUERY` позволяет одному пользователю завершать запросы других пользователей.

<div id="access-management">
  ### УПРАВЛЕНИЕ ДОСТУПОМ
</div>

Позволяет пользователю выполнять запросы для управления пользователями, ролями и политиками строк.

* `ACCESS MANAGEMENT`. Уровень: `GROUP`
  * `CREATE USER`. Уровень: `GLOBAL`
  * `ALTER USER`. Уровень: `GLOBAL`
  * `DROP USER`. Уровень: `GLOBAL`
  * `CREATE ROLE`. Уровень: `GLOBAL`
  * `ALTER ROLE`. Уровень: `GLOBAL`
  * `DROP ROLE`. Уровень: `GLOBAL`
  * `ROLE ADMIN`. Уровень: `GLOBAL`
  * `CREATE ROW POLICY`. Уровень: `GLOBAL`. Псевдонимы: `CREATE POLICY`
  * `ALTER ROW POLICY`. Уровень: `GLOBAL`. Псевдонимы: `ALTER POLICY`
  * `DROP ROW POLICY`. Уровень: `GLOBAL`. Псевдонимы: `DROP POLICY`
  * `CREATE QUOTA`. Уровень: `GLOBAL`
  * `ALTER QUOTA`. Уровень: `GLOBAL`
  * `DROP QUOTA`. Уровень: `GLOBAL`
  * `CREATE SETTINGS PROFILE`. Уровень: `GLOBAL`. Псевдонимы: `CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`. Уровень: `GLOBAL`. Псевдонимы: `ALTER PROFILE`
  * `DROP SETTINGS PROFILE`. Уровень: `GLOBAL`. Псевдонимы: `DROP PROFILE`
  * `SHOW ACCESS`. Уровень: `GROUP`
    * `SHOW_USERS`. Уровень: `GLOBAL`. Псевдонимы: `SHOW CREATE USER`
    * `SHOW_ROLES`. Уровень: `GLOBAL`. Псевдонимы: `SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`. Уровень: `GLOBAL`. Псевдонимы: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
    * `SHOW_QUOTAS`. Уровень: `GLOBAL`. Псевдонимы: `SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`. Уровень: `GLOBAL`. Псевдонимы: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`. Уровень: `GLOBAL`. Псевдонимы: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

Привилегия `ROLE ADMIN` позволяет пользователю назначать и отзывать любые роли, включая те, которые не были назначены ему с параметром admin.

<div id="system">
  ### SYSTEM
</div>

Позволяет пользователю выполнять запросы [SYSTEM](../../sql-reference/statements/system.md) в соответствии со следующей иерархией привилегий.

* `SYSTEM`. Уровень: `GROUP`
  * `SYSTEM SHUTDOWN`. Уровень: `GLOBAL`. Псевдонимы: `SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`. Псевдонимы: `DROP CACHE`
    * `SYSTEM DROP DNS CACHE`. Уровень: `GLOBAL`. Псевдонимы: `SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`. Уровень: `GLOBAL`. Псевдонимы: `SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`. Уровень: `GLOBAL`. Псевдонимы: `SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`. Уровень: `GROUP`
    * `SYSTEM RELOAD CONFIG`. Уровень: `GLOBAL`. Псевдонимы: `RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`. Уровень: `GLOBAL`. Псевдонимы: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Уровень: `GLOBAL`. Псевдонимы: `RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`. Уровень: `GROUP`. Псевдонимы: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`. Уровень: `TABLE`. Псевдонимы: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`. Уровень: `TABLE`. Псевдонимы: `SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`. Уровень: `TABLE`. Псевдонимы: `RESTART REPLICA`
  * `SYSTEM FLUSH`. Уровень: `GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`. Уровень: `TABLE`. Псевдонимы: `FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`. Уровень: `GLOBAL`. Псевдонимы: `FLUSH LOGS`

Привилегия `SYSTEM RELOAD EMBEDDED DICTIONARIES` неявно предоставляется привилегией `SYSTEM RELOAD DICTIONARY ON *.*`.

<div id="introspection">
  ### INTROSPECTION
</div>

Разрешает использовать [функции интроспекции](../../operations/optimizing-performance/sampling-query-profiler.md).

* `INTROSPECTION`. Уровень: `GROUP`. Псевдонимы: `INTROSPECTION FUNCTIONS`
  * `addressToLine`. Уровень: `GLOBAL`
  * `addressToLineWithInlines`. Уровень: `GLOBAL`
  * `addressToSymbol`. Уровень: `GLOBAL`
  * `demangle`. Уровень: `GLOBAL`

<div id="sources">
  ### SOURCES
</div>

Разрешает использовать внешние источники данных. Применяется к [движкам таблиц](../../engines/table-engines/index.md) и [табличным функциям](/ru/sql-reference/table-functions).

* `READ`. Уровень: `GLOBAL_WITH_PARAMETER`
* `WRITE`. Уровень: `GLOBAL_WITH_PARAMETER`

Возможные параметры:

* `AZURE`
* `FILE`
* `HDFS`
* `HIVE`
* `JDBC`
* `KAFKA`
* `MONGO`
* `MYSQL`
* `NATS`
* `ODBC`
* `POSTGRES`
* `RABBITMQ`
* `REDIS`
* `REMOTE`
* `S3`
* `SQLITE`
* `URL`

:::note
Разделение привилегий READ/WRITE для источников доступно начиная с версии 25.7 и только при включённой настройке сервера
`access_control_improvements.enable_read_write_grants`

В противном случае используйте синтаксис `GRANT AZURE ON *.* TO user`, который эквивалентен новому `GRANT READ, WRITE ON AZURE TO user`
:::

Примеры:

* Чтобы создать таблицу на основе [движка таблицы MySQL](../../engines/table-engines/integrations/mysql.md), нужны привилегии `CREATE TABLE (ON db.table_name)` и `MYSQL`.
* Чтобы использовать [табличную функцию MySQL](../../sql-reference/table-functions/mysql.md), нужны привилегии `CREATE TEMPORARY TABLE` и `MYSQL`.

<div id="source-filter-grants">
  ### Привилегии для фильтров источников
</div>

:::note
Эта возможность доступна начиная с версии 25.8 и только при включённой настройке сервера
`access_control_improvements.enable_read_write_grants`
:::

Вы можете предоставлять доступ к определённым URI источников с помощью фильтров по регулярным выражениям. Это позволяет точно контролировать, к каким внешним источникам данных пользователи могут получать доступ.

**Синтаксис:**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

Этот grant позволяет пользователю читать данные только из URI S3, соответствующих указанному шаблону регулярного выражения.

**Примеры:**

Предоставьте доступ к определённым путям S3 бакета:

```sql
-- Allow user to read only from s3://foo/ paths
GRANT READ ON S3('s3://foo/.*') TO john

-- Allow user to read from specific file patterns
GRANT READ ON S3('s3://mybucket/data/2024/.*\.parquet') TO analyst

-- Multiple filters can be granted to the same user
GRANT READ ON S3('s3://foo/.*') TO john
GRANT READ ON S3('s3://bar/.*') TO john
```

:::warning
Фильтр источника использует **regexp** в качестве параметра, поэтому grant
`GRANT READ ON URL('http://www.google.com') TO john;`

позволит выполнять запросы

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

поскольку в регулярных выражениях `.` интерпретируется как `любой одиночный символ`.
Это может привести к потенциальной уязвимости. Правильный grant должен быть

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**Повторная выдача grant с GRANT OPTION:**

Если исходный grant включает `WITH GRANT OPTION`, его можно выдать повторно с помощью `GRANT CURRENT GRANTS`:

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**Важные ограничения:**

* **Частичный отзыв не допускается:** Нельзя отозвать только часть шаблона фильтра, выданного в grant. При необходимости нужно отозвать весь grant и заново выдать его с новыми шаблонами.
* **Wildcard-grant не допускается:** Нельзя использовать `GRANT READ ON *('regexp')` или аналогичные шаблоны, состоящие только из wildcard-символов. Необходимо указать конкретный source.

<div id="dictget">
  ### dictGet
</div>

* `dictGet`. Псевдонимы: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Позволяет пользователю вызывать функции [dictGet](/ru/sql-reference/functions/ext-dict-functions#dictGet), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy), [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn).

Уровень привилегии: `DICTIONARY`.

**Примеры**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

Позволяет пользователю просматривать секреты в запросах `SHOW` и `SELECT`, если включены как
[`display_secrets_in_show_and_select` настройка сервера](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select),
так и
[`format_display_secrets_in_show_and_select` настройка формата](../../operations/settings/formats#format_display_secrets_in_show_and_select).

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

Разрешает определённую операцию с указанной именованной коллекцией. До версии 23.7 эта привилегия называлась NAMED COLLECTION CONTROL, а начиная с 23.7 была добавлена NAMED COLLECTION ADMIN, при этом NAMED COLLECTION CONTROL сохранён как псевдоним.

* `NAMED COLLECTION ADMIN`. Уровень: `NAMED_COLLECTION`. Псевдонимы: `NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`. Уровень: `NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`. Уровень: `NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`. Уровень: `NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`. Уровень: `NAMED_COLLECTION`. Псевдонимы: `SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`. Уровень: `NAMED_COLLECTION`. Псевдонимы: `SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`. Уровень: `NAMED_COLLECTION`. Псевдонимы: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

В отличие от всех остальных привилегий (CREATE, DROP, ALTER, SHOW), grant NAMED COLLECTION был добавлен только в версии 23.7, тогда как все остальные появились раньше — в 22.12.

**Примеры**

Предположим, что именованная коллекция называется abc. Выдадим пользователю john привилегию CREATE NAMED COLLECTION.

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### TABLE ENGINE
</div>

Позволяет использовать указанный движок таблицы при создании таблицы. Применяется к [движкам таблиц](../../engines/table-engines/index.md).

**Примеры**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
По умолчанию из соображений обратной совместимости при создании таблицы с конкретным движком таблицы привилегии игнорируются,
однако это поведение можно изменить, задав [`table_engines_require_grant` значение true](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)
в config.xml.
:::

Для некоторых движков таблиц с внешними источниками могут требоваться разрешения `READ`/`WRITE` для соответствующего источника. См. [Источники](#sources).

Например, для движка таблицы AzureBlobStorage может потребоваться следующая привилегия.

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

Предоставляет учетной записи пользователя или роли все привилегии на регулируемый объект.

:::note
Привилегия `ALL` не поддерживается в ClickHouse Cloud, где пользователь `default` имеет ограниченные разрешения. Пользователи могут предоставить пользователю максимальные разрешения, назначив `default_role`. Подробнее см. [здесь](/ru/cloud/security/manage-cloud-users).
Пользователи также могут использовать `GRANT CURRENT GRANTS` от имени пользователя `default`, чтобы добиться эффекта, аналогичного `ALL`.
:::

<div id="none">
  ### NONE
</div>

Не даёт никаких привилегий.

<div id="admin-option">
  ### ADMIN OPTION
</div>

Привилегия `ADMIN OPTION` позволяет пользователю назначить свою роль другому пользователю.