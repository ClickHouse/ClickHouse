---
description: 'توثيق لتعليمة GRANT'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'تعليمة GRANT'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # تعليمة GRANT
</div>

* تمنح [الامتيازات](#privileges) إلى حسابات مستخدمي ClickHouse أو الأدوار.
* تُسنِد الأدوار إلى حسابات المستخدمين أو إلى أدوار أخرى.

لسحب الامتيازات، استخدم تعليمة [REVOKE](../../sql-reference/statements/revoke.md). ويمكنك أيضًا عرض الامتيازات الممنوحة باستخدام تعليمة [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants).

<div id="granting-privilege-syntax">
  ## صيغة منح الامتياز
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — نوع الامتياز.
* `role` — دور مستخدم في ClickHouse.
* `user` — حساب مستخدم في ClickHouse.

تمنح العبارة `WITH GRANT OPTION` المستخدم `user` أو `role` صلاحية تنفيذ الاستعلام `GRANT`. ويمكن للمستخدمين منح امتيازات ضمن النطاق نفسه الذي يمتلكونه أو ضمن نطاق أضيق.
تستبدل العبارة `WITH REPLACE OPTION` الامتيازات القديمة بامتيازات جديدة لـ `user` أو `role`، وإذا لم تُحدَّد فإنها تُلحق الامتيازات.

<div id="assigning-role-syntax">
  ## صيغة إسناد الدور
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — دور مستخدم في ClickHouse.
* `user` — حساب مستخدم في ClickHouse.

تمنح العبارة `WITH ADMIN OPTION` امتياز [ADMIN OPTION](#admin-option) إلى `user` أو `role`.
تستبدل العبارة `WITH REPLACE OPTION` الأدوار القديمة بالدور الجديد لـ `user` أو `role`، وإذا لم يتم تحديدها، فستُضاف الأدوار.

<div id="grant-current-grants-syntax">
  ## صيغة منح الامتيازات الحالية
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — نوع الامتياز.
* `role` — دور مستخدم ClickHouse.
* `user` — حساب مستخدم ClickHouse.

يتيح لك استخدام التعليمة `CURRENT GRANTS` منح جميع الامتيازات المحددة إلى المستخدم أو الدور المعني.
إذا لم يتم تحديد أي امتيازات، فسيحصل المستخدم أو الدور المعني على جميع الامتيازات المتاحة لـ `CURRENT_USER`.

<div id="usage">
  ## الاستخدام
</div>

لاستخدام `GRANT`، يجب أن يمتلك حسابك امتياز `GRANT OPTION`. ولا يمكنك منح الامتيازات إلا ضمن نطاق امتيازات حسابك.

على سبيل المثال، منح المسؤول امتيازات للحساب `john` باستخدام الاستعلام التالي:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

هذا يعني أن `john` لديه إذن تنفيذ:

* `SELECT x,y FROM db.table`.
* `SELECT x FROM db.table`.
* `SELECT y FROM db.table`.

لا يمكن لـ `john` تنفيذ `SELECT z FROM db.table`. كما أن `SELECT * FROM db.table` غير متاح أيضًا. وعند معالجة هذا الاستعلام، لا يعيد ClickHouse أي بيانات، حتى `x` و`y`. والاستثناء الوحيد هو أن يحتوي الجدول على العمودين `x` و`y` فقط. في هذه الحالة يعيد ClickHouse جميع البيانات.

كما أن `john` لديه امتياز `GRANT OPTION`، لذا يمكنه منح مستخدمين آخرين امتيازات بالنطاق نفسه أو بنطاق أضيق.

يُسمح دائمًا بالوصول إلى قاعدة البيانات `system` (لأن قاعدة البيانات هذه تُستخدم لمعالجة الاستعلامات).

:::note
مع أنه توجد العديد من جداول النظام التي يمكن للمستخدمين الجدد الوصول إليها افتراضيًا، فقد لا يتمكنون من الوصول إلى جميع جداول النظام افتراضيًا من دون امتيازات ممنوحة.
بالإضافة إلى ذلك، يكون الوصول إلى بعض جداول النظام مثل `system.zookeeper` مقيّدًا لمستخدمي Cloud لأسباب أمنية.
:::

يمكنك منح عدة امتيازات لعدة حسابات في استعلام واحد. يتيح الاستعلام `GRANT SELECT, INSERT ON *.* TO john, robin` للحسابين `john` و`robin` تنفيذ استعلامَي `INSERT` و`SELECT` على جميع الجداول في جميع قواعد البيانات على الخادم.

<div id="wildcard-grants">
  ## المنح باستخدام أحرف البدل
</div>

عند تحديد الامتيازات، يمكنك استخدام النجمة (`*`) بدلًا من اسم جدول أو اسم قاعدة بيانات. على سبيل المثال، يتيح الاستعلام `GRANT SELECT ON db.* TO john` للمستخدم `john` تنفيذ الاستعلام `SELECT` على جميع الجداول في قاعدة البيانات `db`.
ويمكنك أيضًا حذف اسم قاعدة البيانات. في هذه الحالة، تُمنَح الامتيازات على قاعدة البيانات الحالية.
على سبيل المثال، يمنح `GRANT SELECT ON * TO john` الامتياز على جميع الجداول في قاعدة البيانات الحالية، بينما يمنح `GRANT SELECT ON mytable TO john` الامتياز على الجدول `mytable` في قاعدة البيانات الحالية.

:::note
الميزة الموضحة أدناه متاحة ابتداءً من إصدار ClickHouse 24.10.
:::

يمكنك أيضًا وضع النجمة في نهاية اسم جدول أو اسم قاعدة بيانات. تتيح لك هذه الميزة منح الامتيازات على بادئة مجردة لمسار الجدول.
مثال: `GRANT SELECT ON db.my_tables* TO john`. يتيح هذا الاستعلام للمستخدم `john` تنفيذ الاستعلام `SELECT` على جميع جداول قاعدة البيانات `db` التي تبدأ بالبادئة `my_tables*`.

مزيد من الأمثلة:

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

سترث جميع الجداول المُنشأة حديثًا ضمن المسارات الممنوحة تلقائيًا جميع الامتيازات من العناصر الأصلية.
على سبيل المثال، إذا نفّذت الاستعلام `GRANT SELECT ON db.* TO john` ثم أنشأت جدولًا جديدًا `db.new_table`، فسيتمكن المستخدم `john` من تنفيذ الاستعلام `SELECT * FROM db.new_table`.

يمكنك تحديد النجمة **فقط** للبادئات:

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## الامتيازات
</div>

الامتياز هو إذن يُمنَح للمستخدم لتنفيذ أنواع محددة من الاستعلامات.

للامتيازات تسلسل هرمي، وتعتمد مجموعة الاستعلامات المسموح بها على نطاق الامتياز.

يوضح ما يلي التسلسل الهرمي للامتيازات في ClickHouse:

* [`ALL`](#all)
  * [`إدارة الوصول`](#access-management)
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
  * [`SET DEFINER`](/ar/sql-reference/statements/create/view#sql_security)
  * [`SHOW`](#show)
    * `SHOW COLUMNS`
    * `SHOW DATABASES`
    * `SHOW DICTIONARIES`
    * `SHOW TABLES`
  * `SHOW FILESYSTEM CACHES`
  * [`المصادر`](#sources)
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

أمثلة على كيفية التعامل مع هذا التسلسل الهرمي:

* يشمل الامتياز `ALTER` جميع امتيازات `ALTER*` الأخرى.
* يشمل `ALTER CONSTRAINT` امتيازات `ALTER ADD CONSTRAINT` و`ALTER DROP CONSTRAINT` و`ALTER MODIFY CONSTRAINT`.

تُطبَّق الامتيازات على مستويات مختلفة. وتساعد معرفة المستوى في تحديد الصياغة المتاحة للامتياز.

المستويات (من الأدنى إلى الأعلى):

* `COLUMN` — يمكن منح الامتياز على مستوى العمود أو الجدول أو قاعدة البيانات أو على المستوى العام.
* `TABLE` — يمكن منح الامتياز على مستوى الجدول أو قاعدة البيانات أو على المستوى العام.
* `VIEW` — يمكن منح الامتياز على مستوى العرض أو قاعدة البيانات أو على المستوى العام.
* `DICTIONARY` — يمكن منح الامتياز على مستوى القاموس أو قاعدة البيانات أو على المستوى العام.
* `DATABASE` — يمكن منح الامتياز على مستوى قاعدة البيانات أو على المستوى العام.
* `GLOBAL` — لا يمكن منح الامتياز إلا على المستوى العام.
* `GROUP` — يجمع امتيازات من مستويات مختلفة. وعند منح امتياز على مستوى `GROUP`، لا تُمنح من المجموعة إلا الامتيازات التي تتوافق مع الصياغة المستخدمة.

أمثلة على الصياغة المسموح بها:

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

أمثلة على الصياغة غير المسموح بها:

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

يمنح الامتياز الخاص [ALL](#all) جميع الامتيازات إلى حساب مستخدم أو دور.

بشكل افتراضي، لا يملك حساب المستخدم أو الدور أي امتيازات.

إذا لم يكن لدى مستخدم أو دور أي امتيازات، فسيُعرض ذلك باعتباره الامتياز [NONE](#none).

تتطلب بعض الاستعلامات، بحسب طريقة تنفيذها، مجموعة من الامتيازات. على سبيل المثال، لتنفيذ استعلام [RENAME](../../sql-reference/statements/optimize.md) تحتاج إلى الامتيازات التالية: `SELECT` و`CREATE TABLE` و`INSERT` و`DROP TABLE`.

<div id="select">
  ### SELECT
</div>

يسمح بتنفيذ استعلامات [SELECT](../../sql-reference/statements/select/index.md).

مستوى الامتياز: `COLUMN`.

**الوصف**

يمكن للمستخدم الممنوح هذا الامتياز تنفيذ استعلامات `SELECT` على قائمة محددة من الأعمدة ضمن الجدول وقاعدة البيانات المحددين. وإذا تضمّن المستخدم أعمدة أخرى غير محددة، فلن يُرجِع الاستعلام أي بيانات.

لننظر إلى الامتياز التالي:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

يتيح هذا الامتياز للمستخدم `john` تنفيذ أي استعلام `SELECT` يتضمن بيانات من العمودين `x` و/أو `y` في `db.table`، على سبيل المثال `SELECT x FROM db.table`. لا يمكن لـ `john` تنفيذ `SELECT z FROM db.table`. كما أن `SELECT * FROM db.table` غير متاح أيضًا. عند معالجة هذا الاستعلام، لا يعيد ClickHouse أي بيانات، حتى `x` و`y`. والاستثناء الوحيد هو إذا كان الجدول يحتوي فقط على العمودين `x` و`y`، ففي هذه الحالة يعيد ClickHouse جميع البيانات.

<div id="insert">
  ### INSERT
</div>

يسمح بتنفيذ استعلامات [INSERT](../../sql-reference/statements/insert-into.md).

مستوى الامتياز: `COLUMN`.

**الوصف**

يمكن للمستخدم الذي مُنح هذا الامتياز تنفيذ استعلامات `INSERT` على قائمة محددة من الأعمدة في الجدول وقاعدة البيانات المحدَّدين. وإذا تضمّن المستخدم أعمدة أخرى غير المحددة، فلن يُدرِج الاستعلام أي بيانات.

**مثال**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

يتيح الامتياز الممنوح لـ `john` إدراج البيانات في العمودين `x` و/أو `y` في `db.table`.

<div id="alter">
  ### ALTER
</div>

يسمح بتنفيذ استعلامات [ALTER](../../sql-reference/statements/alter/index.md) وفقًا للتسلسل الهرمي التالي للامتيازات:

* `ALTER`. المستوى: `COLUMN`.
  * `ALTER TABLE`. المستوى: `GROUP`
  * `ALTER UPDATE`. المستوى: `COLUMN`. الأسماء البديلة: `UPDATE`
  * `ALTER DELETE`. المستوى: `COLUMN`. الأسماء البديلة: `DELETE`
  * `ALTER COLUMN`. المستوى: `GROUP`
  * `ALTER ADD COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `ADD COLUMN`
  * `ALTER DROP COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `DROP COLUMN`
  * `ALTER MODIFY COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `CLEAR COLUMN`
  * `ALTER RENAME COLUMN`. المستوى: `COLUMN`. الأسماء البديلة: `RENAME COLUMN`
  * `ALTER INDEX`. المستوى: `GROUP`. الأسماء البديلة: `INDEX`
  * `ALTER ORDER BY`. المستوى: `TABLE`. الأسماء البديلة: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
  * `ALTER SAMPLE BY`. المستوى: `TABLE`. الأسماء البديلة: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`. المستوى: `TABLE`. الأسماء البديلة: `ADD INDEX`
  * `ALTER DROP INDEX`. المستوى: `TABLE`. الأسماء البديلة: `DROP INDEX`
  * `ALTER MATERIALIZE INDEX`. المستوى: `TABLE`. الأسماء البديلة: `MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`. المستوى: `TABLE`. الأسماء البديلة: `CLEAR INDEX`
  * `ALTER CONSTRAINT`. المستوى: `GROUP`. الأسماء البديلة: `CONSTRAINT`
  * `ALTER ADD CONSTRAINT`. المستوى: `TABLE`. الأسماء البديلة: `ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`. المستوى: `TABLE`. الأسماء البديلة: `DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`. المستوى: `TABLE`. الأسماء البديلة: `MODIFY CONSTRAINT`
  * `ALTER TTL`. المستوى: `TABLE`. الأسماء البديلة: `ALTER MODIFY TTL`, `MODIFY TTL`
  * `ALTER MATERIALIZE TTL`. المستوى: `TABLE`. الأسماء البديلة: `MATERIALIZE TTL`
  * `ALTER SETTINGS`. المستوى: `TABLE`. الأسماء البديلة: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
  * `ALTER MOVE PARTITION`. المستوى: `TABLE`. الأسماء البديلة: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
  * `ALTER FETCH PARTITION`. المستوى: `TABLE`. الأسماء البديلة: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
  * `ALTER FREEZE PARTITION`. المستوى: `TABLE`. الأسماء البديلة: `FREEZE PARTITION`
  * `ALTER EXECUTE`. المستوى: `TABLE`. الأسماء البديلة: `ALTER TABLE EXECUTE`
  * `ALTER VIEW`. المستوى: `GROUP`
  * `ALTER VIEW REFRESH`. المستوى: `VIEW`. الأسماء البديلة: `REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`. المستوى: `VIEW`. الأسماء البديلة: `ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`. المستوى: `VIEW`. الأسماء البديلة: `ALTER TABLE MODIFY SQL SECURITY`

أمثلة على كيفية تطبيق هذا التسلسل الهرمي:

* يشمل الامتياز `ALTER` جميع امتيازات `ALTER*` الأخرى.
* يشمل `ALTER CONSTRAINT` امتيازات `ALTER ADD CONSTRAINT` و`ALTER DROP CONSTRAINT` و`ALTER MODIFY CONSTRAINT`.

**ملاحظات**

* يتيح الامتياز `MODIFY SETTING` تعديل إعدادات محرك الجدول. ولا يؤثر في الإعدادات أو معلمات تكوين الخادم.
* تتطلب العملية `ATTACH` امتياز [CREATE](#create).
* تتطلب العملية `DETACH` امتياز [DROP](#drop).
* لإيقاف عملية mutation بواسطة الاستعلام [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation)، يجب أن يكون لديك امتياز يتيح بدء عملية mutation هذه. على سبيل المثال، إذا كنت تريد إيقاف الاستعلام `ALTER UPDATE`، فستحتاج إلى الامتياز `ALTER UPDATE` أو `ALTER TABLE` أو `ALTER`.

<div id="backup">
  ### BACKUP
</div>

يسمح بتنفيذ [`BACKUP`] ضمن الاستعلامات. لمزيد من المعلومات حول النسخ الاحتياطية، راجع [&quot;النسخ الاحتياطي والاستعادة&quot;](/ar/operations/backup/overview).

<div id="create">
  ### CREATE
</div>

يسمح بتنفيذ استعلامات DDL الخاصة بـ [CREATE](../../sql-reference/statements/create/index.md) و[ATTACH](../../sql-reference/statements/attach.md) وفقًا لتسلسل هرمي الامتيازات التالي:

* `CREATE`. المستوى: `GROUP`
  * `CREATE DATABASE`. المستوى: `DATABASE`
  * `CREATE TABLE`. المستوى: `TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`. المستوى: `GLOBAL`
      * `CREATE TEMPORARY TABLE`. المستوى: `GLOBAL`
  * `CREATE VIEW`. المستوى: `VIEW`
  * `CREATE DICTIONARY`. المستوى: `DICTIONARY`

**ملاحظات**

* لحذف الجدول المُنشأ، يحتاج المستخدم إلى [DROP](#drop).

<div id="cluster">
  ### CLUSTER
</div>

يسمح بتنفيذ الاستعلامات باستخدام `ON CLUSTER`.

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

افتراضيًا، تتطلب الاستعلامات التي تتضمن `ON CLUSTER` أن يكون لدى المستخدم امتياز ‏`CLUSTER`.
ستظهر لك رسالة error التالية إذا حاولت استخدام `ON CLUSTER` في query من دون منح امتياز ‏`CLUSTER` أولًا:

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

يمكن تغيير السلوك الافتراضي بتعيين الإعداد `on_cluster_queries_require_cluster_grant`،
الموجود في قسم `access_control_improvements` ضمن `config.xml` (انظر أدناه)، إلى `false`.

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

يسمح بتنفيذ استعلامات [DROP](../../sql-reference/statements/drop.md) و[DETACH](../../sql-reference/statements/detach.md) وفقًا للتسلسل الهرمي التالي للامتيازات:

* `DROP`. المستوى: `GROUP`
  * `DROP DATABASE`. المستوى: `DATABASE`
  * `DROP TABLE`. المستوى: `TABLE`
  * `DROP VIEW`. المستوى: `VIEW`
  * `DROP DICTIONARY`. المستوى: `DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

يتيح تنفيذ استعلامات [TRUNCATE](../../sql-reference/statements/truncate.md).

مستوى الامتياز: `TABLE`.

<div id="optimize">
  ### OPTIMIZE
</div>

يسمح بتنفيذ استعلامات [OPTIMIZE TABLE](../../sql-reference/statements/optimize.md).

مستوى الامتياز: `TABLE`.

<div id="show">
  ### SHOW
</div>

يسمح بتنفيذ استعلامات `SHOW` و`DESCRIBE` و`USE` و`EXISTS` وفقًا للتسلسل الهرمي التالي للامتيازات:

* `SHOW`. المستوى: `GROUP`
  * `SHOW DATABASES`. المستوى: `DATABASE`. يتيح تنفيذ استعلامات `SHOW DATABASES` و`SHOW CREATE DATABASE` و`USE <database>`.
  * `SHOW TABLES`. المستوى: `TABLE`. يتيح تنفيذ استعلامات `SHOW TABLES` و`EXISTS <table>` و`CHECK <table>`.
  * `SHOW COLUMNS`. المستوى: `COLUMN`. يتيح تنفيذ استعلامات `SHOW CREATE TABLE` و`DESCRIBE`.
  * `SHOW DICTIONARIES`. المستوى: `DICTIONARY`. يتيح تنفيذ استعلامات `SHOW DICTIONARIES` و`SHOW CREATE DICTIONARY` و`EXISTS <dictionary>`.

**ملاحظات**

يكون لدى المستخدم امتياز `SHOW` إذا كان لديه أي امتياز آخر يتعلق بالجدول أو القاموس أو قاعدة البيانات المحددة.

<div id="kill-query">
  ### KILL QUERY
</div>

يسمح بتنفيذ استعلامات [KILL](../../sql-reference/statements/kill.md#kill-query) وفقًا لتسلسل هرمي الامتيازات التالي:

مستوى الامتياز: `GLOBAL`.

**ملاحظات**

يتيح امتياز `KILL QUERY` لمستخدمٍ ما إنهاء استعلامات المستخدمين الآخرين.

<div id="access-management">
  ### إدارة الوصول
</div>

يسمح للمستخدم بتنفيذ استعلامات إدارة المستخدمين والأدوار وسياسات الصفوف.

* `ACCESS MANAGEMENT`. المستوى: `GROUP`
  * `CREATE USER`. المستوى: `GLOBAL`
  * `ALTER USER`. المستوى: `GLOBAL`
  * `DROP USER`. المستوى: `GLOBAL`
  * `CREATE ROLE`. المستوى: `GLOBAL`
  * `ALTER ROLE`. المستوى: `GLOBAL`
  * `DROP ROLE`. المستوى: `GLOBAL`
  * `ROLE ADMIN`. المستوى: `GLOBAL`
  * `CREATE ROW POLICY`. المستوى: `GLOBAL`. الأسماء البديلة: `CREATE POLICY`
  * `ALTER ROW POLICY`. المستوى: `GLOBAL`. الأسماء البديلة: `ALTER POLICY`
  * `DROP ROW POLICY`. المستوى: `GLOBAL`. الأسماء البديلة: `DROP POLICY`
  * `CREATE QUOTA`. المستوى: `GLOBAL`
  * `ALTER QUOTA`. المستوى: `GLOBAL`
  * `DROP QUOTA`. المستوى: `GLOBAL`
  * `CREATE SETTINGS PROFILE`. المستوى: `GLOBAL`. الأسماء البديلة: `CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`. المستوى: `GLOBAL`. الأسماء البديلة: `ALTER PROFILE`
  * `DROP SETTINGS PROFILE`. المستوى: `GLOBAL`. الأسماء البديلة: `DROP PROFILE`
  * `SHOW ACCESS`. المستوى: `GROUP`
    * `SHOW_USERS`. المستوى: `GLOBAL`. الأسماء البديلة: `SHOW CREATE USER`
    * `SHOW_ROLES`. المستوى: `GLOBAL`. الأسماء البديلة: `SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`. المستوى: `GLOBAL`. الأسماء البديلة: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
    * `SHOW_QUOTAS`. المستوى: `GLOBAL`. الأسماء البديلة: `SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`. المستوى: `GLOBAL`. الأسماء البديلة: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`. المستوى: `GLOBAL`. الأسماء البديلة: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

يتيح امتياز `ROLE ADMIN` للمستخدم منح أي أدوار وسحبها، بما في ذلك الأدوار التي لم تُمنح للمستخدم مع خيار المسؤول.

<div id="system">
  ### SYSTEM
</div>

يسمح للمستخدم بتنفيذ استعلامات [SYSTEM](../../sql-reference/statements/system.md) وفق التسلسل الهرمي التالي للامتيازات.

* `SYSTEM`. المستوى: `GROUP`
  * `SYSTEM SHUTDOWN`. المستوى: `GLOBAL`. الأسماء البديلة: `SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`. الأسماء البديلة: `DROP CACHE`
    * `SYSTEM DROP DNS CACHE`. المستوى: `GLOBAL`. الأسماء البديلة: `SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`. المستوى: `GLOBAL`. الأسماء البديلة: `SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`. المستوى: `GLOBAL`. الأسماء البديلة: `SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`. المستوى: `GROUP`
    * `SYSTEM RELOAD CONFIG`. المستوى: `GLOBAL`. الأسماء البديلة: `RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`. المستوى: `GLOBAL`. الأسماء البديلة: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`. المستوى: `GLOBAL`. الأسماء البديلة: `RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`. المستوى: `GROUP`. الأسماء البديلة: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`. المستوى: `TABLE`. الأسماء البديلة: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`. المستوى: `TABLE`. الأسماء البديلة: `SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`. المستوى: `TABLE`. الأسماء البديلة: `RESTART REPLICA`
  * `SYSTEM FLUSH`. المستوى: `GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`. المستوى: `TABLE`. الأسماء البديلة: `FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`. المستوى: `GLOBAL`. الأسماء البديلة: `FLUSH LOGS`

يُمنَح امتياز `SYSTEM RELOAD EMBEDDED DICTIONARIES` ضمنيًا من خلال الامتياز `SYSTEM RELOAD DICTIONARY ON *.*`.

<div id="introspection">
  ### INTROSPECTION
</div>

يسمح باستخدام دوال [الفحص الداخلي](../../operations/optimizing-performance/sampling-query-profiler.md).

* `INTROSPECTION`. المستوى: `GROUP`. الأسماء البديلة: `INTROSPECTION FUNCTIONS`
  * `addressToLine`. المستوى: `GLOBAL`
  * `addressToLineWithInlines`. المستوى: `GLOBAL`
  * `addressToSymbol`. المستوى: `GLOBAL`
  * `demangle`. المستوى: `GLOBAL`

<div id="sources">
  ### المصادر
</div>

يسمح باستخدام مصادر بيانات خارجية. ينطبق ذلك على [محركات الجداول](../../engines/table-engines/index.md) و[دوال الجداول](/ar/sql-reference/table-functions).

* `READ`. المستوى: `GLOBAL_WITH_PARAMETER`
* `WRITE`. المستوى: `GLOBAL_WITH_PARAMETER`

المعلمات المتاحة:

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
أصبح الفصل بين امتيازات READ/WRITE للمصادر متاحًا بدءًا من الإصدار 25.7، وفقط مع إعداد الخادم
`access_control_improvements.enable_read_write_grants`

بخلاف ذلك، يجب استخدام الصيغة `GRANT AZURE ON *.* TO user`، وهي مكافئة للصيغة الجديدة `GRANT READ, WRITE ON AZURE TO user`
:::

أمثلة:

* لإنشاء جدول باستخدام [محرك جدول MySQL](../../engines/table-engines/integrations/mysql.md)، تحتاج إلى `CREATE TABLE (ON db.table_name)` وامتيازات `MYSQL`.
* لاستخدام [دالة جدول MySQL](../../sql-reference/table-functions/mysql.md)، تحتاج إلى `CREATE TEMPORARY TABLE` وامتيازات `MYSQL`.

<div id="source-filter-grants">
  ### امتيازات تصفية المصادر
</div>

:::note
تتوفر هذه الميزة بدءًا من الإصدار 25.8، وفقط عند تفعيل إعداد الخادم
`access_control_improvements.enable_read_write_grants`
:::

يمكنك منح الوصول إلى عناوين URI لمصادر محددة باستخدام عوامل تصفية بالتعبيرات النمطية. يتيح ذلك تحكمًا دقيقًا في مصادر البيانات الخارجية التي يمكن للمستخدمين الوصول إليها.

**الصيغة:**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

سيتيح هذا الامتياز للمستخدم القراءة فقط من عناوين URI في S3 التي تطابق نمط التعبير النمطي المحدد.

**أمثلة:**

امنح حق الوصول إلى مسارات محددة داخل حاوية S3:

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
يقبل مُرشِّح المصدر **regexp** كمعلمة، لذا فإن منح امتياز مثل
`GRANT READ ON URL('http://www.google.com') TO john;`

سيسمح بتنفيذ الاستعلامات

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

لأن `.` يُعامَل كـ `أي محرف واحد` في التعابير النمطية.
وقد يؤدي ذلك إلى ثغرة أمنية محتملة. والصحيح أن يكون منح الصلاحية كما يلي

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**إعادة منح الامتياز باستخدام GRANT OPTION:**

إذا كان الامتياز الممنوح أصلًا يتضمن `WITH GRANT OPTION`، فيمكن إعادة منحه باستخدام `GRANT CURRENT GRANTS`:

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**قيود مهمة:**

* **لا يُسمح بالإلغاء الجزئي للصلاحيات:** لا يمكنك إلغاء جزء من نمط تصفية مُنِح سابقًا. يجب إلغاء المنح بالكامل ثم منحه مجددًا بأنماط جديدة عند الحاجة.
* **لا يُسمح بالمنح باستخدام أحرف البدل:** لا يمكنك استخدام `GRANT READ ON *('regexp')` أو أنماط مشابهة تعتمد فقط على أحرف البدل. يجب تحديد مصدر معيّن.

<div id="dictget">
  ### dictGet
</div>

* `dictGet`. الأسماء البديلة: `dictHas`, `dictGetHierarchy`, `dictIsIn`

يتيح للمستخدم تنفيذ الدوال [dictGet](/ar/sql-reference/functions/ext-dict-functions#dictGet)، و[dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas)، و[dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy)، و[dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn).

مستوى الامتياز: `DICTIONARY`.

**أمثلة**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

يسمح للمستخدم بعرض القيم السرية في استعلامات `SHOW` و`SELECT` إذا كان كلٌّ من
[`display_secrets_in_show_and_select` إعداد الخادم](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
و
[`format_display_secrets_in_show_and_select` إعداد التنسيق](../../operations/settings/formats#format_display_secrets_in_show_and_select)
مفعّلَين.

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

يسمح بتنفيذ عملية معيّنة على مجموعة مسماة محددة. قبل الإصدار 23.7، كان يُسمى `NAMED COLLECTION CONTROL`، وبعد 23.7 أُضيف `NAMED COLLECTION ADMIN` مع الإبقاء على `NAMED COLLECTION CONTROL` كبديل.

* `NAMED COLLECTION ADMIN`. المستوى: `NAMED_COLLECTION`. البدائل: `NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`. المستوى: `NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`. المستوى: `NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`. المستوى: `NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`. المستوى: `NAMED_COLLECTION`. البدائل: `SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`. المستوى: `NAMED_COLLECTION`. البدائل: `SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`. المستوى: `NAMED_COLLECTION`. البدائل: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

بخلاف جميع الامتيازات الأخرى (CREATE وDROP وALTER وSHOW)، لم يُضَف امتياز `NAMED COLLECTION` إلا في الإصدار 23.7، بينما أُضيفت جميع الامتيازات الأخرى سابقًا، في 22.12.

**أمثلة**

على افتراض أن المجموعة المسماة اسمها abc، نمنح الامتياز `CREATE NAMED COLLECTION` للمستخدم john.

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### محرك الجدول
</div>

يسمح باستخدام محرك جدول محدد عند إنشاء جدول. ينطبق ذلك على [محركات الجداول](../../engines/table-engines/index.md).

**أمثلة**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
بشكل افتراضي، ولأسباب تتعلق بالتوافق مع الإصدارات السابقة، يتجاهل إنشاء جدول باستخدام محرك جدول محدد الامتيازات،
ولكن يمكنك تغيير هذا السلوك بضبط [`table_engines_require_grant` على true](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)
في config.xml.
:::

قد تتطلب بعض محركات الجداول التي تستخدم مصادر خارجية أذونات `READ`/`WRITE` على المصدر المقابل. راجع [المصادر](#sources).

على سبيل المثال، بالنسبة إلى محرك الجدول AzureBlobStorage، قد يكون الامتياز التالي مطلوبًا.

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

يمنح جميع الامتيازات على الكيان الخاضع للتحكم إلى حساب مستخدم أو دور.

:::note
الامتياز `ALL` غير مدعوم في ClickHouse Cloud، حيث تكون أذونات المستخدم `default` محدودة. يمكن للمستخدمين منح الحد الأقصى من الأذونات لمستخدم ما من خلال منح `default_role`. راجع [هنا](/ar/cloud/security/manage-cloud-users) لمزيد من التفاصيل.
يمكن للمستخدمين أيضًا استخدام `GRANT CURRENT GRANTS` عند تسجيل الدخول بالمستخدم `default` لتحقيق تأثير مشابه لـ `ALL`.
:::

<div id="none">
  ### NONE
</div>

لا يمنح أي امتيازات.

<div id="admin-option">
  ### ADMIN OPTION
</div>

يتيح امتياز `ADMIN OPTION` للمستخدم منح دوره الخاص لمستخدم آخر.