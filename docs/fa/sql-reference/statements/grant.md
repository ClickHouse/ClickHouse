---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: GRANT
---

# GRANT {#grant}

-   کمک های مالی [امتیازات](#grant-privileges) به حساب کاربر کلیک و یا نقش.
-   اختصاص نقش به حساب های کاربری و یا به نقش های دیگر.

برای لغو امتیازات از [REVOKE](revoke.md) بیانیه. همچنین شما می توانید لیست امتیازات اعطا شده توسط [SHOW GRANTS](show.md#show-grants-statement) بیانیه.

## اعطای نحو امتیاز {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

این `WITH GRANT OPTION` کمک های مالی بند `user` یا `role` با اجازه به انجام `GRANT` پرس و جو. کاربران می توانند امتیازات از همان دامنه دارند و کمتر اعطا.

## اعطای نحو نقش {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

این `WITH ADMIN OPTION` مجموعه بند [ADMIN OPTION](#admin-option-privilege) امتیاز برای `user` یا `role`.

## استفاده {#grant-usage}

برای استفاده `GRANT` حساب شما باید `GRANT OPTION` امتیاز. شما می توانید امتیازات تنها در داخل دامنه امتیازات حساب خود را اعطا.

مثلا, مدیر امتیازات به اعطا کرده است `john` حساب توسط پرس و جو:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

این بدان معنی است که `john` دارای مجوز برای انجام:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` نمی تواند انجام دهد `SELECT z FROM db.table`. این `SELECT * FROM db.table` همچنین در دسترس نیست. پردازش این پرس و جو, تاتر هیچ داده بازگشت نیست, حتی `x` و `y`. تنها استثنا است اگر یک جدول شامل تنها `x` و `y` ستون, در این مورد کلیکهاوس را برمی گرداند تمام داده ها.

همچنین `john` دارد `GRANT OPTION` امتیاز, بنابراین می تواند کاربران دیگر با امتیازات از همان و یا دامنه کوچکتر اعطا.

مشخص امتیازات شما می توانید ستاره استفاده کنید (`*`) به جای یک جدول و یا یک نام پایگاه داده. برای مثال `GRANT SELECT ON db.* TO john` پرسوجو اجازه میدهد `john` برای انجام `SELECT` پرس و جو بیش از همه جداول در `db` بانک اطلاعات. همچنین می توانید نام پایگاه داده را حذف کنید. برای مثال در این مورد امتیازات برای پایگاه داده فعلی اعطا می شود: `GRANT SELECT ON * TO john` اعطا امتیاز در تمام جداول در پایگاه داده فعلی, `GRANT SELECT ON mytable TO john` اعطا امتیاز در `mytable` جدول در پایگاه داده فعلی.

دسترسی به `system` پایگاه داده همیشه مجاز (از این پایگاه داده برای پردازش نمایش داده شد استفاده می شود).

شما می توانید امتیازات متعدد به حساب های متعدد در یک پرس و جو عطا کند. پرسوجو `GRANT SELECT, INSERT ON *.* TO john, robin` اجازه می دهد تا حساب `john` و `robin` برای انجام `INSERT` و `SELECT` نمایش داده شد بیش از همه جداول در تمام پایگاه داده بر روی سرور.

## امتیازات {#grant-privileges}

امتیاز اجازه به انجام نوع خاصی از نمایش داده شد است.

امتیازات یک ساختار سلسله مراتبی. مجموعه ای از نمایش داده شد مجاز بستگی به دامنه امتیاز.

سلسله مراتب امتیازات:

-   [SELECT](#grant-select)
-   [INSERT](#grant-insert)
-   [ALTER](#grant-alter)
    -   `ALTER TABLE`
        -   `ALTER UPDATE`
        -   `ALTER DELETE`
        -   `ALTER COLUMN`
            -   `ALTER ADD COLUMN`
            -   `ALTER DROP COLUMN`
            -   `ALTER MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`
        -   `ALTER INDEX`
            -   `ALTER ORDER BY`
            -   `ALTER ADD INDEX`
            -   `ALTER DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`
        -   `ALTER CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`
        -   `ALTER TTL`
        -   `ALTER MATERIALIZE TTL`
        -   `ALTER SETTINGS`
        -   `ALTER MOVE PARTITION`
        -   `ALTER FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`
    -   `ALTER VIEW`
        -   `ALTER VIEW REFRESH`
        -   `ALTER VIEW MODIFY QUERY`
-   [CREATE](#grant-create)
    -   `CREATE DATABASE`
    -   `CREATE TABLE`
    -   `CREATE VIEW`
    -   `CREATE DICTIONARY`
    -   `CREATE TEMPORARY TABLE`
-   [DROP](#grant-drop)
    -   `DROP DATABASE`
    -   `DROP TABLE`
    -   `DROP VIEW`
    -   `DROP DICTIONARY`
-   [TRUNCATE](#grant-truncate)
-   [OPTIMIZE](#grant-optimize)
-   [SHOW](#grant-show)
    -   `SHOW DATABASES`
    -   `SHOW TABLES`
    -   `SHOW COLUMNS`
    -   `SHOW DICTIONARIES`
-   [KILL QUERY](#grant-kill-query)
-   [ACCESS MANAGEMENT](#grant-access-management)
    -   `CREATE USER`
    -   `ALTER USER`
    -   `DROP USER`
    -   `CREATE ROLE`
    -   `ALTER ROLE`
    -   `DROP ROLE`
    -   `CREATE ROW POLICY`
    -   `ALTER ROW POLICY`
    -   `DROP ROW POLICY`
    -   `CREATE QUOTA`
    -   `ALTER QUOTA`
    -   `DROP QUOTA`
    -   `CREATE SETTINGS PROFILE`
    -   `ALTER SETTINGS PROFILE`
    -   `DROP SETTINGS PROFILE`
    -   `SHOW ACCESS`
        -   `SHOW_USERS`
        -   `SHOW_ROLES`
        -   `SHOW_ROW_POLICIES`
        -   `SHOW_QUOTAS`
        -   `SHOW_SETTINGS_PROFILES`
    -   `ROLE ADMIN`
-   [SYSTEM](#grant-system)
    -   `SYSTEM SHUTDOWN`
    -   `SYSTEM DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`
        -   `SYSTEM DROP MARK CACHE`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`
    -   `SYSTEM RELOAD`
        -   `SYSTEM RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`
    -   `SYSTEM TTL MERGES`
    -   `SYSTEM FETCHES`
    -   `SYSTEM MOVES`
    -   `SYSTEM SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`
    -   `SYSTEM FLUSH`
        -   `SYSTEM FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`
-   [INTROSPECTION](#grant-introspection)
    -   `addressToLine`
    -   `addressToSymbol`
    -   `demangle`
-   [SOURCES](#grant-sources)
    -   `FILE`
    -   `URL`
    -   `REMOTE`
    -   `YSQL`
    -   `ODBC`
    -   `JDBC`
    -   `HDFS`
    -   `S3`
-   [دیکته کردن](#grant-dictget)

نمونه هایی از چگونه این سلسله مراتب درمان می شود:

-   این `ALTER` امتیاز شامل تمام دیگر `ALTER*` امتیازات.
-   `ALTER CONSTRAINT` شامل `ALTER ADD CONSTRAINT` و `ALTER DROP CONSTRAINT` امتیازات.

امتیازات در سطوح مختلف اعمال می شود. دانستن یک سطح نشان می دهد نحو در دسترس برای امتیاز.

سطح (از پایین به بالاتر):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`- امتیاز سطح اعطا شده است, تنها که امتیازات از گروه اعطا که به نحو استفاده مطابقت.

نمونه هایی از نحو مجاز:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

نمونه هایی از نحو غیر مجاز:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

امتیاز ویژه [ALL](#grant-all) اعطا تمام امتیازات به یک حساب کاربری و یا نقش.

به طور پیش فرض, یک حساب کاربری و یا نقش هیچ امتیازات.

اگر یک کاربر یا نقش هیچ امتیاز به عنوان نمایش داده می شود [NONE](#grant-none) امتیاز.

برخی از نمایش داده شد با اجرای خود نیاز به مجموعه ای از امتیازات. برای مثال برای انجام [RENAME](misc.md#misc_operations-rename) پرس و جو شما نیاز به امتیازات زیر: `SELECT`, `CREATE TABLE`, `INSERT` و `DROP TABLE`.

### SELECT {#grant-select}

اجازه می دهد تا برای انجام [SELECT](select/index.md) نمایش داده شد.

سطح امتیاز: `COLUMN`.

**توصیف**

کاربر اعطا شده با این امتیاز می تواند انجام دهد `SELECT` نمایش داده شد بیش از یک لیست مشخص از ستون ها در جدول و پایگاه داده مشخص شده است. اگر کاربر شامل ستون های دیگر و سپس مشخص پرس و جو هیچ داده گرداند.

امتیاز زیر را در نظر بگیرید:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

این امتیاز اجازه می دهد `john` برای انجام هر `SELECT` پرس و جو که شامل داده ها از `x` و / یا `y` ستونها در `db.table`. به عنوان مثال, `SELECT x FROM db.table`. `john` نمی تواند انجام دهد `SELECT z FROM db.table`. این `SELECT * FROM db.table` همچنین در دسترس نیست. پردازش این پرس و جو, تاتر هیچ داده بازگشت نیست, حتی `x` و `y`. تنها استثنا است اگر یک جدول شامل تنها `x` و `y` ستون, در این مورد کلیکهاوس را برمی گرداند تمام داده ها.

### INSERT {#grant-insert}

اجازه می دهد تا انجام [INSERT](insert-into.md) نمایش داده شد.

سطح امتیاز: `COLUMN`.

**توصیف**

کاربر اعطا شده با این امتیاز می تواند انجام دهد `INSERT` نمایش داده شد بیش از یک لیست مشخص از ستون ها در جدول و پایگاه داده مشخص شده است. اگر کاربر شامل ستون های دیگر و سپس مشخص پرس و جو هیچ داده وارد کنید.

**مثال**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

امتیاز اعطا شده اجازه می دهد `john` برای وارد کردن داده ها به `x` و / یا `y` ستونها در `db.table`.

### ALTER {#grant-alter}

اجازه می دهد تا انجام [ALTER](alter.md) نمایش داده شد مربوط به سلسله مراتب زیر از امتیازات:

-   `ALTER`. سطح: `COLUMN`.
    -   `ALTER TABLE`. سطح: `GROUP`
        -   `ALTER UPDATE`. سطح: `COLUMN`. نامگردانها: `UPDATE`
        -   `ALTER DELETE`. سطح: `COLUMN`. نامگردانها: `DELETE`
        -   `ALTER COLUMN`. سطح: `GROUP`
            -   `ALTER ADD COLUMN`. سطح: `COLUMN`. نامگردانها: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. سطح: `COLUMN`. نامگردانها: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. سطح: `COLUMN`. نامگردانها: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. سطح: `COLUMN`. نامگردانها: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. سطح: `COLUMN`. نامگردانها: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. سطح: `COLUMN`. نامگردانها: `RENAME COLUMN`
        -   `ALTER INDEX`. سطح: `GROUP`. نامگردانها: `INDEX`
            -   `ALTER ORDER BY`. سطح: `TABLE`. نامگردانها: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. سطح: `TABLE`. نامگردانها: `ADD INDEX`
            -   `ALTER DROP INDEX`. سطح: `TABLE`. نامگردانها: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. سطح: `TABLE`. نامگردانها: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. سطح: `TABLE`. نامگردانها: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. سطح: `GROUP`. نامگردانها: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. سطح: `TABLE`. نامگردانها: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. سطح: `TABLE`. نامگردانها: `DROP CONSTRAINT`
        -   `ALTER TTL`. سطح: `TABLE`. نامگردانها: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. سطح: `TABLE`. نامگردانها: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. سطح: `TABLE`. نامگردانها: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. سطح: `TABLE`. نامگردانها: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. سطح: `TABLE`. نامگردانها: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. سطح: `TABLE`. نامگردانها: `FREEZE PARTITION`
    -   `ALTER VIEW` سطح: `GROUP`
        -   `ALTER VIEW REFRESH`. سطح: `VIEW`. نامگردانها: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. سطح: `VIEW`. نامگردانها: `ALTER TABLE MODIFY QUERY`

نمونه هایی از چگونه این سلسله مراتب درمان می شود:

-   این `ALTER` امتیاز شامل تمام دیگر `ALTER*` امتیازات.
-   `ALTER CONSTRAINT` شامل `ALTER ADD CONSTRAINT` و `ALTER DROP CONSTRAINT` امتیازات.

**یادداشتها**

-   این `MODIFY SETTING` امتیاز اجازه می دهد تا برای تغییر تنظیمات موتور جدول. در تنظیمات و یا پارامترهای پیکربندی سرور تاثیر نمی گذارد.
-   این `ATTACH` عملیات نیاز دارد [CREATE](#grant-create) امتیاز.
-   این `DETACH` عملیات نیاز دارد [DROP](#grant-drop) امتیاز.
-   برای جلوگیری از جهش توسط [KILL MUTATION](misc.md#kill-mutation) پرس و جو, شما نیاز به یک امتیاز برای شروع این جهش. مثلا, اگر شما می خواهید برای جلوگیری از `ALTER UPDATE` پرس و جو, شما نیاز به `ALTER UPDATE`, `ALTER TABLE` یا `ALTER` امتیاز.

### CREATE {#grant-create}

اجازه می دهد تا برای انجام [CREATE](create.md) و [ATTACH](misc.md#attach) دی ال-پرس و جو مربوط به سلسله مراتب زیر از امتیازات:

-   `CREATE`. سطح: `GROUP`
    -   `CREATE DATABASE`. سطح: `DATABASE`
    -   `CREATE TABLE`. سطح: `TABLE`
    -   `CREATE VIEW`. سطح: `VIEW`
    -   `CREATE DICTIONARY`. سطح: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. سطح: `GLOBAL`

**یادداشتها**

-   برای حذف جدول ایجاد شده نیاز کاربر [DROP](#grant-drop).

### DROP {#grant-drop}

اجازه می دهد تا برای انجام [DROP](misc.md#drop) و [DETACH](misc.md#detach) نمایش داده شد مربوط به سلسله مراتب زیر از امتیازات:

-   `DROP`. سطح:
    -   `DROP DATABASE`. سطح: `DATABASE`
    -   `DROP TABLE`. سطح: `TABLE`
    -   `DROP VIEW`. سطح: `VIEW`
    -   `DROP DICTIONARY`. سطح: `DICTIONARY`

### TRUNCATE {#grant-truncate}

اجازه می دهد تا برای انجام [TRUNCATE](misc.md#truncate-statement) نمایش داده شد.

سطح امتیاز: `TABLE`.

### OPTIMIZE {#grant-optimize}

اجازه می دهد تا برای انجام [OPTIMIZE TABLE](misc.md#misc_operations-optimize) نمایش داده شد.

سطح امتیاز: `TABLE`.

### SHOW {#grant-show}

اجازه می دهد تا برای انجام `SHOW`, `DESCRIBE`, `USE` و `EXISTS` نمایش داده شد, مربوط به سلسله مراتب زیر از امتیازات:

-   `SHOW`. سطح: `GROUP`
    -   `SHOW DATABASES`. سطح: `DATABASE`. اجازه می دهد تا برای اجرای `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` نمایش داده شد.
    -   `SHOW TABLES`. سطح: `TABLE`. اجازه می دهد تا برای اجرای `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` نمایش داده شد.
    -   `SHOW COLUMNS`. سطح: `COLUMN`. اجازه می دهد تا برای اجرای `SHOW CREATE TABLE`, `DESCRIBE` نمایش داده شد.
    -   `SHOW DICTIONARIES`. سطح: `DICTIONARY`. اجازه می دهد تا برای اجرای `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` نمایش داده شد.

**یادداشتها**

یک کاربر است `SHOW` امتیاز اگر هر امتیاز دیگری در مورد جدول مشخص, فرهنگ لغت و یا پایگاه داده.

### KILL QUERY {#grant-kill-query}

اجازه می دهد تا برای انجام [KILL](misc.md#kill-query-statement) نمایش داده شد مربوط به سلسله مراتب زیر از امتیازات:

سطح امتیاز: `GLOBAL`.

**یادداشتها**

`KILL QUERY` امتیاز اجازه می دهد تا یک کاربر برای کشتن نمایش داده شد از کاربران دیگر.

### ACCESS MANAGEMENT {#grant-access-management}

اجازه می دهد تا کاربر را به انجام نمایش داده شد که مدیریت کاربران, نقش ها و سیاست های ردیف.

-   `ACCESS MANAGEMENT`. سطح: `GROUP`
    -   `CREATE USER`. سطح: `GLOBAL`
    -   `ALTER USER`. سطح: `GLOBAL`
    -   `DROP USER`. سطح: `GLOBAL`
    -   `CREATE ROLE`. سطح: `GLOBAL`
    -   `ALTER ROLE`. سطح: `GLOBAL`
    -   `DROP ROLE`. سطح: `GLOBAL`
    -   `ROLE ADMIN`. سطح: `GLOBAL`
    -   `CREATE ROW POLICY`. سطح: `GLOBAL`. نامگردانها: `CREATE POLICY`
    -   `ALTER ROW POLICY`. سطح: `GLOBAL`. نامگردانها: `ALTER POLICY`
    -   `DROP ROW POLICY`. سطح: `GLOBAL`. نامگردانها: `DROP POLICY`
    -   `CREATE QUOTA`. سطح: `GLOBAL`
    -   `ALTER QUOTA`. سطح: `GLOBAL`
    -   `DROP QUOTA`. سطح: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. سطح: `GLOBAL`. نامگردانها: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. سطح: `GLOBAL`. نامگردانها: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. سطح: `GLOBAL`. نامگردانها: `DROP PROFILE`
    -   `SHOW ACCESS`. سطح: `GROUP`
        -   `SHOW_USERS`. سطح: `GLOBAL`. نامگردانها: `SHOW CREATE USER`
        -   `SHOW_ROLES`. سطح: `GLOBAL`. نامگردانها: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. سطح: `GLOBAL`. نامگردانها: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. سطح: `GLOBAL`. نامگردانها: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. سطح: `GLOBAL`. نامگردانها: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

این `ROLE ADMIN` امتیاز اجازه می دهد تا کاربر را به اعطای و لغو هر نقش از جمله کسانی که به کاربر با گزینه مدیریت داده نمی شود.

### SYSTEM {#grant-system}

اجازه می دهد تا کاربر را به انجام [SYSTEM](system.md) نمایش داده شد مربوط به سلسله مراتب زیر از امتیازات.

-   `SYSTEM`. سطح: `GROUP`
    -   `SYSTEM SHUTDOWN`. سطح: `GLOBAL`. نامگردانها: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. نامگردانها: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. سطح: `GLOBAL`. نامگردانها: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. سطح: `GLOBAL`. نامگردانها: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. سطح: `GLOBAL`. نامگردانها: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. سطح: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. سطح: `GLOBAL`. نامگردانها: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. سطح: `GLOBAL`. نامگردانها: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. سطح: `GLOBAL`. نامگردانها: تحقیق`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. سطح: `GROUP`. نامگردانها: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. سطح: `TABLE`. نامگردانها: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. سطح: `TABLE`. نامگردانها: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. سطح: `TABLE`. نامگردانها: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. سطح: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. سطح: `TABLE`. نامگردانها: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. سطح: `GLOBAL`. نامگردانها: `FLUSH LOGS`

این `SYSTEM RELOAD EMBEDDED DICTIONARIES` امتیاز به طور ضمنی توسط اعطا `SYSTEM RELOAD DICTIONARY ON *.*` امتیاز.

### INTROSPECTION {#grant-introspection}

اجازه می دهد تا با استفاده از [درون نگری](../../operations/optimizing-performance/sampling-query-profiler.md) توابع.

-   `INTROSPECTION`. سطح: `GROUP`. نامگردانها: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. سطح: `GLOBAL`
    -   `addressToSymbol`. سطح: `GLOBAL`
    -   `demangle`. سطح: `GLOBAL`

### SOURCES {#grant-sources}

اجازه می دهد تا با استفاده از منابع داده های خارجی. امر به [موتورهای جدول](../../engines/table-engines/index.md) و [توابع جدول](../table-functions/index.md#table-functions).

-   `SOURCES`. سطح: `GROUP`
    -   `FILE`. سطح: `GLOBAL`
    -   `URL`. سطح: `GLOBAL`
    -   `REMOTE`. سطح: `GLOBAL`
    -   `YSQL`. سطح: `GLOBAL`
    -   `ODBC`. سطح: `GLOBAL`
    -   `JDBC`. سطح: `GLOBAL`
    -   `HDFS`. سطح: `GLOBAL`
    -   `S3`. سطح: `GLOBAL`

این `SOURCES` امتیاز را قادر می سازد استفاده از تمام منابع. همچنین شما می توانید یک امتیاز برای هر منبع به صورت جداگانه اعطا. برای استفاده از منابع, شما نیاز به امتیازات اضافی.

مثالها:

-   برای ایجاد یک جدول با [خروجی زیر موتور جدول](../../engines/table-engines/integrations/mysql.md) شما نیاز دارید `CREATE TABLE (ON db.table_name)` و `MYSQL` امتیازات.
-   برای استفاده از [تابع جدول خروجی زیر](../table-functions/mysql.md) شما نیاز دارید `CREATE TEMPORARY TABLE` و `MYSQL` امتیازات.

### دیکته کردن {#grant-dictget}

-   `dictGet`. نامگردانها: `dictHas`, `dictGetHierarchy`, `dictIsIn`

اجازه می دهد تا کاربر را به اجرا [دیکته کردن](../functions/ext-dict-functions.md#dictget), [دیکتس](../functions/ext-dict-functions.md#dicthas), [حکومت دیکتاتوری](../functions/ext-dict-functions.md#dictgethierarchy), [دیکتاتوری](../functions/ext-dict-functions.md#dictisin) توابع.

سطح امتیاز: `DICTIONARY`.

**مثالها**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

اعطا تمام امتیازات در نهاد تنظیم به یک حساب کاربری و یا نقش.

### NONE {#grant-none}

هیچ امتیازاتی به دست نمیاره

### ADMIN OPTION {#admin-option-privilege}

این `ADMIN OPTION` امتیاز اجازه می دهد تا کاربر اعطای نقش خود را به یک کاربر دیگر.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
