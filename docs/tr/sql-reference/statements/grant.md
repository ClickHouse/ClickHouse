---
machine_translated: true
machine_translated_rev: cbd8aa9052361a7ee11c209560cff7175c2b8e42
toc_priority: 39
toc_title: GRANT
---

# GRANT {#grant}

-   Veriyor [ayrıcalıklar](#grant-privileges) kullanıcı hesaplarını veya rollerini tıklamak için.
-   Rolleri kullanıcı hesaplarına veya diğer rollere atar.

Ayrıcalıkları iptal etmek için [REVOKE](../../sql-reference/statements/revoke.md) deyim. Ayrıca, verilen ayrıcalıkları listeleyebilirsiniz [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants-statement) deyim.

## Ayrıcalık Sözdizimi Verme {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

Bu `WITH GRANT OPTION` madde hibeleri `user` veya `role` yürütme izni ile `GRANT` sorgu. Kullanıcılar sahip oldukları ve daha az olan aynı kapsamdaki ayrıcalıkları verebilir.

## Rol Sözdizimi Atama {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

Bu `WITH ADMIN OPTION` madde hibeleri [ADMIN OPTION](#admin-option-privilege) ayrıcalık `user` veya `role`.

## Kullanma {#grant-usage}

Kullanmak `GRANT` hesabınız olmalıdır var `GRANT OPTION` ayrıcalık. Ayrıcalıkları yalnızca hesap ayrıcalıklarınız kapsamında verebilirsiniz.

Örneğin, yönetici için ayrıcalıklar verdi `john` sorguya göre hesap:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Demek ki `john` yürütme izni var mı:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` idam edilemiyor `SELECT z FROM db.table`. Bu `SELECT * FROM db.table` ayrıca mevcut değildir. Bu sorguyu işlerken, ClickHouse bile herhangi bir veri döndürmez `x` ve `y`. Tek istisna, bir tablo yalnızca `x` ve `y` sütun. Bu durumda ClickHouse tüm verileri döndürür.

Ayrıca `john` vardır `GRANT OPTION` ayrıcalık, böylece diğer kullanıcılara aynı veya daha küçük kapsamdaki ayrıcalıklar verebilir.

Ayrıcalıkları belirtme Yıldız İşareti kullanabilirsiniz (`*`) bir tablo veya veritabanı adı yerine. Örneğin, `GRANT SELECT ON db.* TO john` sorgu sağlar `john` yürütmek için `SELECT` tüm tablolar üzerinde sorgu `db` veritabanı. Ayrıca, veritabanı adı atlayabilirsiniz. Bu durumda, geçerli veritabanı için ayrıcalıklar verilir. Mesela, `GRANT SELECT ON * TO john` geçerli veritabanındaki tüm tablolarda ayrıcalık verir, `GRANT SELECT ON mytable TO john` bu ayrıcalığı verir `mytable` geçerli veritabanındaki tablo.

Erişim `system` veritabanına her zaman izin verilir (bu veritabanı sorguları işlemek için kullanıldığından).

Tek bir sorguda birden çok hesaba birden çok ayrıcalık verebilirsiniz. Sorgu `GRANT SELECT, INSERT ON *.* TO john, robin` hesaplara izin verir `john` ve `robin` yürütmek için `INSERT` ve `SELECT` sunucudaki tüm veritabanlarındaki tüm tablolar üzerinden sorgular.

## Ayrıcalıklar {#grant-privileges}

Ayrıcalık, belirli bir sorgu türünü yürütme iznidir.

Ayrıcalıklar hiyerarşik bir yapıya sahiptir. İzin verilen sorgular kümesi ayrıcalık kapsamına bağlıdır.

Ayrıcalıkların hiyerarşisi:

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
-   [dictGet](#grant-dictget)

Bu hiyerarşinin nasıl ele alındığına dair örnekler:

-   Bu `ALTER` ayrıcalık diğer her şeyi içerir `ALTER*` ayrıcalıklar.
-   `ALTER CONSTRAINT` içerir `ALTER ADD CONSTRAINT` ve `ALTER DROP CONSTRAINT` ayrıcalıklar.

Ayrıcalıklar farklı düzeylerde uygulanır. Bir seviyeyi bilmek, ayrıcalık için mevcut sözdizimini önerir.

Seviyeler (düşükten yükseğe):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`- seviye ayrıcalığı verilir, yalnızca kullanılan sözdizimine karşılık gelen grup ayrıcalıkları verilir.

İzin verilen sözdizimi örnekleri:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

İzin verilmeyen sözdizimi örnekleri:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

Özel ayrıcalık [ALL](#grant-all) bir kullanıcı hesabı veya rol için tüm ayrıcalıkları verir.

Varsayılan olarak, bir kullanıcı hesabının veya rolün ayrıcalıkları yoktur.

Eğer bir kullanıcı veya rolü herhangi bir imtiyaz varsa, görüntülenen bu [NONE](#grant-none) ayrıcalık.

Bazı sorgular uygulamalarıyla bir dizi ayrıcalık gerektirir. Örneğin, yürütmek için [RENAME](../../sql-reference/statements/misc.md#misc_operations-rename) sorgu aşağıdaki ayrıcalıklara ihtiyacınız var: `SELECT`, `CREATE TABLE`, `INSERT` ve `DROP TABLE`.

### SELECT {#grant-select}

Çalıştırmaya izin verir [SELECT](../../sql-reference/statements/select/index.md) sorgular.

Ayrıcalık düzeyi: `COLUMN`.

**Açıklama**

Bu ayrıcalığa sahip bir kullanıcı yürütebilir `SELECT` belirtilen tablo ve veritabanındaki sütunların belirtilen bir liste üzerinde sorgular. Kullanıcı başka sütunlar içeriyorsa, belirtilen sorgu herhangi bir veri döndürmez.

Aşağıdaki ayrıcalığı göz önünde bulundurun:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

Bu ayrıcalık sağlar `john` herhangi bir yürütmek için `SELECT` veri içeren sorgu `x` ve / veya `y` içindeki sütunlar `db.table`, mesela, `SELECT x FROM db.table`. `john` idam edilemiyor `SELECT z FROM db.table`. Bu `SELECT * FROM db.table` ayrıca mevcut değildir. Bu sorguyu işlerken, ClickHouse bile herhangi bir veri döndürmez `x` ve `y`. Tek istisna, bir tablo yalnızca `x` ve `y` sütunlar, bu durumda ClickHouse tüm verileri döndürür.

### INSERT {#grant-insert}

Çalıştırmaya izin verir [INSERT](../../sql-reference/statements/insert-into.md) sorgular.

Ayrıcalık düzeyi: `COLUMN`.

**Açıklama**

Bu ayrıcalığa sahip bir kullanıcı yürütebilir `INSERT` belirtilen tablo ve veritabanındaki sütunların belirtilen bir liste üzerinde sorgular. Kullanıcı başka sütunlar içeriyorsa, belirtilen sorgu herhangi bir veri eklemez.

**Örnek**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

Verilen ayrıcalık izin verir `john` veri eklemek için `x` ve / veya `y` içindeki sütunlar `db.table`.

### ALTER {#grant-alter}

Çalıştırmaya izin verir [ALTER](../../sql-reference/statements/alter.md) aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular:

-   `ALTER`. Düzey: `COLUMN`.
    -   `ALTER TABLE`. Düzey: `GROUP`
        -   `ALTER UPDATE`. Düzey: `COLUMN`. Takma ad: `UPDATE`
        -   `ALTER DELETE`. Düzey: `COLUMN`. Takma ad: `DELETE`
        -   `ALTER COLUMN`. Düzey: `GROUP`
            -   `ALTER ADD COLUMN`. Düzey: `COLUMN`. Takma ad: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. Düzey: `COLUMN`. Takma ad: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. Düzey: `COLUMN`. Takma ad: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. Düzey: `COLUMN`. Takma ad: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. Düzey: `COLUMN`. Takma ad: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. Düzey: `COLUMN`. Takma ad: `RENAME COLUMN`
        -   `ALTER INDEX`. Düzey: `GROUP`. Takma ad: `INDEX`
            -   `ALTER ORDER BY`. Düzey: `TABLE`. Takma ad: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. Düzey: `TABLE`. Takma ad: `ADD INDEX`
            -   `ALTER DROP INDEX`. Düzey: `TABLE`. Takma ad: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. Düzey: `TABLE`. Takma ad: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. Düzey: `TABLE`. Takma ad: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. Düzey: `GROUP`. Takma ad: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. Düzey: `TABLE`. Takma ad: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. Düzey: `TABLE`. Takma ad: `DROP CONSTRAINT`
        -   `ALTER TTL`. Düzey: `TABLE`. Takma ad: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. Düzey: `TABLE`. Takma ad: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. Düzey: `TABLE`. Takma ad: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. Düzey: `TABLE`. Takma ad: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. Düzey: `TABLE`. Takma ad: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. Düzey: `TABLE`. Takma ad: `FREEZE PARTITION`
    -   `ALTER VIEW` Düzey: `GROUP`
        -   `ALTER VIEW REFRESH`. Düzey: `VIEW`. Takma ad: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. Düzey: `VIEW`. Takma ad: `ALTER TABLE MODIFY QUERY`

Bu hiyerarşinin nasıl ele alındığına dair örnekler:

-   Bu `ALTER` ayrıcalık diğer her şeyi içerir `ALTER*` ayrıcalıklar.
-   `ALTER CONSTRAINT` içerir `ALTER ADD CONSTRAINT` ve `ALTER DROP CONSTRAINT` ayrıcalıklar.

**Not**

-   Bu `MODIFY SETTING` ayrıcalık, tablo altyapısı ayarlarını değiştirmenize izin verir. Ayarları veya sunucu yapılandırma parametrelerini etkilemez.
-   Bu `ATTACH` operasyon ihtiyacı [CREATE](#grant-create) ayrıcalık.
-   Bu `DETACH` operasyon ihtiyacı [DROP](#grant-drop) ayrıcalık.
-   Mutasyonu durdurmak için [KILL MUTATION](../../sql-reference/statements/misc.md#kill-mutation) sorgu, bu mutasyonu başlatmak için bir ayrıcalığa sahip olmanız gerekir. Örneğin, durdurmak istiyorsanız `ALTER UPDATE` sorgu, ihtiyacınız olan `ALTER UPDATE`, `ALTER TABLE`, veya `ALTER` ayrıcalık.

### CREATE {#grant-create}

Çalıştırmaya izin verir [CREATE](../../sql-reference/statements/create.md) ve [ATTACH](../../sql-reference/statements/misc.md#attach) DDL-aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular:

-   `CREATE`. Düzey: `GROUP`
    -   `CREATE DATABASE`. Düzey: `DATABASE`
    -   `CREATE TABLE`. Düzey: `TABLE`
    -   `CREATE VIEW`. Düzey: `VIEW`
    -   `CREATE DICTIONARY`. Düzey: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. Düzey: `GLOBAL`

**Not**

-   Oluşturulan tabloyu silmek için bir kullanıcının ihtiyacı vardır [DROP](#grant-drop).

### DROP {#grant-drop}

Çalıştırmaya izin verir [DROP](../../sql-reference/statements/misc.md#drop) ve [DETACH](../../sql-reference/statements/misc.md#detach) aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular:

-   `DROP`. Düzey:
    -   `DROP DATABASE`. Düzey: `DATABASE`
    -   `DROP TABLE`. Düzey: `TABLE`
    -   `DROP VIEW`. Düzey: `VIEW`
    -   `DROP DICTIONARY`. Düzey: `DICTIONARY`

### TRUNCATE {#grant-truncate}

Çalıştırmaya izin verir [TRUNCATE](../../sql-reference/statements/misc.md#truncate-statement) sorgular.

Ayrıcalık düzeyi: `TABLE`.

### OPTIMIZE {#grant-optimize}

Çalıştırmaya izin verir [OPTIMIZE TABLE](../../sql-reference/statements/misc.md#misc_operations-optimize) sorgular.

Ayrıcalık düzeyi: `TABLE`.

### SHOW {#grant-show}

Çalıştırmaya izin verir `SHOW`, `DESCRIBE`, `USE`, ve `EXISTS` aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular:

-   `SHOW`. Düzey: `GROUP`
    -   `SHOW DATABASES`. Düzey: `DATABASE`. Yürütmek için izin verir `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` sorgular.
    -   `SHOW TABLES`. Düzey: `TABLE`. Yürütmek için izin verir `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` sorgular.
    -   `SHOW COLUMNS`. Düzey: `COLUMN`. Yürütmek için izin verir `SHOW CREATE TABLE`, `DESCRIBE` sorgular.
    -   `SHOW DICTIONARIES`. Düzey: `DICTIONARY`. Yürütmek için izin verir `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` sorgular.

**Not**

Bir kullanıcı aşağıdaki özelliklere sahiptir `SHOW` belirtilen tablo, sözlük veya veritabanı ile ilgili başka bir ayrıcalık varsa ayrıcalık.

### KILL QUERY {#grant-kill-query}

Çalıştırmaya izin verir [KILL](../../sql-reference/statements/misc.md#kill-query-statement) aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular:

Ayrıcalık düzeyi: `GLOBAL`.

**Not**

`KILL QUERY` ayrıcalık, bir kullanıcının diğer kullanıcıların sorgularını öldürmesine izin verir.

### ACCESS MANAGEMENT {#grant-access-management}

Bir kullanıcının kullanıcıları, rolleri ve satır ilkelerini yöneten sorguları yürütmesine izin verir.

-   `ACCESS MANAGEMENT`. Düzey: `GROUP`
    -   `CREATE USER`. Düzey: `GLOBAL`
    -   `ALTER USER`. Düzey: `GLOBAL`
    -   `DROP USER`. Düzey: `GLOBAL`
    -   `CREATE ROLE`. Düzey: `GLOBAL`
    -   `ALTER ROLE`. Düzey: `GLOBAL`
    -   `DROP ROLE`. Düzey: `GLOBAL`
    -   `ROLE ADMIN`. Düzey: `GLOBAL`
    -   `CREATE ROW POLICY`. Düzey: `GLOBAL`. Takma ad: `CREATE POLICY`
    -   `ALTER ROW POLICY`. Düzey: `GLOBAL`. Takma ad: `ALTER POLICY`
    -   `DROP ROW POLICY`. Düzey: `GLOBAL`. Takma ad: `DROP POLICY`
    -   `CREATE QUOTA`. Düzey: `GLOBAL`
    -   `ALTER QUOTA`. Düzey: `GLOBAL`
    -   `DROP QUOTA`. Düzey: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. Düzey: `GLOBAL`. Takma ad: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. Düzey: `GLOBAL`. Takma ad: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. Düzey: `GLOBAL`. Takma ad: `DROP PROFILE`
    -   `SHOW ACCESS`. Düzey: `GROUP`
        -   `SHOW_USERS`. Düzey: `GLOBAL`. Takma ad: `SHOW CREATE USER`
        -   `SHOW_ROLES`. Düzey: `GLOBAL`. Takma ad: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. Düzey: `GLOBAL`. Takma ad: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. Düzey: `GLOBAL`. Takma ad: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. Düzey: `GLOBAL`. Takma ad: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

Bu `ROLE ADMIN` ayrıcalık, kullanıcının yönetici seçeneğiyle kullanıcıya atanmayanlar da dahil olmak üzere herhangi bir rol atamasına ve iptal etmesine izin verir.

### SYSTEM {#grant-system}

Bir kullanıcının yürütmesine izin verir [SYSTEM](../../sql-reference/statements/system.md) aşağıdaki ayrıcalıklar hiyerarşisine göre sorgular.

-   `SYSTEM`. Düzey: `GROUP`
    -   `SYSTEM SHUTDOWN`. Düzey: `GLOBAL`. Takma ad: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. Takma ad: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. Düzey: `GLOBAL`. Takma ad: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. Düzey: `GLOBAL`. Takma ad: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. Düzey: `GLOBAL`. Takma ad: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. Düzey: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. Düzey: `GLOBAL`. Takma ad: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. Düzey: `GLOBAL`. Takma ad: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Düzey: `GLOBAL`. Takma Adlar: R`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. Düzey: `GROUP`. Takma ad: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. Düzey: `TABLE`. Takma ad: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. Düzey: `TABLE`. Takma ad: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. Düzey: `TABLE`. Takma ad: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. Düzey: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. Düzey: `TABLE`. Takma ad: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. Düzey: `GLOBAL`. Takma ad: `FLUSH LOGS`

Bu `SYSTEM RELOAD EMBEDDED DICTIONARIES` tarafından örtülü olarak verilen ayrıcalık `SYSTEM RELOAD DICTIONARY ON *.*` ayrıcalık.

### INTROSPECTION {#grant-introspection}

Kullanmanıza izin verir [içgözlem](../../operations/optimizing-performance/sampling-query-profiler.md) işlevler.

-   `INTROSPECTION`. Düzey: `GROUP`. Takma ad: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. Düzey: `GLOBAL`
    -   `addressToSymbol`. Düzey: `GLOBAL`
    -   `demangle`. Düzey: `GLOBAL`

### SOURCES {#grant-sources}

Harici veri kaynaklarının kullanılmasına izin verir. İçin geçerlidir [masa motorları](../../engines/table-engines/index.md) ve [tablo işlevleri](../../sql-reference/table-functions/index.md#table-functions).

-   `SOURCES`. Düzey: `GROUP`
    -   `FILE`. Düzey: `GLOBAL`
    -   `URL`. Düzey: `GLOBAL`
    -   `REMOTE`. Düzey: `GLOBAL`
    -   `YSQL`. Düzey: `GLOBAL`
    -   `ODBC`. Düzey: `GLOBAL`
    -   `JDBC`. Düzey: `GLOBAL`
    -   `HDFS`. Düzey: `GLOBAL`
    -   `S3`. Düzey: `GLOBAL`

Bu `SOURCES` ayrıcalık, tüm kaynakların kullanılmasına izin verir. Ayrıca, her kaynak için ayrı ayrı bir ayrıcalık verebilirsiniz. Kaynakları kullanmak için ek ayrıcalıklara ihtiyacınız var.

Örnekler:

-   İle bir tablo oluşturmak için [MySQL tablo motoru](../../engines/table-engines/integrations/mysql.md) ihtiyacınız `CREATE TABLE (ON db.table_name)` ve `MYSQL` ayrıcalıklar.
-   Kullanmak için [mysql tablo işlevi](../../sql-reference/table-functions/mysql.md) ihtiyacınız `CREATE TEMPORARY TABLE` ve `MYSQL` ayrıcalıklar.

### dictGet {#grant-dictget}

-   `dictGet`. Takma ad: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Bir kullanıcının yürütmesine izin verir [dictGet](../../sql-reference/functions/ext-dict-functions.md#dictget), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy), [dictİsİn](../../sql-reference/functions/ext-dict-functions.md#dictisin) işlevler.

Ayrıcalık düzeyi: `DICTIONARY`.

**Örnekler**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

Düzenlenmiş varlık üzerindeki tüm ayrıcalıkları bir kullanıcı hesabına veya bir role verir.

### NONE {#grant-none}

Herhangi bir ayrıcalık vermez.

### ADMIN OPTION {#admin-option-privilege}

Bu `ADMIN OPTION` ayrıcalık, kullanıcının rolünü başka bir kullanıcıya vermesine izin verir.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
