---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: SHOW
---

# Sorguları göster {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Bir tek döndürür `String`-tür ‘statement’ column, which contains a single value – the `CREATE` belirtilen nesneyi oluşturmak için kullanılan sorgu.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Tüm veritabanlarının bir listesini yazdırır.
Bu sorgu ile aynıdır `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

İçeriği verir [sistem.işleyişler](../../operations/system-tables.md#system_tables-processes) şu anda işlenmekte olan sorguların bir listesini içeren tablo, hariç `SHOW PROCESSLIST` sorgular.

Bu `SELECT * FROM system.processes` sorgu, geçerli tüm sorgular hakkında veri döndürür.

İpucu (konsolda Yürüt):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Tablo listesini görüntüler.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Eğer... `FROM` yan tümcesi belirtilmemiş, sorgu geçerli veritabanından tabloların listesini döndürür.

Aynı sonuçları elde edebilirsiniz `SHOW TABLES` aşağıdaki şekilde sorgu:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Örnek**

Aşağıdaki sorgu, tablo listesinden ilk iki satırı seçer. `system` adları içeren veritabanı `co`.

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

Bir listesini görüntüler [dış söz dictionarieslükler](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Eğer... `FROM` yan tümcesi belirtilmemiş, sorgu geçerli veritabanından sözlükler listesini döndürür.

Aynı sonuçları elde edebilirsiniz `SHOW DICTIONARIES` aşağıdaki şekilde sorgu:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Örnek**

Aşağıdaki sorgu, tablo listesinden ilk iki satırı seçer. `system` adları içeren veritabanı `reg`.

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

Bir kullanıcı için ayrıcalıkları gösterir.

### Sözdizimi {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

Kullanıcı belirtilmezse, sorgu geçerli kullanıcı için ayrıcalıklar döndürür.

## SHOW CREATE USER {#show-create-user-statement}

Kullanılan parametreleri gösterir. [kullanıcı oluşturma](create.md#create-user-statement).

`SHOW CREATE USER` kullanıcı şifreleri çıkmıyor.

### Sözdizimi {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Kullanılan parametreleri gösterir. [rol oluşturma](create.md#create-role-statement)

### Sözdizimi {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Kullanılan parametreleri gösterir. [satır ilkesi oluşturma](create.md#create-row-policy-statement)

### Sözdizimi {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Kullanılan parametreleri gösterir. [Ko creationta oluşturma](create.md#create-quota-statement)

### Sözdizimi {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Kullanılan parametreleri gösterir. [ayarlar profil oluşturma](create.md#create-settings-profile-statement)

### Sözdizimi {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
