---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

MySQL motoru gerçekleştirmek için izin verir `SELECT` uzak bir MySQL sunucusunda depolanan veriler üzerinde sorgular.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

Ayrıntılı bir açıklamasını görmek [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) sorgu.

Tablo yapısı orijinal MySQL tablo yapısından farklı olabilir:

-   Sütun adları orijinal MySQL tablosundaki ile aynı olmalıdır, ancak bu sütunların sadece bazılarını ve herhangi bir sırada kullanabilirsiniz.
-   Sütun türleri orijinal MySQL tablosundakilerden farklı olabilir. ClickHouse çalışır [döküm](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouse veri türleri için değerler.

**Motor Parametreleri**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` için sorgular `REPLACE INTO`. Eğer `replace_query=1`, sorgu değiştirilir.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` eklenen ifade `INSERT` sorgu.

    Örnek: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, nere `on_duplicate_clause` oluyor `UPDATE c2 = c2 + 1`. Görmek [MySQL dökü documentationmanları](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) bulmak için hangi `on_duplicate_clause` ile kullanabilirsiniz `ON DUPLICATE KEY` yan.

    Belirtmek `on_duplicate_clause` sen geçmek gerekir `0` to the `replace_query` parametre. Aynı anda geçerseniz `replace_query = 1` ve `on_duplicate_clause`, ClickHouse bir özel durum oluşturur.

Basit `WHERE` gibi maddeler `=, !=, >, >=, <, <=` MySQL sunucusunda yürütülür.

Geri kalan şartlar ve `LIMIT` örnekleme kısıtlaması, yalnızca MySQL sorgusu bittikten sonra Clickhouse'da yürütülür.

## Kullanım Örneği {#usage-example}

MySQL tablo:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Clickhouse'daki tablo, yukarıda oluşturulan MySQL tablosundan veri alma:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Ayrıca Bakınız {#see-also}

-   [Bu ‘mysql’ tablo fonksiyonu](../../../sql-reference/table-functions/mysql.md)
-   [Harici sözlük kaynağı olarak MySQL kullanma](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/mysql/) <!--hide-->
