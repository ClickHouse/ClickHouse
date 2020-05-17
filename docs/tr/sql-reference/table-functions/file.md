---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Dosya
---

# Dosya {#file}

Bir dosyadan bir tablo oluşturur. Bu tablo işlevi benzer [url](url.md) ve [hdf'ler](hdfs.md) biri.

``` sql
file(path, format, structure)
```

**Giriş parametreleri**

-   `path` — The relative path to the file from [user\_files\_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Readonly modunda glob'ları takip eden dosya desteğine giden yol: `*`, `?`, `{abc,def}` ve `{N..M}` nerede `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [biçimli](../../interfaces/formats.md#formats) dosya.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Döndürülen değer**

Belirtilen dosyada veri okumak veya yazmak için belirtilen yapıya sahip bir tablo.

**Örnek**

Ayar `user_files_path` ve dosyanın içeriği `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Tablo fromdan`test.csv` ve ondan ilk iki satır seçimi:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**Yolda Globs**

Birden çok yol bileşenleri globs olabilir. İşlenmek için dosya var olmalı ve tüm yol deseniyle eşleşmelidir (sadece sonek veya önek değil).

-   `*` — Substitutes any number of any characters except `/` boş dize dahil.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

İle yapılar `{}` benzer olan [uzaktan masa fonksiyonu](../../sql-reference/table-functions/remote.md)).

**Örnek**

1.  Aşağıdaki göreli yollara sahip birkaç dosyamız olduğunu varsayalım:

-   ‘some\_dir/some\_file\_1’
-   ‘some\_dir/some\_file\_2’
-   ‘some\_dir/some\_file\_3’
-   ‘another\_dir/some\_file\_1’
-   ‘another\_dir/some\_file\_2’
-   ‘another\_dir/some\_file\_3’

1.  Bu dosyalardaki satır miktarını sorgula:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  Bu iki dizinin tüm dosyalarındaki satır miktarını sorgula:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "Uyarıcı"
    Dosya listenizde önde gelen sıfırlar içeren sayı aralıkları varsa, her basamak için parantez içeren yapıyı ayrı ayrı kullanın veya kullanın `?`.

**Örnek**

Adlı dosy thealardan verileri sorgu thelamak `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## Sanal Sütunlar {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Ayrıca Bakınız**

-   [Sanal sütunlar](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
