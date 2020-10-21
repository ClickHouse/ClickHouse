---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

Bu motor ile entegrasyon sağlar [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) üzerinde veri Yönet allowingilmesine izin vererek ekosist dataem [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)ClickHouse aracılığıyla. Bu motor benzer
to the [Dosya](../special/file.md#table_engines-file) ve [URL](../special/url.md#table_engines-url) motorlar, ancak hadoop özgü özellikleri sağlar.

## Kullanma {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

Bu `URI` parametre, HDFS'DEKİ tüm dosya URI'SIDIR.
Bu `format` parametre kullanılabilir dosya biçimlerinden birini belirtir. Gerçekleştirmek
`SELECT` sorgular, biçim giriş için desteklenmeli ve gerçekleştirmek için
`INSERT` queries – for output. The available formats are listed in the
[Biçimliler](../../../interfaces/formats.md#formats) bölme.
Yol kısmı `URI` globs içerebilir. Bu durumda tablo salt okunur olurdu.

**Örnek:**

**1.** Set up the `hdfs_engine_table` Tablo:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fil filel file:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Verileri sorgula:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Uygulama Detayları {#implementation-details}

-   Okuma ve yazma paralel olabilir
-   Desteklenmiyor:
    -   `ALTER` ve `SELECT...SAMPLE` harekat.
    -   Dizinler.
    -   Çoğalma.

**Yolda Globs**

Birden çok yol bileşenleri globs olabilir. İşlenmek için dosya var olmalı ve tüm yol deseniyle eşleşmelidir. Sırasında dosyaların listelen ofmesini belirler `SELECT` (not at `CREATE` an).

-   `*` — Substitutes any number of any characters except `/` boş dize dahil.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

İle yapılar `{}` benzer olan [uzak](../../../sql-reference/table-functions/remote.md) tablo işlevi.

**Örnek**

1.  HDFS'DE aşağıdaki Urı'lerle TSV formatında birkaç dosyamız olduğunu varsayalım:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  Altı dosyadan oluşan bir tablo oluşturmanın birkaç yolu vardır:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Başka bir yol:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Tablo, her iki dizindeki tüm dosyalardan oluşur (tüm dosyalar, sorguda açıklanan biçimi ve şemayı karşılamalıdır):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "Uyarıcı"
    Dosyaların listelenmesi, önde gelen sıfırlarla sayı aralıkları içeriyorsa, her basamak için parantez içeren yapıyı ayrı ayrı kullanın veya kullanın `?`.

**Örnek**

Adlı dosyaları içeren tablo oluşturma `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## Sanal Sütunlar {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**Ayrıca Bakınız**

-   [Sanal sütunlar](../index.md#table_engines-virtual_columns)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
