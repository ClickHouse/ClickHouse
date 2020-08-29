---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Dosya
---

# Dosya {#table_engines-file}

Dosya tablosu altyapısı, verileri desteklenen dosyalardan birinde tutar [Dosya
biçimliler](../../../interfaces/formats.md#formats) (TabSeparated, yerli, vb.).

Kullanım örnekleri:

-   Clickhouse'dan dosyaya veri aktarımı.
-   Verileri bir biçimden diğerine dönüştürün.
-   Bir diskte bir dosya düzenleme yoluyla ClickHouse veri güncelleme.

## ClickHouse sunucusunda kullanım {#usage-in-clickhouse-server}

``` sql
File(Format)
```

Bu `Format` parametre kullanılabilir dosya biçimlerinden birini belirtir. Gerçekleştirmek
`SELECT` sorgular, biçim giriş için desteklenmeli ve gerçekleştirmek için
`INSERT` queries – for output. The available formats are listed in the
[Biçimliler](../../../interfaces/formats.md#formats) bölme.

ClickHouse dosya sistemi yolunu belirtmek için izin vermiyor`File`. Tarafından tanımlanan klasörü kullan willacaktır [yol](../../../operations/server-configuration-parameters/settings.md) sunucu yapılandırmasında ayarlama.

Kullanarak tablo oluştururken `File(Format)` bu klasörde boş bir alt dizin oluşturur. Veri o tabloya yazıldığında, içine konur `data.Format` bu alt dizinde dosya.

Bu alt klasörü ve dosyayı sunucu dosya sisteminde el ile oluşturabilir ve sonra [ATTACH](../../../sql-reference/statements/misc.md) eşleşen ada sahip tablo bilgilerine, böylece bu dosyadan veri sorgulayabilirsiniz.

!!! warning "Uyarıcı"
    Bu işlevselliğe dikkat edin, çünkü ClickHouse bu tür dosyalarda harici değişiklikleri izlemez. ClickHouse ve ClickHouse dışında eşzamanlı yazma sonucu tanımsızdır.

**Örnek:**

**1.** Set up the `file_engine_table` Tablo:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

Varsayılan olarak ClickHouse klasör oluşturur `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** El ile oluştur `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` içerme:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Verileri sorgula:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Clickhouse'da kullanım-yerel {#usage-in-clickhouse-local}

İçinde [clickhouse-yerel](../../../operations/utilities/clickhouse-local.md) Dosya motoru ek olarak dosya yolunu kabul eder `Format`. Varsayılan giriş / çıkış akışları gibi sayısal veya insan tarafından okunabilir isimler kullanılarak belirtilebilir `0` veya `stdin`, `1` veya `stdout`.
**Örnek:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Uygulama Detayları {#details-of-implementation}

-   Çoklu `SELECT` sorgular aynı anda yapılabilir, ancak `INSERT` sorgular birbirini bekler.
-   Tarafından yeni dosya oluşturma desteklenen `INSERT` sorgu.
-   Dosya varsa, `INSERT` içinde yeni değerler ekler.
-   Desteklenmiyor:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   Dizinler
    -   Çoğalma

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
