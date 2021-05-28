---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: girdi
---

# girdi {#input}

`input(structure)` - etkin bir şekilde dönüştürmek ve veri eklemek sağlar tablo fonksiyonu gönderilen
başka bir yapıya sahip tabloya verilen yapıya sahip sunucu.

`structure` - aşağıdaki formatta sunucuya gönderilen verilerin yapısı `'column1_name column1_type, column2_name column2_type, ...'`.
Mesela, `'id UInt32, name String'`.

Bu işlev yalnızca kullanılabilir `INSERT SELECT` sorgu ve sadece bir kez ama aksi takdirde sıradan tablo işlevi gibi davranır
(örneğin, alt sorguda vb.kullanılabilir.).

Veri sıradan gibi herhangi bir şekilde gönderilebilir `INSERT` sorgu ve herhangi bir kullanılabilir geçti [biçimli](../../interfaces/formats.md#formats)
bu sorgu sonunda belirtilmelidir (sıradan aksine `INSERT SELECT`).

Bu işlevin ana özelliği, sunucu istemciden veri aldığında aynı anda onu dönüştürmesidir
ifadeler listesine göre `SELECT` yan tümcesi ve hedef tabloya ekler. Geçici tablo
aktarılan tüm veriler ile oluşturulmaz.

**Örnekler**

-   L letet the `test` tablo aşağıdaki yapıya sahiptir `(a String, b String)`
    ve veri `data.csv` farklı bir yapıya sahiptir `(col1 String, col2 Date, col3 Int32)`. Insert sorgusu
    bu verileri `data.csv` içine `test` eşzamanlı dönüşüm ile tablo şöyle görünüyor:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   Eğer `data.csv` aynı yapının verilerini içerir `test_structure` tablo olarak `test` sonra bu iki sorgu eşittir:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
