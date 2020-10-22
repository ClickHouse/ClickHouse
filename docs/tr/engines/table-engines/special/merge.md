---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "Birle\u015Ftirmek"
---

# Birleştirmek {#merge}

Bu `Merge` motor (ile karıştırılmamalıdır `MergeTree`) verileri kendisi saklamaz, ancak aynı anda herhangi bir sayıda başka tablodan okumaya izin verir.
Okuma otomatik olarak paralelleştirilir. Bir tabloya yazma desteklenmiyor. Okurken, gerçekten okunmakta olan tabloların dizinleri varsa kullanılır.
Bu `Merge` motor parametreleri kabul eder: veritabanı adı ve tablolar için düzenli ifade.

Örnek:

``` sql
Merge(hits, '^WatchLog')
```

Veri tablolardan okunacak `hits` düzenli ifadeyle eşleşen adlara sahip veritabanı ‘`^WatchLog`’.

Veritabanı adı yerine, bir dize döndüren sabit bir ifade kullanabilirsiniz. Mesela, `currentDatabase()`.

Regular expressions — [re2](https://github.com/google/re2) (pcre bir alt kümesini destekler), büyük / küçük harf duyarlı.
Düzenli ifadelerde kaçan sembollerle ilgili notlara bakın “match” bölme.

Okumak için tabloları seçerken, `Merge` regex ile eşleşse bile tablonun kendisi seçilmeyecektir. Bu döngülerden kaçınmaktır.
İki tane oluşturmak mümkündür `Merge` sonsuza kadar birbirlerinin verilerini okumaya çalışacak tablolar, ancak bu iyi bir fikir değil.

Kullanmak için tipik bir yol `Merge` motor çok sayıda çalışma içindir `TinyLog` tablolar tek bir tablo ile sanki.

Örnek 2:

Diyelim ki eski bir tablonuz (WatchLog_old) var ve verileri yeni bir tabloya (WatchLog_new) taşımadan bölümlemeyi değiştirmeye karar verdiniz ve her iki tablodaki verileri görmeniz gerekiyor.

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## Sanal Sütunlar {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [Dize](../../../sql-reference/data-types/string.md).

    Sabit koşulları ayarlayabilirsiniz `_table` in the `WHERE/PREWHERE` fıkra (sı (örneğin, `WHERE _table='xyz'`). Bu durumda, okuma işlemi yalnızca koşulun açık olduğu tablolar için gerçekleştirilir `_table` memnun olduğunu, bu yüzden `_table` sütun bir dizin görevi görür.

**Ayrıca Bakınız**

-   [Sanal sütunlar](index.md#table_engines-virtual_columns)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->
