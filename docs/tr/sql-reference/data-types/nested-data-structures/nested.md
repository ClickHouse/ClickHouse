---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u0130\xE7 \u0130\xE7e (Name1 Type1, Name2 Type2,...)"
---

# Nested(name1 Type1, Name2 Type2, …) {#nestedname1-type1-name2-type2}

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create.md) sorgu. Her tablo satırı, iç içe geçmiş veri yapısındaki herhangi bir sayıda satıra karşılık gelebilir.

Örnek:

``` sql
CREATE TABLE test.visits
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    IsNew UInt8,
    VisitID UInt64,
    UserID UInt64,
    ...
    Goals Nested
    (
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32
    ),
    ...
) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)
```

Bu örnek bildirir `Goals` dönüşümlerle ilgili verileri içeren iç içe veri yapısı (ulaşılan hedefler). Her satır içinde ‘visits’ tablo sıfır veya dönüşüm herhangi bir sayıda karşılık gelebilir.

Sadece tek bir yuvalama seviyesi desteklenir. Diziler içeren iç içe geçmiş yapıların sütunları çok boyutlu dizilere eşdeğerdir, bu nedenle sınırlı desteğe sahiptirler (bu sütunları MergeTree altyapısı ile tablolarda depolamak için destek yoktur).

Çoğu durumda, iç içe geçmiş bir veri yapısıyla çalışırken, sütunları bir nokta ile ayrılmış sütun adlarıyla belirtilir. Bu sütunlar eşleşen türleri bir dizi oluşturur. Tek bir iç içe geçmiş veri yapısının tüm sütun dizileri aynı uzunluğa sahiptir.

Örnek:

``` sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goals.ID───────────────────────┬─Goals.EventTime───────────────────────────────────────────────────────────────────────────┐
│ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
│ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
│ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
│ []                             │ []                                                                                        │
│ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
│ []                             │ []                                                                                        │
│ []                             │ []                                                                                        │
│ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
└────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

İç içe geçmiş bir veri yapısını aynı uzunlukta birden çok sütun dizisi kümesi olarak düşünmek en kolay yoldur.

Bir SELECT sorgusunun tek tek sütunlar yerine tüm iç içe geçmiş veri yapısının adını belirtebileceği tek yer array JOIN yan tümcesi. Daha fazla bilgi için, bkz. “ARRAY JOIN clause”. Örnek:

``` sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goal.ID─┬──────Goal.EventTime─┐
│ 1073752 │ 2014-03-17 16:38:10 │
│  591325 │ 2014-03-17 16:38:48 │
│  591325 │ 2014-03-17 16:42:27 │
│ 1073752 │ 2014-03-17 00:28:25 │
│ 1073752 │ 2014-03-17 10:46:20 │
│ 1073752 │ 2014-03-17 13:59:20 │
│  591325 │ 2014-03-17 22:17:55 │
│  591325 │ 2014-03-17 22:18:07 │
│  591325 │ 2014-03-17 22:18:51 │
│ 1073752 │ 2014-03-17 11:37:06 │
└─────────┴─────────────────────┘
```

İç içe geçmiş veri yapısının tamamı için SELECT gerçekleştiremezsiniz. Yalnızca bir parçası olan tek tek sütunları açıkça listeleyebilirsiniz.

Bir INSERT sorgusu için, iç içe geçmiş bir veri yapısının tüm bileşen sütun dizilerini ayrı ayrı (tek tek sütun dizileri gibi) iletmelisiniz. Ekleme sırasında, sistem aynı uzunluğa sahip olduklarını kontrol eder.

Bir tanımlama sorgusu için, iç içe geçmiş bir veri yapısındaki sütunlar aynı şekilde ayrı olarak listelenir.

İç içe geçmiş bir veri yapısındaki öğeler için ALTER sorgusu sınırlamaları vardır.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/nested_data_structures/nested/) <!--hide-->
