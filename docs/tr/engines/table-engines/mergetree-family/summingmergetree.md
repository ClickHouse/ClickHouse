---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: SummingMergeTree
---

# SummingMergeTree {#summingmergetree}

Motor devralır [MergeTree](mergetree.md#table_engines-mergetree). Fark, veri parçalarını birleştirirken `SummingMergeTree` tablolar ClickHouse tüm satırları aynı birincil anahtarla değiştirir (veya daha doğru olarak, aynı [sıralama anahtarı](mergetree.md)) sayısal veri türüne sahip sütunlar için özetlenen değerleri içeren bir satır ile. Sıralama anahtarı, tek bir anahtar değeri çok sayıda satıra karşılık gelecek şekilde oluşturulursa, bu, depolama birimini önemli ölçüde azaltır ve veri seçimini hızlandırır.

Motoru birlikte kullanmanızı öneririz `MergeTree`. Mağaza tam veri `MergeTree` tablo ve kullanım `SummingMergeTree` örneğin, rapor hazırlarken toplu veri depolamak için. Böyle bir yaklaşım, yanlış oluşturulmuş bir birincil anahtar nedeniyle değerli verileri kaybetmenizi önleyecektir.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

İstek parametrelerinin açıklaması için bkz. [istek açıklaması](../../../sql-reference/statements/create.md).

**SummingMergeTree parametreleri**

-   `columns` - değerlerin özetleneceği sütunların adlarına sahip bir tuple. İsteğe bağlı parametre.
    Sütunlar sayısal tipte olmalı ve birincil anahtarda olmamalıdır.

    Eğer `columns` belirtilmemiş, ClickHouse birincil anahtarda olmayan bir sayısal veri türü ile tüm sütunlardaki değerleri özetler.

**Sorgu yan tümceleri**

Oluştururken bir `SummingMergeTree` tablo aynı [yanlar](mergetree.md) oluşturul ,urken olduğu gibi gerekli `MergeTree` Tablo.

<details markdown="1">

<summary>Bir tablo oluşturmak için kullanımdan kaldırılan yöntem</summary>

!!! attention "Dikkat"
    Bu yöntemi yeni projelerde kullanmayın ve mümkünse eski projeleri yukarıda açıklanan yönteme geçin.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

Hariç tüm parametreler `columns` içinde olduğu gibi aynı anlama sahip `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## Kullanım Örneği {#usage-example}

Aşağıdaki tabloyu düşünün:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Veri Ekle:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouse tüm satırları tamamen toplayabilir ([aşağıya bakın](#data-processing)), bu yüzden bir toplama işlevi kullanıyoruz `sum` ve `GROUP BY` sorguda yan tümcesi.

``` sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

``` text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## Veri İşleme {#data-processing}

Veriler bir tabloya eklendiğinde, bunlar olduğu gibi kaydedilir. ClickHouse, verilerin eklenen bölümlerini periyodik olarak birleştirir ve bu, aynı birincil anahtara sahip satırların toplandığı ve sonuçta elde edilen her veri parçası için bir tane ile değiştirildiği zamandır.

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) bir toplama fonksiyonu [toplam()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) ve `GROUP BY` yukarıdaki örnekte açıklandığı gibi yan tümcesi bir sorguda kullanılmalıdır.

### Toplama için ortak kurallar {#common-rules-for-summation}

Sayısal veri türüne sahip sütunlardaki değerler özetlenir. Sütun kümesi parametre tarafından tanımlanır `columns`.

Değerler toplamı için tüm sütunlarda 0 ise, satır silinir.

Sütun birincil anahtarda değilse ve özetlenmezse, mevcut olanlardan rasgele bir değer seçilir.

Değerler, birincil anahtardaki sütunlar için özetlenmez.

### Aggregatefunction Sütunlarındaki toplama {#the-summation-in-the-aggregatefunction-columns}

Sütunlar için [AggregateFunction türü](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse olarak davranır [AggregatingMergeTree](aggregatingmergetree.md) işleve göre motor toplama.

### İç İçe Yapılar {#nested-structures}

Tablo, özel bir şekilde işlenen iç içe geçmiş veri yapılarına sahip olabilir.

İç içe geçmiş bir tablonun adı ile bitiyorsa `Map` ve aşağıdaki kriterleri karşılayan en az iki sütun içerir:

-   ilk sütun sayısal `(*Int*, Date, DateTime)` veya bir dize `(String, FixedString)` hadi diyelim `key`,
-   diğer sütunlar aritmetik `(*Int*, Float32/64)` hadi diyelim `(values...)`,

sonra bu iç içe geçmiş tablo bir eşleme olarak yorumlanır `key => (values...)` ve satırlarını birleştirirken, iki veri kümesinin öğeleri şu şekilde birleştirilir `key` karşılık gelen bir toplamı ile `(values...)`.

Örnekler:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

Veri isterken, [sumMap (anahtar, değer)](../../../sql-reference/aggregate-functions/reference.md) toplama fonksiyonu `Map`.

İç içe geçmiş veri yapısı için, sütunlarının toplamı için sütun kümesinde belirtmeniz gerekmez.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
