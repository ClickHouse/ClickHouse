---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: AggregatingMergeTree
---

# Aggregatingmergetree {#aggregatingmergetree}

Motor devralır [MergeTree](mergetree.md#table_engines-mergetree), veri parçaları birleştirme mantığı değiştirme. ClickHouse, tüm satırları aynı birincil anahtarla değiştirir (veya daha doğru olarak, aynı [sıralama anahtarı](mergetree.md)) tek bir satırla (bir veri parçası içinde), toplama işlevlerinin durumlarının bir kombinasyonunu saklar.

Kullanabilirsiniz `AggregatingMergeTree` artımlı veri toplama, toplanan materialized görünümleri de dahil olmak üzere tablolar.

Motor, tüm sütunları aşağıdaki türlerle işler:

-   [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
-   [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

Kullanmak uygundur `AggregatingMergeTree` siparişlere göre satır sayısını azaltırsa.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

İstek parametrelerinin açıklaması için bkz. [istek açıklaması](../../../sql-reference/statements/create.md).

**Sorgu yan tümceleri**

Oluştururken bir `AggregatingMergeTree` tablo aynı [yanlar](mergetree.md) oluşturul ,urken olduğu gibi gerekli `MergeTree` Tablo.

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
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

Tüm parametreler, aşağıdaki gibi aynı anlama sahiptir `MergeTree`.
</details>

## Seç ve Ekle {#select-and-insert}

Veri eklemek için şunları kullanın [INSERT SELECT](../../../sql-reference/statements/insert-into.md) agrega-Devlet-fonksiyonları ile sorgu.
Veri seçerken `AggregatingMergeTree` tablo kullanın `GROUP BY` yan tümce ve veri eklerken aynı toplama işlevleri, ancak kullanarak `-Merge` sonek.

Sonuç inlarında `SELECT` sorgu, değerleri `AggregateFunction` türü, Tüm ClickHouse çıktı biçimleri için uygulamaya özgü ikili gösterime sahiptir. Örneğin, veri dökümü, `TabSeparated` ile format `SELECT` sorgu daha sonra bu dökümü kullanarak geri yüklenebilir `INSERT` sorgu.

## Toplu bir Somutlaştırılmış Görünüm örneği {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` saatler hayata görünüm `test.visits` Tablo:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Veri ekleme `test.visits` Tablo.

``` sql
INSERT INTO test.visits ...
```

Veriler hem tablo hem de görünümde eklenir `test.basic` toplama işlemini gerçekleştir .ecektir.

Toplanan verileri almak için, aşağıdaki gibi bir sorgu yürütmemiz gerekir `SELECT ... GROUP BY ...` görünüm fromden `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
