---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: AggregateFunction (ad, types_of_arguments...)
---

# AggregateFunction(name, types\_of\_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [materyalize bir görünüm](../../sql-reference/statements/create.md#create-view). Bir toplama işlevi durumu üretmek için ortak yolu ile toplama işlevi çağırarak olduğunu `-State` sonek. Gelecekte toplanmanın nihai sonucunu elde etmek için, aynı toplama işlevini `-Merge`sonek.

`AggregateFunction` — parametric data type.

**Parametre**

-   Toplama işlevinin adı.

        If the function is parametric, specify its parameters too.

-   Toplama işlevi bağımsız değişkenleri türleri.

**Örnek**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq), anyİf ([herhangi](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[Eğer](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) ve [quantiles](../../sql-reference/aggregate-functions/reference.md) ClickHouse desteklenen toplam işlevleri vardır.

## Kullanma {#usage}

### Veri Ekleme {#data-insertion}

Veri eklemek için şunları kullanın `INSERT SELECT` agr aggregateega ile `-State`- işlevler.

**Fonksiyon örnekleri**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

Karşılık gelen fonksiyonların aksine `uniq` ve `quantiles`, `-State`- fonksiyonlar son değer yerine durumu döndürür. Başka bir deyişle, bir değer döndürür `AggregateFunction` tür.

Sonuç inlarında `SELECT` sorgu, değerleri `AggregateFunction` türü, Tüm ClickHouse çıktı biçimleri için uygulamaya özgü ikili gösterime sahiptir. Örneğin, veri dökümü, `TabSeparated` ile format `SELECT` sorgu, daha sonra bu dökümü kullanarak geri yüklenebilir `INSERT` sorgu.

### Veri Seçimi {#data-selection}

Veri seçerken `AggregatingMergeTree` tablo kullanın `GROUP BY` yan tümce ve veri eklerken aynı toplama işlevleri, ancak kullanarak `-Merge`sonek.

Bir toplama fonksiyonu ile `-Merge` sonek, bir dizi durum alır, bunları birleştirir ve tam veri toplama sonucunu döndürür.

Örneğin, aşağıdaki iki sorgu aynı sonucu döndürür:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Kullanım Örneği {#usage-example}

Görmek [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) motor açıklaması.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
