---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "Birle\u015Ftiriciler"
---

# Toplama Fonksiyonu Birleştiriciler {#aggregate_functions_combinators}

Bir toplama işlevinin adı, ona eklenmiş bir sonek olabilir. Bu, toplama işlevinin çalışma şeklini değiştirir.

## -Eğer {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Örnekler: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` ve böyle devam eder.

Koşullu toplama işlevleriyle, alt sorgular kullanmadan aynı anda birkaç koşul için toplamları hesaplayabilirsiniz ve `JOIN`s. Örneğin, Üye olarak.Metrica, koşullu toplama işlevleri segment karşılaştırma işlevselliğini uygulamak için kullanılır.

## -Dizi {#agg-functions-combinator-array}

\- Array soneki herhangi bir toplama işlevine eklenebilir. Bu durumda, toplama işlevi, ‘Array(T)’ type (ar arraysra )ys) yerine ‘T’ bağımsız değişkenleri yazın. Toplama işlevi birden çok bağımsız değişken kabul ederse, bu eşit uzunlukta diziler olmalıdır. Dizileri işlerken, toplama işlevi tüm dizi öğelerinde orijinal toplama işlevi gibi çalışır.

Örnek 1: `sumArray(arr)` - Tüm unsurları toplamları ‘arr’ diziler. Bu örnekte, daha basit yazılmış olabilir: `sum(arraySum(arr))`.

Örnek 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ diziler. Bu daha kolay bir şekilde yapılabilir: `uniq(arrayJoin(arr))`, ancak eklemek her zaman mümkün değildir ‘arrayJoin’ bir sorguya.

\- Eğer ve-dizi kombine edilebilir. Ancak, ‘Array’ önce gel mustmeli, sonra ‘If’. Örnekler: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Nedeniyle bu sipariş için, ‘cond’ argüman bir dizi olmayacak.

## -Devlet {#agg-functions-combinator-state}

Bu birleştiriciyi uygularsanız, toplama işlevi elde edilen değeri döndürmez (örneğin, [uniq](reference.md#agg_function-uniq) fonksiyonu), ancak top aggreglamanın bir ara durumu (for `uniq`, bu benzersiz değerlerin sayısını hesaplamak için karma tablodur). Bu bir `AggregateFunction(...)` bu, daha fazla işlem için kullanılabilir veya daha sonra toplanmayı bitirmek için bir tabloda saklanabilir.

Bu durumlarla çalışmak için şunları kullanın:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) masa motoru.
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) İşlev.
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) İşlev.
-   [-Birleştirmek](#aggregate_functions_combinators-merge) birleştirici.
-   [- MergeState](#aggregate_functions_combinators-mergestate) birleştirici.

## -Birleştirmek {#aggregate_functions_combinators-merge}

Bu birleştiriciyi uygularsanız, toplama işlevi Ara toplama durumunu bağımsız değişken olarak alır, toplama işlemini tamamlamak için durumları birleştirir ve elde edilen değeri döndürür.

## - MergeState {#aggregate_functions_combinators-mergestate}

Ara toplama durumlarını-birleştirme Birleştiricisi ile aynı şekilde birleştirir. Bununla birlikte, elde edilen değeri döndürmez, ancak-State combinator'a benzer bir ara toplama durumu döndürür.

## - ForEach {#agg-functions-combinator-foreach}

Tablolar için bir toplama işlevi, karşılık gelen dizi öğelerini toplayan ve bir dizi sonuç döndüren diziler için bir toplama işlevine dönüştürür. Mesela, `sumForEach` diz theiler için `[1, 2]`, `[3, 4, 5]`ve`[6, 7]`sonucu döndürür `[10, 13, 5]` karşılık gelen dizi öğelerini bir araya getirdikten sonra.

## - OrDefault {#agg-functions-combinator-ordefault}

Toplama işlevinin davranışını değiştirir.

Bir toplama işlevinin giriş değerleri yoksa, bu birleştirici ile dönüş veri türü için varsayılan değeri döndürür. Boş giriş verilerini alabilen toplama işlevlerine uygulanır.

`-OrDefault` diğer birleştiriciler ile kullanılabilir.

**Sözdizimi**

``` sql
<aggFunction>OrDefault(x)
```

**Parametre**

-   `x` — Aggregate function parameters.

**Döndürülen değerler**

Toplamak için hiçbir şey yoksa, bir toplama işlevinin dönüş türünün Varsayılan değerini döndürür.

Türü kullanılan toplama işlevine bağlıdır.

**Örnek**

Sorgu:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

Sonuç:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Ayrıca `-OrDefault` başka bir birleştiriciler ile kullanılabilir. Toplama işlevi boş girişi kabul etmediğinde yararlıdır.

Sorgu:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Sonuç:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## - OrNull {#agg-functions-combinator-ornull}

Toplama işlevinin davranışını değiştirir.

Bu birleştirici, bir toplama işlevinin sonucunu [Nullable](../data-types/nullable.md) veri türü. Toplama işlevi hesaplamak için değerleri yoksa döndürür [NULL](../syntax.md#null-literal).

`-OrNull` diğer birleştiriciler ile kullanılabilir.

**Sözdizimi**

``` sql
<aggFunction>OrNull(x)
```

**Parametre**

-   `x` — Aggregate function parameters.

**Döndürülen değerler**

-   Toplama işlev resultinin sonucu, `Nullable` veri türü.
-   `NULL`, toplamak için bir şey yoksa.

Tür: `Nullable(aggregate function return type)`.

**Örnek**

Eklemek `-orNull` toplama işlevinin sonuna kadar.

Sorgu:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

Sonuç:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

Ayrıca `-OrNull` başka bir birleştiriciler ile kullanılabilir. Toplama işlevi boş girişi kabul etmediğinde yararlıdır.

Sorgu:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Sonuç:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## - Resample {#agg-functions-combinator-resample}

Verileri gruplara ayırmanızı sağlar ve ardından bu gruplardaki verileri ayrı ayrı toplar. Gruplar, değerleri bir sütundan aralıklara bölerek oluşturulur.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Parametre**

-   `start` — Starting value of the whole required interval for `resampling_key` değerler.
-   `stop` — Ending value of the whole required interval for `resampling_key` değerler. Tüm Aralık içermez `stop` değer `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` bu alt aralıkların her biri üzerinde bağımsız olarak yürütülür.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` parametre.

**Döndürülen değerler**

-   Ar arrayray of `aggFunction` her subinterval için sonuçlar.

**Örnek**

Düşünün `people` aşağıdaki verilerle tablo:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Yaş aralığı içinde olan kişilerin isimlerini alalım `[30,60)` ve `[60,75)`. Yaş için tamsayı temsilini kullandığımızdan, yaşları `[30, 59]` ve `[60,74]` aralıklılar.

Bir dizideki isimleri toplamak için, [groupArray](reference.md#agg_function-grouparray) toplama işlevi. Bir argüman alır. Bizim durumumuzda, bu `name` sütun. Bu `groupArrayResample` fonksiyon kullanmalıdır `age` yaşlara göre isimleri toplamak için sütun. Gerekli aralıkları tanımlamak için `30, 75, 30` argü themanlar içine `groupArrayResample` İşlev.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Sonuçları düşünün.

`Jonh` çok genç olduğu için numunenin dışında. Diğer insanlar belirtilen yaş aralıklarına göre dağıtılır.

Şimdi toplam insan sayısını ve ortalama ücretlerini belirtilen yaş aralıklarında sayalım.

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
