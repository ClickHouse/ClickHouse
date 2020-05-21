---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: ReplacingMergeTree
---

# ReplacingMergeTree {#replacingmergetree}

Motor farklıdır [MergeTree](mergetree.md#table_engines-mergetree) aynı birincil anahtar değerine sahip yinelenen girdileri kaldırır (veya daha doğru bir şekilde, aynı [sıralama anahtarı](mergetree.md) değer).

Veri tekilleştirme yalnızca birleştirme sırasında oluşur. Birleştirme, arka planda bilinmeyen bir zamanda gerçekleşir, bu nedenle bunu planlayamazsınız. Bazı veriler işlenmemiş kalabilir. Kullanarak programsız bir birleştirme çalıştırabilirsiniz, ancak `OPTIMIZE` sorgu, kullanmaya güvenmeyin, çünkü `OPTIMIZE` sorgu büyük miktarda veri okuyacak ve yazacaktır.

Böyle, `ReplacingMergeTree` yerden tasarruf etmek için arka planda yinelenen verileri temizlemek için uygundur, ancak kopyaların yokluğunu garanti etmez.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

İstek parametrelerinin açıklaması için bkz. [istek açıklaması](../../../sql-reference/statements/create.md).

**ReplacingMergeTree Parametreleri**

-   `ver` — column with version. Type `UInt*`, `Date` veya `DateTime`. İsteğe bağlı parametre.

    Birleş whenirken, `ReplacingMergeTree` aynı birincil anahtara sahip tüm satırlardan sadece bir tane bırakır:

    -   Seç inimde son, eğer `ver` set değil.
    -   Maksimum sürümü ile, eğer `ver` belirtilen.

**Sorgu yan tümceleri**

Oluştururken bir `ReplacingMergeTree` tablo aynı [yanlar](mergetree.md) oluşturul ,urken olduğu gibi gerekli `MergeTree` Tablo.

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
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

Hariç tüm parametreler `ver` içinde olduğu gibi aynı anlama sahip `MergeTree`.

-   `ver` - sürümü ile sütun. İsteğe bağlı parametre. Bir açıklama için yukarıdaki metne bakın.

</details>

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
