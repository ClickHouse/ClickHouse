---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: VersionedCollapsingMergeTree
---

# VersionedCollapsingMergeTree {#versionedcollapsingmergetree}

Bu motor:

-   Sürekli değişen nesne durumlarının hızlı yazılmasını sağlar.
-   Arka planda eski nesne durumlarını siler. Bu, depolama hacmini önemli ölçüde azaltır.

Bölümüne bakınız [Çökme](#table_engines_versionedcollapsingmergetree) ayrıntılar için.

Motor devralır [MergeTree](mergetree.md#table_engines-mergetree) ve veri parçalarını birleştirmek için algoritmaya satırları daraltmak için mantığı ekler. `VersionedCollapsingMergeTree` aynı amaca hizmet eder [CollapsingMergeTree](collapsingmergetree.md) ancak, verilerin birden çok iş parçacığıyla herhangi bir sıraya yerleştirilmesine izin veren farklı bir çökme algoritması kullanır. Özellikle, `Version` sütun, yanlış sıraya yerleştirilmiş olsalar bile satırları düzgün bir şekilde daraltmaya yardımcı olur. Tersine, `CollapsingMergeTree` sadece kesinlikle ardışık ekleme sağlar.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Sorgu parametrelerinin açıklaması için bkz: [sorgu açıklaması](../../../sql-reference/statements/create.md).

**Motor Parametreleri**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` is a “state” satır, `-1` is a “cancel” satır.

    Sütun veri türü olmalıdır `Int8`.

-   `version` — Name of the column with the version of the object state.

    Sütun veri türü olmalıdır `UInt*`.

**Sorgu Yan Tümceleri**

Oluştururken bir `VersionedCollapsingMergeTree` tablo, aynı [yanlar](mergetree.md) oluşturul aurken gerekli `MergeTree` Tablo.

<details markdown="1">

<summary>Bir tablo oluşturmak için kullanımdan kaldırılan yöntem</summary>

!!! attention "Dikkat"
    Bu yöntemi yeni projelerde kullanmayın. Mümkünse, eski projeleri yukarıda açıklanan yönteme geçin.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

Dışındaki tüm parametreler `sign` ve `version` içinde olduğu gibi aynı anlama sahip `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` is a “state” satır, `-1` is a “cancel” satır.

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    Sütun veri türü olmalıdır `UInt*`.

</details>

## Çökme {#table_engines_versionedcollapsingmergetree}

### Veriler {#data}

Bazı nesneler için sürekli değişen verileri kaydetmeniz gereken bir durumu düşünün. Bir nesne için bir satıra sahip olmak ve değişiklikler olduğunda satırı güncellemek mantıklıdır. Ancak, depolama alanındaki verileri yeniden yazmayı gerektirdiğinden, güncelleştirme işlemi bir DBMS için pahalı ve yavaştır. Verileri hızlı bir şekilde yazmanız gerekiyorsa güncelleştirme kabul edilemez, ancak değişiklikleri bir nesneye sırayla aşağıdaki gibi yazabilirsiniz.

Kullan... `Sign` satır yazarken sütun. Eğer `Sign = 1` bu, satırın bir nesnenin durumu olduğu anlamına gelir (diyelim “state” satır). Eğer `Sign = -1` aynı özelliklere sahip bir nesnenin durumunun iptal edildiğini gösterir (buna “cancel” satır). Ayrıca kullanın `Version` bir nesnenin her durumunu ayrı bir sayı ile tanımlaması gereken sütun.

Örneğin, kullanıcıların bazı sitede kaç sayfa ziyaret ettiğini ve ne kadar süre orada olduklarını hesaplamak istiyoruz. Bir noktada, kullanıcı etkinliği durumu ile aşağıdaki satırı yazıyoruz:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Bir noktada daha sonra kullanıcı aktivitesinin değişikliğini kaydediyoruz ve aşağıdaki iki satırla yazıyoruz.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

İlk satır, nesnenin (kullanıcı) önceki durumunu iptal eder. Dışında iptal edilen Devletin tüm alanlarını kopya shouldlama shouldlıdır `Sign`.

İkinci satır geçerli durumu içerir.

Sadece kullanıcı etkinliğinin son durumuna ihtiyacımız olduğundan, satırlar

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

nesnenin geçersiz (eski) durumunu daraltarak silinebilir. `VersionedCollapsingMergeTree` veri parçalarını birleştirirken bunu yapar.

Her değişiklik için neden iki satıra ihtiyacımız olduğunu bulmak için bkz. [Algoritma](#table_engines-versionedcollapsingmergetree-algorithm).

**Kullanımı ile ilgili notlar**

1.  Verileri yazan program, iptal etmek için bir nesnenin durumunu hatırlamalıdır. Bu “cancel” dize bir kopyası olmalıdır “state” tersi ile dize `Sign`. Bu, ilk depolama boyutunu arttırır, ancak verileri hızlı bir şekilde yazmanıza izin verir.
2.  Sütunlardaki uzun büyüyen diziler, yazma yükü nedeniyle motorun verimliliğini azaltır. Daha basit veri, daha iyi verim.
3.  `SELECT` sonuçlara itiraz değişiklikleri tarihinin tutarlılık bağlıdır. Ekleme için veri hazırlarken doğru olun. Oturum derinliği gibi negatif olmayan metrikler için negatif değerler gibi tutarsız verilerle öngörülemeyen sonuçlar alabilirsiniz.

### Algoritma {#table_engines-versionedcollapsingmergetree-algorithm}

ClickHouse veri parçalarını birleştirdiğinde, aynı birincil anahtar ve sürüm ve farklı olan her satır çiftini siler `Sign`. Satırların sırası önemli değil.

ClickHouse veri eklediğinde, satırları birincil anahtarla sipariş eder. Eğer... `Version` sütun birincil anahtarda değil, ClickHouse onu birincil anahtara örtük olarak son alan olarak ekler ve sipariş vermek için kullanır.

## Veri Seçme {#selecting-data}

ClickHouse, aynı birincil anahtara sahip tüm satırların aynı sonuçtaki veri bölümünde veya hatta aynı fiziksel sunucuda olacağını garanti etmez. Bu, hem verileri yazmak hem de veri parçalarının daha sonra birleştirilmesi için geçerlidir. Ayrıca, ClickHouse süreçleri `SELECT` birden çok iş parçacıklarıyla sorgular ve sonuçtaki satırların sırasını tahmin edemez. Bu tamamen almak için bir ihtiyaç varsa toplama gerekli olduğu anlamına gelir “collapsed” bir veri `VersionedCollapsingMergeTree` Tablo.

Daraltmayı sonuçlandırmak için, bir sorgu ile bir sorgu yazın `GROUP BY` yan tümce ve işareti için hesap toplama işlevleri. Örneğin, miktarı hesaplamak için kullanın `sum(Sign)` yerine `count()`. Bir şeyin toplamını hesaplamak için şunları kullanın `sum(Sign * x)` yerine `sum(x)` ve Ekle `HAVING sum(Sign) > 0`.

Toplanan `count`, `sum` ve `avg` bu şekilde hesaplanabilir. Toplanan `uniq` bir nesnenin en az bir daraltılmamış durumu varsa hesaplanabilir. Toplanan `min` ve `max` hesaplan becauseamaz çünkü `VersionedCollapsingMergeTree` çökmüş durumların değerlerinin geçmişini kaydetmez.

İle verileri ayıklamak gerekiyorsa “collapsing” ancak toplama olmadan (örneğin, en yeni değerleri belirli koşullarla eşleşen satırların mevcut olup olmadığını kontrol etmek için) `FINAL` değiştirici için `FROM` yan. Bu yaklaşım verimsizdir ve büyük tablolarla kullanılmamalıdır.

## Kullanım örneği {#example-of-use}

Örnek veriler:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Tablo oluşturma:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Veri ekleme:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Biz iki kullanın `INSERT` iki farklı veri parçası oluşturmak için sorgular. Verileri tek bir sorgu ile eklersek, ClickHouse bir veri parçası oluşturur ve hiçbir zaman birleştirme gerçekleştirmez.

Veri alma:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Burada ne görüyoruz ve çökmüş parçalar nerede?
İki veri parçasını iki kullanarak oluşturduk `INSERT` sorgular. Bu `SELECT` sorgu iki iş parçacığında gerçekleştirildi ve sonuç rastgele bir satır sırasıdır.
Veri bölümleri henüz birleştirilmediği için çökme gerçekleşmedi. ClickHouse biz tahmin edemez zaman içinde bilinmeyen bir noktada veri parçalarını birleştirir.

Bu yüzden toplamaya ihtiyacımız var:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Toplamaya ihtiyacımız yoksa ve çökmeyi zorlamak istiyorsak, `FINAL` değiştirici için `FROM` yan.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Bu, verileri seçmek için çok verimsiz bir yoldur. Büyük tablolar için kullanmayın.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
