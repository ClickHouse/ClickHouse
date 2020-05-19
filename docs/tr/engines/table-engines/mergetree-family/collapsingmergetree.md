---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: CollapsingMergeTree
---

# CollapsingMergeTree {#table_engine-collapsingmergetree}

Motor devralır [MergeTree](mergetree.md) ve veri parçaları birleştirme algoritmasına çöken satırların mantığını ekler.

`CollapsingMergeTree` sıralama anahtarındaki tüm alanlar zaman uyumsuz olarak siler (daraltır) satır çiftleri (`ORDER BY`) belirli alan hariç eşdeğerdir `Sign` hangi olabilir `1` ve `-1` değerler. Çift olmayan satırlar tutulur. Daha fazla bilgi için bkz: [Çökme](#table_engine-collapsingmergetree-collapsing) belgenin bölümü.

Motor depolama hacmini önemli ölçüde azaltabilir ve `SELECT` sonuç olarak sorgu.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Sorgu parametrelerinin açıklaması için bkz. [sorgu açıklaması](../../../sql-reference/statements/create.md).

**CollapsingMergeTree Parametreleri**

-   `sign` — Name of the column with the type of row: `1` is a “state” satır, `-1` is a “cancel” satır.

    Column data type — `Int8`.

**Sorgu yan tümceleri**

Oluştururken bir `CollapsingMergeTree` tablo, aynı [sorgu yan tümceleri](mergetree.md#table_engine-mergetree-creating-a-table) oluşturul ,urken olduğu gibi gerekli `MergeTree` Tablo.

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
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

Hariç tüm parametreler `sign` içinde olduğu gibi aynı anlama sahip `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” satır, `-1` — “cancel” satır.

    Column Data Type — `Int8`.

</details>

## Çökme {#table_engine-collapsingmergetree-collapsing}

### Veriler {#data}

Bazı nesneler için sürekli değişen verileri kaydetmeniz gereken durumu düşünün. Bir nesne için bir satıra sahip olmak ve herhangi bir değişiklikte güncellemek mantıklı geliyor, ancak güncelleme işlemi dbms için pahalı ve yavaş çünkü depolama alanındaki verilerin yeniden yazılmasını gerektiriyor. Verileri hızlı bir şekilde yazmanız gerekiyorsa, güncelleme kabul edilemez, ancak bir nesnenin değişikliklerini sırayla aşağıdaki gibi yazabilirsiniz.

Belirli sütunu kullanın `Sign`. Eğer `Sign = 1` bu, satırın bir nesnenin durumu olduğu anlamına gelir, diyelim ki “state” satır. Eğer `Sign = -1` aynı özelliklere sahip bir nesnenin durumunun iptali anlamına gelir, diyelim ki “cancel” satır.

Örneğin, kullanıcıların bazı sitelerde ne kadar sayfa kontrol ettiğini ve ne kadar süre orada olduklarını hesaplamak istiyoruz. Bir anda kullanıcı etkinliği durumu ile aşağıdaki satırı yazıyoruz:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Bir an sonra kullanıcı aktivitesinin değişikliğini kaydedip aşağıdaki iki satırla yazıyoruz.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

İlk satır, nesnenin (kullanıcı) önceki durumunu iptal eder. İptal edilen durumun sıralama anahtar alanlarını kopyalamalıdır `Sign`.

İkinci satır geçerli durumu içerir.

Sadece kullanıcı etkinliğinin son durumuna ihtiyacımız olduğu için, satırlar

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

bir nesnenin geçersiz (eski) durumunu daraltarak silinebilir. `CollapsingMergeTree` veri parçalarının birleştirilmesi sırasında bunu yapar.

Neden her değişiklik için 2 satıra ihtiyacımız var [Algoritma](#table_engine-collapsingmergetree-collapsing-algorithm) paragraf.

**Bu yaklaşımın kendine özgü özellikleri**

1.  Verileri yazan program, iptal edebilmek için bir nesnenin durumunu hatırlamalıdır. “Cancel” dize, sıralama anahtar alanlarının kopyalarını içermelidir. “state” dize ve tersi `Sign`. Bu depolama başlangıç boyutunu artırır ama hızlı bir şekilde veri yazmak için izin verir.
2.  Sütunlardaki uzun büyüyen diziler, yazma yükü nedeniyle motorun verimliliğini azaltır. Daha basit veriler, verimlilik o kadar yüksek olur.
3.  Bu `SELECT` sonuçlara itiraz değişiklikler tarihin tutarlılık bağlıdır. Ekleme için veri hazırlarken doğru olun. Tutarsız verilerde öngörülemeyen sonuçlar elde edebilirsiniz, örneğin, oturum derinliği gibi negatif olmayan metrikler için negatif değerler.

### Algoritma {#table_engine-collapsingmergetree-collapsing-algorithm}

ClickHouse veri parçalarını birleştirdiğinde, her ardışık satır grubu aynı sıralama anahtarıyla (`ORDER BY`) en fazla iki satır reduceda indir isgen ,ir, biri `Sign = 1` (“state” satır) ve başka bir `Sign = -1` (“cancel” satır). Başka bir deyişle, girişler çöker.

Elde edilen her veri parçası için ClickHouse kaydeder:

1.  Birincilik “cancel” ve son “state” satır sayısı ise “state” ve “cancel” satırlar eşleşir ve son satır bir “state” satır.
2.  Son “state” satır, daha varsa “state” satırlar daha “cancel” satırlar.
3.  Birincilik “cancel” satır, daha varsa “cancel” satırlar daha “state” satırlar.
4.  Diğer tüm durumlarda satırların hiçbiri.

Ayrıca en az 2 tane daha olduğunda “state” satırlar daha “cancel” satırlar veya en az 2 tane daha “cancel” r rowsows th thenen “state” satırlar, birleştirme devam eder, ancak ClickHouse bu durumu mantıksal bir hata olarak değerlendirir ve sunucu günlüğüne kaydeder. Aynı veriler birden çok kez eklendiğinde, bu hata oluşabilir.

Bu nedenle, çöken istatistik hesaplama sonuçlarını değiştirmemelidir.
Değişiklikler yavaş yavaş çöktü, böylece sonunda hemen hemen her nesnenin sadece son durumu kaldı.

Bu `Sign` birleştirme algoritması, aynı sıralama anahtarına sahip tüm satırların aynı sonuçtaki veri bölümünde ve hatta aynı fiziksel sunucuda olacağını garanti etmediğinden gereklidir. ClickHouse süreci `SELECT` birden çok iş parçacığına sahip sorgular ve sonuçtaki satırların sırasını tahmin edemez. Tamamen almak için bir ihtiyaç varsa toplama gereklidir “collapsed” veri `CollapsingMergeTree` Tablo.

Daraltmayı sonuçlandırmak için bir sorgu yazın `GROUP BY` yan tümce ve işareti için hesap toplama işlevleri. Örneğin, miktarı hesaplamak için kullanın `sum(Sign)` yerine `count()`. Bir şeyin toplamını hesaplamak için şunları kullanın `sum(Sign * x)` yerine `sum(x)`, ve böylece, ve ayrıca ekleyin `HAVING sum(Sign) > 0`.

Toplanan `count`, `sum` ve `avg` bu şekilde hesaplanmış olabilir. Toplanan `uniq` bir nesnenin en az bir durumu çökmüş değilse hesaplanabilir. Toplanan `min` ve `max` hesaplan becauseamadı çünkü `CollapsingMergeTree` daraltılmış durumların değerleri geçmişini kaydetmez.

Toplama olmadan veri ayıklamanız gerekiyorsa (örneğin, en yeni değerleri belirli koşullarla eşleşen satırların mevcut olup olmadığını kontrol etmek için) `FINAL` değiştirici için `FROM` yan. Bu yaklaşım önemli ölçüde daha az etkilidir.

## Kullanım örneği {#example-of-use}

Örnek veriler:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Tablonun oluşturulması:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Veri ekleme:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Biz iki kullanın `INSERT` iki farklı veri parçası oluşturmak için sorgular. Verileri bir sorgu ile eklersek ClickHouse bir veri parçası oluşturur ve hiç bir birleştirme gerçekleştirmez.

Veri alma:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Ne görüyoruz ve nerede çöküyor?

İki ile `INSERT` sorgular, 2 Veri parçası oluşturduk. Bu `SELECT` sorgu 2 iş parçacığında yapıldı ve rastgele bir satır sırası aldık. Veri parçalarının henüz birleştirilmediği için çökme gerçekleşmedi. ClickHouse biz tahmin edemez bilinmeyen bir anda veri kısmını birleştirir.

Böylece toplama ihtiyacımız var:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Toplamaya ihtiyacımız yoksa ve çökmeyi zorlamak istiyorsak, şunları kullanabiliriz `FINAL` değiştirici için `FROM` yan.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Verileri seçmenin bu yolu çok verimsizdir. Büyük masalar için kullanmayın.

## Başka bir yaklaşım örneği {#example-of-another-approach}

Örnek veriler:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Fikir, birleştirmelerin yalnızca anahtar alanları hesaba katmasıdır. Ve içinde “Cancel” satır işareti sütununu kullanmadan toplanırken satırın önceki sürümünü eşitleyen negatif değerleri belirtebiliriz. Bu yaklaşım için veri türünü değiştirmek gerekir `PageViews`,`Duration` uint8 -\> Int16 negatif değerlerini saklamak için.

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Yaklaşımı test edelim:

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // avoid using final in production (just for a test or small tables)
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

``` sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sqk
select count() FROM UAct
```

``` text
┌─count()─┐
│       3 │
└─────────┘
```

``` sql
optimize table UAct final;

select * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
