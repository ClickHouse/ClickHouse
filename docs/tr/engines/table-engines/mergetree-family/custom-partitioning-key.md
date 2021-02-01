---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "\xD6zel B\xF6l\xFCmleme Anahtar\u0131"
---

# Özel Bölümleme Anahtarı {#custom-partitioning-key}

Bölümleme için kullanılabilir [MergeTree](mergetree.md) aile tabloları (dahil [çoğaltıyordu](replication.md) Tablolar). [Hayata görünümler](../special/materializedview.md#materializedview) MergeTree tablolarına dayanarak bölümlemeyi de destekler.

Bir bölüm, bir tablodaki kayıtların belirtilen bir kritere göre mantıksal bir birleşimidir. Bir bölümü, ay, gün veya olay türü gibi rasgele bir ölçütle ayarlayabilirsiniz. Bu verilerin manipülasyonlarını basitleştirmek için her bölüm ayrı ayrı saklanır. Verilere erişirken, ClickHouse mümkün olan en küçük bölüm alt kümesini kullanır.

Bölüm belirtilen `PARTITION BY expr` fık whenra ne zaman [tablo oluşturma](mergetree.md#table_engine-mergetree-creating-a-table). Bölüm anahtarı tablo sütunlarından herhangi bir ifade olabilir. Örneğin, aya göre bölümleme belirtmek için ifadeyi kullanın `toYYYYMM(date_column)`:

``` sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

Bölüm anahtarı ayrıca bir ifade kümesi olabilir ( [birincil anahtar](mergetree.md#primary-keys-and-indexes-in-queries)). Mesela:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

Bu örnekte, bölümlemeyi geçerli hafta boyunca meydana gelen olay türlerine göre ayarladık.

Bir tabloya yeni veri eklerken, bu veriler birincil anahtara göre sıralanmış ayrı bir parça (yığın) olarak depolanır. Taktıktan 10-15 dakika sonra, aynı bölümün parçaları tüm parçaya birleştirilir.

!!! info "Bilgin"
    Birleştirme yalnızca bölümleme ifadesi için aynı değere sahip veri parçaları için çalışır. Bu demektir **aşırı granüler bölümler yapmamalısınız** (yaklaşık binden fazla bölüm). Aksi takdirde, `SELECT` sorgu, dosya sistemindeki ve açık dosya tanımlayıcılarındaki makul olmayan sayıda dosya nedeniyle yetersiz performans gösterir.

Kullan... [sistem.parçalar](../../../operations/system-tables.md#system_tables-parts) tablo tablo parçaları ve bölümleri görüntülemek için. Örneğin, bir var varsayalım `visits` aya göre bölümleme ile tablo. Hadi gerçekleştirelim `SELECT` sorgu için `system.parts` Tablo:

``` sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

Bu `partition` sütun bölümlerin adlarını içerir. Bu örnekte iki bölüm vardır: `201901` ve `201902`. Bölüm adını belirtmek için bu sütun değerini kullanabilirsiniz [ALTER … PARTITION](#alter_manipulations-with-partitions) sorgular.

Bu `name` sütun, bölüm veri parçalarının adlarını içerir. Bölümün adını belirtmek için bu sütunu kullanabilirsiniz. [ALTER ATTACH PART](#alter_attach-partition) sorgu.

İlk bölümün adını kıralım: `201901_1_3_1`:

-   `201901` bölüm adıdır.
-   `1` en az veri bloğu sayısıdır.
-   `3` veri bloğunun maksimum sayısıdır.
-   `1` yığın düzeyidir (oluşturduğu birleştirme ağacının derinliği).

!!! info "Bilgin"
    Eski tip tabloların parçaları adı vardır: `20190117_20190123_2_2_0` (minimum tarih - maksimum tarih - minimum blok numarası - maksimum blok numarası - seviye).

Bu `active` sütun, parçanın durumunu gösterir. `1` aktif istir; `0` etkin değil. Etkin olmayan parçalar, örneğin, daha büyük bir parçaya birleştirildikten sonra kalan kaynak parçalarıdır. Bozuk veri parçaları da etkin olarak gösterilir.

Örnekte gördüğünüz gibi, aynı bölümün birkaç ayrı parçası vardır (örneğin, `201901_1_3_1` ve `201901_1_9_2`). Bu, bu parçaların henüz birleştirilmediği anlamına gelir. ClickHouse, eklendikten yaklaşık 15 dakika sonra eklenen veri parçalarını periyodik olarak birleştirir. Buna ek olarak, kullanarak zamanlanmış olmayan birleştirme gerçekleştirebilirsiniz [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) sorgu. Örnek:

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

Etkin olmayan parçalar birleştirildikten yaklaşık 10 dakika sonra silinecektir.

Bir parça ve bölüm kümesini görüntülemenin başka bir yolu da tablonun dizinine gitmektir: `/var/lib/clickhouse/data/<database>/<table>/`. Mesela:

``` bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

Klasör ‘201901_1_1_0’, ‘201901_1_7_1’ ve böylece parçaların dizinleri vardır. Her bölüm karşılık gelen bir bölümle ilgilidir ve yalnızca belirli bir ay için veri içerir (Bu örnekteki tabloda aylara göre bölümleme vardır).

Bu `detached` dizin kullanarak tablodan ayrılmış parçaları içerir [DETACH](../../../sql-reference/statements/alter.md#alter_detach-partition) sorgu. Bozuk parçalar da silinmek yerine bu dizine taşınır. Sunucu parçaları kullanmaz `detached` directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you run the [ATTACH](../../../sql-reference/statements/alter.md#alter_attach-partition) sorgu.

İşletim sunucusunda, sunucu bunu bilmediğinden, dosya sistemindeki parça kümesini veya verilerini el ile değiştiremeyeceğinizi unutmayın. Çoğaltılmamış tablolar için, sunucu durdurulduğunda bunu yapabilirsiniz, ancak önerilmez. Çoğaltılmış tablolar için, parça kümesi her durumda değiştirilemez.

ClickHouse, bölümlerle işlemleri gerçekleştirmenize izin verir: bunları silin, bir tablodan diğerine kopyalayın veya bir yedek oluşturun. Bölümdeki tüm işlemlerin listesine bakın [Bölümler ve parçalar ile manipülasyonlar](../../../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
