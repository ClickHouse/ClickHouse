---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Masa Motorlar\u0131"
toc_priority: 26
toc_title: "Giri\u015F"
---

# Masa Motorları {#table_engines}

Tablo motoru (tablo türü) belirler:

-   Verilerin nasıl ve nerede depolandığı, nereye yazılacağı ve nereden okunacağı.
-   Hangi sorgular desteklenir ve nasıl.
-   Eşzamanlı veri erişimi.
-   Varsa indeks uselerin kullanımı.
-   Çok iş parçacıklı istek yürütme mümkün olup olmadığı.
-   Veri çoğaltma parametreleri.

## Motor Aileleri {#engine-families}

### MergeTree {#mergetree}

Yüksek yük görevleri için en evrensel ve fonksiyonel masa motorları. Bu motorlar tarafından paylaşılan özellik, sonraki arka plan veri işleme ile hızlı veri ekleme ' dir. `MergeTree` aile motorları destek veri çoğaltma (ile [Çoğaltıyordu\*](mergetree-family/replication.md#table_engines-replication) sürümleri), bölümleme ve diğer özellikler diğer motorlarda desteklenmez.

Ailede motorlar:

-   [MergeTree](mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [Graphıtemergetree](mergetree-family/graphitemergetree.md#graphitemergetree)

### Günlük {#log}

Hafiflik [motorlar](log-family/index.md) minimum işlevsellik ile. Birçok küçük tabloyu (yaklaşık 1 milyon satıra kadar) hızlı bir şekilde yazmanız ve daha sonra bir bütün olarak okumanız gerektiğinde en etkili olanlardır.

Ailede motorlar:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [StripeLog](log-family/stripelog.md#stripelog)
-   [Günlük](log-family/log.md#log)

### Entegrasyon Motorları {#integration-engines}

Diğer veri depolama ve işleme sistemleri ile iletişim kurmak için motorlar.

Ailede motorlar:

-   [Kafka](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### Özel Motorlar {#special-engines}

Ailede motorlar:

-   [Dağılı](special/distributed.md#distributed)
-   [MaterializedView](special/materializedview.md#materializedview)
-   [Sözlük](special/dictionary.md#dictionary)
-   \[Mer \]ge\] (spec /ial / mer #ge. md#mer #ge
-   [Dosya](special/file.md#file)
-   [Boş](special/null.md#null)
-   [Koymak](special/set.md#set)
-   [Katmak](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [Görünüm](special/view.md#table_engines-view)
-   [Hafıza](special/memory.md#memory)
-   [Arabellek](special/buffer.md#buffer)

## Sanal Sütunlar {#table_engines-virtual_columns}

Sanal sütun, motor kaynak kodunda tanımlanan ayrılmaz bir tablo altyapısı özniteliğidir.

Sanal sütunları belirtmemelisiniz `CREATE TABLE` sorgula ve onları göremezsin `SHOW CREATE TABLE` ve `DESCRIBE TABLE` sorgu sonuçları. Sanal sütunlar da salt okunur, bu nedenle sanal sütunlara veri ekleyemezsiniz.

Sanal bir sütundan veri seçmek için, adını `SELECT` sorgu. `SELECT *` sanal sütunlardan değerler döndürmez.

Tablo sanal sütunlarından biriyle aynı ada sahip bir sütuna sahip bir tablo oluşturursanız, sanal sütuna erişilemez hale gelir. Bunu yapmayı önermiyoruz. Çakışmaları önlemek için, sanal sütun adları genellikle bir alt çizgi ile öneki.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
