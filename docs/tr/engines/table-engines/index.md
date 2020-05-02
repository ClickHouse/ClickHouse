---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
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

Yüksek yük görevleri için en evrensel ve fonksiyonel masa motorları. Bu motorlar tarafından paylaşılan özellik, sonraki arka plan veri işleme ile hızlı veri ekleme ’ dir. `MergeTree` aile motorları destek veri çoğaltma (ile [Çoğaltıyordu\*](mergetree-family/replication.md) sürümleri), bölümleme ve diğer özellikler diğer motorlarda desteklenmez.

Ailede motorlar:

-   [MergeTree](mergetree-family/mergetree.md)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md)
-   [SummingMergeTree](mergetree-family/summingmergetree.md)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md)
-   [Graphıtemergetree](mergetree-family/graphitemergetree.md)

### Günlük {#log}

Hafiflik [motorlar](log-family/index.md) minimum işlevsellik ile. Birçok küçük tabloyu (yaklaşık 1 milyon satıra kadar) hızlı bir şekilde yazmanız ve daha sonra bir bütün olarak okumanız gerektiğinde en etkili olanlardır.

Ailede motorlar:

-   [TinyLog](log-family/tinylog.md)
-   [StripeLog](log-family/stripelog.md)
-   [Günlük](log-family/log.md)

### Entegrasyon Motorları {#integration-engines}

Diğer veri depolama ve işleme sistemleri ile iletişim kurmak için motorlar.

Ailede motorlar:

-   [Kafka](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### Özel Motorlar {#special-engines}

Ailede motorlar:

-   [Dağılı](special/distributed.md)
-   [MaterializedView](special/materializedview.md)
-   [Sözlük](special/dictionary.md)
-   [Birleştirmek](special/merge.md)
-   [Dosya](special/file.md)
-   [Boş](special/null.md)
-   [Koymak](special/set.md)
-   [Katmak](special/join.md)
-   [URL](special/url.md)
-   [Görünüm](special/view.md)
-   [Bellek](special/memory.md)
-   [Arabellek](special/buffer.md)

## Sanal Sütunlar {#table_engines-virtual-columns}

Sanal sütun, motor kaynak kodunda tanımlanan ayrılmaz bir tablo altyapısı özniteliğidir.

Sanal sütunları belirtmemelisiniz `CREATE TABLE` sorgula ve onları göremezsin `SHOW CREATE TABLE` ve `DESCRIBE TABLE` sorgu sonuçları. Sanal sütunlar da salt okunur, bu nedenle sanal sütunlara veri ekleyemezsiniz.

Sanal bir sütundan veri seçmek için, adını `SELECT` sorgu. `SELECT *` sanal sütunlardan değerler döndürmez.

Tablo sanal sütunlarından biriyle aynı ada sahip bir sütuna sahip bir tablo oluşturursanız, sanal sütuna erişilemez hale gelir. Bunu yapmayı önermiyoruz. Çakışmaları önlemek için, sanal sütun adları genellikle bir alt çizgi ile öneki.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
