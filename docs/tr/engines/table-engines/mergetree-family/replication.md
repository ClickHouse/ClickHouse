---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "Veri \xC7o\u011Faltma"
---

# Veri Çoğaltma {#table_engines-replication}

Çoğaltma yalnızca mergetree ailesindeki tablolar için desteklenir:

-   ReplicatedMergeTree
-   ReplicatedSummingMergeTree
-   ReplicatedReplacingMergeTree
-   ReplicatedAggregatingMergeTree
-   ReplicatedCollapsingMergeTree
-   ReplicatedVersionedCollapsingMergetree
-   ReplicatedGraphiteMergeTree

Çoğaltma, tüm sunucu değil, tek bir tablo düzeyinde çalışır. Bir sunucu hem çoğaltılmış hem de çoğaltılmamış tabloları aynı anda depolayabilir.

Çoğaltma, parçaya bağlı değildir. Her parçanın kendi bağımsız çoğaltması vardır.

İçin sıkıştırılmış veri `INSERT` ve `ALTER` sorgular çoğaltılır (daha fazla bilgi için bkz. [ALTER](../../../sql-reference/statements/alter.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` ve `RENAME` sorgular tek bir sunucuda yürütülür ve çoğaltılmaz:

-   Bu `CREATE TABLE` sorgu sorgu çalıştırıldığı sunucuda yeni bir replicatable tablo oluşturur. Bu tablo diğer sunucularda zaten varsa, yeni bir yineleme ekler.
-   Bu `DROP TABLE` sorgu, sorgunun çalıştırıldığı sunucuda bulunan yinelemeyi siler.
-   Bu `RENAME` sorgu yinelemeler birinde tabloyu yeniden adlandırır. Başka bir deyişle, çoğaltılmış tablolar farklı yinelemeler üzerinde farklı adlara sahip olabilir.

ClickHouse kullanır [Apache ZooKeeper](https://zookeeper.apache.org) kopyaları meta bilgilerini saklamak için. ZooKeeper sürüm 3.4.5 veya daha yeni kullanın.

Çoğaltma kullanmak için, parametreleri [zookeeper](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) sunucu yapılandırma bölümü.

!!! attention "Dikkat"
    Güvenlik ayarını ihmal etmeyin. ClickHouse destekler `digest` [ACLL şeması](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) ZooKeeper Güvenlik alt sisteminin.

ZooKeeper kümesinin adreslerini ayarlama örneği:

``` xml
<zookeeper>
    <node index="1">
        <host>example1</host>
        <port>2181</port>
    </node>
    <node index="2">
        <host>example2</host>
        <port>2181</port>
    </node>
    <node index="3">
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Varolan herhangi bir ZooKeeper kümesini belirtebilirsiniz ve sistem kendi verileri için bir dizin kullanır (replicatable tablo oluştururken dizin belirtilir).

Zookeeper yapılandırma dosyasında ayarlanmamışsa, çoğaltılmış tablolar oluşturamazsınız ve varolan çoğaltılmış tablolar salt okunur olacaktır.

ZooKeeper kullanılmaz `SELECT` çoğaltma performansını etkilemez çünkü sorgular `SELECT` ve sorgular, çoğaltılmamış tablolar için yaptıkları kadar hızlı çalışır. Dağıtılmış çoğaltılmış tabloları sorgularken, ClickHouse davranışı ayarlar tarafından denetlenir [max\_replica\_delay\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) ve [fallback\_to\_stale\_replicas\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

Her biri için `INSERT` sorgu, yaklaşık on girişleri zookeeper birkaç işlemler aracılığıyla eklenir. (Daha kesin olmak gerekirse, bu eklenen her veri bloğu içindir; bir ekleme sorgusu her bir blok veya bir blok içerir `max_insert_block_size = 1048576` satırlar.) Bu, biraz daha uzun gecikmelere yol açar `INSERT` çoğaltılmamış tablolarla karşılaştırıldığında. Ancak, birden fazla olmayan gruplar halinde veri eklemek için önerileri izlerseniz `INSERT` saniyede, herhangi bir sorun yaratmaz. Bir ZooKeeper kümesini koordine etmek için kullanılan tüm ClickHouse kümesinin toplam birkaç yüzü vardır `INSERTs` saniyede. Veri eklerindeki verim (saniyede satır sayısı), çoğaltılmamış veriler için olduğu kadar yüksektir.

Çok büyük kümeler için, farklı kırıklar için farklı ZooKeeper kümelerini kullanabilirsiniz. Ancak, bu Yandex'de gerekli değildir.Metrica küme (yaklaşık 300 sunucu).

Çoğaltma zaman uyumsuz ve çok ana. `INSERT` sorgular (yanı sıra `ALTER`) mevcut herhangi bir sunucuya gönderilebilir. Veri sorgu çalıştırıldığı sunucuda eklenir ve sonra diğer sunuculara kopyalanır. Zaman uyumsuz olduğundan, son eklenen veriler bazı gecikme ile diğer yinelemeler görünür. Yinelemelerin bir kısmı mevcut değilse, veriler kullanılabilir olduklarında yazılır. Bir çoğaltma varsa, gecikme, sıkıştırılmış veri bloğunu ağ üzerinden aktarmak için gereken süredir.

Varsayılan olarak, bir INSERT sorgusu yalnızca bir yinelemeden veri yazma onayı bekler. Verileri başarıyla yalnızca bir yineleme için yazılmıştır ve bu yineleme ile sunucu varolmaya sona erer, depolanan veriler kaybolur. Birden çok yinelemeden veri yazma onayını almayı etkinleştirmek için `insert_quorum` seçenek.

Her veri bloğu atomik olarak yazılır. Ekle sorgusu kadar bloklara ayrılmıştır `max_insert_block_size = 1048576` satırlar. Diğer bir deyişle, `INSERT` sorgu 1048576 satırdan daha az, atomik olarak yapılır.

Veri blokları tekilleştirilmiştir. Aynı veri bloğunun (aynı sırayla aynı satırları içeren aynı boyuttaki veri blokları) birden fazla yazımı için, blok yalnızca bir kez yazılır. Bunun nedeni, istemci uygulaması verilerin DB'YE yazılıp yazılmadığını bilmediğinde ağ arızaları durumunda, `INSERT` sorgu sadece tekrar edilebilir. Hangi çoğaltma eklerinin aynı verilerle gönderildiği önemli değildir. `INSERTs` idempotent vardır. Tekilleştirme parametreleri tarafından kontrol edilir [merge\_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) sunucu ayarları.

Çoğaltma sırasında, yalnızca eklenecek kaynak veriler ağ üzerinden aktarılır. Daha fazla veri dönüşümü (birleştirme), tüm kopyalarda aynı şekilde koordine edilir ve gerçekleştirilir. Bu, ağ kullanımını en aza indirir; bu, çoğaltmaların farklı veri merkezlerinde bulunduğu zaman çoğaltmanın iyi çalıştığı anlamına gelir. (Farklı veri merkezlerinde çoğaltmanın çoğaltmanın ana hedefi olduğunu unutmayın .)

Aynı verilerin çoğaltmaları herhangi bir sayıda olabilir. Üye.Metrica üretimde çift çoğaltma kullanır. Her sunucu, bazı durumlarda RAID-5 veya RAID-6 ve RAID-10 kullanır. Bu nispeten güvenilir ve kullanışlı bir çözümdür.

Sistem, yinelemelerdeki veri senkronizasyonunu izler ve bir hatadan sonra kurtarabilir. Yük devretme otomatik (verilerde küçük farklılıklar için) veya yarı otomatik (veriler çok fazla farklılık gösterdiğinde, bu da bir yapılandırma hatasını gösterebilir).

## Çoğaltılmış Tablolar Oluşturma {#creating-replicated-tables}

Bu `Replicated` önek tablo motoru adına eklenir. Mesela:`ReplicatedMergeTree`.

**Çoğaltılan \* MergeTree parametreleri**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.

Örnek:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

<details markdown="1">

<summary>Kullanımdan kaldırılmış sözdizimi örneği</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

</details>

Örnekte gösterildiği gibi, bu parametreler kıvırcık köşeli ayraçlarda ikameler içerebilir. İkame edilen değerler ‘macros’ yapılandırma dosyasının bölümü. Örnek:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

Zookeeper tablonun yolunu her çoğaltılmış tablo için benzersiz olmalıdır. Farklı parçalardaki tabloların farklı yolları olmalıdır.
Bu durumda, yol aşağıdaki parçalardan oluşur:

`/clickhouse/tables/` ortak önek. Tam olarak bunu kullanmanızı öneririz.

`{layer}-{shard}` shard tanımlayıcısıdır. Bu örnekte Yandex'den beri iki bölümden oluşmaktadır.Metrica küme iki seviyeli sharding kullanır. Çoğu görev için, yalnızca shard tanımlayıcısına genişletilecek olan {shard} ikamesini bırakabilirsiniz.

`table_name` ZooKeeper tablo için düğüm adıdır. Tablo adı ile aynı yapmak için iyi bir fikirdir. Açıkça tanımlanır, çünkü tablo adının aksine, bir yeniden adlandırma sorgusundan sonra değişmez.
*HINT*: önüne bir veritabanı adı ekleyebilirsiniz `table_name` ayrıca. E. g. `db_name.table_name`

Çoğaltma adı, aynı tablonun farklı yinelemelerini tanımlar. Örnekte olduğu gibi bunun için sunucu adını kullanabilirsiniz. Adın sadece her parça içinde benzersiz olması gerekir.

Değiştirmeleri kullanmak yerine parametreleri açıkça tanımlayabilirsiniz. Bu, test etmek ve küçük kümeleri yapılandırmak için uygun olabilir. Ancak, dağıtılmış DDL sorguları kullanamazsınız (`ON CLUSTER` bu durumda).

Büyük kümelerle çalışırken, hata olasılığını azalttıkları için değiştirmeleri kullanmanızı öneririz.

Run the `CREATE TABLE` her yineleme üzerinde sorgu. Bu sorgu, yeni bir çoğaltılmış tablo oluşturur veya varolan bir yeni bir yineleme ekler.

Tablo zaten diğer yinelemeler üzerinde bazı veriler içerdikten sonra yeni bir yineleme eklerseniz, verileri diğer yinelemeler için yeni bir sorgu çalıştırdıktan sonra kopyalanır. Başka bir deyişle, yeni çoğaltma kendisini diğerleriyle eşitler.

Bir yineleme silmek için çalıştırın `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## Arızalardan Sonra Kurtarma {#recovery-after-failures}

Bir sunucu başlatıldığında ZooKeeper kullanılamıyorsa, çoğaltılmış tablolar salt okunur moda geçer. Sistem periyodik olarak ZooKeeper bağlanmaya çalışır.

ZooKeeper sırasında kullanılamıyorsa bir `INSERT`, veya ZooKeeper ile etkileşimde bulunurken bir hata oluşur, bir istisna atılır.

ZooKeeper bağlandıktan sonra, sistem yerel dosya sistemindeki veri kümesinin beklenen veri kümesiyle eşleşip eşleşmediğini kontrol eder (ZooKeeper bu bilgileri saklar). Küçük tutarsızlıklar varsa, sistem verileri kopyalarla senkronize ederek bunları çözer.

Sistem bozuk veri parçalarını (yanlış dosya boyutu ile) veya tanınmayan parçaları (dosya sistemine yazılmış ancak Zookeeper'da kaydedilmemiş parçalar) tespit ederse, bunları `detached` alt dizin (silinmez). Eksik parçalar kopyalardan kopyalanır.

Clickhouse'un büyük miktarda veriyi otomatik olarak silme gibi yıkıcı eylemler gerçekleştirmediğini unutmayın.

Sunucu başlatıldığında (veya ZooKeeper ile yeni bir oturum kurduğunda), yalnızca tüm dosyaların miktarını ve boyutlarını kontrol eder. Dosya boyutları eşleşirse, ancak bayt ortasında bir yerde değiştirilmişse, bu hemen algılanmaz, ancak yalnızca bir dosya için verileri okumaya çalışırken algılanmaz. `SELECT` sorgu. Sorgu, eşleşen olmayan bir sağlama toplamı veya sıkıştırılmış bir bloğun boyutu hakkında bir özel durum atar. Bu durumda, veri parçaları doğrulama kuyruğuna eklenir ve gerekirse kopyalardan kopyalanır.

Yerel veri kümesi beklenenden çok fazla farklıysa, bir güvenlik mekanizması tetiklenir. Sunucu bunu günlüğe girer ve başlatmayı reddeder. Bunun nedeni, bu durumda, bir parçadaki bir kopya yanlışlıkla farklı bir parçadaki bir kopya gibi yapılandırılmışsa gibi bir yapılandırma hatası gösterebilir. Ancak, bu mekanizma için eşikleri oldukça düşük ayarlanır ve bu durum normal hata kurtarma sırasında ortaya çıkabilir. Bu durumda, veriler yarı otomatik olarak geri yüklenir “pushing a button”.

Kurtarma işlemini başlatmak için düğümü oluşturun `/path_to_table/replica_name/flags/force_restore_data` herhangi bir içerik ile ZooKeeper veya tüm çoğaltılmış tabloları geri yüklemek için komutu çalıştırın:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Sunucuyu yeniden başlatın. Başlangıçta, sunucu bu bayrakları siler ve kurtarma işlemini başlatır.

## Tam Veri Kaybından Sonra Kurtarma {#recovery-after-complete-data-loss}

Tüm veriler ve meta veriler sunuculardan birinden kaybolduysa, kurtarma için şu adımları izleyin:

1.  Clickhouse'u sunucuya yükleyin. Bunları kullanırsanız, shard tanımlayıcısı ve yinelemeleri içeren yapılandırma dosyasında doğru değiştirmelerin tanımlayın.
2.  Sunucularda el ile çoğaltılması gereken yinelenmemiş tablolar varsa, verilerini bir kopyadan kopyalayın (dizinde `/var/lib/clickhouse/data/db_name/table_name/`).
3.  Bulunan tablo tanım copylarını kopyala `/var/lib/clickhouse/metadata/` bir kopyadan. Tablo tanımlarında bir parça veya çoğaltma tanımlayıcısı açıkça tanımlanmışsa, bu kopyaya karşılık gelecek şekilde düzeltin. (Alternatif olarak, sunucuyu başlatın ve tüm `ATTACH TABLE` içinde olması gereken sorgular .sql dosyaları `/var/lib/clickhouse/metadata/`.)
4.  Kurtarma işlemini başlatmak için ZooKeeper düğümünü oluşturun `/path_to_table/replica_name/flags/force_restore_data` herhangi bir içerikle veya tüm çoğaltılmış tabloları geri yüklemek için komutu çalıştırın: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Ardından sunucuyu başlatın (zaten çalışıyorsa yeniden başlatın). Veriler kopyalardan indirilecektir.

Alternatif bir kurtarma seçeneği zookeeper kayıp yineleme hakkında bilgi silmektir (`/path_to_table/replica_name`), daha sonra açıklandığı gibi yinelemeyi tekrar oluşturun “[Çoğaltılmış tablolar oluşturma](#creating-replicated-tables)”.

Kurtarma sırasında ağ bant genişliği üzerinde herhangi bir kısıtlama yoktur. Aynı anda birçok yinelemeyi geri yüklüyorsanız bunu aklınızda bulundurun.

## Mergetree'den Replicatedmergetree'ye dönüştürme {#converting-from-mergetree-to-replicatedmergetree}

Terimi kullanıyoruz `MergeTree` tüm tablo motorlarına başvurmak için `MergeTree family` için aynı `ReplicatedMergeTree`.

Eğer olsaydı bir `MergeTree` el ile çoğaltılmış tablo, çoğaltılmış bir tabloya dönüştürebilirsiniz. Zaten büyük miktarda veri topladıysanız bunu yapmanız gerekebilir. `MergeTree` tablo ve şimdi çoğaltmayı etkinleştirmek istiyorsunuz.

Veriler çeşitli yinelemelerde farklılık gösteriyorsa, önce onu eşitleyin veya bu verileri biri dışındaki tüm yinelemelerde silin.

Varolan MergeTree tablosunu yeniden adlandırın, sonra bir `ReplicatedMergeTree` eski adı olan tablo.
Eski tablodan veri taşıma `detached` yeni tablo verileri ile dizin içindeki alt dizin (`/var/lib/clickhouse/data/db_name/table_name/`).
Sonra koş `ALTER TABLE ATTACH PARTITION` bu veri parçalarını çalışma kümesine eklemek için yinelemelerden birinde.

## Replicatedmergetree'den Mergetree'ye dönüştürme {#converting-from-replicatedmergetree-to-mergetree}

Farklı bir adla bir MergeTree tablosu oluşturun. İle dizinden tüm verileri taşıyın `ReplicatedMergeTree` yeni tablonun veri dizinine tablo verileri. Sonra Sil `ReplicatedMergeTree` tablo ve sunucuyu yeniden başlatın.

Eğer bir kurtulmak istiyorsanız `ReplicatedMergeTree` sunucu başlatmadan tablo:

-   İlgili sil `.sql` meta veri dizinindeki dosya (`/var/lib/clickhouse/metadata/`).
-   ZooKeeper ilgili yolu silin (`/path_to_table/replica_name`).

Bundan sonra, sunucuyu başlatabilir, bir `MergeTree` tablo, verileri kendi dizinine taşıyın ve sonra sunucuyu yeniden başlatın.

## Zookeeper kümesindeki meta veriler kaybolduğunda veya zarar gördüğünde kurtarma {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

ZooKeeper içindeki veriler kaybolduysa veya hasar gördüyse, verileri yukarıda açıklandığı gibi yinelenmemiş bir tabloya taşıyarak kaydedebilirsiniz.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
