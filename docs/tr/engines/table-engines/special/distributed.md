---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: "Da\u011F\u0131l\u0131"
---

# Dağılı {#distributed}

**Dağıtılmış altyapısı olan tablolar kendileri tarafından herhangi bir veri depolamaz**, ancak birden çok sunucuda dağıtılmış sorgu işleme izin verir.
Okuma otomatik olarak paralelleştirilir. Bir okuma sırasında, varsa uzak sunucularda tablo dizinleri kullanılır.

Dağıtılmış motor parametreleri kabul eder:

-   sunucunun yapılandırma dosyasındaki küme adı

-   uzak veritabanı adı

-   uzak bir tablonun adı

-   (isteğe bağlı olarak) sharding anahtarı

-   (isteğe bağlı olarak) ilke adı, zaman uyumsuz göndermek için geçici dosyaları depolamak için kullanılacaktır

    Ayrıca bakınız:

    -   `insert_distributed_sync` ayar
    -   [MergeTree](../mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) örnekler için

Örnek:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

Veri tüm sunuculardan okunacak ‘logs’ küme, varsayılan değerden.kümedeki her sunucuda bulunan hits tablosu.
Veriler yalnızca okunmakla kalmaz, aynı zamanda uzak sunucularda kısmen işlenir (bunun mümkün olduğu ölçüde).
Örneğin, GROUP BY ile bir sorgu için uzak sunucularda veri toplanır ve toplama işlevlerinin Ara durumları istek sahibi sunucuya gönderilir. Daha sonra veriler daha fazla toplanacaktır.

Veritabanı adı yerine, bir dize döndüren sabit bir ifade kullanabilirsiniz. Örneğin: currentDatabase ().

logs – The cluster name in the server's config file.

Kümeler şöyle ayarlanır:

``` xml
<remote_servers>
    <logs>
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

Burada bir küme adı ile tanımlanır ‘logs’ bu, her biri iki kopya içeren iki parçadan oluşur.
Kırıklar, verilerin farklı bölümlerini içeren sunuculara başvurur (tüm verileri okumak için tüm kırıklara erişmeniz gerekir).
Yinelemeler sunucuları çoğaltılıyor (tüm verileri okumak için, yinelemelerden herhangi birinde verilere erişebilirsiniz).

Küme adları nokta içermemelidir.

Parametre `host`, `port` ve isteğe bağlı olarak `user`, `password`, `secure`, `compression` her sunucu için belirtilir:
- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server doesn't start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (‘tcp_port’ yapılandırmada, genellikle 9000 olarak ayarlanır). Http_port ile karıştırmayın.
- `user` – Name of the user for connecting to a remote server. Default value: default. This user must have access to connect to the specified server. Access is configured in the users.xml file. For more information, see the section [Erişim hakları](../../../operations/access-rights.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` - Bağlantı için ssl kullanın, genellikle de tanımlamanız gerekir `port` = 9440. Sunucu dinlem shouldeli `<tcp_port_secure>9440</tcp_port_secure>` ve doğru sertifikalara sahip.
- `compression` - Kullanım veri sıkıştırma. Varsayılan değer: true.

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [dengeleme](../../../operations/settings/settings.md#settings-load_balancing) ayar.
Sunucu ile bağlantı kurulmamışsa, kısa bir zaman aşımı ile bağlanma girişimi olacaktır. Bağlantı başarısız olursa, sonraki yineleme seçilir ve benzeri tüm yinelemeler için. Bağlantı girişimi tüm yinelemeler için başarısız olursa, girişimi aynı şekilde, birkaç kez tekrarlanır.
Bu esneklik lehine çalışır, ancak tam hataya dayanıklılık sağlamaz: uzak bir sunucu bağlantıyı kabul edebilir, ancak çalışmayabilir veya kötü çalışabilir.

Parçalardan yalnızca birini belirtebilirsiniz (bu durumda, sorgu işleme dağıtılmış yerine uzak olarak adlandırılmalıdır) veya herhangi bir sayıda parçaya kadar. Her parçada, bir ila herhangi bir sayıda yinelemeyi belirtebilirsiniz. Her parça için farklı sayıda çoğaltma belirtebilirsiniz.

Yapılandırmada istediğiniz kadar küme belirtebilirsiniz.

Kümelerinizi görüntülemek için ‘system.clusters’ Tablo.

Dağıtılmış motor, yerel bir sunucu gibi bir küme ile çalışmaya izin verir. Ancak, küme uzatılamaz: yapılandırmasını sunucu yapılandırma dosyasına yazmanız gerekir (tüm kümenin sunucuları için daha da iyisi).

The Distributed engine requires writing clusters to the config file. Clusters from the config file are updated on the fly, without restarting the server. If you need to send a query to an unknown set of shards and replicas each time, you don't need to create a Distributed table – use the ‘remote’ bunun yerine tablo işlevi. Bölümüne bakınız [Tablo fonksiyonları](../../../sql-reference/table-functions/index.md).

Bir kümeye veri yazmak için iki yöntem vardır:

İlk olarak, hangi sunucuların hangi verileri yazacağını ve her bir parçaya doğrudan yazmayı gerçekleştireceğini tanımlayabilirsiniz. Başka bir deyişle, dağıtılmış tablo içinde INSERT gerçekleştirmek “looks at”. Bu, konu alanının gereklilikleri nedeniyle önemsiz olmayan herhangi bir sharding şemasını kullanabileceğiniz için en esnek çözümdür. Bu aynı zamanda en uygun çözümdür, çünkü veriler farklı parçalara tamamen bağımsız olarak yazılabilir.

İkinci olarak, dağıtılmış bir tabloda ekleme gerçekleştirebilirsiniz. Bu durumda, tablo eklenen verileri sunucuların kendisine dağıtacaktır. Dağıtılmış bir tabloya yazmak için, bir sharding anahtar kümesi (son parametre) olmalıdır. Ek olarak, yalnızca bir parça varsa, yazma işlemi sharding anahtarını belirtmeden çalışır, çünkü bu durumda hiçbir şey ifade etmez.

Her parça yapılandırma dosyasında tanımlanan bir ağırlığa sahip olabilir. Varsayılan olarak, ağırlık bir eşittir. Veriler, parça ağırlığı ile orantılı miktarda parçalara dağıtılır. Örneğin, iki parça varsa ve birincisi 9'luk bir ağırlığa sahipse, ikincisi 10'luk bir ağırlığa sahipse, ilk satırların 9 / 19 parçası gönderilir ve ikincisi 10 / 19 gönderilir.

Her shard olabilir ‘internal_replication’ yapılandırma dosyasında tanımlanan parametre.

Bu parametre şu şekilde ayarlanırsa ‘true’, yazma işlemi ilk sağlıklı yinelemeyi seçer ve ona veri yazar. Dağıtılmış tablo ise bu alternatifi kullanın “looks at” çoğaltılan tablolar. Başka bir deyişle, verilerin yazılacağı tablo kendilerini çoğaltacaktır.

Olarak ayarlan ifmışsa ‘false’ (varsayılan), veriler tüm kopyalara yazılır. Özünde, bu, dağıtılmış tablonun verilerin kendisini çoğalttığı anlamına gelir. Bu, çoğaltılmış tabloları kullanmaktan daha kötüdür, çünkü kopyaların tutarlılığı denetlenmez ve zamanla biraz farklı veriler içerirler.

Bir veri satırının gönderildiği parçayı seçmek için, parçalama ifadesi analiz edilir ve kalan kısmı, parçaların toplam ağırlığına bölünmesinden alınır. Satır, kalanların yarı aralığına karşılık gelen parçaya gönderilir. ‘prev_weight’ -e doğru ‘prev_weights + weight’, nere ‘prev_weights’ en küçük sayıya sahip parçaların toplam ağırlığı ve ‘weight’ bu parçanın ağırlığı. Örneğin, iki parça varsa ve birincisi 9'luk bir ağırlığa sahipse, ikincisi 10'luk bir ağırlığa sahipse, satır \[0, 9) aralığından kalanlar için ilk parçaya ve ikincisine \[9, 19) aralığından kalanlar için gönderilecektir.

Sharding ifadesi, bir tamsayı döndüren sabitler ve tablo sütunlarından herhangi bir ifade olabilir. Örneğin, ifadeyi kullanabilirsiniz ‘rand()’ verilerin rastgele dağılımı için veya ‘UserID’ kullanıcının kimliğinin bölünmesinden kalanın dağıtımı için (daha sonra tek bir kullanıcının verileri, kullanıcılar tarafından çalışmayı ve katılmayı basitleştiren tek bir parçada bulunur). Sütunlardan biri yeterince eşit olarak dağıtılmazsa, onu bir karma işleve sarabilirsiniz: ınthash64(Userıd).

Bölüm'den basit bir hatırlatma, sharding için sınırlı bir çözümdür ve her zaman uygun değildir. Orta ve büyük hacimlerde veri (düzinelerce sunucu) için çalışır, ancak çok büyük hacimlerde veri (yüzlerce sunucu veya daha fazla) için değildir. İkinci durumda, dağıtılmış tablolarda girdileri kullanmak yerine konu alanı tarafından gerekli olan sharding şemasını kullanın.

SELECT queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you don't have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

Aşağıdaki durumlarda sharding şeması hakkında endişelenmelisiniz:

-   Belirli bir anahtar tarafından veri (veya birleştirme) birleştirme gerektiren sorgular kullanılır. Veriler bu anahtar tarafından parçalanırsa, GLOBAL IN veya GLOBAL JOİN yerine local IN veya JOİN kullanabilirsiniz, bu da çok daha etkilidir.
-   Çok sayıda küçük Sorgu ile çok sayıda sunucu (yüzlerce veya daha fazla) kullanılır (bireysel müşterilerin sorguları - web siteleri, reklamverenler veya ortaklar). Küçük sorguların tüm kümeyi etkilememesi için, tek bir istemci için tek bir parça üzerinde veri bulmak mantıklıdır. Alternatif olarak, Yandex'te yaptığımız gibi.Metrica, iki seviyeli sharding kurabilirsiniz: tüm kümeyi bölün “layers”, bir katmanın birden fazla parçadan oluşabileceği yer. Tek bir istemci için veriler tek bir katmanda bulunur, ancak kırıklar gerektiğinde bir katmana eklenebilir ve veriler rastgele dağıtılır. Her katman için dağıtılmış tablolar oluşturulur ve genel sorgular için tek bir paylaşılan dağıtılmış tablo oluşturulur.

Veriler zaman uyumsuz olarak yazılır. Tabloya eklendiğinde, veri bloğu sadece yerel dosya sistemine yazılır. Veriler en kısa sürede arka planda uzak sunuculara gönderilir. Veri gönderme süresi tarafından yönetilir [distributed_directory_monitor_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms) ve [distributed_directory_monitor_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) ayarlar. Bu `Distributed` motor ayrı ayrı eklenen verilerle her dosyayı gönderir, ancak toplu dosya gönderme etkinleştirebilirsiniz [distributed_directory_monitor_batch_ınserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts) ayar. Bu ayar, yerel sunucu ve ağ kaynaklarını daha iyi kullanarak küme performansını artırır. Tablo dizinindeki dosyaların listesini (gönderilmeyi bekleyen veriler) kontrol ederek verilerin başarıyla gönderilip gönderilmediğini kontrol etmelisiniz: `/var/lib/clickhouse/data/database/table/`.

Sunucu varlığını durdurdu veya (örneğin, bir aygıt arızasından sonra) dağıtılmış bir tabloya bir ekleme sonra kaba bir yeniden başlatma vardı, eklenen veriler kaybolabilir. Tablo dizininde bozuk bir veri parçası tespit edilirse, ‘broken’ alt dizin ve artık kullanılmıyor.

Max_parallel_replicas seçeneği etkinleştirildiğinde, sorgu işleme tek bir parça içindeki tüm yinelemeler arasında paralelleştirilir. Daha fazla bilgi için bölüme bakın [max_parallel_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas).

## Sanal Sütunlar {#virtual-columns}

-   `_shard_num` — Contains the `shard_num` (itibaren `system.clusters`). Tür: [Uİnt32](../../../sql-reference/data-types/int-uint.md).

!!! note "Not"
    Beri [`remote`](../../../sql-reference/table-functions/remote.md)/`cluster` tablo işlevleri DAHİLİ olarak aynı dağıtılmış altyapının geçici örneğini oluşturur, `_shard_num` de kullanılabilir.

**Ayrıca Bakınız**

-   [Sanal sütunlar](index.md#table_engines-virtual_columns)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/distributed/) <!--hide-->
