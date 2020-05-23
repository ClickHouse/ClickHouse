---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: Veri Yedekleme
---

# Veri Yedekleme {#data-backup}

Karşın [çoğalma](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [50 GB'den fazla veri içeren MergeTree benzeri bir motorla tabloları bırakamazsınız](https://github.com/ClickHouse/ClickHouse/blob/v18.14.18-stable/programs/server/config.xml#L322-L330). Ancak, bu önlemler olası tüm davaları kapsamaz ve atlatılabilir.

Olası insan hatalarını etkili bir şekilde azaltmak için, verilerinizi yedeklemek ve geri yüklemek için dikkatli bir şekilde bir strateji hazırlamanız gerekir **önceden**.

Her şirketin farklı kaynakları ve iş gereksinimleri vardır, bu nedenle her duruma uyacak ClickHouse yedeklemeleri ve geri yüklemeleri için evrensel bir çözüm yoktur. Bir gigabayt veri için ne işe yarar, muhtemelen onlarca petabayt için çalışmaz. Aşağıda tartışılacak olan kendi artıları ve eksileri ile çeşitli Olası yaklaşımlar vardır. Çeşitli eksikliklerini telafi etmek için sadece bir tane yerine birkaç yaklaşım kullanmak iyi bir fikirdir.

!!! note "Not"
    Bir şeyi yedeklediyseniz ve geri yüklemeyi hiç denemediyseniz, aslında ihtiyacınız olduğunda Geri Yüklemenin düzgün çalışmayacağını (veya en azından işin tahammül edebileceğinden daha uzun süreceğini) unutmayın. Bu nedenle, seçtiğiniz yedekleme yaklaşımı ne olursa olsun, geri yükleme işlemini de otomatikleştirdiğinizden emin olun ve düzenli olarak yedek bir ClickHouse kümesinde uygulayın.

## Kaynak Verileri Başka Bir Yerde Çoğaltma {#duplicating-source-data-somewhere-else}

Genellikle Clickhouse'a alınan veriler, aşağıdaki gibi bir tür kalıcı sıra yoluyla teslim edilir [Apache Kafka](https://kafka.apache.org). Bu durumda, Clickhouse'a yazılırken aynı veri akışını okuyacak ve bir yerde soğuk depoda depolayacak ek bir abone kümesi yapılandırmak mümkündür. Çoğu şirket zaten bir nesne deposu veya dağıtılmış bir dosya sistemi gibi olabilecek bazı varsayılan önerilen soğuk depolamaya sahiptir [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## Dosya Sistemi Anlık Görüntüleri {#filesystem-snapshots}

Bazı yerel dosya sistemleri anlık görüntü işlevselliği sağlar (örneğin, [ZFS](https://en.wikipedia.org/wiki/ZFS)), ancak canlı sorguları sunmak için en iyi seçenek olmayabilir. Olası bir çözüm, bu tür dosya sistemi ile ek kopyalar oluşturmak ve bunları [Dağılı](../engines/table-engines/special/distributed.md) için kullanılan tablolar `SELECT` sorgular. Bu tür yinelemelerdeki anlık görüntüler, verileri değiştiren sorguların erişemeyeceği bir yerde olacaktır. Bonus olarak, bu yinelemeler, sunucu başına daha fazla disk eklenmiş özel donanım yapılandırmalarına sahip olabilir ve bu da uygun maliyetli olabilir.

## clickhouse-fotokopi makinesi {#clickhouse-copier}

[clickhouse-fotokopi makinesi](utilities/clickhouse-copier.md) başlangıçta yeniden shard petabyte boyutlu tablolar için oluşturulan çok yönlü bir araçtır. Ayrıca yedekleme için kullanılan ve güvenilir clickhouse tablolar ve kümeler arasında veri kopyalar çünkü amaçlar geri olabilir.

Daha küçük veri hacimleri için, basit bir `INSERT INTO ... SELECT ...` uzak tablolara da çalışabilir.

## Parçalar ile manipülasyonlar {#manipulations-with-parts}

ClickHouse kullanarak sağlar `ALTER TABLE ... FREEZE PARTITION ...` tablo bölümleri yerel bir kopyasını oluşturmak için sorgu. Bu hardlinks kullanarak uygulanır `/var/lib/clickhouse/shadow/` klasör, bu yüzden genellikle eski veriler için ekstra disk alanı tüketmez. Oluşturulan dosyaların kopyaları ClickHouse server tarafından işlenmez, bu yüzden onları orada bırakabilirsiniz: herhangi bir ek harici sistem gerektirmeyen basit bir yedeklemeniz olacak, ancak yine de donanım sorunlarına eğilimli olacaktır. Bu nedenle, bunları uzaktan başka bir konuma kopyalamak ve ardından yerel kopyaları kaldırmak daha iyidir. Dağıtılmış dosya sistemleri ve nesne depoları bunun için hala iyi bir seçenektir, ancak yeterince büyük kapasiteye sahip normal ekli dosya sunucuları da işe yarayabilir (bu durumda aktarım ağ dosya sistemi veya belki de [rsync](https://en.wikipedia.org/wiki/Rsync)).

Bölüm işlemleriyle ilgili sorgular hakkında daha fazla bilgi için bkz. [ALTER belgeleri](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

Bu yaklaşımı otomatikleştirmek için üçüncü taraf bir araç kullanılabilir: [clickhouse-yedekleme](https://github.com/AlexAkulov/clickhouse-backup).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
