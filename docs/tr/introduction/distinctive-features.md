---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 4
toc_title: "Ay\u0131rt Edici \xD6zellikler"
---

# Clickhouse'un ayırt edici özellikleri {#distinctive-features-of-clickhouse}

## Doğru sütun yönelimli DBMS {#true-column-oriented-dbms}

Bir gerçek sütun yönelimli DBMS, hiçbir ek veri değerleri ile depolanır. Diğer şeylerin yanı sıra, bu, uzunluklarının saklanmasını önlemek için sabit uzunluk değerlerinin desteklenmesi gerektiği anlamına gelir “number” değerlerin yanında. Örnek olarak, bir milyar Uİnt8 tipi değerler yaklaşık 1 GB sıkıştırılmamış tüketmelidir veya bu CPU kullanımını güçlü bir şekilde etkiler. Verileri kompakt bir şekilde saklamak esastır (herhangi bir “garbage”) sıkıştırılmamış olsa bile, dekompresyon hızı (CPU kullanımı) esas olarak sıkıştırılmamış verilerin hacmine bağlıdır.

Farklı sütunların değerlerini ayrı ayrı depolayabilen, ancak diğer senaryolar için optimizasyonları nedeniyle analitik sorguları etkili bir şekilde işleyemeyen sistemler olduğu için dikkat çekicidir. Örnekler HBase, BigTable, Cassandra ve HyperTable. Bu sistemlerde, saniyede yüz bin satır civarında verim elde edersiniz, ancak saniyede yüz milyonlarca satır olmaz.

Clickhouse'un tek bir veritabanı değil, bir veritabanı yönetim sistemi olduğunu da belirtmek gerekir. ClickHouse, çalışma zamanında tablolar ve veritabanları oluşturmak, veri yüklemek ve sunucuyu yeniden yapılandırmadan ve yeniden başlatmadan sorguları çalıştırmaya izin verir.

## Veri Sıkıştırma {#data-compression}

Bazı sütun yönelimli DBMSs (InfiniDB CE ve MonetDB) veri sıkıştırma kullanmayın. Bununla birlikte, veri sıkıştırma mükemmel performans elde etmede önemli bir rol oynar.

## Verilerin Disk Depolama {#disk-storage-of-data}

Verileri fiziksel olarak birincil anahtara göre sıralamak, belirli değerleri veya değer aralıkları için düşük gecikme süresi ile birkaç düzine milisaniyeden daha az veri ayıklamayı mümkün kılar. Bazı sütun yönelimli Dbms'ler (SAP HANA ve Google PowerDrill gibi) yalnızca RAM'de çalışabilir. Bu yaklaşım, gerçek zamanlı analiz için gerekenden daha büyük bir donanım bütçesinin tahsisini teşvik eder. ClickHouse düzenli sabit diskler üzerinde çalışmak üzere tasarlanmıştır, bu da GB veri depolama başına maliyetin düşük olduğu anlamına gelir, ancak varsa SSD ve ek RAM de tamamen kullanılır.

## Birden fazla çekirdekte paralel işleme {#parallel-processing-on-multiple-cores}

Büyük sorgular, geçerli sunucuda bulunan tüm gerekli kaynakları alarak doğal olarak paralelleştirilir.

## Birden çok sunucuda dağıtılmış işleme {#distributed-processing-on-multiple-servers}

Yukarıda belirtilen sütunlu Dbms'lerin neredeyse hiçbiri dağıtılmış sorgu işleme desteğine sahip değildir.
Clickhouse'da, veriler farklı parçalarda bulunabilir. Her parça, hata toleransı için kullanılan bir grup kopya olabilir. Tüm kırıklar, kullanıcı için şeffaf olarak paralel bir sorgu çalıştırmak için kullanılır.

## SQL desteği {#sql-support}

ClickHouse, çoğu durumda SQL standardına özdeş olan sql'i temel alan bildirime dayalı bir sorgu dilini destekler.
Desteklenen sorgular arasında GROUP BY, ORDER BY, from, ın ve JOIN yan tümceleri ve skaler alt sorgular bulunur.
Bağımlı alt sorgular ve pencere işlevleri desteklenmez.

## Vektör Motoru {#vector-engine}

Veriler yalnızca sütunlar tarafından saklanmakla kalmaz, aynı zamanda yüksek CPU verimliliği elde etmeyi sağlayan vektörler (sütunların parçaları) tarafından işlenir.

## Gerçek zamanlı veri güncellemeleri {#real-time-data-updates}

ClickHouse, birincil anahtarlı tabloları destekler. Birincil anahtar aralığındaki sorguları hızlı bir şekilde gerçekleştirmek için, veriler birleştirme ağacını kullanarak aşamalı olarak sıralanır. Bu nedenle, veriler sürekli olarak tabloya eklenebilir. Yeni veri Yutulduğunda hiçbir kilit alınır.

## Dizin {#index}

Birincil anahtara göre fiziksel olarak sıralanmış bir veriye sahip olmak, belirli değerleri veya değer aralıkları için düşük gecikme süresi ile birkaç düzine milisaniyeden daha az veri çıkarmayı mümkün kılar.

## Çevrimiçi sorgular için uygundur {#suitable-for-online-queries}

Düşük gecikme süresi, kullanıcı arayüzü sayfası yüklenirken, sorguların gecikmeden ve önceden bir cevap hazırlamaya çalışmadan işlenebileceği anlamına gelir. Başka bir deyişle, çevrimiçi.

## Yaklaşık hesaplamalar için destek {#support-for-approximated-calculations}

ClickHouse performans için doğruluk ticaret için çeşitli yollar sağlar:

1.  Farklı değerler, medyan ve quantiles sayısı yaklaşık hesaplama için toplam işlevleri.
2.  Verilerin bir bölümünü (örnek) temel alan bir sorguyu çalıştırmak ve yaklaşık bir sonuç almak. Bu durumda, diskten orantılı olarak daha az veri alınır.
3.  Tüm anahtarlar yerine, sınırlı sayıda rastgele anahtar için bir toplama çalıştırma. Verilerde anahtar dağıtımı için belirli koşullar altında, bu daha az kaynak kullanırken makul derecede doğru bir sonuç sağlar.

## Veri çoğaltma ve Veri Bütünlüğü desteği {#data-replication-and-data-integrity-support}

ClickHouse zaman uyumsuz çoklu ana çoğaltma kullanır. Kullanılabilir herhangi bir yineleme için yazıldıktan sonra kalan tüm yinelemeler arka planda kendi kopyasını almak. Sistem, farklı yinelemelerde aynı verileri korur. Çoğu arızadan sonra kurtarma, karmaşık durumlarda otomatik olarak veya yarı otomatik olarak gerçekleştirilir.

Daha fazla bilgi için bölüme bakın [Veri çoğaltma](../engines/table-engines/mergetree-family/replication.md).

## Dezavantajları olarak kabul edilebilir özellikler {#clickhouse-features-that-can-be-considered-disadvantages}

1.  Tam teşekküllü işlemler yok.
2.  Yüksek oranda ve düşük gecikme ile zaten eklenen verileri değiştirme veya silme yeteneği eksikliği. Verileri temizlemek veya değiştirmek için toplu silme ve güncellemeler vardır, örneğin Aşağıdakilere uymak için [GDPR](https://gdpr-info.eu).
3.  Seyrek dizin, Clickhouse'u anahtarlarıyla tek satırları almak için nokta sorguları için çok uygun değildir.

[Orijinal makale](https://clickhouse.tech/docs/en/introduction/distinctive_features/) <!--hide-->
