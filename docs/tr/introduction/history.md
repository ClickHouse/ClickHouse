---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 7
toc_title: Tarih
---

# ClickHouse Geçmişi {#clickhouse-history}

ClickHouse güç başlangıçta geliştirilmiştir [Üye.Metrica](https://metrica.yandex.com/), [dünyanın en büyük ikinci web analiz platformu](http://w3techs.com/technologies/overview/traffic_analysis/all) ve bu sistemin temel bileşeni olmaya devam ediyor. Veritabanında 13 trilyondan fazla kayıt ve günlük 20 milyardan fazla etkinlik ile ClickHouse, doğrudan toplanmamış verilerden anında özel raporlar oluşturmanıza olanak tanır. Bu makale Kısaca Clickhouse'un gelişiminin ilk aşamalarında hedeflerini kapsamaktadır.

Üye.Metrica kullanıcı tarafından tanımlanan keyfi kesimleri ile, hit ve oturumları dayalı anında özelleştirilmiş raporlar oluşturur. Bunu sık sık yapmak, benzersiz kullanıcı sayısı gibi karmaşık agregalar oluşturmayı gerektirir. Bir rapor oluşturmak için yeni veriler gerçek zamanlı olarak gelir.

Nisan 2014 itibariyle, Yandex.Metrica, günlük olarak yaklaşık 12 milyar olayı (sayfa görüntüleme ve tıklama) izliyordu. Tüm bu olaylar özel raporlar oluşturmak için saklanmalıdır. Tek bir sorgu, birkaç yüz milisaniye içinde milyonlarca satırı veya sadece birkaç saniye içinde yüz milyonlarca satırı taramayı gerektirebilir.

## Yandex kullanımı.Metrica ve diğer Yandex Hizmetleri {#usage-in-yandex-metrica-and-other-yandex-services}

ClickHouse, Yandex'te birden fazla amaca hizmet eder.Metrica.
Ana görevi, toplanmamış verileri kullanarak çevrimiçi modda raporlar oluşturmaktır. Veritabanında 20.3 trilyon satırdan fazla depolayan 374 sunucu kümesi kullanır. Sıkıştırılmış verilerin hacmi, yinelenenleri ve kopyaları hesaba katmadan yaklaşık 2 PB'DİR. Sıkıştırılmamış verilerin hacmi (TSV formatında) Yaklaşık 17 PB olacaktır.

ClickHouse ayrıca aşağıdaki süreçlerde önemli bir rol oynar:

-   Yandex'den oturum tekrarı için veri saklama.Metrica.
-   Ara veri işleme.
-   Analitik ile küresel raporlar oluşturma.
-   Yandex hata ayıklama için sorguları çalıştırma.Metrica motoru.
-   API ve kullanıcı arayüzü günlükleri analiz.

Günümüzde, diğer Yandex hizmetlerinde ve bölümlerinde birden fazla düzine ClickHouse kurulumu bulunmaktadır: arama dikey, e-ticaret, reklam, iş analitiği, mobil geliştirme, kişisel hizmetler ve diğerleri.

## Toplanmış ve toplanmamış veriler {#aggregated-and-non-aggregated-data}

İstatistikleri etkili bir şekilde hesaplamak için, veri hacmini azalttığından verileri toplamanız gerektiğine dair yaygın bir görüş vardır.

Ancak veri toplama birçok sınırlama ile birlikte gelir:

-   Gerekli raporların önceden tanımlanmış bir listesine sahip olmanız gerekir.
-   Kullanıcı özel raporlar yapamaz.
-   Çok sayıda farklı anahtar üzerinde toplanırken, veri hacmi zorlukla azaltılır, bu nedenle toplama işe yaramaz.
-   Çok sayıda rapor için çok fazla toplama varyasyonu vardır (kombinatoryal patlama).
-   Anahtarları yüksek önemlilik (URL'ler gibi) ile toplarken, veri hacmi çok fazla azaltılmaz (iki kattan daha az).
-   Bu nedenle, toplama ile veri hacmi küçültmek yerine büyüyebilir.
-   Kullanıcılar onlar için oluşturduğumuz tüm raporları görüntülemez. Bu hesaplamaların büyük bir kısmı işe yaramaz.
-   Verilerin mantıksal bütünlüğü, çeşitli toplamalar için ihlal edilebilir.

Hiçbir şeyi toplamazsak ve toplanmamış verilerle çalışırsak, bu hesaplamaların hacmini azaltabilir.

Bununla birlikte, toplama ile, çalışmanın önemli bir kısmı çevrimdışı olarak alınır ve nispeten sakin bir şekilde tamamlanır. Buna karşılık, çevrimiçi hesaplamalar, kullanıcı sonucu beklediğinden mümkün olduğunca hızlı hesaplamayı gerektirir.

Üye.Metrica, raporların çoğunluğu için kullanılan Metrage adı verilen verileri toplamak için özel bir sisteme sahiptir.
2009'dan itibaren Yandex.Metrica, daha önce Rapor Oluşturucusu için kullanılan OLAPServer adlı toplanmamış veriler için özel bir OLAP veritabanı da kullandı.
OLAPServer, toplanmamış veriler için iyi çalıştı, ancak tüm raporlar için istenildiği gibi kullanılmasına izin vermeyen birçok kısıtlamaya sahipti. Bunlar, veri türleri için destek eksikliği (yalnızca sayılar) ve verileri gerçek zamanlı olarak aşamalı olarak güncelleyememe (yalnızca verileri günlük olarak yeniden yazarak yapılabilir) içeriyordu. OLAPServer bir DBMS değil, özel bir dB'dir.

ClickHouse için ilk hedef OLAPServer sınırlamaları kaldırmak ve tüm raporlar için toplanmamış verilerle çalışma sorunu çözmek oldu, ama yıllar içinde, analitik görevler geniş bir yelpazede için uygun bir genel amaçlı veritabanı yönetim sistemi haline gelmiştir.

[Orijinal makale](https://clickhouse.tech/docs/en/introduction/history/) <!--hide-->
