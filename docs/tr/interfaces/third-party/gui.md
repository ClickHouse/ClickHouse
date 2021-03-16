---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 28
toc_title: "G\xF6rsel Aray\xFCzler"
---

# Üçüncü taraf geliştiricilerin görsel arayüzleri {#visual-interfaces-from-third-party-developers}

## Açık Kaynak {#open-source}

### Tabix {#tabix}

ClickHouse için web arayüzü [Tabix](https://github.com/tabixio/tabix) projelendirmek.

Özellikler:

-   Ek yazılım yüklemeye gerek kalmadan doğrudan tarayıcıdan ClickHouse ile çalışır.
-   Sözdizimi vurgulama ile sorgu editörü.
-   Komutların otomatik tamamlanması.
-   Sorgu yürütme grafik analizi için araçlar.
-   Renk düzeni seçenekleri.

[Tabix belgeleri](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) OSX, Linux ve Windows için bir UI / IDE.

Özellikler:

-   Sözdizimi vurgulama ile sorgu oluşturucu. Yanıtı bir tablo veya JSON görünümünde görüntüleyin.
-   CSV veya JSON olarak ihracat sorgu sonuçları.
-   Açıklamaları ile süreçlerin listesi. Yazma modu. Durdurmak için yeteneği (`KILL`) işleyiş.
-   Veritabanı grafiği. Tüm tabloları ve sütunlarını ek bilgilerle gösterir.
-   Sütun boyutunun hızlı bir görünümü.
-   Sunucu yapılandırması.

Aşağıdaki özellikler geliştirme için planlanmıştır:

-   Veritabanı yönetimi.
-   Kullanıcı yönetimi.
-   Gerçek zamanlı veri analizi.
-   Küme izleme.
-   Küme yönetimi.
-   Çoğaltılmış ve Kafka tablolarının izlenmesi.

### Fener {#lighthouse}

[Fener](https://github.com/VKCOM/lighthouse) ClickHouse için hafif bir web arayüzüdür.

Özellikler:

-   Filtreleme ve meta veriler içeren tablo listesi.
-   Filtreleme ve sıralama ile tablo önizleme.
-   Salt okunur sorgu yürütme.

### Redash {#redash}

[Redash](https://github.com/getredash/redash) veri görselleştirme için bir platformdur.

ClickHouse dahil olmak üzere birden fazla veri kaynağı için destekler, Redash bir son veri kümesi içine farklı veri kaynaklarından gelen sorguların sonuçlarını katılabilir.

Özellikler:

-   Sorguların güçlü editörü.
-   Veritabanı Gezgini.
-   Verileri farklı formlarda temsil etmenize izin veren görselleştirme araçları.

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - ClickHouse desteği ile evrensel masaüstü veritabanı istemcisi.

Özellikler:

-   Sözdizimi vurgulama ve otomatik tamamlama ile sorgu geliştirme.
-   Filtreler ve meta arama ile tablo listesi.
-   Tablo veri önizleme.
-   Tam metin arama.

### clickhouse-clı {#clickhouse-cli}

[clickhouse-clı](https://github.com/hatarist/clickhouse-cli) Python 3 ile yazılmış ClickHouse için alternatif bir komut satırı istemcisidir.

Özellikler:

-   Otomatik tamamlama.
-   Sorgular ve veri çıkışı için sözdizimi vurgulama.
-   Veri çıkışı için çağrı cihazı desteği.
-   Özel PostgreSQL benzeri komutlar.

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) görselleştirmek için özel bir araçtır `system.trace_log` olarak [flamegraph](http://www.brendangregg.com/flamegraphs.html).

### clickhouse-plantuml {#clickhouse-plantuml}

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) oluşturmak için bir komut dosyası mı [PlantUML](https://plantuml.com/) tablo şemalarının diyagramı.

## Ticari {#commercial}

### Datriagrpip {#datagrip}

[Datriagrpip](https://www.jetbrains.com/datagrip/) ClickHouse için özel destek ile JetBrains bir veritabanı IDE mi. PyCharm, IntelliJ IDEA, GoLand, PhpStorm ve diğerleri: aynı zamanda diğer IntelliJ tabanlı araçlar gömülüdür.

Özellikler:

-   Çok hızlı kod tamamlama.
-   ClickHouse sözdizimi vurgulama.
-   Clickhouse'a özgü özellikler için destek, örneğin iç içe geçmiş sütunlar, tablo motorları.
-   Veri Editörü.
-   Refactorings.
-   Arama ve navigasyon.

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens) veri görselleştirme ve analitik bir hizmettir.

Özellikler:

-   Basit çubuk grafiklerden karmaşık panolara kadar geniş bir yelpazede mevcut görselleştirmeler.
-   Panolar kamuya açık hale getirilebilir.
-   ClickHouse dahil olmak üzere birden fazla veri kaynağı için destek.
-   ClickHouse dayalı hayata veri depolama.

DataLens olduğunu [ücretsiz olarak kullanılabilir](https://cloud.yandex.com/docs/datalens/pricing) düşük yük projeleri için, ticari kullanım için bile.

-   [DataLens belgeleri](https://cloud.yandex.com/docs/datalens/).
-   [Öğretici](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) bir ClickHouse veritabanından veri görselleştirme üzerinde.

### Holistik Yazılım {#holistics-software}

[Holistik](https://www.holistics.io/) tam yığın veri platformu ve iş zekası aracıdır.

Özellikler:

-   Otomatik e-posta, bolluk ve raporların Google levha programları.
-   Görselleştirmeler, sürüm kontrolü, Otomatik tamamlama, yeniden kullanılabilir sorgu bileşenleri ve dinamik filtreler ile SQL editörü.
-   IFRAME aracılığıyla raporların ve gösterge panellerinin gömülü analitiği.
-   Veri hazırlama ve ETL yetenekleri.
-   Verilerin ilişkisel haritalama için SQL veri modelleme desteği.

### Looker {#looker}

[Looker](https://looker.com) ClickHouse dahil 50+ veritabanı lehçeleri desteği ile bir veri platformu ve iş zekası aracıdır. Looker bir SaaS platformu olarak kullanılabilir ve kendi kendine barındırılan. Kullanıcılar, verileri keşfetmek görselleştirme ve panoları, zamanlama raporları oluşturmak ve meslektaşları ile kendi görüşlerini paylaşmak için tarayıcı üzerinden Looker kullanabilirsiniz. Looker, bu özellikleri diğer uygulamalara gömmek için zengin bir araç seti ve bir API sağlar
verileri diğer uygulamalarla entegre etmek.

Özellikler:

-   Küratörlüğünü destekleyen bir dil olan LookML kullanarak kolay ve çevik geliştirme
    [Veri Modelleme](https://looker.com/platform/data-modeling) rapor yazarları ve son kullanıcıları desteklemek.
-   Looker ile güçlü iş akışı entegrasyonu [Veri İşlemleri](https://looker.com/platform/actions).

[Looker içinde ClickHouse nasıl yapılandırılır.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
