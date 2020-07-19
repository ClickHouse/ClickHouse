---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 12
toc_title: "\xD6\u011Fretici"
---

# ClickHouse Eğitimi {#clickhouse-tutorial}

## Bu Öğreticiden ne beklenir? {#what-to-expect-from-this-tutorial}

Bu öğreticiden geçerek, basit bir ClickHouse kümesinin nasıl kurulacağını öğreneceksiniz. Küçük ama hataya dayanıklı ve ölçeklenebilir olacak. Ardından, verilerle doldurmak ve bazı demo sorguları yürütmek için örnek veri kümelerinden birini kullanacağız.

## Tek Düğüm Kurulumu {#single-node-setup}

Dağıtılmış bir ortamın karmaşıklığını ertelemek için, Clickhouse'u tek bir sunucu veya sanal makinede dağıtmaya başlayacağız. ClickHouse genellikle [deb](install.md#install-from-deb-packages) veya [rpm](install.md#from-rpm-packages) paketler, ama var [alternatifler](install.md#from-docker-image) onları desteklemeyen işletim sistemleri için.

Örneğin, seçtiğiniz `deb` paketler ve yürütülen:

``` bash
{% include 'install/deb.sh' %}
```

Yüklü olan paketlerde ne var:

-   `clickhouse-client` paket içerir [clickhouse-müşteri](../interfaces/cli.md) uygulama, interaktif ClickHouse konsol istemcisi.
-   `clickhouse-common` paket clickhouse yürütülebilir dosya içerir.
-   `clickhouse-server` paket bir sunucu olarak ClickHouse çalıştırmak için yapılandırma dosyalarını içerir.

Sunucu yapılandırma dosyaları bulunur `/etc/clickhouse-server/`. Daha fazla gitmeden önce, fark lütfen `<path>` element in `config.xml`. Path, veri depolama için konumu belirler, bu nedenle büyük disk kapasitesine sahip birimde bulunmalıdır; varsayılan değer `/var/lib/clickhouse/`. Yapılandırmayı ayarlamak istiyorsanız, doğrudan düzenlemek kullanışlı değildir `config.xml` dosya, gelecekteki paket güncellemelerinde yeniden yazılabileceğini düşünüyor. Yapılandırma öğelerini geçersiz kılmak için önerilen yol oluşturmaktır [config dosyaları.d dizin](../operations/configuration-files.md) hizmet olarak “patches” config için.xml.

Fark etmiş olabileceğiniz gibi, `clickhouse-server` paket kurulumdan sonra otomatik olarak başlatılmaz. Güncellemelerden sonra otomatik olarak yeniden başlatılmaz. Sunucuyu başlatma şekliniz init sisteminize bağlıdır, genellikle:

``` bash
sudo service clickhouse-server start
```

veya

``` bash
sudo /etc/init.d/clickhouse-server start
```

Sunucu günlükleri için varsayılan konum `/var/log/clickhouse-server/`. Sunucu, oturum açtıktan sonra istemci bağlantılarını işlemeye hazırdır. `Ready for connections` ileti.

Bir kez `clickhouse-server` yukarı ve çalışıyor, biz kullanabilirsiniz `clickhouse-client` sunucuya bağlanmak ve aşağıdaki gibi bazı test sorguları çalıştırmak için `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Clickhouse-client için hızlı ipuçları</summary>

İnteraktif mod:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Çok satırlı sorguları etkinleştirme:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Toplu iş modunda sorguları çalıştırma:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Belirtilen biçimde bir dosyadan veri ekleme:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Örnek Veri Kümesini İçe Aktar {#import-sample-dataset}

Şimdi ClickHouse sunucumuzu bazı örnek verilerle doldurmanın zamanı geldi. Bu eğitimde, yandex'in anonim verilerini kullanacağız.Metrica, açık kaynak olmadan önce Clickhouse'u üretim yolunda çalıştıran ilk hizmet (daha fazlası [tarih bölümü](../introduction/history.md)). Var [Yandex'i içe aktarmanın birden fazla yolu.Metrica veri kümesi](example-datasets/metrica.md) ve öğretici uğruna, en gerçekçi olanı ile gideceğiz.

### Tablo verilerini indirin ve ayıklayın {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

Çıkarılan dosyalar yaklaşık 10GB boyutundadır.

### Tablo Oluşturma {#create-tables}

Çoğu veritabanı yönetim sisteminde olduğu gibi, ClickHouse tabloları mantıksal olarak gruplar “databases”. Bir `default` veritabanı, ancak adında yeni bir tane oluşturacağız `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

Tablolar oluşturmak için sözdizimi veritabanlarına kıyasla çok daha karmaşıktır (bkz. [başvurma](../sql-reference/statements/create.md). Genelde `CREATE TABLE` deyimi üç önemli şeyi belirtmek zorundadır:

1.  Oluşturulacak tablonun adı.
2.  Table schema, i.e. list of columns and their [veri türleri](../sql-reference/data-types/index.md).
3.  [Masa motoru](../engines/table-engines/index.md) ve bu tabloya yapılan sorguların fiziksel olarak nasıl yürütüleceği ile ilgili tüm ayrıntıları belirleyen ayarları.

Üye.Metrica bir web analitiği hizmetidir ve örnek veri kümesi tam işlevselliğini kapsamaz, bu nedenle oluşturmak için sadece iki tablo vardır:

-   `hits` hizmet kapsamındaki tüm web sitelerinde tüm kullanıcılar tarafından yapılan her eylem ile bir tablodur.
-   `visits` bireysel eylemler yerine önceden oluşturulmuş oturumlar içeren bir tablodur.

Bu tablolar için gerçek create table sorgularını görelim ve yürütelim:

``` sql
CREATE TABLE tutorial.hits_v1
(
    `WatchID` UInt64,
    `JavaEnable` UInt8,
    `Title` String,
    `GoodEvent` Int16,
    `EventTime` DateTime,
    `EventDate` Date,
    `CounterID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RegionID` UInt32,
    `UserID` UInt64,
    `CounterClass` Int8,
    `OS` UInt8,
    `UserAgent` UInt8,
    `URL` String,
    `Referer` String,
    `URLDomain` String,
    `RefererDomain` String,
    `Refresh` UInt8,
    `IsRobot` UInt8,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `FlashMinor2` String,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` FixedString(2),
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `MobilePhone` UInt8,
    `MobilePhoneModel` String,
    `Params` String,
    `IPNetworkID` UInt32,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `IsArtifical` UInt8,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `ClientTimeZone` Int16,
    `ClientEventTime` DateTime,
    `SilverlightVersion1` UInt8,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion3` UInt32,
    `SilverlightVersion4` UInt16,
    `PageCharset` String,
    `CodeVersion` UInt32,
    `IsLink` UInt8,
    `IsDownload` UInt8,
    `IsNotBounce` UInt8,
    `FUniqID` UInt64,
    `HID` UInt32,
    `IsOldCounter` UInt8,
    `IsEvent` UInt8,
    `IsParameter` UInt8,
    `DontCountHits` UInt8,
    `WithHash` UInt8,
    `HitColor` FixedString(1),
    `UTCEventTime` DateTime,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `WindowName` Int32,
    `OpenerName` Int32,
    `HistoryLength` Int16,
    `BrowserLanguage` FixedString(2),
    `BrowserCountry` FixedString(2),
    `SocialNetwork` String,
    `SocialAction` String,
    `HTTPError` UInt16,
    `SendTiming` Int32,
    `DNSTiming` Int32,
    `ConnectTiming` Int32,
    `ResponseStartTiming` Int32,
    `ResponseEndTiming` Int32,
    `FetchTiming` Int32,
    `RedirectTiming` Int32,
    `DOMInteractiveTiming` Int32,
    `DOMContentLoadedTiming` Int32,
    `DOMCompleteTiming` Int32,
    `LoadEventStartTiming` Int32,
    `LoadEventEndTiming` Int32,
    `NSToDOMContentLoadedTiming` Int32,
    `FirstPaintTiming` Int32,
    `RedirectCount` Int8,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `ParamPrice` Int64,
    `ParamOrderID` String,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `GoalsReached` Array(UInt32),
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `RefererHash` UInt64,
    `URLHash` UInt64,
    `CLID` UInt32,
    `YCLID` UInt64,
    `ShareService` String,
    `ShareURL` String,
    `ShareTitle` String,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `IslandID` FixedString(16),
    `RequestNum` UInt32,
    `RequestTry` UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

``` sql
CREATE TABLE tutorial.visits_v1
(
    `CounterID` UInt32,
    `StartDate` Date,
    `Sign` Int8,
    `IsNew` UInt8,
    `VisitID` UInt64,
    `UserID` UInt64,
    `StartTime` DateTime,
    `Duration` UInt32,
    `UTCStartTime` DateTime,
    `PageViews` Int32,
    `Hits` Int32,
    `IsBounce` UInt8,
    `Referer` String,
    `StartURL` String,
    `RefererDomain` String,
    `StartURLDomain` String,
    `EndURL` String,
    `LinkURL` String,
    `IsDownload` UInt8,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `PlaceID` Int32,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `IsYandex` UInt8,
    `GoalReachesDepth` Int32,
    `GoalReachesURL` Int32,
    `GoalReachesAny` Int32,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `MobilePhoneModel` String,
    `ClientEventTime` DateTime,
    `RegionID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `IPNetworkID` UInt32,
    `SilverlightVersion3` UInt32,
    `CodeVersion` UInt32,
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` UInt16,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion4` UInt16,
    `FlashVersion3` UInt16,
    `FlashVersion4` UInt16,
    `ClientTimeZone` Int16,
    `OS` UInt8,
    `UserAgent` UInt8,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `MobilePhone` UInt8,
    `SilverlightVersion1` UInt8,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `JavaEnable` UInt8,
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `BrowserLanguage` UInt16,
    `BrowserCountry` UInt16,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `Params` Array(String),
    `Goals` Nested(
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32),
    `WatchIDs` Array(UInt64),
    `ParamSumPrice` Int64,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `ClickLogID` UInt64,
    `ClickEventID` Int32,
    `ClickGoodEvent` Int32,
    `ClickEventTime` DateTime,
    `ClickPriorityID` Int32,
    `ClickPhraseID` Int32,
    `ClickPageID` Int32,
    `ClickPlaceID` Int32,
    `ClickTypeID` Int32,
    `ClickResourceID` Int32,
    `ClickCost` UInt32,
    `ClickClientIP` UInt32,
    `ClickDomainID` UInt32,
    `ClickURL` String,
    `ClickAttempt` UInt8,
    `ClickOrderID` UInt32,
    `ClickBannerID` UInt32,
    `ClickMarketCategoryID` UInt32,
    `ClickMarketPP` UInt32,
    `ClickMarketCategoryName` String,
    `ClickMarketPPName` String,
    `ClickAWAPSCampaignName` String,
    `ClickPageName` String,
    `ClickTargetType` UInt16,
    `ClickTargetPhraseID` UInt64,
    `ClickContextType` UInt8,
    `ClickSelectType` Int8,
    `ClickOptions` String,
    `ClickGroupBannerID` Int32,
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `FirstVisit` DateTime,
    `PredLastVisit` Date,
    `LastVisit` Date,
    `TotalVisits` UInt32,
    `TraficSource` Nested(
        ID Int8,
        SearchEngineID UInt16,
        AdvEngineID UInt8,
        PlaceID UInt16,
        SocialSourceNetworkID UInt8,
        Domain String,
        SearchPhrase String,
        SocialSourcePage String),
    `Attendance` FixedString(16),
    `CLID` UInt32,
    `YCLID` UInt64,
    `NormalizedRefererHash` UInt64,
    `SearchPhraseHash` UInt64,
    `RefererDomainHash` UInt64,
    `NormalizedStartURLHash` UInt64,
    `StartURLDomainHash` UInt64,
    `NormalizedEndURLHash` UInt64,
    `TopLevelDomain` UInt64,
    `URLScheme` UInt64,
    `OpenstatServiceNameHash` UInt64,
    `OpenstatCampaignIDHash` UInt64,
    `OpenstatAdIDHash` UInt64,
    `OpenstatSourceIDHash` UInt64,
    `UTMSourceHash` UInt64,
    `UTMMediumHash` UInt64,
    `UTMCampaignHash` UInt64,
    `UTMContentHash` UInt64,
    `UTMTermHash` UInt64,
    `FromHash` UInt64,
    `WebVisorEnabled` UInt8,
    `WebVisorActivity` UInt32,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `Market` Nested(
        Type UInt8,
        GoalID UInt32,
        OrderID String,
        OrderPrice Int64,
        PP UInt32,
        DirectPlaceID UInt32,
        DirectOrderID UInt32,
        DirectBannerID UInt32,
        GoodID String,
        GoodName String,
        GoodQuantity Int32,
        GoodPrice Int64),
    `IslandID` FixedString(16)
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

Etkileşimli modunu kullanarak bu sorguları yürütebilirsiniz `clickhouse-client` (sadece önceden bir sorgu belirtmeden bir terminalde başlatın) veya bazı deneyin [alternatif arayüz](../interfaces/index.md) Eğer isterseniz.

Gördüğümüz gibi, `hits_v1` kullanır [temel MergeTree motoru](../engines/table-engines/mergetree-family/mergetree.md), WH whileile the `visits_v1` kullanır [Çökme](../engines/table-engines/mergetree-family/collapsingmergetree.md) varyant.

### Verileri İçe Aktar {#import-data}

Clickhouse'a veri aktarımı yapılır [INSERT INTO](../sql-reference/statements/insert-into.md) diğer birçok SQL veritabanlarında olduğu gibi sorgu. Bununla birlikte, veriler genellikle [desteklenen seri hale getirme biçimleri](../interfaces/formats.md) yerine `VALUES` fıkra clausesı (ayrıca desteklenmektedir).

Onları almak için ne kadar daha önce indirdiğimiz dosyaları sekme ayrılmış biçimde, yani burada konsol istemci ile :

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse bir yeri vardır [ayarlan settingsacak ayarlar](../operations/settings/index.md) ve bunları konsol istemcisinde belirtmenin bir yolu, görebildiğimiz gibi argümanlar aracılığıyla `--max_insert_block_size`. Hangi ayarların mevcut olduğunu, ne anlama geldiğini ve varsayılanların ne olduğunu anlamanın en kolay yolu `system.settings` Tablo:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

İsteğe bağlı olarak şunları yapabilirsiniz [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) ithalattan sonra tablolar. MergeTree-family'den bir motorla yapılandırılmış tablolar, veri depolamayı en iyi duruma getirmek (veya en azından mantıklı olup olmadığını kontrol etmek) için her zaman arka planda veri parçalarının birleştirilmesini sağlar. Bu sorgular, tablo motorunu bir süre sonra yerine şu anda depolama optimizasyonu yapmaya zorlar:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

Bu sorgular bir G/Ç ve CPU yoğun işlem başlatır, bu nedenle tablo sürekli olarak yeni veriler alırsa, onu yalnız bırakmak ve birleştirmelerin arka planda çalışmasına izin vermek daha iyidir.

Şimdi tablo ithalatının başarılı olup olmadığını kontrol edebiliriz:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Örnek Sorgular {#example-queries}

``` sql
SELECT
    StartURL AS URL,
    AVG(Duration) AS AvgDuration
FROM tutorial.visits_v1
WHERE StartDate BETWEEN '2014-03-23' AND '2014-03-30'
GROUP BY URL
ORDER BY AvgDuration DESC
LIMIT 10
```

``` sql
SELECT
    sum(Sign) AS visits,
    sumIf(Sign, has(Goals.ID, 1105530)) AS goal_visits,
    (100. * goal_visits) / visits AS goal_percent
FROM tutorial.visits_v1
WHERE (CounterID = 912887) AND (toYYYYMM(StartDate) = 201403) AND (domain(StartURL) = 'yandex.ru')
```

## Küme Dağıtımı {#cluster-deployment}

ClickHouse kümesi homojen bir kümedir. Kurulum adımları:

1.  Kümenin tüm makinelerine ClickHouse Server'ı yükleyin
2.  Yapılandırma dosyalarında küme yapılandırmalarını ayarlama
3.  Her örnekte yerel tablolar oluşturun
4.  Create a [Dağıtılmış tablo](../engines/table-engines/special/distributed.md)

[Dağıtılmış tablo](../engines/table-engines/special/distributed.md) aslında bir tür “view” ClickHouse kümesinin yerel tablolarına. Dağıtılmış bir tablodan sorgu seçin, tüm kümenin parçalarının kaynaklarını kullanarak yürütür. Birden çok küme için yapılandırmalar belirtebilir ve farklı kümelere görünümler sağlayan birden çok dağıtılmış tablo oluşturabilirsiniz.

Her biri bir kopya olan üç parçalı bir küme için örnek yapılandırma:

``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

Daha fazla gösteri için, aynı ile yeni bir yerel tablo oluşturalım `CREATE TABLE` için kullandığımız sorgu `hits_v1` ama farklı bir tablo adı:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Kümenin yerel tablolarına bir görünüm sağlayan dağıtılmış bir tablo oluşturma:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

Yaygın bir uygulama, kümenin tüm makinelerinde benzer dağıtılmış tablolar oluşturmaktır. Kümenin herhangi bir makinesinde dağıtılmış sorguları çalıştırmaya izin verir. Ayrıca, belirli bir SELECT sorgusu için geçici dağıtılmış tablo oluşturmak için alternatif bir seçenek vardır [uzak](../sql-reference/table-functions/remote.md) tablo işlevi.

Hadi koşalım [INSERT SELECT](../sql-reference/statements/insert-into.md) tabloyu birden çok sunucuya yaymak için dağıtılmış tabloya.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "Bildirim"
    Bu yaklaşım büyük tabloların parçalanması için uygun değildir. Ayrı bir araç var [clickhouse-fotokopi makinesi](../operations/utilities/clickhouse-copier.md) bu keyfi büyük tabloları yeniden parçalayabilir.

Tahmin edebileceğiniz gibi, hesaplamalı olarak ağır sorgular, bir yerine 3 sunucu kullanıyorsa n kat daha hızlı çalışır.

Bu durumda, 3 parçaya sahip bir küme kullandık ve her biri tek bir kopya içeriyor.

Bir üretim ortamında esneklik sağlamak için, her bir parçanın birden çok kullanılabilirlik bölgesi veya veri merkezleri (veya en azından raflar) arasında yayılmış 2-3 kopya içermesi önerilir. ClickHouse yinelemeler sınırsız sayıda desteklediğini unutmayın.

Üç yineleme içeren bir parça kümesi için örnek yapılandırma:

``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

Yerel çoğaltmayı etkinleştirmek için [ZooKeeper](http://zookeeper.apache.org/) gereklidir. ClickHouse tüm yinelemeler üzerinde veri tutarlılığı ilgilenir ve otomatik olarak hatadan sonra prosedürü geri çalışır. ZooKeeper kümesinin ayrı sunuculara dağıtılması önerilir(ClickHouse dahil başka hiçbir işlem çalışmaz).

!!! note "Not"
    ZooKeeper sıkı bir gereklilik değildir: bazı basit durumlarda, verileri uygulama kodunuzdan tüm kopyalara yazarak çoğaltabilirsiniz. Bu yaklaşım **değil** önerilen, bu durumda, ClickHouse tüm yinelemelerde veri tutarlılığını garanti edemez. Böylece başvurunuzun sorumluluğu haline gelir.

Zookeeper konumları yapılandırma dosyasında belirtilir:

``` xml
<zookeeper>
    <node>
        <host>zoo01.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.yandex.ru</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Ayrıca, tablo oluşturulmasında kullanılan her bir parça ve kopyayı tanımlamak için makrolar ayarlamamız gerekir:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

Yinelenmiş tablo oluşturma şu anda hiçbir yinelemeler varsa, yeni bir ilk yineleme örneği. Zaten canlı yinelemeler varsa, yeni yineleme varolan verileri klonlar. Önce tüm çoğaltılmış tablolar oluşturmak ve sonra veri eklemek için bir seçeneğiniz vardır. Başka bir seçenek, bazı yinelemeler oluşturmak ve veri ekleme sırasında veya sonrasında diğerlerini eklemektir.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Burada kullanıyoruz [ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) masa motoru. Parametrelerde, Shard ve çoğaltma tanımlayıcılarını içeren ZooKeeper yolunu belirtiyoruz.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

Çoğaltma çok ana modda çalışır. Veriler herhangi bir kopyaya yüklenebilir ve sistem daha sonra otomatik olarak diğer örneklerle eşitler. Çoğaltma zaman uyumsuz olduğundan, belirli bir anda, tüm yinelemeler son eklenen verileri içerebilir. Veri alımına izin vermek için en az bir kopya olmalıdır. Diğerleri verileri senkronize eder ve tekrar aktif hale geldiklerinde tutarlılığı onarır. Bu yaklaşımın, yakın zamanda eklenen veri kaybı olasılığının düşük olmasına izin verdiğini unutmayın.

[Orijinal makale](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
