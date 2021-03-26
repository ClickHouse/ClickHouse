---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 12
toc_title: "\u0627\u0645\u0648\u0632\u0634"
---

# اموزش کلیک {#clickhouse-tutorial}

## چه انتظار از این مقاله? {#what-to-expect-from-this-tutorial}

با رفتن را از طریق این مقاله, شما یاد بگیرند که چگونه به راه اندازی یک خوشه ساده تاتر. این کوچک خواهد بود, اما مقاوم در برابر خطا و مقیاس پذیر. سپس ما از یکی از مجموعه داده های نمونه برای پر کردن داده ها و اجرای برخی از نمایش های نسخه ی نمایشی استفاده خواهیم کرد.

## راه اندازی تک گره {#single-node-setup}

برای به تعویق انداختن پیچیدگی های یک محیط توزیع, ما با استقرار کلیک بر روی یک سرور و یا ماشین مجازی شروع. خانه کلیک است که معمولا از نصب [دب](install.md#install-from-deb-packages) یا [دور در دقیقه](install.md#from-rpm-packages) بسته, اما وجود دارد [جایگزین ها](install.md#from-docker-image) برای سیستم عامل هایی که هیچ پشتیبانی نمی کنند.

مثلا, شما را انتخاب کرده اند `deb` بسته ها و اعدام:

``` bash
{% include 'install/deb.sh' %}
```

در بسته هایی که نصب شده اند چه چیزی داریم:

-   `clickhouse-client` بسته شامل [کلیک مشتری](../interfaces/cli.md) کاربرد, تعاملی مشتری کنسول تاتر.
-   `clickhouse-common` بسته شامل یک فایل اجرایی کلیک.
-   `clickhouse-server` بسته شامل فایل های پیکربندی برای اجرای تاتر به عنوان یک سرور.

فایل های پیکربندی سرور در واقع `/etc/clickhouse-server/`. قبل از رفتن بیشتر, لطفا توجه کنید `<path>` عنصر در `config.xml`. مسیر تعیین محل ذخیره سازی داده ها, بنابراین باید در حجم با ظرفیت دیسک بزرگ واقع; مقدار پیش فرض است `/var/lib/clickhouse/`. اگر شما می خواهید برای تنظیم پیکربندی, این دستی به طور مستقیم ویرایش کنید `config.xml` فایل, با توجه به اینکه ممکن است در به روز رسانی بسته های بعدی بازنویسی. راه توصیه می شود به نادیده گرفتن عناصر پیکربندی است که برای ایجاد [فایل ها در پیکربندی.فهرست راهنما](../operations/configuration-files.md) که به عنوان خدمت می کنند “patches” برای پیکربندی..

همانطور که شما ممکن است متوجه, `clickhouse-server` به طور خودکار پس از نصب بسته راه اندازی نشده است. این به طور خودکار پس از به روز رسانی دوباره راه اندازی نخواهد شد. راه شما شروع به سرور بستگی به سیستم اینیت خود را, معمولا, این:

``` bash
sudo service clickhouse-server start
```

یا

``` bash
sudo /etc/init.d/clickhouse-server start
```

محل پیش فرض برای سیاهههای مربوط به سرور است `/var/log/clickhouse-server/`. سرور برای رسیدگی به اتصالات مشتری پس از ورود به سیستم `Ready for connections` پیام

هنگامی که `clickhouse-server` است و در حال اجرا, ما می توانیم با استفاده از `clickhouse-client` برای اتصال به سرور و اجرای برخی از نمایش داده شد تست مانند `SELECT "Hello, world!";`.

<details markdown="1">

<summary>راهنمایی سریع برای کلیک-مشتری</summary>

حالت تعاملی:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

فعالسازی پرسشهای چند خطی:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

نمایش داده شد اجرا در دسته حالت:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

درج داده از یک پرونده در قالب مشخص شده:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## واردات مجموعه داده نمونه {#import-sample-dataset}

در حال حاضر زمان برای پر کردن سرور کلیک ما با برخی از داده های نمونه است. در این مقاله ما از داده های ناشناس یاندکس استفاده خواهیم کرد.متریکا, اولین سرویس اجرا می شود که کلیک در راه تولید قبل از منبع باز شد (بیشتر در که در [بخش تاریخچه](../introduction/history.md)). وجود دارد [راه های متعدد برای وارد کردن یاندکس.مجموعه داده های متریکا](example-datasets/metrica.md), و به خاطر تدریس خصوصی, ما با یکی از واقع بینانه ترین رفتن.

### دانلود و استخراج داده های جدول {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

فایل های استخراج شده حدود 10 گیگابایت است.

### ایجاد جداول {#create-tables}

همانطور که در بسیاری از سیستم های مدیریت پایگاه داده, تاتر منطقی جداول گروه به “databases”. یک `default` پایگاه داده, اما ما یکی از جدید به نام ایجاد `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

نحو برای ایجاد جداول راه پیچیده تر در مقایسه با پایگاه داده است (نگاه کنید به [مرجع](../sql-reference/statements/create.md). به طور کلی `CREATE TABLE` بیانیه باید سه چیز کلیدی را مشخص کند:

1.  نام جدول برای ایجاد.
2.  Table schema, i.e. list of columns and their [انواع داده ها](../sql-reference/data-types/index.md).
3.  [موتور جدول](../engines/table-engines/index.md) و تنظیمات خود را, که تعیین تمام اطلاعات در مورد نحوه نمایش داده شد به این جدول خواهد شد از لحاظ جسمی اجرا.

یاندکسمتریکا یک سرویس تجزیه و تحلیل وب است و مجموعه داده نمونه قابلیت های کامل خود را پوشش نمی دهد بنابراین تنها دو جدول برای ایجاد وجود دارد:

-   `hits` یک جدول با هر عمل انجام شده توسط همه کاربران در تمام وب سایت های تحت پوشش این سرویس است.
-   `visits` یک جدول است که شامل جلسات از پیش ساخته شده به جای اقدامات فردی است.

بیایید ببینید و اجرای واقعی ایجاد نمایش داده شد جدول برای این جداول:

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

شما می توانید این پرسش ها را با استفاده از حالت تعاملی اجرا کنید `clickhouse-client` (فقط در یک ترمینال راه اندازی بدون مشخص کردن یک پرس و جو در پیش) و یا سعی کنید برخی از [رابط جایگزین](../interfaces/index.md) اگر شما می خواهید.

همانطور که می بینیم, `hits_v1` با استفاده از [موتور ادغام عمومی](../engines/table-engines/mergetree-family/mergetree.md) در حالی که `visits_v1` با استفاده از [سقوط](../engines/table-engines/mergetree-family/collapsingmergetree.md) گزینه.

### وارد کردن داده {#import-data}

وارد کردن داده ها به تاتر از طریق انجام می شود [INSERT INTO](../sql-reference/statements/insert-into.md) پرس و جو مانند در بسیاری از پایگاه داده های دیگر گذاشتن. با این حال, داده ها معمولا در یکی از [پشتیبانی از فرمت های ترتیب](../interfaces/formats.md) به جای `VALUES` بند (که همچنین پشتیبانی).

فایل هایی که قبلا دانلود کردیم در قالب تب جدا شده اند بنابراین در اینجا نحوه وارد کردن از طریق مشتری کنسول است:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

تاتر است که بسیاری از [تنظیمات برای تنظیم](../operations/settings/index.md) و یک راه برای مشخص کردن انها در کنسول مشتری از طریق استدلال است همانطور که ما می توانید ببینید با `--max_insert_block_size`. ساده ترین راه برای کشف کردن چه تنظیمات در دسترس هستند, چه معنی می دهند و چه پیش فرض است به پرس و جو `system.settings` جدول:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

در صورت تمایل شما می توانید [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) جداول پس از واردات. جداول است که با یک موتور از ادغام خانواده پیکربندی همیشه ادغام قطعات داده ها در پس زمینه برای بهینه سازی ذخیره سازی داده ها (یا حداقل چک کنید اگر حس می کند). این نمایش داده شد نیروی موتور جدول به انجام بهینه سازی ذخیره سازی در حال حاضر به جای برخی از زمان بعد:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

این نمایش داده شد شروع یک عملیات فشرده من/ای و پردازنده, بنابراین اگر جدول به طور مداوم داده های جدید دریافت, بهتر است به تنهایی ترک و اجازه دهید ادغام در پس زمینه اجرا.

در حال حاضر ما می توانید بررسی کنید اگر واردات جدول موفق بود:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## به عنوان مثال نمایش داده شد {#example-queries}

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

## استقرار خوشه {#cluster-deployment}

خوشه کلیک یک خوشه همگن است. مراحل برای راه اندازی:

1.  نصب سرور کلیک بر روی تمام ماشین های خوشه
2.  تنظیم پیکربندی خوشه در فایل های پیکربندی
3.  ایجاد جداول محلی در هر نمونه
4.  ایجاد یک [جدول توزیع شده](../engines/table-engines/special/distributed.md)

[جدول توزیع شده](../engines/table-engines/special/distributed.md) در واقع یک نوع از “view” به جداول محلی خوشه فاحشه خانه. پرس و جو را انتخاب کنید از یک جدول توزیع اجرا با استفاده از منابع خرده ریز تمام خوشه. شما ممکن است تنظیمات برای خوشه های متعدد مشخص و ایجاد جداول توزیع های متعدد فراهم کردن دیدگاه ها به خوشه های مختلف.

به عنوان مثال پیکربندی برای یک خوشه با سه خرده ریز, یک ماکت هر:

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

برای تظاهرات بیشتر, اجازه دهید یک جدول محلی جدید با همان ایجاد `CREATE TABLE` پرس و جو که ما برای استفاده `hits_v1`, اما نام جدول های مختلف:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

ایجاد یک جدول توزیع شده برای نمایش در جداول محلی خوشه:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

یک روش معمول این است که جداول توزیع شده مشابه را در تمام ماشین های خوشه ایجاد کنید. این اجازه می دهد در حال اجرا نمایش داده شد توزیع در هر دستگاه از خوشه. همچنین یک گزینه جایگزین برای ایجاد جدول توزیع موقت برای پرس و جو انتخاب داده شده با استفاده از وجود دارد [دور](../sql-reference/table-functions/remote.md) تابع جدول.

بیا فرار کنیم [INSERT SELECT](../sql-reference/statements/insert-into.md) به جدول توزیع شده برای گسترش جدول به چندین سرور.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "اطلاع"
    این روش مناسب برای شارژ جداول بزرگ نیست. یک ابزار جداگانه وجود دارد [تاتر-کپی](../operations/utilities/clickhouse-copier.md) که می تواند جداول دلخواه بزرگ دوباره سفال.

همانطور که شما می توانید انتظار, نمایش داده شد محاسباتی سنگین اجرا نفر بار سریع تر در صورتی که استفاده 3 سرور به جای یک.

در این مورد, ما یک خوشه با استفاده کرده اند 3 خرده ریز, و هر شامل یک ماکت تک.

برای انعطاف پذیری در یک محیط تولید, توصیه می کنیم که هر سفال باید شامل 2-3 کپی بین مناطق در دسترس بودن متعدد و یا مراکز داده گسترش (یا حداقل قفسه). توجه داشته باشید که کلیک خانه پشتیبانی از تعداد نامحدودی از کپی.

به عنوان مثال پیکربندی برای یک خوشه از یک سفال حاوی سه کپی:

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

برای فعال کردن تکثیر بومی [باغ وحش](http://zookeeper.apache.org/) الزامی است. تاتر طول می کشد مراقبت از سازگاری داده ها در تمام کپی و اجرا می شود بازگرداندن روش پس از شکست به طور خودکار. توصیه می شود برای استقرار خوشه باغ وحش بر روی سرورهای جداگانه (جایی که هیچ پروسه های دیگر از جمله کلیک در حال اجرا هستند).

!!! note "یادداشت"
    باغ وحش یک نیاز سخت نیست: در برخی موارد ساده می توانید داده ها را با نوشتن به تمام کپی ها از کد درخواست خود کپی کنید. این رویکرد است **نه** توصیه می شود, در این مورد, تاتر قادر نخواهد بود برای تضمین ثبات داده ها در تمام کپی. بنابراین وظیفه درخواست شما می شود.

مکان های باغ وحش در فایل پیکربندی مشخص شده است:

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

همچنین, ما نیاز به تنظیم ماکروها برای شناسایی هر سفال و ماکت که در ایجاد جدول استفاده می شود:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

اگر هیچ کپی در حال حاضر در ایجاد جدول تکرار وجود دارد, اولین ماکت جدید نمونه است. اگر در حال حاضر زندگی می کنند کپی جدید کلون داده ها از موجود. شما ابتدا یک گزینه برای ایجاد تمام جداول تکرار شده دارید و سپس داده ها را وارد کنید. یکی دیگر از گزینه این است که برای ایجاد برخی از کپی و اضافه کردن دیگران بعد یا در هنگام درج داده ها.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

در اینجا ما با استفاده از [تکرار غذای اصلی](../engines/table-engines/mergetree-family/replication.md) موتور جدول. در پارامترهای مشخص می کنیم مسیر باغ وحش حاوی سفال و کپی شناسه.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

تکرار عمل در حالت چند استاد. داده ها را می توان به هر ماکت بارگذاری کرد و سپس سیستم را با موارد دیگر به طور خودکار همگام سازی می کند. تکرار ناهمزمان است بنابراین در یک لحظه معین, همه کپی ممکن است حاوی داده به تازگی قرار داده شده. حداقل یک ماکت باید اجازه می دهد تا مصرف داده ها. دیگران همگام سازی داده ها و قوام تعمیر هنگامی که دوباره فعال تبدیل خواهد شد. توجه داشته باشید که این روش اجازه می دهد تا برای امکان کم از دست دادن داده ها به تازگی قرار داده شده.

[مقاله اصلی](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
