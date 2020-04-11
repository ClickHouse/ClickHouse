---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Учебник По Клик-Хаусу {#clickhouse-tutorial}

## Чего ожидать от этого урока? {#what-to-expect-from-this-tutorial}

Пройдя через этот учебник,вы узнаете, как настроить простой кластер ClickHouse. Он будет небольшим, но отказоустойчивым и масштабируемым. Затем мы будем использовать один из примеров наборов данных, чтобы заполнить его данными и выполнить некоторые демонстрационные запросы.

## Настройка Одного Узла {#single-node-setup}

Чтобы избежать сложностей распределенной среды, мы начнем с развертывания ClickHouse на одном сервере или виртуальной машине. ClickHouse обычно устанавливается из [дебютантка](install.md#install-from-deb-packages) или [оборотов в минуту](install.md#from-rpm-packages) пакеты, но есть и такие [альтернативы](install.md#from-docker-image) для операционных систем, которые их не поддерживают.

Например, вы выбрали `deb` пакеты и выполненные работы:

``` bash
sudo apt-get install dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

echo "deb http://repo.clickhouse.tech/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client
```

Что у нас есть в пакетах, которые были установлены:

-   `clickhouse-client` пакет содержит [clickhouse-клиент](../interfaces/cli.md) приложение, интерактивный консольный клиент ClickHouse.
-   `clickhouse-common` пакет содержит исполняемый файл ClickHouse.
-   `clickhouse-server` пакет содержит файлы конфигурации для запуска ClickHouse в качестве сервера.

Файлы конфигурации сервера находятся в `/etc/clickhouse-server/`. Прежде чем идти дальше, пожалуйста, обратите внимание на `<path>` элемент в `config.xml`. Путь определяет место для хранения данных, поэтому он должен быть расположен на Томе с большой емкостью диска; значение по умолчанию равно `/var/lib/clickhouse/`. Если вы хотите настроить конфигурацию, то это не удобно для непосредственного редактирования `config.xml` файл, учитывая, что он может быть переписан при будущих обновлениях пакета. Рекомендуемый способ переопределения элементов конфигурации заключается в создании [файлы в конфигурации.D каталог](../operations/configuration_files.md) которые служат в качестве «patches» к конфигурации.XML.

Как вы могли заметить, `clickhouse-server` не запускается автоматически после установки пакета. Он также не будет автоматически перезапущен после обновления. То, как вы запускаете сервер, зависит от вашей системы init, как правило, это так:

``` bash
sudo service clickhouse-server start
```

или

``` bash
sudo /etc/init.d/clickhouse-server start
```

По умолчанию для журналов сервера используется следующее расположение `/var/log/clickhouse-server/`. Сервер готов к обработке клиентских подключений, как только он регистрирует `Ready for connections` сообщение.

Как только это произойдет `clickhouse-server` все готово и работает, мы можем использовать `clickhouse-client` чтобы подключиться к серверу и выполнить некоторые тестовые запросы, такие как `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Быстрые советы для clickhouse-клиента</summary>
Интерактивный режим:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Включить многострочные запросы:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Запуск запросов в пакетном режиме:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Вставка данных из файла в заданном формате:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Импорт Образца Набора Данных {#import-sample-dataset}

Теперь пришло время заполнить наш сервер ClickHouse некоторыми образцами данных. В этом уроке мы будем использовать анонимизированные данные Яндекса.Metrica, первый сервис, который запускает ClickHouse в производственном режиме до того, как он стал открытым исходным кодом (подробнее об этом в [раздел истории](../introduction/history.md)). Есть [несколько способов импорта Яндекса.Набор метрика](example_datasets/metrica.md), и ради учебника мы пойдем с самым реалистичным из них.

### Загрузка и извлечение данных таблицы {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

Извлеченные файлы имеют размер около 10 ГБ.

### Создавать таблицы {#create-tables}

Как и в большинстве систем управления базами данных, ClickHouse логически группирует таблицы в «databases». Там есть еще один `default` база данных, но мы создадим новую с именем `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

Синтаксис для создания таблиц намного сложнее по сравнению с базами данных (см. [ссылка](../sql_reference/statements/create.md). В общем `CREATE TABLE` в заявлении должны быть указаны три ключевых момента:

1.  Имя таблицы для создания.
2.  Table schema, i.e. list of columns and their [тип данных](../sql_reference/data_types/index.md).
3.  [Настольный двигатель](../engines/table_engines/index.md) и это настройки, которые определяют все детали того, как запросы к этой таблице будут физически выполняться.

Яндекс.Metrica - это сервис веб-аналитики, и пример набора данных не охватывает его полную функциональность, поэтому для создания необходимо создать только две таблицы:

-   `hits` это таблица с каждым действием, выполняемым всеми пользователями на всех веб-сайтах, охватываемых сервисом.
-   `visits` это таблица, которая содержит предварительно построенные сеансы вместо отдельных действий.

Давайте посмотрим и выполним реальные запросы create table для этих таблиц:

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

Вы можете выполнить эти запросы с помощью интерактивного режима `clickhouse-client` (просто запустите его в терминале, не указывая заранее запрос) или попробуйте некоторые [альтернативный интерфейс](../interfaces/index.md) если ты хочешь.

Как мы видим, `hits_v1` использует [базовый движок MergeTree](../engines/table_engines/mergetree_family/mergetree.md), в то время как `visits_v1` использует [Разрушение](../engines/table_engines/mergetree_family/collapsingmergetree.md) вариант.

### Импортировать данные {#import-data}

Импорт данных в ClickHouse осуществляется через [INSERT INTO](../sql_reference/statements/insert_into.md) запрос, как и во многих других базах данных SQL. Однако данные обычно приводятся в одном из следующих документов: [поддерживаемые форматы сериализации](../interfaces/formats.md) вместо `VALUES` предложение (которое также поддерживается).

Файлы, которые мы загрузили ранее, находятся в формате с разделенными вкладками, поэтому вот как импортировать их через консольный клиент:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

У ClickHouse их очень много [настройки для настройки](../operations/settings/index.md) и один из способов указать их в консольном клиенте - это через аргументы, как мы видим с помощью `--max_insert_block_size`. Самый простой способ выяснить, какие настройки доступны, что они означают и каковы значения по умолчанию, - это запросить `system.settings` стол:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

По желанию вы можете [OPTIMIZE](../sql_reference/misc/#misc_operations-optimize) таблицы после импорта. Таблицы, настроенные с помощью движка из семейства MergeTree, всегда выполняют слияние частей данных в фоновом режиме для оптимизации хранения данных (или, по крайней мере, проверяют, имеет ли это смысл). Эти запросы заставляют механизм таблиц выполнять оптимизацию хранилища прямо сейчас, а не некоторое время спустя:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

Эти запросы запускают интенсивную работу ввода-вывода и процессора, поэтому, если таблица постоянно получает новые данные, лучше оставить ее в покое и позволить слияниям работать в фоновом режиме.

Теперь мы можем проверить, был ли импорт таблицы успешным:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Пример запроса {#example-queries}

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

## Развертывание Кластера {#cluster-deployment}

Кластер ClickHouse-это однородный кластер. Шаги для настройки:

1.  Установите сервер ClickHouse на всех компьютерах кластера
2.  Настройка конфигураций кластера в файлах конфигурации
3.  Создание локальных таблиц на каждом экземпляре
4.  Создать [Распространены таблицы](../engines/table_engines/special/distributed.md)

[Распространены таблицы](../engines/table_engines/special/distributed.md) это на самом деле своего рода «view» к локальным таблицам кластера ClickHouse. Запрос SELECT из распределенной таблицы выполняется с использованием ресурсов всех сегментов кластера. Вы можете указать конфигурации для нескольких кластеров и создать несколько распределенных таблиц, предоставляющих представления для разных кластеров.

Пример конфигурации для кластера с тремя сегментами, по одной реплике в каждом:

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

Для дальнейшей демонстрации давайте создадим новую локальную таблицу с тем же именем `CREATE TABLE` запрос, который мы использовали для `hits_v1`, но другое имя таблицы:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Создание распределенной таблицы, предоставляющей представление в локальные таблицы кластера:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

Распространенной практикой является создание одинаковых распределенных таблиц на всех машинах кластера. Он позволяет выполнять распределенные запросы на любой машине кластера. Кроме того, существует альтернативный вариант создания временной распределенной таблицы для данного запроса SELECT с помощью [удаленный](../sql_reference/table_functions/remote.md) табличная функция.

Давай убежим [INSERT SELECT](../sql_reference/statements/insert_into.md) в распределенную таблицу, чтобы распространить таблицу на несколько серверов.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "Уведомление"
    Такой подход не подходит для сегментации больших столов. Есть отдельный инструмент [clickhouse-копировальный аппарат](../operations/utilities/clickhouse-copier.md) это может повторно осколить произвольные большие таблицы.

Как и следовало ожидать, вычислительно тяжелые запросы выполняются в N раз быстрее, если они используют 3 сервера вместо одного.

В этом случае мы использовали кластер с 3 осколками, и каждый из них содержит одну реплику.

Для обеспечения устойчивости в рабочей среде рекомендуется, чтобы каждый сегмент содержал 2-3 реплики, распределенные между несколькими зонами доступности или центрами обработки данных (или, по крайней мере, стойками). Обратите внимание, что ClickHouse поддерживает неограниченное количество реплик.

Пример конфигурации для кластера из одного осколка, содержащего три реплики:

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

Чтобы включить собственную репликацию [Смотритель зоопарка](http://zookeeper.apache.org/) требуемый. ClickHouse заботится о согласованности данных во всех репликах и автоматически запускает процедуру восстановления после сбоя. Рекомендуется развернуть кластер ZooKeeper на отдельных серверах (где не выполняются никакие другие процессы, включая ClickHouse).

!!! note "Примечание"
    ZooKeeper не является строгим требованием: в некоторых простых случаях вы можете дублировать данные, записав их во все реплики из кода вашего приложения. Такой подход является **нет** рекомендуется, чтобы в этом случае ClickHouse не мог гарантировать согласованность данных на всех репликах. Таким образом, это становится ответственностью вашего приложения.

Расположение ZooKeeper указано в конфигурационном файле:

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

Кроме того, нам нужно установить макросы для идентификации каждого осколка и реплики, которые используются при создании таблицы:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

Если в данный момент при создании реплицированной таблицы реплик нет, то создается новая первая реплика. Если уже существуют живые реплики, то новая реплика клонирует данные из существующих. У вас есть возможность сначала создать все реплицированные таблицы, а затем вставить в них данные. Другой вариант-создать некоторые реплики и добавить другие после или во время вставки данных.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Здесь мы используем [ReplicatedMergeTree](../engines/table_engines/mergetree_family/replication.md) настольный двигатель. В параметрах мы указываем путь ZooKeeper, содержащий идентификаторы сегментов и реплик.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

Репликация работает в режиме мульти-мастер. Данные могут быть загружены в любую реплику, а затем система автоматически синхронизирует их с другими экземплярами. Репликация является асинхронной, поэтому в данный момент не все реплики могут содержать недавно вставленные данные. По крайней мере, одна реплика должна быть готова, чтобы обеспечить прием данных. Другие будут синхронизировать данные и восстанавливать согласованность, как только они снова станут активными. Обратите внимание, что этот подход допускает низкую вероятность потери недавно вставленных данных.

[Оригинальная статья](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
