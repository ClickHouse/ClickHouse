---
slug: /ru/getting-started/tutorial
sidebar_position: 12
sidebar_label: Tutorial
---

# Руководство {#clickhouse-tutorial}

## Что вы получите, пройдя это руководство? {#what-to-expect-from-this-tutorial}

Пройдя это руководство вы научитесь устанавливать простой кластер Clickhouse. Он будет небольшим, но отказоустойчивым и масштабируемым. Далее мы воспользуемся одним из готовых наборов данных для наполнения кластера данными и выполнения над ними нескольких демонстрационных запросов.

## Установка на одном узле {#single-node-setup}

Чтобы не погружаться сразу в сложности распределённого окружения мы начнём с развёртывания ClickHouse на одном сервере или одной виртуальной машине. ClickHouse обычно устанавливаается из [deb](../getting-started/install.md#install-from-deb-packages)- или [rpm](../getting-started/install.md#from-rpm-packages)-пакетов, но есть и [альтернативы](../getting-started/install.md#from-docker-image) для операционных систем без соответствующих пакетных менеджеров.

Например, выбираем нужные `deb`-пакеты и выполняем:

``` bash
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you've set up a password.
```

Что мы получим по результатам установки этих пакетов:

-   с пакетом `clickhouse-client` будет установлена программа [clickhouse-client](../interfaces/cli.md) — интерактивный консольный клиент ClickHouse.
-   пакет `clickhouse-common` включает исполняемый файл ClickHouse.
-   пакет `clickhouse-server` содержит конфигурационные файлы для запуска ClickHouse в качестве сервера.

Файлы конфигурации сервера располагаются в каталоге `/etc/clickhouse-server/`. Прежде чем идти дальше, обратите внимание на элемент `<path>` в файле `config.xml`. Путь, задаваемый этим элементом, определяет местоположение данных, таким образом, он должен быть расположен на томе большой ёмкости; значение по умолчанию — `/var/lib/clickhouse/`. Если вы хотите изменить конфигурацию, то лучше не редактировать вручную файл `config.xml`, поскольку  он может быть переписан будущими пакетными обновлениями; рекомендуется создать файлы с необходимыми конфигурационными элементами [в каталоге config.d](../operations/configuration-files.md), которые рассматриваются как “патчи” к config.xml.

Вы могли заметить, что `clickhouse-server` не запускается автоматически после установки пакетов. Также сервер не будет автоматически перезапускаться после обновлений. Способ запуска сервера зависит от используемой подсистемы инициализации, обычно это делается так:

``` bash
sudo service clickhouse-server start
```

или

``` bash
sudo /etc/init.d/clickhouse-server start
```

Журналы сервера по умолчанию ведутся в `/var/log/clickhouse-server/`. Как только в журнале появится сообщение `Ready for connections` — сервер готов принимать клиентские соединения.

Теперь, когда `clickhouse-server` запущен, можно подключиться к нему с использованием `clickhouse-client` и выполнить тестовый запрос, например, `SELECT 'Hello, world!';`.

<details markdown="1">

<summary>Советы по использованию clickhouse-client</summary>

Интерактивный режим:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Включить многострочный режим запросов:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Включить пакетный режим запуска запросов:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Вставить данные из файла заданного формата:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Загрузка набора данных из примеров {#import-sample-dataset}

Настало время загрузить в ClickHouse данные из примеров. В этом руководстве мы используем анонимизированные данные посещений сайтов (веб-метрики). Существует [множество способов импортировать набор данных](../getting-started/example-datasets/metrica.md), но для целей данного руководства мы используем наиболее практичный из них.

### Загрузка и извлечение табличных данных {#download-and-extract-table-data}

``` bash
curl https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

Распакованные файлы занимают около 10 ГБ.

### Создание таблиц {#create-tables}

ClickHouse, как и большинство СУБД, логически объединяет таблицы в «базы данных». Существует база данных по умолчанию — `default`, но мы созданим новую, дав ей наименование `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

Ситаксис для создания таблиц более сложен в сравнении с другими СУБД (см. [руководство по SQL](../sql-reference/statements/create/table.md). Оператор `CREATE TABLE` должен указывать на три ключевых момента:

1.  Имя создаваемой таблицы.
2.  Схему таблицы, то есть задавать список столбцов и их [типы данных](../sql-reference/data-types/index.md).
3.  [Движок таблицы](../engines/table-engines/index.md) и его параметры, которые определяют все детали того, как запросы к данной таблице будут физически исполняться.

Мы создадим все лишь две таблицы:

-   таблицу `hits` с действиями, осуществлёнными всеми пользователями на всех сайтах, обслуживаемых сервисом;
-   таблицу `visits`, содержащую посещения — преднастроенные сессии вместо каждого действия.

Выполним операторы `CREATE TABLE` для создания этих таблиц:

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
```

Эти операторы можно выполнить с использованием интерактивного режима в `clickhouse-client` (запустите его из командной строки не указывая заранее запросы) или, при желании, воспользоваться [альтернативным интерфейсом](../interfaces/index.md) .

Как вы можете видеть, `hits_v1` использует [базовый вариант движка MergeTree](../engines/table-engines/mergetree-family/mergetree.md), тогда как `visits_v1` использует вариант [Collapsing](../engines/table-engines/mergetree-family/collapsingmergetree.md).

### Импорт данных {#import-data}

Импорт данных в ClickHouse выполняется оператором [INSERT INTO](../sql-reference/statements/insert-into.md) как в большинстве SQL-систем. Однако данные для вставки в таблицы ClickHouse обычно предоставляются в одном из [поддерживаемых форматов](../interfaces/formats.md) вместо их непосредственного указания в предложении `VALUES` (хотя и этот способ поддерживается).

В нашем случае файлы были загружены ранее в формате со значениями, разделёнными знаком табуляции; импортируем их, указав соответствующие запросы в аргументах командной строки:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse оснащён множеством [изменяемых настроек](../operations/settings/index.md) и один из способов их указать — передать при запуске консольного клиенте их в качестве аргументов, как вы видели в этом примере с `--max_insert_block_size`. Простейший способ узнать, какие настройки доступны, что они означают и какие у них значения по умолчанию — запросить содержимое таблицы `system.settings`:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

Можно также применить оператор [OPTIMIZE](../sql-reference/statements/optimize.md) к таблицам после импорта. Для таблиц, созданных с движками семейства MergeTree, слияние частей загруженных данных выполняется в фоновом режиме (по крайней мере проверяется, имеет ли смысл его осуществить); этот оператор принудительно запускает соответствующие процессы слияния вместо того, чтобы эти действия были выполнены в фоне когда-нибудь позже.

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

Эти запросы запускают интеснивные по отношению к вводу-выводу и процессорным ресурсам операции, таким образом, если таблица всё ещё получает новые данные, лучше дать возможность слияниям запуститься в фоне.

Проверим, успешно ли загрузились данные:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Примеры запросов {#example-queries}

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
WHERE (CounterID = 912887) AND (toYYYYMM(StartDate) = 201403)
```

## Кластерное развёртывание {#cluster-deployment}

Кластер ClickHouse — гомогенный, то есть все узлы в нём равны, ведущие и ведомые не выделяются. Шаги по установке кластера:

1.  Установить сервер ClickHouse на всех узлах будущего кластера.
2.  Прописать кластерные конфигурации в конфигурационных файлах.
3.  Создать локальные таблицы на каждом экземпляре.
4.  Создать [распределённую таблицу](../engines/table-engines/special/distributed.md).

[Распределённая таблица](../engines/table-engines/special/distributed.md) — в некотором смысле «представление» над локальными таблицами кластера ClickHouse. Запрос SELECT к распределённой таблице выполняется на всех узлах кластера. Вы можете указать конфигурации для нескольких кластеров и создать множество распределённых таблиц, «смотрящих» на разные кластеры.

Пример конфигурации кластера с тремя сегментами и одной репликой для каждой:

``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.clickhouse.com</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

Далее создадим новую локальную таблицу с помощью того же запроса `CREATE TABLE`, что использовался для таблицы `hits_v1`, но с другим именем:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Создадим распределённую таблицу, обеспечивающую представление над локальными таблицами кластера:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

Стандартная практика — создание одинаковых распределённых таблиц на всех узлах кластера. Это позволит запускать распределённые запросы с любого узла. Альтернативой может быть создание временной распределённой таблицы для заданного отдельно взятого запроса с использованием табличной функции [remote](../sql-reference/table-functions/remote.md).

Выполним [INSERT SELECT](../sql-reference/statements/insert-into.md) в распределённую таблицу, чтобы распределить данные по нескольким узлам.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

Как и следовало ожидать, вычислительно сложные запросы работают втрое быстрее, если они выполняются на трёх серверах, а не на одном.

В данном случае мы использовали кластер из трёх сегментов с одной репликой для каждого.

В продуктивных окружениях для обеспечения надёжности мы рекомендуем чтобы каждый сегмент был защищён 2—3 репликами, разнесёнными на разные зоны отказоустойчивости или разные центры обработки данных (или хотя бы разные стойки). Особо отметим, что ClickHouse поддерживает неограниченное количество реплик.

Пример конфигурации кластера с одним сегментом и тремя репликами:

``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.clickhouse.com</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.clickhouse.com</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.clickhouse.com</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

Для работы встроенной репликации необходимо использовать [ZooKeeper](http://zookeeper.apache.org/). ClickHouse заботится о согласованности данных на всех репликах и автоматически запускает процедуры восстановления в случае сбоев. Рекомендуется развёртывание кластера ZooKeeper на отдельных серверах (на которых не запущено других процессов, в том числе ClickHouse).

:::note Примечание
Использование ZooKeeper — нестрогая рекомендация: можно продублировать данные, записывая их непосредственно из приложения на несколько реплик. Но этот поход **не рекомедуется** в общем случае, поскольку  ClickHouse не сможет гарантировать согласованность данных на всех репликах; обеспечение согласованности станет заботой вашего приложения.
:::

Адреса узлов ZooKeeper указываются в файле конфиуграции:

``` xml
<zookeeper>
    <node>
        <host>zoo01.clickhouse.com</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.clickhouse.com</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.clickhouse.com</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Также необходимо в секции macros указать идентификаторы для сегментов и реплик, они нужны будут при создании таблиц:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

Если в момент создания реплцированной таблицы ни одной реплики ещё нет, то будет создана первая из них. Если уже есть работающие реплики, то в новые реплики данные будут склонированы из существующих. Есть возможность вначале создать реплицируемые таблицы, а затем вставить в них данные. Но можно создать вначале создать только часть реплик и добавить ещё несколько после вставки или в процессе вставки данных.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplicatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Здесь мы используем движок [ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md). Указываем в параметрах путь в Zookeeper к идентификаторам сегмента и реплики.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

Репликация работает в режиме мультимастера. Это означает, что данные могут быть загружены на любую из реплик и система автоматически синхронизирует данные между остальными репликами. Репликация асинхронна, то есть в конкретный момент времени не все реплики могут содержать недавно добавленные данные. Как минимум одна реплика должна быть в строю для приёма данных. Прочие реплики синхронизируются и восстановят согласованное состояния как только снова станут активными. Заметим, что при таком подходе есть вероятность утраты недавно добавленных данных.
