---
toc_priority: 12
toc_title: Tutorial
---

# ClickHouse Tutorial {#clickhouse-tutorial}

## What to Expect from This Tutorial? {#what-to-expect-from-this-tutorial}

By going through this tutorial, you’ll learn how to set up a simple ClickHouse cluster. It’ll be small, but fault-tolerant and scalable. Then we will use one of the example datasets to fill it with data and execute some demo queries.

## Single Node Setup {#single-node-setup}

To postpone the complexities of a distributed environment, we’ll start with deploying ClickHouse on a single server or virtual machine. ClickHouse is usually installed from [deb](../getting-started/install.md#install-from-deb-packages) or [rpm](../getting-started/install.md#from-rpm-packages) packages, but there are [alternatives](../getting-started/install.md#from-docker-image) for the operating systems that do no support them.

For example, you have chosen `deb` packages and executed:

``` bash
{% include 'install/deb.sh' %}
```

What do we have in the packages that got installed:

-   `clickhouse-client` package contains [clickhouse-client](../interfaces/cli.md) application, interactive ClickHouse console client.
-   `clickhouse-common` package contains a ClickHouse executable file.
-   `clickhouse-server` package contains configuration files to run ClickHouse as a server.

Server config files are located in `/etc/clickhouse-server/`. Before going further, please notice the `<path>` element in `config.xml`. Path determines the location for data storage, so it should be located on volume with large disk capacity; the default value is `/var/lib/clickhouse/`. If you want to adjust the configuration, it’s not handy to directly edit `config.xml` file, considering it might get rewritten on future package updates. The recommended way to override the config elements is to create [files in config.d directory](../operations/configuration-files.md) which serve as “patches” to config.xml.

As you might have noticed, `clickhouse-server` is not launched automatically after package installation. It won’t be automatically restarted after updates, either. The way you start the server depends on your init system, usually, it is:

``` bash
sudo service clickhouse-server start
```

or

``` bash
sudo /etc/init.d/clickhouse-server start
```

The default location for server logs is `/var/log/clickhouse-server/`. The server is ready to handle client connections once it logs the `Ready for connections` message.

Once the `clickhouse-server` is up and running, we can use `clickhouse-client` to connect to the server and run some test queries like `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Quick tips for clickhouse-client</summary>

Interactive mode:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Enable multiline queries:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Run queries in batch-mode:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Insert data from a file in specified format:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Import Sample Dataset {#import-sample-dataset}

Now it’s time to fill our ClickHouse server with some sample data. In this tutorial, we’ll use the anonymized data of Yandex.Metrica, the first service that runs ClickHouse in production way before it became open-source (more on that in [history section](../introduction/history.md)). There are [multiple ways to import Yandex.Metrica dataset](../getting-started/example-datasets/metrica.md), and for the sake of the tutorial, we’ll go with the most realistic one.

### Download and Extract Table Data {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

The extracted files are about 10GB in size.

### Create Tables {#create-tables}

As in most databases management systems, ClickHouse logically groups tables into “databases”. There’s a `default` database, but we’ll create a new one named `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

Syntax for creating tables is way more complicated compared to databases (see [reference](../sql-reference/statements/create.md). In general `CREATE TABLE` statement has to specify three key things:

1.  Name of table to create.
2.  Table schema, i.e. list of columns and their [data types](../sql-reference/data-types/index.md).
3.  [Table engine](../engines/table-engines/index.md) and its settings, which determines all the details on how queries to this table will be physically executed.

Yandex.Metrica is a web analytics service, and sample dataset doesn’t cover its full functionality, so there are only two tables to create:

-   `hits` is a table with each action done by all users on all websites covered by the service.
-   `visits` is a table that contains pre-built sessions instead of individual actions.

Let’s see and execute the real create table queries for these tables:

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

You can execute those queries using the interactive mode of `clickhouse-client` (just launch it in a terminal without specifying a query in advance) or try some [alternative interface](../interfaces/index.md) if you want.

As we can see, `hits_v1` uses the [basic MergeTree engine](../engines/table-engines/mergetree-family/mergetree.md), while the `visits_v1` uses the [Collapsing](../engines/table-engines/mergetree-family/collapsingmergetree.md) variant.

### Import Data {#import-data}

Data import to ClickHouse is done via [INSERT INTO](../sql-reference/statements/insert-into.md) query like in many other SQL databases. However, data is usually provided in one of the [supported serialization formats](../interfaces/formats.md) instead of `VALUES` clause (which is also supported).

The files we downloaded earlier are in tab-separated format, so here’s how to import them via console client:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse has a lot of [settings to tune](../operations/settings/index.md) and one way to specify them in console client is via arguments, as we can see with `--max_insert_block_size`. The easiest way to figure out what settings are available, what do they mean and what the defaults are is to query the `system.settings` table:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

Optionally you can [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) the tables after import. Tables that are configured with an engine from MergeTree-family always do merges of data parts in the background to optimize data storage (or at least check if it makes sense). These queries force the table engine to do storage optimization right now instead of some time later:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

These queries start an I/O and CPU intensive operation, so if the table consistently receives new data, it’s better to leave it alone and let merges run in the background.

Now we can check if the table import was successful:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Example Queries {#example-queries}

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

## Cluster Deployment {#cluster-deployment}

ClickHouse cluster is a homogenous cluster. Steps to set up:

1.  Install ClickHouse server on all machines of the cluster
2.  Set up cluster configs in configuration files
3.  Create local tables on each instance
4.  Create a [Distributed table](../engines/table-engines/special/distributed.md)

[Distributed table](../engines/table-engines/special/distributed.md) is actually a kind of “view” to local tables of ClickHouse cluster. SELECT query from a distributed table executes using resources of all cluster’s shards. You may specify configs for multiple clusters and create multiple distributed tables providing views to different clusters.

Example config for a cluster with three shards, one replica each:

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

For further demonstration, let’s create a new local table with the same `CREATE TABLE` query that we used for `hits_v1`, but different table name:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Creating a distributed table providing a view into local tables of the cluster:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

A common practice is to create similar Distributed tables on all machines of the cluster. It allows running distributed queries on any machine of the cluster. Also there’s an alternative option to create temporary distributed table for a given SELECT query using [remote](../sql-reference/table-functions/remote.md) table function.

Let’s run [INSERT SELECT](../sql-reference/statements/insert-into.md) into the Distributed table to spread the table to multiple servers.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "Notice"
    This approach is not suitable for the sharding of large tables. There’s a separate tool [clickhouse-copier](../operations/utilities/clickhouse-copier.md) that can re-shard arbitrary large tables.

As you could expect, computationally heavy queries run N times faster if they utilize 3 servers instead of one.

In this case, we have used a cluster with 3 shards, and each contains a single replica.

To provide resilience in a production environment, we recommend that each shard should contain 2-3 replicas spread between multiple availability zones or datacenters (or at least racks). Note that ClickHouse supports an unlimited number of replicas.

Example config for a cluster of one shard containing three replicas:

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

To enable native replication [ZooKeeper](http://zookeeper.apache.org/) is required. ClickHouse takes care of data consistency on all replicas and runs restore procedure after failure automatically. It’s recommended to deploy the ZooKeeper cluster on separate servers (where no other processes including ClickHouse are running).

!!! note "Note"
    ZooKeeper is not a strict requirement: in some simple cases, you can duplicate the data by writing it into all the replicas from your application code. This approach is **not** recommended, in this case, ClickHouse won’t be able to guarantee data consistency on all replicas. Thus it becomes the responsibility of your application.

ZooKeeper locations are specified in the configuration file:

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

Also, we need to set macros for identifying each shard and replica which are used on table creation:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

If there are no replicas at the moment on replicated table creation, a new first replica is instantiated. If there are already live replicas, the new replica clones data from existing ones. You have an option to create all replicated tables first, and then insert data to it. Another option is to create some replicas and add the others after or during data insertion.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Here we use [ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) table engine. In parameters we specify ZooKeeper path containing shard and replica identifiers.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

Replication operates in multi-master mode. Data can be loaded into any replica, and the system then syncs it with other instances automatically. Replication is asynchronous so at a given moment, not all replicas may contain recently inserted data. At least one replica should be up to allow data ingestion. Others will sync up data and repair consistency once they will become active again. Note that this approach allows for the low possibility of a loss of recently inserted data.

[Original article](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
