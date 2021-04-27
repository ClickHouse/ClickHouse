---
toc_priority: 12
toc_title: 使用教程
---

# ClickHouse教程 {#clickhouse-tutorial}

## 从本教程中可以获得什么？ {#what-to-expect-from-this-tutorial}

通过学习本教程，您将了解如何设置一个简单的ClickHouse集群。它会很小，但是可以容错和扩展。然后，我们将使用其中一个示例数据集来填充数据并执行一些演示查询。

## 单节点设置 {#single-node-setup}

为了延迟演示分布式环境的复杂性，我们将首先在单个服务器或虚拟机上部署ClickHouse。ClickHouse通常是从[deb](install.md#install-from-deb-packages)或[rpm](install.md#from-rpm-packages)包安装，但对于不支持它们的操作系统也有[其他方法](install.md#from-docker-image)。

例如，您选择`deb`安装包，执行:

``` bash
{% include 'install/deb.sh' %}
```

在我们安装的软件中包含这些包:

-   `clickhouse-client` 包，包含[clickhouse-client](../interfaces/cli.md)客户端，它是交互式ClickHouse控制台客户端。
-   `clickhouse-common` 包，包含一个ClickHouse可执行文件。
-   `clickhouse-server` 包，包含要作为服务端运行的ClickHouse配置文件。

服务器配置文件位于`/etc/clickhouse-server/`。在继续之前，请注意`config.xml`中的`<path>`元素。它决定了数据存储的位置，因此它应该位于磁盘容量的卷上;默认值是`/var/lib/clickhouse/`。如果你想调整配置，直接编辑config是不方便的。考虑到它可能会在将来的包更新中被重写。建议重写配置元素的方法是在配置中创建[config.d文件夹](../operations/configuration-files.md)，作为config.xml的重写方式。

你可能已经注意到了,`clickhouse-server`安装后不会自动启动。 它也不会在更新后自动重新启动。 您启动服务端的方式取决于您的初始系统，通常情况下是这样:

``` bash
sudo service clickhouse-server start
```

或

``` bash
sudo /etc/init.d/clickhouse-server start
```

服务端日志的默认位置是`/var/log/clickhouse-server/`。当服务端在日志中记录`Ready for connections`消息，即表示服务端已准备好处理客户端连接。

一旦`clickhouse-server`启动并运行，我们可以利用`clickhouse-client`连接到服务端，并运行一些测试查询，如`SELECT "Hello, world!";`.

<details markdown="1">

<summary>Clickhouse-client的快速提示</summary>

交互模式:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

启用多行查询:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

以批处理模式运行查询:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

从指定格式的文件中插入数据:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## 导入示例数据集 {#import-sample-dataset}

现在是时候用一些示例数据填充我们的ClickHouse服务端。 在本教程中，我们将使用Yandex.Metrica的匿名数据，它是在ClickHouse成为开源之前作为生产环境运行的第一个服务（关于这一点的更多内容请参阅[ClickHouse历史](../introduction/history.md))。[多种导入Yandex.Metrica数据集方法](example-datasets/metrica.md)，为了本教程，我们将使用最现实的一个。

### 下载并提取表数据 {#download-and-extract-table-data}

``` bash
curl https://datasets.clickhouse.tech/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://datasets.clickhouse.tech/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

提取的文件大小约为10GB。

### 创建表 {#create-tables}

与大多数数据库管理系统一样，ClickHouse在逻辑上将表分组为数据库。包含一个`default`数据库，但我们将创建一个新的数据库`tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

与创建数据库相比，创建表的语法要复杂得多（请参阅[参考资料](../sql-reference/statements/create.md). 一般`CREATE TABLE`声明必须指定三个关键的事情:

1.  要创建的表的名称。
2.  表结构，例如：列名和对应的[数据类型](../sql-reference/data-types/index.md)。
3.  [表引擎](../engines/table-engines/index.md)及其设置，这决定了对此表的查询操作是如何在物理层面执行的所有细节。

Yandex.Metrica是一个网络分析服务，样本数据集不包括其全部功能，因此只有两个表可以创建:

-   `hits` 表包含所有用户在服务所涵盖的所有网站上完成的每个操作。
-   `visits` 表包含预先构建的会话，而不是单个操作。

让我们看看并执行这些表的实际创建表查询:

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

您可以使用`clickhouse-client`的交互模式执行这些查询（只需在终端中启动它，而不需要提前指定查询）。或者如果你愿意，可以尝试一些[替代接口](../interfaces/index.md)。

正如我们所看到的, `hits_v1`使用 [MergeTree引擎](../engines/table-engines/mergetree-family/mergetree.md)，而`visits_v1`使用 [Collapsing](../engines/table-engines/mergetree-family/collapsingmergetree.md)引擎。

### 导入数据 {#import-data}

数据导入到ClickHouse是通过[INSERT INTO](../sql-reference/statements/insert-into.md)方式完成的，查询类似许多SQL数据库。然而，数据通常是在一个提供[支持序列化格式](../interfaces/formats.md)而不是`VALUES`子句（也支持）。

我们之前下载的文件是以制表符分隔的格式，所以这里是如何通过控制台客户端导入它们:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse有很多[要调整的设置](../operations/settings/index.md)在控制台客户端中指定它们的一种方法是通过参数，就像我们看到上面语句中的`--max_insert_block_size`。找出可用的设置、含义及其默认值的最简单方法是查询`system.settings` 表:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

您也可以[OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize)导入后的表。使用MergeTree-family引擎配置的表总是在后台合并数据部分以优化数据存储（或至少检查是否有意义）。 这些查询强制表引擎立即进行存储优化，而不是稍后一段时间执行:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

这些查询开始I/O和CPU密集型操作，所以如果表一直接收到新数据，最好不要管它，让合并在后台运行。

现在我们可以检查表导入是否成功:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## 查询示例 {#example-queries}

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

## 集群部署 {#cluster-deployment}

ClickHouse集群是一个同质集群。 设置步骤:

1.  在群集的所有机器上安装ClickHouse服务端
2.  在配置文件中设置群集配置
3.  在每个实例上创建本地表
4.  创建一个[分布式表](../engines/table-engines/special/distributed.md)

[分布式表](../engines/table-engines/special/distributed.md)实际上是一种`view`，映射到ClickHouse集群的本地表。 从分布式表中执行**SELECT**查询会使用集群所有分片的资源。 您可以为多个集群指定configs，并创建多个分布式表，为不同的集群提供视图。

具有三个分片，每个分片一个副本的集群的示例配置:

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

为了进一步演示，让我们使用和创建`hits_v1`表相同的`CREATE TABLE`语句创建一个新的本地表，但表名不同:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

创建提供集群本地表视图的分布式表:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

常见的做法是在集群的所有计算机上创建类似的分布式表。 它允许在群集的任何计算机上运行分布式查询。 还有一个替代选项可以使用以下方法为给定的SELECT查询创建临时分布式表[远程](../sql-reference/table-functions/remote.md)表功能。

让我们运行[INSERT SELECT](../sql-reference/statements/insert-into.md)将该表传播到多个服务器。

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "注意:"
    这种方法不适合大型表的分片。 有一个单独的工具 [clickhouse-copier](../operations/utilities/clickhouse-copier.md) 这可以重新分片任意大表。

正如您所期望的那样，如果计算量大的查询使用3台服务器而不是一个，则运行速度快N倍。

在这种情况下，我们使用了具有3个分片的集群，每个分片都包含一个副本。

为了在生产环境中提供弹性，我们建议每个分片应包含分布在多个可用区或数据中心（或至少机架）之间的2-3个副本。 请注意，ClickHouse支持无限数量的副本。

包含三个副本的一个分片集群的示例配置:

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

启用本机复制[Zookeeper](http://zookeeper.apache.org/)是必需的。 ClickHouse负责所有副本的数据一致性，并在失败后自动运行恢复过程。建议将ZooKeeper集群部署在单独的服务器上（其中没有其他进程，包括运行的ClickHouse）。

!!! note "注意"
    ZooKeeper不是一个严格的要求：在某些简单的情况下，您可以通过将数据写入应用程序代码中的所有副本来复制数据。 这种方法是**不**建议的，在这种情况下，ClickHouse将无法保证所有副本上的数据一致性。 因此需要由您的应用来保证这一点。

ZooKeeper位置在配置文件中指定:

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

此外，我们需要设置宏来识别每个用于创建表的分片和副本:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

如果在创建复制表时没有副本，则会实例化新的第一个副本。 如果已有实时副本，则新副本将克隆现有副本中的数据。 您可以选择首先创建所有复制的表，然后向其中插入数据。 另一种选择是创建一些副本，并在数据插入之后或期间添加其他副本。

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

在这里，我们使用[ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md)表引擎。 在参数中，我们指定包含分片和副本标识符的ZooKeeper路径。

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

复制在多主机模式下运行。数据可以加载到任何副本中，然后系统自动将其与其他实例同步。复制是异步的，因此在给定时刻，并非所有副本都可能包含最近插入的数据。至少应该有一个副本允许数据摄入。另一些则会在重新激活后同步数据并修复一致性。请注意，这种方法允许最近插入的数据丢失的可能性很低。

[原始文章](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
