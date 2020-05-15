---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 12
toc_title: Tutorial
---

# ClickHouseチュートリアル {#clickhouse-tutorial}

## このチュートリアルから何を期待する？ {#what-to-expect-from-this-tutorial}

このチュートリアルでは、クリックハウスクラスタを設定する方法について説明します。 それは小さい、しかし耐障害性および拡張可能である。 次に、例のデータセットのいずれかを使用してデータを入力し、いくつかのデモクエリを実行します。

## 単一ノード設定 {#single-node-setup}

分散環境の複雑さを延期するには、まず、単一のサーバーまたは仮想マシンにclickhouseを展開します。 clickhouseは通常からインストール [deb](install.md#install-from-deb-packages) または [rpm](install.md#from-rpm-packages) パッケージがあります [代替案](install.md#from-docker-image) の営業システムな支援します。

たとえば、次の項目を選択しました `deb` パッケージと実行:

``` bash
{% include 'install/deb.sh' %}
```

インストールされたパッケージには何がありますか:

-   `clickhouse-client` パッケージ内容: [クリックハウス-顧客](../interfaces/cli.md) ケイClickHouseコンソールです。
-   `clickhouse-common` パッケージが含まれてClickHouse実行可能ファイルです。
-   `clickhouse-server` パッケージを含む設定ファイルを実行ClickHouseしています。

サーバ設定ファイルを置 `/etc/clickhouse-server/`. さらに進む前に、 `<path>` 要素の `config.xml`. パスはデータストレージの場所を決定するので、ディスク容量の大きいボリュームに配置する必要があります。 `/var/lib/clickhouse/`. 設定を調整したい場合は、直接編集するのは便利ではありません `config.xml` ファイルは、将来のパッケージ更新で書き直される可能性があります。 のオーバーライドは、config要素の作成 [config内のファイル。dディレクトリ](../operations/configuration-files.md) として役立つ “patches” 設定する。xmlだ

あなたが気づいたように, `clickhouse-server` パッケージのイ 更新後も自動的に再起動されることはありません。 サーバーを起動する方法は、initシステムによって異なります。:

``` bash
sudo service clickhouse-server start
```

または

``` bash
sudo /etc/init.d/clickhouse-server start
```

サーバーログのデフォルトの場所は、 `/var/log/clickhouse-server/`. サーバーはクライアント接続を処理する準備ができています。 `Ready for connections` メッセージ

一度 `clickhouse-server` 稼働している、我々は使用することができます `clickhouse-client` サーバーに接続し、次のようなテストクエリを実行するには `SELECT "Hello, world!";`.

<details markdown="1">

<summary>索のヒントclickhouse-クライアント</summary>
対話モード:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

複数行のクエリを有効にする:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

バッチモードでのクエリの実行:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

指定した形式のファイルからデータを挿入する:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## インポートサンプル {#import-sample-dataset}

今回は入社clickhouseサーバーサンプルデータです。 このチュートリアルでは、yandexの匿名化されたデータを使用します。metricaは、オープンソースになる前にclickhouseを運用方法で実行する最初のサービスです（詳細は [履歴セクション](../introduction/history.md)). あります [Yandexをインポートする複数の方法。Metricaデータセット](example-datasets/metrica.md)、そしてチュートリアルのために、我々は最も現実的なもので行くよ。

### 表データのダウンロードと抽出 {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

抽出されたファイルのサイズは約10gbです。

### テーブルの作成 {#create-tables}

ほとんどのデータベース管理システムclickhouse論理的にグテーブル “databases”. そこには `default` データベースが、我々は名前の新しいものを作成します `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

テーブルを作成するための構文は、データベースに比べて複雑です [参照](../sql-reference/statements/create.md). 一般的に `CREATE TABLE` 声明を設定するつもの:

1.  作成するテーブルの名前。
2.  Table schema, i.e. list of columns and their [データ型](../sql-reference/data-types/index.md).
3.  [表エンジン](../engines/table-engines/index.md) これは、このテーブルへのクエリが物理的にどのように実行されるかに関するすべての詳細を決定します。

Yandexの。Metricaはweb分析サービスであり、サンプルデータセットはその完全な機能をカバーしていません。:

-   `hits` とができるテーブルの各行動によるすべてのユーザーはすべてのwebサイトのサービスです。
-   `visits` はテーブルを含む組み立て済みセッションの代わりに個別に行動します。

これらのテーブルの実際のcreate tableクエリを見て、実行しましょう:

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

ドできるようになりました。方のクエリのインタラクティブモードの `clickhouse-client` （事前にクエリを指定せずに端末で起動するだけです）またはいくつか試してみてください [代わりとなるインターフェース](../interfaces/index.md) あなたが望むなら。

ご覧の通り, `hits_v1` 使用します [基本的なMergeTreeエンジン](../engines/table-engines/mergetree-family/mergetree.md)、ながら `visits_v1` 使用します [折りたたみ](../engines/table-engines/mergetree-family/collapsingmergetree.md) バリアント。

### デー {#import-data}

ClickHouseへのデータのインポートは、 [INSERT INTO](../sql-reference/statements/insert-into.md) 他の多くのSQLデータベースのような照会。 ただし、データは通常、次のいずれかで提供されます。 [対応するシリアル化形式](../interfaces/formats.md) 代わりに `VALUES` 句(これもサポートされています)。

以前にダウンロードしたファ:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouseには多くのものがあります [調整する設定](../operations/settings/index.md) そして、コンソールクライアントでそれらを指定する一つの方法は、引数を使用することです `--max_insert_block_size`. どのような設定が利用可能であるか、それらが何を意味するのか、そしてデフォルトが何であるかを理解する最も簡単な方法は、 `system.settings` テーブル:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

必要に応じ [OPTIMIZE](../query_language/misc/#misc_operations-optimize) インポート後のテーブル。 MergeTree-familyのエンジンで構成されたテーブルは、データストレージを最適化するために、常にバックグラウンドでデータ部分のマージを行います（または少なくとも これらのクエリのテーブルエンジンな保管の最適化現在の代わりについては後日、:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

したがって、テーブルが一貫して新しいデータを受け取る場合は、そのままにして、マージをバックグラウンドで実行する方がよいでしょう。

テーブルインポートが成功したかどうかを確認できます:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## クエリ例 {#example-queries}

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

## クラスターの展開 {#cluster-deployment}

ClickHouseの集りは同種の集りである。 セットアップの手順:

1.  イclickhouseサーバーのすべての機械のクラスター
2.  構成ファイルでのクラスタ構成のセットアップ
3.  各インスタ
4.  作成する [分散テーブル](../engines/table-engines/special/distributed.md)

[分散テーブル](../engines/table-engines/special/distributed.md) 実際には “view” 地元のテーブルのClickHouse。 SELECTクエリから分散型のテーブル実行が持つリソースを活用したすべてのクラスターの破片. を指定しますconfigs複数のクラスターを作成した複数のテーブルのビューを提供する別のクラスター

ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂづ債つｱﾂつｿﾂづｧ:

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

さらなる実証しましょう新しい地域のテーブルと同じ `CREATE TABLE` 私たちが使用したクエリ `hits_v1`、しかし、異なるテーブル名:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

クラスターのローカルテーブルにビューを提供する分散テーブルの作成:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

一般的な方法は、クラスターのすべてのマシンで同様の分散テーブルを作成することです。 クラスターの任意のマシンで分散クエリを実行できます。 また、特定のselectクエリを使用して一時分散テーブルを作成する代替オプションもあります [リモート](../sql-reference/table-functions/remote.md) テーブル機能。

逃げよう [INSERT SELECT](../sql-reference/statements/insert-into.md) 分散テーブルに分散テーブルを複数のサーバーに分散させます。

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "気づく"
    このアプローチは適しませんのshardingの大きます。 別のツールがあります [クリックハウスコピー機](../operations/utilities/clickhouse-copier.md) できるre-ザ-シャーを任意の大きます。

予想されるように、計算量の多いクエリは、3つのサーバーを使用する場合にn倍高速に実行されます。

この場合、3つのシャードを持つクラスターを使用し、それぞれに単一のレプリカが含まれています。

運用環境で復元性を提供するには、各シャードに、複数のアベイラビリティーゾーンまたはデータセンター(または少なくともラック)の間に2～3個のレプリカ clickhouseでは、レプリカの数に制限はありません。

レプリカを含むシャードのクラスタの設定例:

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

ネイティブ複製を有効にする [ZooKeeper](http://zookeeper.apache.org/) は必須です。 ClickHouseは、すべてのレプリカでデータの整合性を管理し、障害後に自動的に復元手順を実行します。 ZooKeeperクラスターを別々のサーバーに展開することをお勧めします（ClickHouseを含む他のプロセスは実行されていません）。

!!! note "メモ"
    いくつかの簡単なケースでは、アプリケーションコードからすべてのレプリカにデータを書き込むことでデータを複製できます。 このアプローチは **ない** 推奨、この場合、ClickHouseはすべてのレプリカでデータの整合性を保証することはできません。 従ってそれはあなたの適用の責任になります。

ZooKeeperの場所は設定ファイルで指定します:

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

また、テーブル作成時に使用される各シャードとレプリカを識別するマクロを設定する必要があります:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

がない場合にレプリカの瞬間に複製表を作成し、新しい最初のレプリカスのインスタンスが作成. がある場合でライブレプリカを新たなレプリカのクローンからデータを設定しています。 最初にすべての複製テーブルを作成し、それにデータを挿入するオプションがあります。 別のオプションを作れるレプリカを追加しその他の長期データを挿入出来ます。

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

ここでは、 [レプリケートされたmergetree](../engines/table-engines/mergetree-family/replication.md) テーブルエンジン。 パラメータを指定飼育係のパスを含むザ-シャープ識別子のことです。

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

複製はマルチマスターモードで動作します。 データは任意のレプリカにロードすることができ、システムは他のインスタンスと自動的に同期します。 複製は非同期で一定の瞬間にも、すべてのレプリカを含む場合があり、最近に挿入されます。 少なくとも一つのレプリカのようにするためのデータで測定す その同期データの修理整合性が活躍できます。 このアプローチでは、最近挿入されたデータの損失の可能性が低いことができます。

[元の記事](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
