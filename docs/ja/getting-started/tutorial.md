---
toc_priority: 12
toc_title: "チュートリアル"
---

# ClickHouse チュートリアル {#clickhouse-tutorial}

## このチュートリアルに期待されることは？ {#what-to-expect-from-this-tutorial}

このチュートリアルでは、単純なClickHouseクラスターを設定する方法について説明します。それは小規模ですが、耐障害性とスケーラブルになります。
次に、データセット例のいずれかを使用してデータを入力し、いくつかのデモクエリを実行します。

## 単一ノードの設定 {#single-node-setup}

分散環境は複雑なので、まずは単一のサーバーまたは仮想マシンにClickHouseを展開することから始めます。
ClickHouseは通常、[deb](../getting-started/install.md#install-from-deb-packages) or [rpm](../getting-started/install.md#from-rpm-packages) パッケージからインストールしますが、サポートされていないOSのために [代替案](install.md#from-docker-image) があります。

例えば、`deb` パッケージを選択して実行する場合:

``` bash
{% include 'install/deb.sh' %}
```

インストールされたパッケージの内容:

-   `clickhouse-client` パッケージには [clickhouse-client](../interfaces/cli.md) アプリケーション、インタラクティブな ClickHouse コンソールクライアントが含まれています。
-   `clickhouse-common` パッケージには、ClickHouse実行可能ファイルが含まれています。
-   `clickhouse-server` パッケージには、ClickHouseをサーバとして実行するための設定ファイルが含まれています。

サーバ設定ファイルは `/etc/clickhouse-server/` にあります。先に進む前に、`config.xml` の `<path>` 要素に注目してください。
パスはデータを保存する場所を決めるので、ディスク容量の大きいボリュームに配置する必要があります。デフォルト値は `/var/lib/clickhouse/` です。
設定を調整したい場合、`config.xml` ファイルを直接編集するのは不便です。`config.xml` の要素をオーバーライドするために推奨される方法は、`config.xml` の「パッチ」として機能する [config.d ディレクトリ内のファイル](../operations/configuration-files.md) を作成することです。

お気づきかもしれませんが、`clickhouse-server` は パッケージインストール後に自動的に起動されたり、アップデート後に自動的に再起動されることはありません。サーバの起動方法は init システムに依存しており、大抵は以下のようになっています:

``` bash
sudo service clickhouse-server start
```

または

``` bash
sudo /etc/init.d/clickhouse-server start
```

サーバログのデフォルトの場所は `/var/log/clickhouse-server/` です。サーバが クライアントからの接続の準備が整うと、`Ready for connections` メッセージをログに出力します。

一度 `clickhouse-server` を起動して実行すると、`clickhouse-client` を使ってサーバに接続し、`SELECT "Hello, world!";` のようなテストクエリを実行することができます。

<details markdown="1">

<summary>Clickhouse-クライアントのクイックtips</summary>

インタラクティブモード:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

複数行クエリを有効にする:

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

指定したフォーマットでファイルからデータを insert する:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## サンプルデータセットのインポート {#import-sample-dataset}

ClickHouseサーバーにいくつかのサンプルデータを入れてみましょう。
このチュートリアルでは、ClickHouseがオープンソースになる以前(詳しくは [歴史について](../introduction/history.md) を参照)に初めて本番用途で使われたサービスである、Yandex.Metricaの匿名化されたデータを使用します。

[Yandex.Metrica データセットをインポートするにはいくつかの方法](../getting-started/example-datasets/metrica.md) がありますが, チュートリアルとして、最もよく使う方法を使用します。

### テーブルデータのダウンロードと展開 {#download-and-extract-table-data}

``` bash
curl https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

展開されたファイルのサイズは約10GBです。

### テーブルの作成 {#create-tables}

ほとんどのデータベース管理システムのように, ClickHouseは論理的にテーブルを "データベース" にグループ化します。
`default` データベースがありますが、`tutorial` という名前の新しいものを作成します:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

テーブルを作成するための構文はデータベースに比べてはるかに複雑です([参照](../sql-reference/statements/create.md))。通常、`CREATE TABLE` 文は3つのキーを指定しなければなりません。

1.  作成するテーブルの名前。
2.  テーブルのスキーマ。つまり、カラムと [データ型](../sql-reference/data-types/index.md) のリスト。
3.  [テーブルエンジン](../engines/table-engines/index.md) と、このテーブルへのクエリが物理的に実行される方法に関するすべての詳細を決定する設定。

Yandex.Metricaはウェブ解析サービスであり、サンプルデータセットでは機能が網羅されていないため、作成するテーブルは2つしかありません:

-   `hits` はサービスの対象となるすべてのウェブサイト上ですべてのユーザが行った各アクションのテーブルです。
-   `visits` は、個々のアクションではなく、あらかじめ構築されたセッションを含むテーブルです。

これらのテーブルの実際の CREATE TABLE クエリを実行しましょう:

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

これらのクエリは、`clickhouse-client` の対話型モード(事前にクエリを指定せずにターミナルで起動するだけです)を使って実行するか、[代替インターフェイス](../interfaces/index.md) で実行できます。

見ての通り、 `hits_v1` は [基本的な MergeTree エンジン](../engines/table-engines/mergetree-family/mergetree.md) を使っており、`visits_v1` は [Collapsing MergeTree](../engines/table-engines/mergetree-family/collapsingmergetree.md) という変種を使っています。

### データのインポート {#import-data}

ClickHouseへのデータのインポートは、他の多くのSQLデータベースのように[INSERT INTO](../sql-reference/statements/insert-into.md)クエリを介して行われます。しかし、データは通常、`VALUES` 句 (これもサポートされています) の代わりに、[サポートされているシリアライズ形式](../interfaces/formats.md) のいずれかで提供されます。

先ほどダウンロードしたファイルはタブ区切り形式になっており、コンソールクライアントから次のようにインポートします:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouseには多くの [調整のための設定](../operations/settings/index.md) があり、コンソールクライアントで引数として指定する方法があります。どのような設定が利用可能で、それが何を意味し、デフォルトは何なのかを知る最も簡単な方法は、`system.settings` テーブルにクエリすることです:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

必要に応じて、インポート後のテーブルを[OPTIMIZE](../sql-reference/statements/optimize.md)することができます。
MergeTree-familyのエンジンで設定されているテーブルは、常にバックグラウンドでデータ部分のマージを行い、データストレージを最適化します(あるいは、少なくともそれが意味のあるものかどうかをチェックします)。

以下のクエリは、テーブルエンジンがストレージの最適化を後で行うのではなく、今すぐに行うように強制します:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

これらのクエリは、I/OとCPUに負荷のかかる処理を開始するので、テーブルが継続して新しいデータを受け取る場合には、そのままにしてバックグラウンドでマージを実行させた方が良いでしょう。

これで、テーブルのインポートが成功したかどうかを確認することができます:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## クエリの例 {#example-queries}

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

## クラスタのデプロイ {#cluster-deployment}

ClickHouseクラスタは均質なクラスタ(homogenous cluster)です。セットアップ手順は以下です:

1.  クラスタのすべてのマシンにClickHouse serverをインストールする
2.  設定ファイルにクラスタ設定を行う
3.   各インスタンスにローカルテーブルを作成する
4.  [分散テーブル](../engines/table-engines/special/distributed.md) を作成する

[分散テーブル](../engines/table-engines/special/distributed.md) は、
実際には ClickHouse クラスタのローカルテーブルへの "ビュー "のようなものです。
分散テーブルからの SELECT クエリは、すべてのクラスタのシャードのリソースを使用して実行されます。
また、複数のクラスタのための設定を行い、異なるクラスタにビューを提供する複数の分散テーブルを作成することができます。

3つのシャード、各1つのレプリカを持つクラスタの設定例:

``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

さらにデモンストレーションとして、`hits_v1` で使ったのと同じ `CREATE TABLE` クエリを使って新しいローカルテーブルを作成してみましょう:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

クラスタのローカルテーブルへのビューを提供する分散テーブルを作成します:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

通常は、クラスタのすべてのマシンに同様の分散テーブルを作成します。これにより、クラスタのどのマシンでも分散クエリを実行することができます。
また、[remote](../sql-reference/table-functions/remote.md) テーブル関数を使用して、与えられたSELECTクエリのための一時的な分散テーブルを作成する代替オプションもあります。

分散テーブルに [INSERT SELECT](../sql-reference/statements/insert-into.md)を実行して、テーブルを複数のサーバに分散させてみましょう。

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "注意"
    この方法は、大きなテーブルのシャーディングには適していません。
    別のツール [clickhouse-copier](../operations/utilities/clickhouse-copier.md) があり、任意の大きなテーブルを再シャーディングすることができます。

予想通り、計算量の多いクエリは、1台のサーバではなく3台のサーバを利用した方がN倍速く実行されます。

このケースでは、3つのシャードを持つクラスタを使用しており、それぞれに1つのレプリカが含まれています。

本番環境でレジリエンスを提供するためには、各シャードが複数のアベイラビリティゾーンまたはデータセンター(または少なくともラック)の間で2~3個のレプリカを含むことをお勧めします。

3つのレプリカを含む1つのシャードのクラスタの設定例:

``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

ネイティブレプリケーションを有効にするには [ZooKeeper](http://zookeeper.apache.org/) が必要です。
ClickHouseは全てのレプリカ上でデータの整合性を取り、障害発生時には自動的にリストア処理を行います。
ZooKeeperクラスタは別サーバ(ClickHouseを含む他のプロセスが稼働していない場所)に配置することをお勧めします。

ネイティブ複製を有効にする [飼育係](http://zookeeper.apache.org/) 必須です。 ClickHouseのデータの整合性はすべてのレプリカと回復手続き後の不動します。 別のサーバー(ClickHouseを含む他のプロセスが実行されていない場所)にZooKeeperクラスターを展開することをお勧めします。

!!! note "備考"
    ZooKeeperは厳密な要件ではありません: いくつかの簡単なケースでは、アプリケーションコードからすべてのレプリカにデータを書き込むことでデータを複製することができます。このケースにおいて、このアプローチは **お勧めできません**。ClickHouseはすべてのレプリカ上でデータの一貫性を保証することができません。したがって、それはあなたのアプリケーションの責任になります。

ZooKeeperの場所は設定ファイルで指定します:

``` xml
<zookeeper>
    <node>
        <host>zoo01</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03</host>
        <port>2181</port>
    </node>
</zookeeper>
```

また、テーブル作成時に使用する各シャードとレプリカを識別するためのマクロを設定する必要があります:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

レプリケートされたテーブルの作成時にレプリカが存在しない場合、新しい最初のレプリカがインスタンス化されます。既に有効なレプリカがある場合は、新しいレプリカが既存のレプリカからデータをクローンします。
最初にすべての複製されたテーブルを作成し、そこにデータを挿入する方法があります。別の方法として、いくつかのレプリカを作成して、データ挿入後またはデータ挿入中に他のレプリカを追加するという方法もあります。


``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

ここでは、[ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) テーブルエンジンを使用しています。
パラメータには、シャードとレプリカの識別子を含む ZooKeeper path を指定します。

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

レプリケーションはマルチマスターモードで動作します。
どのレプリカにもデータをロードすることができ、システムはそれを他のインスタンスと自動的に同期させます。
レプリケーションは非同期なので、ある時点ですべてのレプリカに最近挿入されたデータが含まれているとは限りません。
少なくとも1つのレプリカは、データの取り込みを可能にするために起動しておく必要があります。
他のレプリカはデータを同期させ、再びアクティブになると整合性を修復します。
この方法では、最近挿入されたデータが失われる可能性が低いことに注意してください。

[元の記事](https://clickhouse.com/docs/en/getting_started/tutorial/) <!--hide-->
