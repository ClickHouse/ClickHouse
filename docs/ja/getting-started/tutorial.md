---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 12
toc_title: "\u30C1\u30E5\u30FC\u30C8\u30EA"
---

# ClickHouseチュートリアル {#clickhouse-tutorial}

## このチュートリアルから何を期待する？ {#what-to-expect-from-this-tutorial}

このチュートリアルでは、単純なClickHouseクラスターを設定する方法について説明します。 それは小さいが、耐障害性とスケーラブルになります。 次に、例のデータセットのいずれかを使用してデータを入力し、いくつかのデモクエリを実行します。

## 単一ノードの設定 {#single-node-setup}

分散環境の複雑さを先送りするために、単一のサーバーまたは仮想マシンにClickHouseを展開することから始めます。 ClickHouseは通常、 [deb](install.md#install-from-deb-packages) または [rpm](install.md#from-rpm-packages) パッケージが、あります [代替案](install.md#from-docker-image) の営業システムな支援します。

たとえば、次のように選択します `deb` パッケージと実行:

``` bash
{% include 'install/deb.sh' %}
```

インストールされたパッケージには何がありますか:

-   `clickhouse-client` パッケージ [clickhouse-クライアント](../interfaces/cli.md) 適用、相互ClickHouseコンソール顧客。
-   `clickhouse-common` パッケージが含まれてClickHouse実行可能ファイルです。
-   `clickhouse-server` パッケージを含む設定ファイルを実行ClickHouseしています。

サーバー設定ファイルは `/etc/clickhouse-server/`. 更に行く前に、気づいて下さい `<path>` の要素 `config.xml`. Pathはデータストレージの場所を決定するため、ディスク容量の大きいボリューム上に配置する必要があります。 `/var/lib/clickhouse/`. 設定を調整したい場合は、直接編集するのは便利ではありません `config.xml` ファイルで、このように書き換え、将来のパッケージです。 のオーバーライドは、config要素の作成 [config内のファイル。dディレクトリ](../operations/configuration-files.md) として役立つ “patches” 設定する。xml。

お気づきの通り, `clickhouse-server` が開始後、自動的にパッケージ設置できます。 更新後も自動的に再起動されることはありません。 サーバーの起動方法はinitシステムによって異なります。:

``` bash
sudo service clickhouse-server start
```

または

``` bash
sudo /etc/init.d/clickhouse-server start
```

サーバーログの既定の場所 `/var/log/clickhouse-server/`. サーバーは、クライアント接続を処理する準備ができています。 `Ready for connections` メッセージ

一度 `clickhouse-server` 稼働中であれば `clickhouse-client` サーバーに接続し、次のようなテストクエリを実行するには `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Clickhouse-クライアントのクイックヒント</summary>

対話モード:

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

指定した形式でファイルからデータを挿入する:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## インポート {#import-sample-dataset}

今度は、ClickHouseサーバーにサンプルデータを入力します。 このチュートリアルでは、Yandexの匿名化されたデータを使用します。Metrica、それがオープンソースになる前に、本番の方法でClickHouseを実行する最初のサービス（その詳細については [歴史セクション](../introduction/history.md)). そこには [Yandexのをインポートする複数の方法。メトリカデータセット](example-datasets/metrica.md) そして、チュートリアルのために、私たちは最も現実的なものに行きます。

### 表データのダウンロードと抽出 {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

抽出されたファイルのサイズは約10GBです。

### テーブルの作成 {#create-tables}

ほとんどのデータベース管理システムClickHouse論理的にグテーブル “databases”. そこには `default` データベースが、我々は名前の新しいものを作成します `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

テーブルを作成するための構文は、データベースに比べて複雑です(参照 [参照](../sql-reference/statements/create.md). 一般に `CREATE TABLE` 声明を設定するつもの:

1.  作成するテーブルの名前。
2.  Table schema, i.e. list of columns and their [データ型](../sql-reference/data-types/index.md).
3.  [表エンジン](../engines/table-engines/index.md) このテーブルへのクエリが物理的に実行される方法に関するすべての詳細を決定します。

Yandex.Metricaはweb分析サービスであり、サンプルデータセットは完全な機能をカバーしていないため、作成するテーブルは二つしかありません:

-   `hits` とができるテーブルの各行動によるすべてのユーザーはすべてのwebサイトのサービスです。
-   `visits` はテーブルを含む組み立て済みセッションの代わりに個別に行動します。

これらのテーブルの実際のcreate tableクエリを見て実行しましょう:

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

ドできるようになりました。方のクエリのインタラクティブモードの `clickhouse-client` （事前にクエリを指定せずに端末で起動するだけです）またはいくつか試してみてください [代替インターフェ](../interfaces/index.md) 望むなら

ご覧のとおり, `hits_v1` を使用して [基本的なMergeTreeエンジン](../engines/table-engines/mergetree-family/mergetree.md) は、 `visits_v1` を使用して [崩壊](../engines/table-engines/mergetree-family/collapsingmergetree.md) バリアント

### デー {#import-data}

ClickHouseへのデータのインポートは [INSERT INTO](../sql-reference/statements/insert-into.md) 他の多くのSQLデータベースと同様のクエリ。 ただし、データは通常、次のいずれかで提供されます [対応するシリアル化形式](../interfaces/formats.md) 代わりに `VALUES` 句(これもサポートされています)。

ファイルをダウンロード前にタブ区切り形式では下記のように輸入したいお客様のコンソール:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouseは多くの [調整する設定](../operations/settings/index.md) そして、コンソールクライアントでそれらを指定する一つの方法は、引数を介してです。 `--max_insert_block_size`. どのような設定が利用可能か、それらが何を意味し、デフォルトが何であるかを把握する最も簡単な方法は、 `system.settings` テーブル:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

必要に応じ [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) インポート後のテーブル。 MergeTree-familyのエンジンで構成されているテーブルは、常にバックグラウンドでデータ部分のマージを行い、データストレージを最適化します（または少なくとも理にか これらのクエリのテーブルエンジンな保管の最適化現在の代わりについては後日、:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

これらのクエリはI/OとCPUを大量に消費する操作を開始するため、テーブルが一貫して新しいデータを受信する場合は、そのままにして、マージをバックグ

今までチェックできる場合、テーブルの輸入に成功した:

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

## クラスター展開 {#cluster-deployment}

ClickHouseクラスターは均質なクラスターです。 設定手順:

1.  イClickHouseサーバーのすべての機械のクラスター
2.  構成ファイルでのクラスター設定の設定
3.  各インスタ
4.  作成 [分散テーブル](../engines/table-engines/special/distributed.md)

[分散テーブル](../engines/table-engines/special/distributed.md) 実際には “view” ClickHouseクラスタのローカルテーブルに。 SELECTクエリから分散型のテーブル実行が持つリソースを活用したすべてのクラスターの破片. を指定しますconfigs複数のクラスターを作成した複数のテーブルのビューを提供する別のクラスター

クラスターの設定例:

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

さらなる実証しましょう新しい地域のテーブルと同じ `CREATE TABLE` 私たちが使用したクエリ `hits_v1` しかし、異なるテーブル名:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

クラスターのローカルテーブルにビューを提供する分散テーブルの作成:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

一般的な方法は、クラスターのすべてのマシンで同様の分散テーブルを作成することです。 これは、クラスタの任意のマシン上で分散クエリを実行できます。 また、特定のSELECTクエリに対して一時的な分散テーブルを作成する別のオプションもあります [リモート](../sql-reference/table-functions/remote.md) テーブル関数。

走ろう [INSERT SELECT](../sql-reference/statements/insert-into.md) 分散テーブルにテーブルを複数のサーバーに分散させます。

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "通知"
    この方法は、大規模なテーブルのシャーディングには適していません。 別のツールがあります [クリックハウス-複写機](../operations/utilities/clickhouse-copier.md) 任意の大きなテーブルを再シャードできます。

予想されるように、計算上重いクエリは、3つのサーバーの代わりに使用するとn倍高速に実行されます。

この場合、3つのシャードを持つクラスターを使用し、それぞれに単一のレプリカが含まれています。

運用環境でレジリエンスを提供するには、各シャードに複数のアベイラビリティーゾーンまたはデータセンター(または少なくともラック)間に2-3個のレプリ ClickHouseでは、レプリカの数に制限はありません。

レプリカを含むシャードのクラスターの設定例:

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

ネイティブ複製を有効にする [飼育係](http://zookeeper.apache.org/) 必須です。 ClickHouseのデータの整合性はすべてのレプリカと回復手続き後の不動します。 別のサーバー(ClickHouseを含む他のプロセスが実行されていない場所)にZooKeeperクラスターを展開することをお勧めします。

!!! note "注"
    単純なケースでは、アプリケーションコードからすべてのレプリカにデータを書き込むことでデータを複製できます。 このアプローチは **ない** この場合、ClickHouseはすべてのレプリカでデータの一貫性を保証できません。 従ってそれはあなたの適用の責任になる。

ZooKeeperの場所は設定ファイルで指定されます:

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

また、テーブル作成時に使用する各シャードとレプリカを識別するためのマクロを設定する必要があります:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

がない場合にレプリカの瞬間に複製表を作成し、新しい最初のレプリカスのインスタンスが作成. がある場合でライブレプリカを新たなレプリカのクローンからデータを設定しています。 するオプションを複製のテーブル、データを挿入します。 別のオプションを作れるレプリカを追加しその他の長期データを挿入出来ます。

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

ここでは、 [複製マージツリー](../engines/table-engines/mergetree-family/replication.md) テーブルエンジン。 パラメータでは、シャードとレプリカ識別子を含むZooKeeper pathを指定します。

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

複製はマルチマスターモードで動作します。 データは任意のレプリカにロードでき、システムはそれを他のインスタンスと自動的に同期します。 複製は非同期で一定の瞬間にも、すべてのレプリカを含む場合があり、最近に挿入されます。 少なくとも一つのレプリカのようにするためのデータで測定す 他の人は、データを同期し、再びアクティブになると一貫性を修復します。 この方法では、最近挿入されたデータが失われる可能性が低いことに注意してください。

[元の記事](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
