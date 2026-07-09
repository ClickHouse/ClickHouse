---
description: 'ClickHouse開発の前提条件とセットアップ手順'
sidebar_label: '前提条件'
sidebar_position: 5
slug: /development/developer-instruction
title: '開発者向け前提条件'
doc_type: 'guide'
---

ClickHouseはLinux、FreeBSD、macOS上でビルドできます。
Windowsを使用している場合でも、Ubuntuをインストールした[VirtualBox](https://www.virtualbox.org/)などのLinux仮想マシン上でClickHouseをビルドできます。

<div id="create-a-repository-on-github">
  ## GitHub でリポジトリを作成する
</div>

ClickHouse の開発を始めるには、[GitHub](https://www.github.com/) アカウントが必要です。
また、ローカルで SSHキー を生成し (まだ持っていない場合) 、その公開鍵を GitHub にアップロードしてください。これはパッチを投稿するための前提条件です。

次に、右上隅の &quot;fork&quot; ボタンをクリックして、自分のアカウントに [ClickHouse リポジトリ](https://github.com/ClickHouse/ClickHouse/) をフォークします。

変更を投稿するには、たとえばイシューの修正や機能追加の場合、まずフォーク内のブランチに変更をコミットし、その後、変更内容を含む &quot;プルリクエスト&quot; をメインリポジトリに対して作成します。

Git リポジトリを扱うには、Git をインストールしてください。たとえば Ubuntu では、次を実行します。

```sh
sudo apt update
sudo apt install git
```

Git のチートシートは[こちら](https://education.github.com/git-cheat-sheet-education.pdf)から参照できます。
Git の詳細なマニュアルは[こちら](https://git-scm.com/book/en/v2)です。

<div id="clone-the-repository-to-your-development-machine">
  ## リポジトリを開発マシンにクローンする
</div>

まず、リポジトリをクローンして、ソースファイルを作業用マシンにダウンロードします。

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

このコマンドは、ソースコード、テスト、その他のファイルを含む `ClickHouse/` ディレクトリを作成します。
URL の後にチェックアウト先としてカスタムディレクトリを指定できますが、このパスに空白文字を含めないことが重要です。空白が含まれていると、後でビルドが失敗する可能性があります。

ClickHouse の Git リポジトリでは、3rd party libraries を取り込むためにサブモジュールを使用しています。
サブモジュールはデフォルトではチェックアウトされません。
次のいずれかを実行できます

* `--recurse-submodules` オプションを付けて `git clone` を実行する

* `git clone` を `--recurse-submodules` なしで実行した場合は、`git submodule update --init --jobs <N>` を実行して、すべてのサブモジュールを明示的にチェックアウトする。(`\<N>` には、ダウンロードを並列化するために、たとえば `12` を設定できます。)

* `git clone` を `--recurse-submodules` なしで実行し、容量を節約するためにサブモジュール内の history を省く [shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/) なサブモジュールチェックアウトを使用したい場合は、`./contrib/update-submodules.sh` を実行する。この方法は CI で使用されていますが、サブモジュールの扱いが不便になり、動作も遅くなるため、ローカル開発には推奨されません。

Git サブモジュールのステータスを確認するには、`git submodule status` を実行します。

次の error メッセージが表示された場合

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

GitHub に接続するための SSHキー が見つかりません。
これらのキーは通常 `~/.ssh` にあります。
SSHキーを使用するには、GitHub の設定でアップロードする必要があります。

HTTPS 経由でリポジトリをクローンすることもできます:

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

ただし、この方法では変更内容をサーバーに送信することはできません。
それでも一時的には利用でき、後から `git remote` コマンドで repository のリモートアドレスを置き換えて、SSHキー を追加できます。

また、元の ClickHouse repo のアドレスをローカルの repository に追加して、そこから更新を pull することもできます:

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

このコマンドを正常に実行すると、`git pull upstream master` を実行して、ClickHouse のメインリポジトリから更新を取得できるようになります。

:::tip
単に `git push` を実行しないでください。誤ったリモートやブランチに push してしまうおそれがあります。
代わりに、`git push origin my_branch_name` のように、リモート名とブランチ名を明示的に指定することをおすすめします。
:::

<div id="writing-code">
  ## コードを書く
</div>

以下に、ClickHouse のコードを記述する際に役立つクイックリンクをいくつか示します。

* [ClickHouseアーキテクチャ](/ja/development/architecture/).
* [コードスタイルガイド](/ja/development/style/).
* [サードパーティライブラリ](/ja/development/contrib#adding-and-maintaining-third-party-libraries)
* [テストの作成](/ja/development/tests/)
* [未解決のIssue](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

<div id="ide">
  ### IDE
</div>

[Visual Studio Code](https://code.visualstudio.com/) と [Neovim](https://neovim.io/) は、これまで ClickHouse の開発で実績のある 2 つの選択肢です。VS Code を使用している場合は、IntelliSense の代わりに [clangd extension](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) を使うことをおすすめします。こちらのほうが大幅に高速だからです。

[CLion](https://www.jetbrains.com/clion/) も優れた代替候補です。ただし、ClickHouse のような大規模プロジェクトでは動作が遅くなることがあります。CLion を使う際は、次の点に注意してください。

* CLion は独自に `build` パスを作成し、ビルドタイプとして自動的に `debug` を選択します
* 使用する CMake は、ローカルにインストールしたものではなく、CLion で設定されているバージョンです
* CLion は `ninja` ではなく `make` を使ってビルドタスクを実行します (これは通常の動作です)

そのほかに使用できる IDE としては、[Sublime Text](https://www.sublimetext.com/)、[Qt Creator](https://www.qt.io/product/development-tools)、[Kate](https://kate-editor.org/) があります。

<div id="create-a-pull-request">
  ## プルリクエストを作成する
</div>

GitHub の UI で、自分のフォーク先リポジトリに移動します。
ブランチで作業していた場合は、そのブランチを選択する必要があります。
画面上に &quot;プルリクエスト&quot; ボタンがあります。
つまりこれは、「自分の変更をメインリポジトリに取り込んでもらうためのリクエストを作成する」という意味です。

作業がまだ完了していなくても、プルリクエストは作成できます。
その場合は、タイトルの先頭に &quot;WIP&quot; (作業中) を付けてください。後から変更できます。
これは、変更内容の共同レビューや議論に役立つほか、利用可能なすべてのテストを実行するためにも便利です。
変更内容の簡単な説明を必ず記載してください。これは後でリリースの変更履歴の生成に使用されます。

ClickHouse の担当者があなたの PR に &quot;can be tested&quot; というタグを付けると、テストが開始されます。
最初のいくつかのチェックの結果 (たとえばコードスタイル) は数分以内に返ってきます。
ビルドチェックの結果は 30 分以内に返ってきます。
主要なテスト一式の結果は 1 時間以内に報告されます。

システムは、あなたのプルリクエストごとに ClickHouse のバイナリビルドを個別に用意します。
これらのビルドを取得するには、チェック一覧で &quot;Builds&quot; エントリの横にある &quot;Details&quot; リンクをクリックしてください。
そこには、ビルドされた ClickHouse の .deb パッケージへの直接リンクがあります。必要であれば、本番サーバーにデプロイすることもできます (怖くなければですが) 。

<div id="write-documentation">
  ## ドキュメントを作成する
</div>

新機能を追加するすべてのプルリクエストには、適切なドキュメントを必ず含める必要があります。
ドキュメントの変更内容をプレビューしたい場合は、ドキュメントページをローカルでビルドする手順を [こちら](https://github.com/ClickHouse/clickhouse-docs) の README.md ファイルで確認できます。
ClickHouse に新しい関数を追加する場合は、以下のテンプレートを参考にしてください。

````markdown
# newFunctionName

A short description of the function goes here. It should describe briefly what it does and a typical usage case.

**Syntax**

\```sql
newFunctionName(arg1, arg2[, arg3])
\```

**Arguments**

- `arg1` — Description of the argument. [DataType](../data-types/float.md)
- `arg2` — Description of the argument. [DataType](../data-types/float.md)
- `arg3` — Description of optional argument (optional). [DataType](../data-types/float.md)

**Implementation Details**

A description of implementation details if relevant.

**Returned value**

- Returns {insert what the function returns here}. [DataType](../data-types/float.md)

**Example**

\```sql title="Query"
SELECT 'write your example query here';
\```

\```response title="Response"
┌───────────────────────────────────┐
│ the result of the query           │
└───────────────────────────────────┘
\```
````

<div id="using-test-data">
  ## テストデータの使用
</div>

ClickHouse の開発では、実際に近いデータセットを読み込む必要が生じることがよくあります。
これは特に性能テストにおいて重要です。
そのため、匿名化した web analytics 用の特別なデータセットを用意しています。
これには、追加で約 3GB の空きディスク容量が必要です。

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

clickhouse-client では:

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

データをインポートします:

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```