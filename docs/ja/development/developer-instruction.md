---
slug: /ja/development/developer-instruction
sidebar_position: 61
sidebar_label: はじめに
description: ClickHouseのビルド方法の概要と前提条件について
---

# ClickHouseビルドのためのガイド

ClickHouseはLinux、FreeBSD、macOSでビルドできます。Windowsを使用している場合でも、Linuxを実行している仮想マシン（例：Ubuntuを使用した[VirtualBox](https://www.virtualbox.org/)）でClickHouseをビルドすることができます。

ClickHouseをコンパイルして実行するには64ビットシステムが必要です。32ビットシステムでは動作しません。

## GitHubでのリポジトリ作成 {#creating-a-repository-on-github}

ClickHouseの開発を開始するには、[GitHub](https://www.virtualbox.org/) アカウントが必要です。また、ローカルでSSHキーの生成（未生成の場合）および公開キーをGitHubにアップロードすることも、パッチを提供するための前提条件です。

次に、パーソナルアカウントで[ClickHouseリポジトリ](https://github.com/ClickHouse/ClickHouse/)をフォークするには、右上にある"フォーク" ボタンをクリックしてください。

たとえば、問題の修正や機能の提供を貢献するには、フォークされたリポジトリのブランチに変更をコミットし、その変更をメインリポジトリに"プルリクエスト"として作成してください。

Gitリポジトリを操作するために、`git`をインストールしてください。Ubuntuでは、ターミナルで以下のコマンドを実行します：

```sh
sudo apt update
sudo apt install git
```

Gitの使用に関するチートシートは[こちら](https://education.github.com/git-cheat-sheet-education.pdf)にあります。Gitの詳細なマニュアルは[こちら](https://git-scm.com/book/en/v2)をご覧ください。

## 開発マシンへのリポジトリのクローン {#cloning-a-repository-to-your-development-machine}

まず、作業マシンにソースファイルをダウンロードします。つまり、リポジトリをクローンします：

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # プレースホルダーをあなたのGitHubユーザ名で置き換えてください
cd ClickHouse
```

このコマンドは、ClickHouseのソースコードを含むディレクトリ`ClickHouse/`を作成します。URLの後にカスタムチェックアウトディレクトリを指定することもできますが、このパスに空白が含まれていないことが重要です。後でビルドに問題を引き起こす可能性があります。

ClickHouseリポジトリには、Gitサブモジュール、つまり、外部リポジトリ（通常、ClickHouseで使用されるサードパーティライブラリ）への参照が使用されています。これらはデフォルトではチェックアウトされません。チェックアウトするには、次のいずれかを実行します：

- `--recurse-submodules` オプションを付けて`git clone`を実行します。

- `git clone`がサブモジュールをチェックアウトしなかった場合、`git submodule update --init --jobs <N>`（例：`<N> = 12`と指定するとチェックアウトの並列化が可能）を実行し、前述のと同様の結果を得ることもできます。

- `git clone`がサブモジュールをチェックアウトしなかった場合、不要なファイルや履歴を省いてサブモジュールをチェックアウトするために[sparse](https://github.blog/2020-01-17-bring-your-monorepo-down-to-size-with-sparse-checkout/)および[shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/)チェックアウトを使用する場合は、`./contrib/update-submodules.sh`を実行します。一般的には、サブモジュールの操作が不便で遅くなるため、お勧めしません。

Gitの状態を確認するためには、以下のコマンドを実行します：`git submodule status`。

次のエラーメッセージが表示された場合：

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

一般に、GitHubに接続するためのSSHキーが不足していることを示しています。これらのキーは通常、`~/.ssh`にあります。SSHキーが受け入れられるためには、GitHubの設定でこれをアップロードする必要があります。

httpsプロトコルを介してリポジトリをクローンすることもできます：

    git clone --recursive --shallow-submodules https://github.com/ClickHouse/ClickHouse.git

しかし、この方法では変更をサーバに送信することはできません。一時的に使用し、SSHキーを追加して、`git remote` コマンドを使用してリポジトリのリモートアドレスを置き換えることができます。

作業中のローカルリポジトリにオリジナルのClickHouseリポジトリアドレスを追加して、そこから更新を引き出すこともできます：

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

このコマンドが正常に実行された後、`git pull upstream master`を実行することで、メインのClickHouseリポジトリから更新を引き出すことができます。

:::note 
以下の説明は、Linuxでビルドしていること前提としています。クロスコンパイルやmacOSでのビルドを行う場合、特定のガイドも確認してください。例えば、[macOSでのビルド](build-osx.md)、[LinuxでのmacOS向けビルド](build-cross-osx.md)、[LinuxでのLinux/RISC-V向けビルド](build-cross-riscv.md)、[LinuxでのLinux/LoongArch向けビルド](build-cross-loongarch.md)などです。
:::

## ビルドシステム {#build-system}

ClickHouseはビルドにCMakeとNinjaを使用します。

- CMake - Ninjaファイル（ビルドタスク）を生成するメタビルドシステム。

- Ninja - それらのcmake生成タスクを実行するためのスピードに焦点を当てた小規模なビルドシステム。

- ccache - コンパイラのキャッシュ。以前のコンパイルをキャッシュし、同じコンパイルが再度行われるときにそれを検出して再コンパイルを高速化します。

:::tip
ccacheの代替として、[sccache](https://github.com/mozilla/sccache)を使用することもできます。それを使用するには、`-DCOMPILER_CACHE=sccache` CMakeフラグを使用してください。
:::

Ubuntu、DebianまたはMintの場合、`sudo apt install cmake ninja-build ccache`を実行してインストールします。

CentOS、RedHatの場合、`sudo yum install cmake ninja-build ccache`を実行します。

ArchまたはGentooを使用している場合、CMakeおよびその他のインストール方法を知っていますでしょう。

## C++コンパイラ {#c-compiler}

ClickHouseのビルドには、Clangバージョン16からのコンパイラをサポートしています。

Clangはgccの代わりに使用されるべきです。ただし、我々の継続的インテグレーション（CI）プラットフォームでは、約12種類のビルドの組み合わせをチェックしています。

Ubuntu/Debianでは、（公式ウェブページを確認してください）[公式ウェブページ](https://apt.llvm.org/)の自動インストールスクリプトを使用できます。

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## ビルドプロセス {#the-building-process}

ClickHouseをビルドする準備ができたら、`ClickHouse`内にビルド成果物をすべて含む別のディレクトリ`build`を作成することをお勧めします：

    mkdir build
    cd build

異なるビルドタイプのために複数のディレクトリを持つことができます（例：build_release、build_debug等）。

`build`ディレクトリ内で、最初にコンパイラを指定する環境変数を定義した後、CMakeを実行してビルドを構成します。

    export CC=clang CXX=clang++
    cmake ..

上の自動インストールスクリプトを使用してclangをインストールした場合は、最初のコマンド内でインストールされたclangのバージョンも指定してください。例：`export CC=clang-18 CXX=clang++-18`。clangのバージョンはスクリプトの出力に表示されます。

`CC`変数はCコンパイラを指定し、`CXX`変数はビルドに使用するC++コンパイラを指定します。

より速いビルドのために、最適化なしのデバッグビルドタイプを使用することができます。その場合、以下のパラメータを指定してください：`-D CMAKE_BUILD_TYPE=Debug`：

    cmake -D CMAKE_BUILD_TYPE=Debug ..

このコマンドを`build`ディレクトリ内で実行することにより、ビルドタイプを変更することができます。

ninjaを実行してビルドします：

    ninja clickhouse-server clickhouse-client

この例では、必要なバイナリのみがビルドされます。

すべてのバイナリ（ユーティリティやテスト）をビルドする必要がある場合は、パラメータなしでninjaを実行します：

    ninja

完全なビルドには約30GBの空きディスクスペースが必要で、メインバイナリをビルドするためには約15GBが必要です。

十分な量のRAMがビルドマシンにある場合は、`-j`パラメータで同時に実行されるビルドタスクの数を制限する必要があります：

    ninja -j 1 clickhouse-server clickhouse-client

4GBのRAMを持つマシンでは、1を指定することが推奨され、8GBのRAMでは`-j 2`を指定することが推奨されます。

メッセージ`ninja: error: loading 'build.ninja': No such file or directory`を受けた場合、ビルド構成の生成が失敗していることを意味し、上記のメッセージを確認する必要があります。

ビルドプロセスが正常に開始されると、処理されたタスクの数とタスクの総数を示すビルド進行状況が表示されます。

ビルド中にLLVMライブラリに関するメッセージが表示されることがあります。これは何も影響せず、安全に無視することができます。

ビルドが正常に完了すると、実行可能ファイル`ClickHouse/<build_dir>/programs/clickhouse`が得られます：

    ls -l programs/clickhouse

### 高度なビルドプロセス {#advanced-building-process}

#### 最小ビルド {#minimal-build}

サードパーティライブラリによる機能に興味がない場合、ビルドをさらに高速化するために、`cmake`オプションを使用できます。

```
cmake -DENABLE_LIBRARIES=OFF
```

開発オプションに関して問題が発生した場合、自己責任です。

#### Rustサポート {#rust-support}

Rustはインターネット接続が必要で、接続がない場合はRustサポートを無効にすることができます：

```
cmake -DENABLE_RUST=OFF
```

## ClickHouseの実行可能ファイルの実行 {#running-the-built-executable-of-clickhouse}

現在のユーザーでサーバーを実行するには、`ClickHouse/programs/server/`に移動して実行します（`build`の外にあります）：

    ../../build/programs/clickhouse server

この場合、ClickHouseは現在のディレクトリにある設定ファイルを使用します。任意のディレクトリから実行し、設定ファイルへのパスをコマンドラインパラメータ`--config-file`として指定できます。

別のターミナルでclickhouse-clientを使用してClickHouseに接続するには、`ClickHouse/build/programs/`に移動し、`./clickhouse client`を実行します。

macOSまたはFreeBSDで`Connection refused`メッセージが表示された場合、ホストアドレス127.0.0.1を指定してみてください：

    clickhouse client --host 127.0.0.1

システムにインストールされたClickHouseバイナリを、カスタムビルドしたClickHouseバイナリで置き換えることができます。これを行うには、公式ウェブサイトの指示に従い、マシンにClickHouseをインストールしてください。その後、以下を実行します：

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

`clickhouse-client`、`clickhouse-server`などは、共通の`clickhouse`バイナリへのシンボリックリンクであることに注意してください。

また、システムにインストールされたClickHouseパッケージの設定ファイルを使用してカスタムビルドしたClickHouseバイナリを実行することもできます：

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

## IDE（統合開発環境） {#ide-integrated-development-environment}

**CLion（推奨）**

どのIDEを使用するか分からない場合、[CLion](https://www.jetbrains.com/clion/) を使用することをお勧めします。CLionは商用ソフトウェアですが、30日間の無料トライアルを提供しています。また、学生は無料で利用できます。CLionはLinuxとmacOSの両方で使用できます。

CLionを使用してClickHouseを開発する際に知っておくべきこと：

- CLionは独自に`build`パスを作成し、ビルドタイプとして自動的に`debug`を選択します。
- 使用するCMakeのバージョンは、インストールしたものではなく、CLion内で定義されているものです。
- CLionは`ninja`ではなく`make`を使用してビルドタスクを実行します（これは通常の動作です）。

**その他の代替IDE**

[QTCreator](https://www.qt.io/product/development-tools) および [KDevelop](https://kdevelop.org/) は、ClickHouse開発のための他の素晴らしい代替IDEです。KDevelopは優れたIDEですが、時々不安定になることがあります。プロジェクトを開く際にKDevelopがクラッシュした場合、プロジェクトのファイルリストが開かれたらすぐに「すべてを停止」ボタンをクリックする必要があります。これを行うと、KDevelopは問題なく動作するはずです。

その他のIDEとしては、[Sublime Text](https://www.sublimetext.com/)、[Visual Studio Code](https://code.visualstudio.com/)、または[Kate](https://kate-editor.org/) （いずれもLinuxで利用可能）を使用できます。VS Codeを使用している場合、IntelliSenseよりもパフォーマンスが大幅に向上するため、[clangd拡張](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd)の使用をお勧めします。

## コードを書く {#writing-code}

ClickHouseのためにコードを書く際に役立つクイックリンクを以下に示します：

- [ClickHouseアーキテクチャの説明](https://clickhouse.com/docs/ja/development/architecture/)。
- [コードスタイルガイド](https://clickhouse.com/docs/ja/development/style/)。
- [サードパーティライブラリの追加](https://clickhouse.com/docs/ja/development/contrib/#adding-third-party-libraries)。
- [テストを書く](https://clickhouse.com/docs/ja/development/tests/)。
- [オープンな問題のリスト](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)。

## ドキュメントを書く {#writing-documentation}

新機能を追加するプルリクエストの一環として、その機能のドキュメントを書く必要があります。ドキュメントの変更をプレビューしたい場合、ローカルでドキュメントページをビルドするための手順が[こちらのREADME.mdファイル](https://github.com/ClickHouse/clickhouse-docs)にあります。ClickHouseに新しい関数を追加する際には、以下のテンプレートをガイドとして使用できます：

```markdown
# newFunctionName

ここに関数の簡単な説明を記載します。関数の機能と典型的な使用例について簡単に説明してください。

**Syntax**

\```sql
newFunctionName(arg1, arg2[, arg3])
\```

**Arguments**

- `arg1` — 引数の説明。[DataType](../data-types/float.md)
- `arg2` — 引数の説明。[DataType](../data-types/float.md)
- `arg3` — オプションの引数の説明（オプション）。[DataType](../data-types/float.md)

**Implementation Details**

関連がある場合は、実装の詳細について説明します。

**Returned value**

- {関数が返す内容をここに記載します}。[DataType](../data-types/float.md)

**Example**

クエリ:

\```sql
SELECT 'write your example query here';
\```

レスポンス:

\```response
┌───────────────────────────────────┐
│ the result of the query           │
└───────────────────────────────────┘
\```
```

## テストデータ {#test-data}

ClickHouseの開発では、現実的なデータセットのロードが頻繁に必要です。これは特にパフォーマンステストに重要です。我々は特別に準備されたWebアナリティクスの匿名データセットを持っています。このデータのロードには追加で約3GBの空きディスクスペースが必要です。このデータはほとんどの開発作業には必須ではありません。

    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE DATABASE IF NOT EXISTS test

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID(UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType(UInt16,  ClickTargetPhraseID(UInt64,  ClickContextType(UInt8,  ClickSelectType(Int8,  ClickOptions String,  ClickGroupBannerID(Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID(String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign(String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits(UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash(UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain(UInt64,  URLScheme(UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash(UInt64,  OpenstatSourceIDHash(UInt64,  UTMSourceHash(UInt64,  UTMMediumHash(UInt64,  UTMCampaignHash(UInt64,  UTMContentHash(UInt64,  UTMTermHash(UInt64,  FromHash(UInt64,  WebVisorEnabled(UInt8,  WebVisorActivity(UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2(Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID(Array(String),  `Market.OrderPrice(Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID(Array(UInt32),  `Market.DirectBannerID(Array(UInt32),  `Market.GoodID(Array(String),  `Market.GoodName(Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice(Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

## プルリクエストの作成 {#creating-pull-request}

GitHubのUIでフォークリポジトリに移動します。他のブランチで開発している場合は、そのブランチを選択する必要があります。"プルリクエスト"ボタンが画面にあります。これは本質的に「メインリポジトリへの変更を受け入れるためのリクエストを作成する」という意味です。

作業が完了していない場合でも、プルリクエストを作成できます。この場合、タイトルの冒頭に「WIP」（作業中）という単語を置いてください。これは後で変更できます。変更の共同レビューや議論、および利用可能なすべてのテストの実行に役立ちます。変更の簡単な説明を必ず提供してください。これは後でリリースのチェンジログを生成する際に使用されます。

ClickHouseの社員がプルリクエストに"テスト可能"のタグを付けるとすぐにテストが開始されます。最初のいくつかのチェックの結果（例：コードスタイル）は数分以内に届きます。ビルドチェック結果は30分以内に届きます。主要なテストセットは1時間以内に報告されます。

システムはプルリクエストごとにClickHouseバイナリビルドを個別に準備します。[詳細]リンクをクリックすると、「ビルド」のエントリの横にあるチェックリストが表示されます。そこにはビルトされた.debパッケージの直リンクがあり、必要に応じてプロダクションサーバーにデプロイすることもできます（恐れない場合）。

最初の試みでは、因るところ多くのビルドが失敗することがあります。我々はgccとclangの両方でビルドをチェックし、clangではほぼすべての警告（常に`-Werror`フラグ）を有効にしています。その同じページで、すべてのビルドログが見つかるので、全ての可能な方法でClickHouseをビルドする必要はありません。

## ClickHouseソースコードを見る {#browse-clickhouse-source-code}

GitHubの統合コードブラウザを[こちらで](https://github.dev/ClickHouse/ClickHouse)使用することができます。

また、通常のように[GitHub](https://github.com/ClickHouse/ClickHouse)でソースを閲覧することもできます。
