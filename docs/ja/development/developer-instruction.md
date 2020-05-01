---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 61
toc_title: "\u521D\u5FC3\u8005\u306E\u65B9ClickHouse\u958B\u767A\u8005\u306E\u6307\
  \u793A"
---

ClickHouseの建物は、Linux、FreeBSDおよびMac OS Xでサポートされています。

# Windowsを使用する場合 {#if-you-use-windows}

Windowsを使用する場合は、Ubuntuで仮想マシンを作成する必要があります。 するのは、仮想マシンをインストールしてくださいVirtualBox. ダウンロードできますUbuntuのウェブサイト：https://www.ubuntu.com/\#download. を作成してください仮想マシンからダウンロードした画像を保少なくとも4GB RAMめます。 Ubuntuでコマンドライン端末を実行するには、その単語を含むプログラムを探してください “terminal” その名前（gnome端末、konsoleなど）で）またはCtrl+Alt+Tを押すだけです。

# 32ビットシステムを使用する場合 {#if-you-use-a-32-bit-system}

ClickHouseは32ビットシステム上で動作または構築することはできません。 きの獲得へのアクセスでは、64ビットのシステムを継続できる。

# GitHubでのリポジトリの作成 {#creating-a-repository-on-github}

ClickHouseリポジトリでの作業を開始するには、GitHubアカウントが必要です。

きょうてつもない場合は、ご登録が必要でhttps://github.com. についてsshキーを押すと、すべき操作を行うというテーマをアップロードしてgithub. それはあなたのパッチを送信するために必要です。 他のsshサーバーと同じsshキーを使用することも可能です。

ClickHouseリポジトリのフォークを作成します。 それを行うにはクリックしてください “fork” 右上のボタンhttps://github.com/ClickHouse/ClickHouse.それはフォークあなた自身のコピーのClickHouse/ClickHouseにあなたのアカウント。

開発プロセスは、最初に意図した変更をclickhouseのフォークにコミットし、次に “pull request” これらの変更がメインリポジトリ（ClickHouse/ClickHouse）に受け入れられるために。

Gitリポジトリで作業するには `git`.

Ubuntuでこれを行うには、コマンドラインターミナルで実行します:

    sudo apt update
    sudo apt install git

Gitの使用に関する簡単なマニュアルは、ここにあります：https://services.github.com/on-demand/downloads/github-git-cheat-sheet.pdf。
Gitの詳細なマニュアルについては、https://git-scm.com/book/en/v2。

# 開発マシンへのリポジトリのクローン作成 {#cloning-a-repository-to-your-development-machine}

次に、ソースファイルを作業マシンにダウンロードする必要があります。 これは “to clone a repository” 作業マシン上にリポジトリのローカルコピーを作成するためです。

コマンドライン端末で:

    git clone --recursive git@guthub.com:your_github_username/ClickHouse.git
    cd ClickHouse

注:ください、代用 *your\_github\_username* 適切なもので！

このコマ `ClickHouse` プロジェクトの作業コピーを含む。

作業ディレクトリへのパスには、ビルドシステムの実行に問題が生じる可能性があるため、空白が含まれていないことが重要です。

ごclickhouseリポジトリ用 `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` 上記の例のようにフラグ。 場合のリポジトリにてクローニングなsubmodules、ダウンロードを実行する必要がありますの:

    git submodule init
    git submodule update

ステータスは次のコマンドで確認できます: `git submodule status`.

次のエラーメッセージが表示された場合:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

一般的には、githubに接続するためのsshキーがないことを意味します。 これらのキーは普通あります `~/.ssh`. SSHキーを受け入れるには、GitHub UIの設定セクションにそれらをアップロードする必要があります。

またクローンをリポジトリによhttpsプロトコル:

    git clone https://github.com/ClickHouse/ClickHouse.git

ただし、これにより、変更をサーバーに送信することはできません。 一時的に使用して、後でリポジトリのリモートアドレスを置き換えるsshキーを追加することはできます `git remote` 司令部

元のclickhouseレポのアドレスをローカルリポジトリに追加して、そこから更新を取得することもできます:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

このコマンドを正常に実行すると、メインのclickhouseレポから更新をプルすることができます `git pull upstream master`.

## サブモジュールの操作 {#working-with-submodules}

Gitでサブモジュールを操作するのは苦痛です。 次のコマンドは管理に役立ちます:

    # ! each command accepts --recursive
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

次のコマンドは、すべてのサブモジュールを初期状態にリセットするのに役立ちます（！警告！ -内部の変更は削除されます):

    # Synchronizes submodules' remote URL with .gitmodules
    git submodule sync --recursive
    # Update the registered submodules with initialize not yet initialized
    git submodule update --init --recursive
    # Reset all changes done after HEAD
    git submodule foreach git reset --hard
    # Clean files from .gitignore
    git submodule foreach git clean -xfd
    # Repeat last 4 commands for all submodule
    git submodule foreach git submodule sync --recursive
    git submodule foreach git submodule update --init --recursive
    git submodule foreach git submodule foreach git reset --hard
    git submodule foreach git submodule foreach git clean -xfd

# ビルドシステム {#build-system}

ClickHouseはCMakeとNinjaを使用して建物を建てます。

CMake-Ninjaファイル（ビルドタスク）を生成できるメタビルドシステム。
Ninja-これらのcmake生成タスクを実行するために使用される速度に焦点を当てた小さなビルドシステム。

Ubuntu、DebianまたはMint runにインストールするには `sudo apt install cmake ninja-build`.

セントスでは、redhatは実行します `sudo yum install cmake ninja-build`.

ArchまたはGentooを使用している場合は、CMakeをインストールする方法を知っているでしょう。

のためのcmakeおよび忍者mac os x初めて自作としてインストールインストールさんによbrew:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

次に、cmakeのバージョンを確認します: `cmake --version`. 場合は以下の3.3v、必要新しいバージョンからのウェブサイト：https://cmake.org/download/.

# 省略可能な外部ライブラリ {#optional-external-libraries}

ClickHouse複数の外部ライブラリのためのビルです。 それらのすべては、サブモジュールにあるソースからClickHouseと一緒に構築されるので、別々にインストールする必要はありません。 リストをチェックすることができ `contrib`.

# C++コンパイラ {#c-compiler}

バージョン9とclangバージョン8以上から始まるコンパイラgccは、clickhouseの構築に対応しています。

公式のyandexビルドは、現在、gccを使用しています。 そしてclangは開発のために通常より便利です。 が、当社の継続的インテグレーション（ci)プラットフォームを運チェックのための十数の組み合わせとなります。

UbuntuにGCCをインストールするには: `sudo apt install gcc g++`

Gccのバージョンを確認する: `gcc --version`. それが9以下の場合は、ここで指示に従ってください：https://clickhouse。テック/ドキュメント/en/開発/ビルド/\#インストール-gcc-9.

Mac OS XのビルドはClangでのみサポートされています。 ちょうど実行 `brew install llvm`

Clangを使用する場合は、インストールすることもできます `libc++` と `lld` くださるのです。 を使用して `ccache` また、推奨されます。

# 建築プロセス {#the-building-process}

ClickHouseをビルドする準備ができたら、別のディレクトリを作成することをお勧めします `build` 中 `ClickHouse` それはすべてのビルドの成果物が含まれています:

    mkdir build
    cd build

いくつかの異なるディレクトリ（build\_release、build\_debugなど）を持つことができます。）ビルドの異なるタイプのために。

内部にいる間 `build` ディレクトリ、CMakeを実行してビルドを設定します。 最初の実行の前に、コンパイラ（この例ではバージョン9gccコンパイラ）を指定する環境変数を定義する必要があります。

Linux:

    export CC=gcc-9 CXX=g++-9
    cmake ..

Mac OS X:

    export CC=clang CXX=clang++
    cmake ..

その `CC` variableはCのコンパイラ(Cコンパイラの略)を指定する。 `CXX` 変数は、どのC++コンパイラをビルドに使用するかを指示します。

より速いビルドのために、あなたは `debug` ビルドタイプ-最適化のないビルド。 その供給のために次のパラメータ `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

このコマンドを実行することで、ビルドのタイプを変更できます。 `build` ディレクトリ。

ビルドするために忍者を実行:

    ninja clickhouse-server clickhouse-client

この例では、必要なバイナリのみがビルドされます。

すべてのバイナリ（ユーティリティとテス:

    ninja

フルビルドでは、メインバイナリを構築するために約30gbの空きディスク容量または15gbが必要です。

ビルドマシンで大量のramが利用可能な場合は、並列に実行されるビルドタスクの数を制限する必要があります `-j` パラメータcomment:

    ninja -j 1 clickhouse-server clickhouse-client

RAMが4GBのマシンでは、1をRAM8GBに指定することをお勧めします `-j 2` は推奨。

メッセージが表示された場合: `ninja: error: loading 'build.ninja': No such file or directory` これは、ビルド構成の生成に失敗し、上記のメッセージを調べる必要があることを意味します。

ビルドプロセスが正常に開始されると、ビルドの進行状況、つまり処理されたタスクの数とタスクの総数が表示されます。

ながらメッセージについてprotobufファイルlibhdfs2図書館のような `libprotobuf WARNING` 現れるかもしれない 彼らは何にも影響せず、無視されても安全です。

成功を構築するの実行ファイル `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# ClickHouseの構築された実行可能ファイルを実行する {#running-the-built-executable-of-clickhouse}

のサーバーの現在のユーザーに必要なナビゲート `ClickHouse/programs/server/` (の外にあります `build`)と実行:

    ../../../build/programs/clickhouse server

この場合、clickhouseは現在のディレクトリにある設定ファイルを使用します。 実行することができ `clickhouse server` からのディレクトリのパスを指定し、設定ファイルとしてコマンドラインパラメータ `--config-file`.

別のターミナルのclickhouse-clientでclickhouseに接続するには、次のように移動します `ClickHouse/build/programs/` と実行 `clickhouse client`.

あなたが得れば `Connection refused` Mac OS XまたはFreeBSDで、ホストアドレス127.0.0.1を指定してみてください:

    clickhouse client --host 127.0.0.1

に置き換えることができ生産版clickhouseバイナリインストールされるシステムのカスタム構築clickhouseバイナリー. これを行うには、公式サイトの指示に従ってマシンにclickhouseをインストールします。 次に、以下を実行します:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

それに注意 `clickhouse-client`, `clickhouse-server` どsymlinksの共通 `clickhouse` バイナリ

を運営することも可能ですカスタム構築clickhouseバイナリのコンフィグファイルからのclickhouseパッケージをインストールシステム:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE(統合開発環境) {#ide-integrated-development-environment}

使用するideがわからない場合は、clionを使用することをお勧めします。 clionは商用ソフトウェアですが、30日間の無料試用期間を提供しています。 また、学生のための無料です。 clionは、linuxとmac os xの両方で使用できます。

KDevelopとQTCreatorは、ClickHouseを開発するためのIDEの他の優れた選択肢です。 KDevelopは非常に便利なIDEとして提供されますが、不安定です。 プロジェクトを開いてしばらくしてからKDevelopがクラッシュした場合は、 “Stop All” プロジェクトのファイルのリストを開くとすぐにボタンを押します。 その後、KDevelopはうまく動作するはずです。

シンプルなコードエディタとして、sublime textまたはvisual studio code、またはkate（すべてlinuxで利用可能）を使用できます。

その場合には、clionが `build` パスはそれ自身で、それはまたそれ自身で選択します `debug` ビルドタイプの場合は、設定のために、あなたがインストールしたものではなく、CLionで定義されたバージョンのCMakeを使用し、最後にCLionが使用します `make` ビルドタスクを実行するには `ninja`. これは通常の動作ですが、混乱を避けるためにそれを念頭に置いてください。

# コードの記述 {#writing-code}

ClickHouseの建築の記述はここに見つけることができる:https://clickhouse。技術/ドキュメント/en/開発/アーキテクチャ/

コードスタイルガイド：https://clickhouse。テック/ドキュメント/en/開発/スタイル/

書き込みテスト:https://clickhouse。技術/ドキュメント/en/開発/テスト/

タスクのリスト：https://github.com/clickhouse/clickhouse/blob/master/testsructions/easy\_tasks\_sorted\_en.md

# テストデータ {#test-data}

開発clickhouseが必要となり載荷実ックスです。 パフォーマンステストでは特に重要です。 yandexからの匿名化されたデータの特別に準備されたセットがあります。メトリカ さらに、3gbの空きディスク容量が必要です。 このデータがないの達成に必要なものの開発事ができます。

    sudo apt install wget xz-utils

    wget https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz
    wget https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

# プル要求の作成 {#creating-pull-request}

GitHubのUIであなたのフォークリポジトリに移動します。 ブランチで開発している場合は、そのブランチを選択する必要があります。 があるでしょう “Pull request” 画面にあるボタン。 本質的に、これは “create a request for accepting my changes into the main repository”.

作業がまだ完了していない場合でも、プル要求を作成できます。 この場合、単語を入れてください “WIP” （進行中の作業）タイトルの冒頭で、後で変更することができます。 これは、利用可能なすべてのテストを実行するだけでなく、変更の協調的なレビューや議論にも役立ちます。 変更内容の簡単な説明を入力することが重要ですが、後でリリースチェンジログの生成に使用されます。

テストは、yandexの従業員がタグであなたのprにラベルを付けるとすぐに開始します “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

システムは、プル要求のclickhouseバイナリビルドを個別に準備します。 これらのビルドを取得するには “Details” 次へのリンク “ClickHouse build check” チェックのリストのエントリ。 そこには、ビルドへの直接リンクがあります。あなたも、あなたの生産サーバー上に展開することができClickHouseのdebパッケージ(あなたは恐れていない場合).

おそらくいくつかのビルドは最初は失敗します。 これは、gccとclangの両方のビルドをチェックし、既存の警告のほとんどすべてをチェックするためです（常に `-Werror` フラグ)clangで有効になっています。 その同じページでは、すべてのビルドログを見つけることができるので、可能な限りすべての方法でClickHouseをビルドする必要はありません。
