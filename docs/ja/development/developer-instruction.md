---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u521D\u5FC3\u8005ClickHouse\u958B\u767A\u8005\u306E\u6307\u793A"
---

ClickHouseのビルドはLinux、FreeBSD、Mac OS Xでサポートされています。

# Windowsを使用する場合 {#if-you-use-windows}

Windowsを使用する場合は、Ubuntuで仮想マシンを作成する必要があります。 するのは、仮想マシンをインストールしてくださいVirtualBox. ダウンロードできますUbuntuのウェブサイト：https://www.ubuntu.com/#download. を作成してください仮想マシンからダウンロードした画像を保少なくとも4GB RAMめます。 Ubuntuでコマンドライン端末を実行するには、その単語を含むプログラムを見つけてください “terminal” その名前で（gnome-terminal、konsoleなど。）または単にCtrl+Alt+Tを押します。

# 32ビットシステムを使用する場合 {#if-you-use-a-32-bit-system}

ClickHouseできない仕事を32ビットのシステム。 きの獲得へのアクセスでは、64ビットのシステムを継続できる。

# GitHubでのリポジトリの作成 {#creating-a-repository-on-github}

ClickHouse repositoryで作業を開始するには、GitHubアカウントが必要です。

おそらく既に持っていますが、そうでない場合は、登録してくださいhttps://github.com.SSHキーがない場合は、それらを生成してGitHubにアップロードする必要があります。 あなたのパッチを送信するために必要です。 他のSSHサーバーで使用するのと同じSSHキーを使用することも可能です。

ClickHouseリポジトリのフォークを作成します。 それを行うには、 “fork” 右上のボタンhttps://github.com/ClickHouse/ClickHouse.それはあなたのアカウントにClickHouse/ClickHouseの独自のコピーをフォークします。

開発プロセスは、最初に意図した変更をClickHouseのフォークにコミットし、次に “pull request” これらの変更をメインリポジトリ（ClickHouse/ClickHouse）に受け入れる。

作gitリポジトリをインストールしてください `git`.

Ubuntuでこれを行うには、コマンドラインターミナルで実行します:

    sudo apt update
    sudo apt install git

簡単なマニュアルを使用Gitで、できるだけ早く送ってくださhttps://education.github.com/git-cheat-sheet-education.pdf.
詳細なマニュアルGit見https://git-scm.com/book/en/v2.

# 開発マシンへのリポジトリの複製 {#cloning-a-repository-to-your-development-machine}

次に、ソースファイルを作業マシンにダウンロードする必要があります。 これは “to clone a repository” 作業マシン上にリポジトリのローカルコピーを作成するためです。

コマンドラインターミナルで実行:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

注:、代理して下さい *your_github_username* 適切なもので！

このコマンドディレクトリの作成 `ClickHouse` プロジェクトの作業コピーを含む。

ビルドシステムの実行に問題が生じる可能性があるため、作業ディレクトリへのパスに空白が含まれていないことが重要です。

ClickHouseリポジトリは以下を使用します `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` 上記の例のようにフラグ。 場合のリポジトリにてクローニングなsubmodules、ダウンロードを実行する必要がありますの:

    git submodule init
    git submodule update

このコマンドでステータスを確認できます: `git submodule status`.

次のエラーメッセージが表示される場合:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

一般に、GitHubに接続するためのSSHキーがないことを意味します。 これらのキーは、通常、 `~/.ssh`. SSHキーを受け入れるには、GitHub UIの設定セクションにアップロードする必要があります。

またクローンをリポジトリによhttpsプロトコル:

    git clone https://github.com/ClickHouse/ClickHouse.git

ただし、変更をサーバーに送信することはできません。 にもそのままお使いいただけで一時的にメモリの使用範囲のサイズはSSHキーの後に交換し、リモートアドレスのリポジトリ `git remote` コマンド

元のClickHouseレポのアドレスをローカルリポジトリに追加して、そこから更新を取得することもできます:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

このコマンドを正常に実行すると、次のようにしてメインのClickHouseリポジトリから更新を取得できます `git pull upstream master`.

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

次のコマンドは、すべてのサブモジュールを初期状態にリセットするのに役立ちます（！ﾂづﾂつｷﾂ。 -内部の変更は削除されます):

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

ClickHouseは、構築のためのCMakeと忍者を使用しています。

CMake-忍者ファイル（ビルドタスク）を生成することができるメタビルドシステム。
忍者-これらのcmake生成されたタスクを実行するために使用される速度に焦点を当てた小さなビルドシステム。

Ubuntu、DebianまたはMint runにインストールするには `sudo apt install cmake ninja-build`.

セントスでは、レッドハットラン `sudo yum install cmake ninja-build`.

ArchまたはGentooを使用する場合は、おそらくCMakeのインストール方法を自分で知っています。

のためのCMakeおよび忍者Mac OS X初めて自作としてインストールインストールさんによbrew:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

次に、CMakeのバージョンを確認します: `cmake --version`. 3.3未満の場合は、webサイトから新しいバージョンをインストールしてください。https://cmake.org/download/.

# 任意の外部ライブラリ {#optional-external-libraries}

ClickHouseはビルドに複数の外部ライブラリを使用します。 それらのすべては、サブモジュールにあるソースからClickHouseと一緒に構築されているので、別々にインストールする必要はありません。 リストは次の場所で確認できます `contrib`.

# C++コンパイラ {#c-compiler}

ClickHouseのビルドには、バージョン9以降のGCCとClangバージョン8以降のコンパイラがサポートされます。

公式のYandexビルドは、わずかに優れたパフォーマンスのマシンコードを生成するため、GCCを使用しています（私たちのベンチマークに応じて最大数パーセントの そしてClangは開発のために通常より便利です。 が、当社の継続的インテグレーション（CI)プラットフォームを運チェックのための十数の組み合わせとなります。

UBUNTUにGCCをインストールするには: `sudo apt install gcc g++`

Gccのバージョンを確認する: `gcc --version`. の場合は下記9その指示に従う。https://clickhouse.tech/docs/ja/development/build/#install-gcc-10.

Mac OS XのビルドはClangでのみサポートされています。 ちょうど実行 `brew install llvm`

Clangを使用する場合は、次のものもインストールできます `libc++` と `lld` あなたがそれが何であるか知っていれば。 を使用して `ccache` また、推奨されます。

# 建築プロセス {#the-building-process}

ClickHouseを構築する準備ができたので、別のディレクトリを作成することをお勧めします `build` 内部 `ClickHouse` それはすべてのビルド人工物が含まれています:

    mkdir build
    cd build

いくつかの異なるディレクトリ（build_release、build_debugなど）を持つことができます。）ビルドの異なるタイプのために。

中の間 `build` cmakeを実行してビルドを構成します。 最初の実行の前に、コンパイラ（この例ではバージョン9gccコンパイラ）を指定する環境変数を定義する必要があります。

Linux:

    export CC=gcc-10 CXX=g++-10
    cmake ..

Mac OS X:

    export CC=clang CXX=clang++
    cmake ..

その `CC` 変数は、Cのコンパイラを指定します(cコンパイラの略)。 `CXX` variableは、どのC++コンパイラをビルドに使用するかを指示します。

より高速なビルドのために、あなたは `debug` ビルドタイプ-最適化のないビルド。 その供給は、以下のパラメータ `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

ビルドのタイプを変更するには、次のコマンドを実行します。 `build` ディレクトリ。

忍者を実行して構築する:

    ninja clickhouse-server clickhouse-client

この例では、必要なバイナリのみを構築します。

必要な場合は、構築すべてのバイナリ(光熱費および試験）を動かして行く必要がある忍者のないパラメータ:

    ninja

フルの構築が必要約30GBのディスクスペースまたは15GBの主binaries.

ビルドマシン上で大量のRAMが利用可能な場合は、以下と並行して実行されるビルドタスクの数を制限する必要があります `-j` param:

    ninja -j 1 clickhouse-server clickhouse-client

4GBのRAMを搭載しているマシンでは、1を8GBのRAMに指定することをお勧めします `-j 2` 推奨されます。

メッセージが届いたら: `ninja: error: loading 'build.ninja': No such file or directory` これは、ビルド構成の生成が失敗し、上記のメッセージを検査する必要があることを意味します。

ビルドプロセスが正常に開始されると、ビルドの進行状況、つまり処理されたタスクの数とタスクの総数が表示されます。

ながらメッセージについてprotobufファイルlibhdfs2図書館のような `libprotobuf WARNING` 現れるかも 彼らは何も影響を与えず、無視されても安全です。

ビルドが成功すると、実行可能ファイルを取得します `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# ClickHouseのビルドされた実行可能ファイルの実行 {#running-the-built-executable-of-clickhouse}

現在のユーザーの下でサーバーを実行するには、次の場所に移動します `ClickHouse/programs/server/` （外にあります `build`)と実行:

    ../../build/programs/clickhouse server

この場合、ClickHouseは現在のディレクトリにある設定ファイルを使用します。 実行できます `clickhouse server` からのディレクトリのパスを指定し、設定ファイルとしてコマンドラインパラメータ `--config-file`.

別のターミナルでclickhouse-clientを使用してClickHouseに接続するには、次の場所に移動します `ClickHouse/build/programs/` と実行 `./clickhouse client`.

あなたが得る場合 `Connection refused` メッセージMac OS XまたはFreeBSDでは、ホストアドレス127.0.0.1を指定してみます:

    clickhouse client --host 127.0.0.1

に置き換えることができ生産版ClickHouseバイナリインストールされるシステムのカスタム構築ClickHouseバイナリー. いるイClickHouse利用するマシンの指示に従って公式サイトから 次に、以下を実行します:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

なお `clickhouse-client`, `clickhouse-server` そして、他のものは、一般的に共有される `clickhouse` バイナリ

を運営することも可能ですカスタム構築ClickHouseバイナリのコンフィグファイルからのClickHouseパッケージをインストールシステム:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE(統合開発環境) {#ide-integrated-development-environment}

使用するIDEがわからない場合は、CLionを使用することをお勧めします。 CLionは商用ソフトウェアですが、30日間の無料試用期間を提供しています。 また、学生のための無料です。 CLionはLinuxとMac OS Xの両方で使用できます。

KDevelopとQTCreatorは、ClickHouseを開発するためのIDEの他の優れた選択肢です。 KDevelopは非常に便利なIDEとして提供されますが、不安定です。 まKDevelopクラッシュが開設すプロジェクト、クリック “Stop All” プロジェクトのファイルのリストを開くとすぐにボタンを押します。 そうした後、KDevelopはうまくいくはずです。

単純なコードエディターとして、Sublime TextまたはVisual Studio Code、またはKate(すべてLinuxで利用可能)を使用できます。

念のため、CLionが作成することに言及する価値があります `build` 独自のパス、それはまた、独自の選択に `debug` ビルドタイプの場合、構成のために、CLionで定義されているCMakeのバージョンを使用し、インストールされているものではありません。 `make` ビルドタスクを実行するには `ninja`. これは通常の動作ですが、混乱を避けるためにそれを念頭に置いてください。

# コードの作成 {#writing-code}

の説明ClickHouse建築で、できるだけ早く送ってくださhttps://clickhouse.tech/docs/en/開発/アーキテクチャ/

コードのスタイルガイド:https://clickhouse.tech/docs/en/開発/スタイル/

筆記試験：https://clickhouse.tech/docs/en/development/tests/

タスクのリスト：https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22

# テストデータ {#test-data}

開発ClickHouseが必要となり載荷実ックスです。 パフォーマンステストでは特に重要です。 して特定の匿名化データからのYandex.メトリカ さらに3GBの空きディスク領域が必要です。 このデータは、ほとんどの開発タスクを実行するためには必要ありません。

    sudo apt install wget xz-utils

    wget https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz
    wget https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE DATABASE IF NOT EXISTS test

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

# プル要求の作成 {#creating-pull-request}

GitHubのUIでforkリポジトリに移動します。 ブランチで開発している場合は、そのブランチを選択する必要があります。 があるでしょう “Pull request” 画面上にあるボタン。 本質的に、これは “create a request for accepting my changes into the main repository”.

プル要求は、作業がまだ完了していない場合でも作成できます。 この場合、単語を入れてください “WIP” （進行中の作業）タイトルの先頭に、それは後で変更することができます。 これは、変更の協調的なレビューと議論、および利用可能なすべてのテストの実行に役立ちます。 変更の簡単な説明を提供することが重要です。

Yandexの従業員がタグであなたのPRにラベルを付けるとすぐにテストが開始されます “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

システムは、プル要求用にClickHouseバイナリビルドを個別に準備します。 これらのビルドを取得するには “Details” 次のリンク “ClickHouse build check” 小切手のリストのエントリ。 そこには、ビルドへの直接リンクがあります。ClickHouseのdebパッケージは、本番サーバーにも展開できます（恐れがない場合）。

ほとんどの場合、ビルドの一部は最初に失敗します。 これは、gccとclangの両方でビルドをチェックするという事実によるものです。 `-Werror` flag)clangを有効にします。 その同じページで、すべてのビルドログを見つけることができるので、ClickHouseをすべての可能な方法でビルドする必要はありません。
