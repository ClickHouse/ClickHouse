ClickHose支持Linux,FreeBSD 及 Mac OS X 系统。

# Windows使用指引 {#windowsshi-yong-zhi-yin}

如果您的系统是Windows，则需要创建Ubuntu虚拟机。可以安装VirtualBox来构建虚拟机。Ubuntu的下载链接为：https://www.ubuntu.com/#download 。请使用下载好的镜像创建一个虚拟机（请确保虚拟机有至少4GB的内存容量）。在Ubuntu中使用«terminal»程序（gnome-terminal，konsole等）运行命令行终端，或使用快捷键Ctrl+Alt+T。

# 在GitHub上创建源码库 {#zai-githubshang-chuang-jian-yuan-ma-ku}

您需要(申请)一个GitHub账户来使用ClickHouse。

如果没有账户，请在https://github.com上注册一个。如果没有SSH密钥，请在本地创建密钥并将公钥上传到GitHub上。这有助于你提交更新代码。并且在不同的SSH服务端，你也可以使用相同的SSH密钥。

要创建ClickHouse源码库的分支，请在https://github.com/ClickHouse/ClickHouse页面上点击右上角的«fork»按钮。它会在本账户上创建您个人的ClickHouse/ClickHouse分支。

若要参与开发，首先请在ClickHouse的分支中提交您期望的变更，然后创建一个«pull请求»，以便这些变更能够被(ClickHouse/ClickHouse)主库接受。

请先安装`git`来使用git源码库。

请在Ubuntu终端上使用下列的指令来安装`git`:

    sudo apt update
    sudo apt install git

在https://education.github.com/git-cheat-sheet-education.pdf中找到有关使用Git的简易手册。有关Git的详细手册，请参见: https://git-scm.com/book/ru/v2 。

# 拷贝源码库到开发机 {#kao-bei-yuan-ma-ku-dao-kai-fa-ji}

接下来，请将源码下载到开发机上。这步操作被称为«拷贝源码库»，是因为它在您的开发机上创建了源码库的本地副本。

在终端命令行输入下列指令：

    git clone git@guthub.com:your_github_username/ClickHouse.git
    cd ClickHouse

请注意，您需要将*your_github_username* 替换成实际使用的账户名!

这个指令将创建一个包含项目副本的`ClickHouse`工作目录。

重要的是，工作目录的路径中不应包含空格，因为这可能会导致运行构建系统时出现问题。

请注意，ClickHouse源码库使用了`submodules`。这是对其他库的引用（即项目所依赖的外部库）。即在拷贝源码库时，需要如上述指令中那样指定`--recursive`。如果在拷贝源码库时没有包含子模块，需要执行使用下列的指令：

    git submodule init
    git submodule update

可以通过 `git submodule status`来检查子模块的状态。

如果提示下列的错误信息:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

这通常表示缺少用于连接GitHub的SSH密钥。这些密钥一般都在`~/.ssh`中。要接受SSH密钥，请在GitHub UI的设置页面中上传它们。

您还可以通过https协议来拷贝源码库:

    git clone https://github.com/ClickHouse/ClickHouse.git

但是，这无法将变更提交到服务器上。您仍然可以暂时使用，并后续再添加SSH密钥，用`git remote`命令替换源码库的远程地址。

还可以将原始ClickHouse库的地址添加到本地库中，以便从那里获取更新：

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

命令执行成功后，可以通过执行`git pull upstream master`，从ClickHouse的主分支中拉去更新。

## 使用子模块 {#working-with-submodules}

在git中使用子模块可能会很痛苦。 接下来的命令将有助于管理它:

    # ! each command accepts
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

接下来的命令将帮助您将所有子模块重置为初始状态（！华林！ -里面的任何chenges将被删除):

    # Synchronizes submodules' remote URL with .gitmodules
    git submodule sync
    # Update the registered submodules with initialize not yet initialized
    git submodule update --init
    # Reset all changes done after HEAD
    git submodule foreach git reset --hard
    # Clean files from .gitignore
    git submodule foreach git clean -xfd
    # Repeat last 4 commands for all submodule
    git submodule foreach git submodule sync
    git submodule foreach git submodule update --init
    git submodule foreach git submodule foreach git reset --hard
    git submodule foreach git submodule foreach git clean -xfd

# 构建系统 {#gou-jian-xi-tong}

ClickHouse使用 CMake 和 Ninja 来构建系统。

CMake - 一个可以生成Ninja文件的元构建系统（构建任务）。
Ninja - 一个轻量级的构建系统，专注于速度，用于执行这些cmake生成的任务。

在Ubuntu,Debian或者Mint系统上执行`sudo apt install cmake ninja-build`来安装ninja。

在CentOS,RedHat系统上执行`sudo yum install cmake ninja-build`。

如果您曾经使用过Arch或Gentoo，那么也许知道如何安装CMake。

若要在Mac OS X上安装CMake和Ninja，请先安装Homebrew，然后再通过brew安装其他内容：

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

接下来，检查CMake的版本：`cmake --version`。如果版本低于3.3，则需要从以下网站安装更新版本：https://cmake.org/download/ 。

# 可供选择的外部库 {#ke-gong-xuan-ze-de-wai-bu-ku}

ClickHouse使用多个外部库进行构建。大多数外部库不需要单独安装，而是和ClickHouse一起在子模块中构建。可以查看`contrib`中罗列的清单。

# C++ 编译器 {#c-bian-yi-qi}

We support clang starting from version 11.

On Ubuntu/Debian you can use the automatic installation script (check [official webpage](https://apt.llvm.org/))

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

# 构建的过程 {#gou-jian-de-guo-cheng}

如果当前已经准备好构建ClickHouse，我们建议您在`ClickHouse`中创建一个单独的目录`build`，其中包含所有构建组件:

    mkdir build
    cd build

您也可以有多个不同类型的构建目录（例如，build_release, build_debug等等)。

在`build`目录下，通过运行CMake配置构建。 在第一次运行之前，请定义用于指定编译器的环境变量（本示例中为gcc 9 编译器）。

    export CC=clang CXX=clang++
    cmake ..

`CC`变量指代C的编译器（C Compiler的缩写），而`CXX`变量指代要使用哪个C++编译器进行编译。

为了更快的构建，请使用`debug`构建类型-不含优化的构建。为此提供以下的参数`-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

您可以通过在`build`目录中运行此命令来更改构建类型。

运行ninja进行构建:

    ninja clickhouse-server clickhouse-client

在此示例中，仅将构建所需的二进制文件。

如果您需要构建所有的二进制文件（utilities和tests），请运行不带参数的ninja：

    ninja

全量构建需要大约30GB的可用磁盘空间或15GB的空间来构建主要的二进制文件。

当构建的机器上有大量内存时，可考虑设置与`-j`参数并行运行的构建任务数量：

    ninja -j 1 clickhouse-server clickhouse-client

在拥有4GB内存的机器上，建议设置成1，在拥有8GB内存的机器上，建议按`-j 2`设置。

如果您收到以下消息：

`ninja：error：loading'build.ninja'：No such file or directory`

则表示生成构建配置失败，请检查上述消息。

成功启动构建过程后，您将看到构建进度-已处理任务的数量和任务总数。

在libhdfs2库中生成有关protobuf文件的消息时，可能会显示诸如`libprotobuf WARNING`。它们没有影响，可以忽略不计。

成功构建后，会得到一个可执行文件`ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# 运行ClickHouse可执行文件 {#yun-xing-clickhouseke-zhi-xing-wen-jian}

要以当前的用户身份运行服务，请进入到`ClickHouse/programs/server/` 目录（在`build`文件夹外）并运行：

    ../../build/programs/clickhouse server

在这种情况下，ClickHouse将使用位于当前目录中的配置文件。您可以从任何目录运行`Clickhouse server`，并将配置文件`--config-file`的路径指定为命令行参数。

在另外一个终端上连接ClickHouse的clickhouse-client客户端，请进入到`ClickHouse/build/programs/` 并运行`./clickhouse client`。

如果您在Mac OS X 或者 FreeBSD上收到`Connection refused`的消息，请尝试指定主机地址为127.0.0.1：

    clickhouse client --host 127.0.0.1

您可以使用自定义构建的ClickHouse二进制文件替换系统中安装的ClickHouse二进制文件的生成版本。为此，请参照官方网站上的说明在计算机上安装ClickHouse。 接下来，运行以下命令：

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

请注意，`clickhouse-client`，`clickhouse-server`和其他服务通常共享`clickhouse`二进制文件的符号链接。

您还可以使用系统上安装的ClickHouse软件包中的配置文件运行自定义构建的ClickHouse二进制文件：

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE (集成开发环境) {#ide-ji-cheng-kai-fa-huan-jing}

如果您还不知道使用哪款IDE，我们推荐使用CLion。CLion是一款商业软件，但能够有30天的免费使用时间。它同时也对学生免费。CLion可以在Linux和Mac OS X上使用。

KDevelop和QTCreator是另外两款适合开发ClickHouse的替代IDE。尽管不太稳定，但KDevelop还是作为一款非常便捷的IDE。如果KDevelop在打开项目后不久崩溃，则您应该在打开项目文件列表后立即单击«全部停止»按钮。按此处理后，KDevelop可以正常使用。

作为简易的代码编辑器，您可以使用Sublime Text或Visual Studio Code或Kate（在Linux上都可用）。

值得一提的是CLion会创建自己的`build`路径，它还会自行选择`debug`作为构建类型。对于配置，它使用CLion中定义的CMake版本，而不是您安装的版本。最后，CLion会使用`make`而不是`ninja`去构建任务。这属于正常的现象，请记住这一点，以免造成混淆。

# 编写代码 {#bian-xie-dai-ma}

ClickHouse的架构描述可以在此处查看：https://clickhouse.com/docs/en/development/architecture/

代码风格指引：https://clickhouse.com/docs/en/development/style/

编写测试用例：https://clickhouse.com/docs/en/development/tests/

任务列表：https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22

# 测试数据 {#ce-shi-shu-ju}

开发ClickHouse通常需要加载现实的数据集，尤其是在性能测试的场景。我们可以从Yandex.Metrica获取一组特别准备的匿名数据。这些数据需要额外使用3GB的空闲磁盘空间。请注意，完成大多数开发任务并不需要此数据。

    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

# 创建拉取请求 {#chuang-jian-la-qu-qing-qiu}

进入到GitHub 用户界面中的fork库。如果您已经在某个分支中进行开发，则需要选择该分支。在屏幕中有一个 «拉取请求»的按钮。实际上这等价于«创建一个请求以接受对主库的变更»。

即使工作尚未完成，也可以创建拉取请求。在这种情况下，请在标题的开头加上«WIP»（正在进行中），以便后续更改。这对于协同审查和讨论更改以及运行所有可用测试用例很有用。提供有关变更的简短描述很重要，这将在后续用于生成重新发布变更日志。

ClickHouse成员一旦在您的拉取请求上贴上«可以测试»标签，就会开始测试。一些初始检查项（例如，代码类型）的结果会在几分钟内反馈。构建的检查结果将在半小时内完成。而主要的测试用例集结果将在一小时内报告给您。

系统将分别为您的拉取请求准备ClickHouse二进制版本。若要检索这些构建信息，请在检查列表中单击« ClickHouse构建检查»旁边的«详细信息»链接。在这里，您会找到指向ClickHouse的.deb软件包的直接链接，此外，甚至可以将其部署在生产服务器上（如果您不担心）。

某些构建项很可能会在首次构建时失败。这是因为我们同时检查了基于gcc和clang的构建，几乎所有现有的被clang启用的警告（总是带有`-Werror`标志）。在同一页面上，您可以找到所有构建的日志，因此不必以所有可能的方式构建ClickHouse。

## 浏览ClickHouse源代码 {#browse-clickhouse-source-code}

您可以使用 **Woboq** 在线代码浏览器 [点击这里](https://clickhouse.com/codebrowser/ClickHouse/src/index.html). 它提供了代码导航和语义突出显示、搜索和索引。 代码快照每天更新。

此外，您还可以像往常一样浏览源代码 [GitHub](https://github.com/ClickHouse/ClickHouse)
