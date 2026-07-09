---
description: 'ClickHouse 开发的前置条件和设置说明'
sidebar_label: '前置条件'
sidebar_position: 5
slug: /development/developer-instruction
title: '开发者前置条件'
doc_type: 'guide'
---

ClickHouse 可以在 Linux、FreeBSD 和 macOS 上构建。
如果你使用 Windows，仍然可以在运行 Linux 的虚拟机中构建 ClickHouse，例如使用安装了 Ubuntu 的 [VirtualBox](https://www.virtualbox.org/) 虚拟机。

<div id="create-a-repository-on-github">
  ## 在 GitHub 上创建仓库
</div>

要开始参与 ClickHouse 的开发，你需要一个 [GitHub](https://www.github.com/) 账号。
此外，请在本地生成一个 SSH 密钥 (如果你还没有的话) ，并将公钥上传到 GitHub，因为这是提交补丁的前置条件。

接下来，点击右上角的 &quot;fork&quot; 按钮，在你的个人账号下 fork [ClickHouse repository](https://github.com/ClickHouse/ClickHouse/)。

要贡献更改 (例如修复某个 issue 或添加某项 feature) ，请先将更改提交到你 fork 出来的仓库中的某个分支，然后创建一个包含这些更改并提交到主仓库的 &quot;拉取请求&quot;。

要使用 Git 仓库，请先安装 Git。例如，在 Ubuntu 中，运行：

```sh
sudo apt update
sudo apt install git
```

可在[此处](https://education.github.com/git-cheat-sheet-education.pdf)查看 Git 速查表。
Git 详细手册见[此处](https://git-scm.com/book/en/v2)。

<div id="clone-the-repository-to-your-development-machine">
  ## 将仓库克隆到本地开发机器
</div>

首先，将源文件下载到你的工作机器，即克隆该仓库：

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

此命令会创建一个名为 `ClickHouse/` 的目录，其中包含源代码、测试文件和其他文件。
你也可以在 URL 后指定一个自定义目录作为 checkout 目录，但务必确保该路径中不包含空格，否则后续可能会导致构建失败。

ClickHouse 的 Git 仓库使用子模块来引入第三方库。
默认情况下，子模块不会被 checkout。
你可以选择以下任一方式：

* 运行带有 `--recurse-submodules` 选项的 `git clone`，

* 如果运行 `git clone` 时未使用 `--recurse-submodules`，请运行 `git submodule update --init --jobs <N>` 来显式 checkout 所有子模块。 (`<N>` 例如可设为 `12`，以并行下载。)

* 如果运行 `git clone` 时未使用 `--recurse-submodules`，并且你希望使用[浅层](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/)子模块 checkout，省略子模块中的历史记录以节省一些空间，请运行 `./contrib/update-submodules.sh`。CI 会使用这种替代方式，但不建议在本地开发中使用，因为这会让子模块的操作变得更不方便，也更慢。

要检查 Git 子模块的状态，请运行 `git submodule status`。

如果你收到以下错误消息

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

用于连接 GitHub 的 SSH 密钥未找到。
这些密钥通常位于 `~/.ssh`。
要让 SSH 密钥生效，你需要在 GitHub 的设置中上传它们。

你也可以通过 HTTPS 克隆该仓库：

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

不过，这样你就无法将更改推送到服务器。
你仍然可以暂时先这样使用，之后再通过 `git remote` 命令替换仓库的远程地址并添加 SSH 密钥。

你也可以将原始 ClickHouse 仓库的地址添加到本地仓库，以便从那里拉取更新：

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

成功运行此命令后，你就可以通过执行 `git pull upstream master` 从 ClickHouse 主仓库拉取更新。

:::tip
请不要直接使用 `git push`，否则你可能会推送到错误的远程仓库和/或错误的分支。
最好显式指定远程仓库和分支名称，例如 `git push origin my_branch_name`。
:::

<div id="writing-code">
  ## 编写代码
</div>

下面列出了一些快捷链接，在为 ClickHouse 编写代码时可能会用到：

* [ClickHouse 架构](/zh/development/architecture/).
* [代码风格指南](/zh/development/style/).
* [第三方库](/zh/development/contrib#adding-and-maintaining-third-party-libraries)
* [编写测试](/zh/development/tests/)
* [开放的 issue](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

<div id="ide">
  ### IDE
</div>

[Visual Studio Code](https://code.visualstudio.com/) 和 [Neovim](https://neovim.io/) 是过去开发 ClickHouse 时一直表现不错的两个选择。如果你使用 VS Code，我们建议使用 [clangd extension](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) 替代 IntelliSense，因为它的性能要好得多。

[CLion](https://www.jetbrains.com/clion/) 也是一个很好的选择。不过，对于 ClickHouse 这样的大型项目，它可能会更慢。使用 CLion 时，有几点需要注意：

* CLion 会自行创建一个 `build` path，并自动选择 `debug` 作为构建类型
* 它使用的是 CLion 中定义的 CMake 版本，而不是你自己安装的版本
* CLion 会使用 `make` 而不是 `ninja` 来运行构建任务 (这是正常行为)

你还可以使用其他 IDE，例如 [Sublime Text](https://www.sublimetext.com/)、[Qt Creator](https://www.qt.io/product/development-tools) 或 [Kate](https://kate-editor.org/)。

<div id="create-a-pull-request">
  ## 创建拉取请求
</div>

在 GitHub 的 UI 中前往你的 fork 仓库。
如果你是在某个分支上进行开发，则需要选择该分支。
屏幕上会有一个“拉取请求”按钮。
其实，它的意思就是“创建一个请求，将我的更改合并到主仓库”。

即使工作尚未完成，也可以创建拉取请求。
这种情况下，请在标题开头加上“WIP” (进行中) ，之后可以再修改。
这有助于协作审查和讨论更改，也能运行所有可用测试。
请务必简要说明你的更改，后续生成发布更新日志时会用到这段说明。

一旦 ClickHouse 员工给你的 PR 加上“can be tested”标签，测试就会开始。
部分初始检查 (例如代码风格) 的结果会在几分钟内返回。
构建检查结果会在半小时内返回。
主要测试集会在一小时内给出结果。

系统会单独为你的拉取请求准备 ClickHouse 二进制文件构建。
要获取这些构建，请点击检查列表中“Builds”条目旁边的“Details”链接。
在那里你会找到已构建的 ClickHouse `.deb` 软件包的直接链接，甚至可以将其部署到你的生产环境服务器上 (如果你不介意这么做的话) 。

<div id="write-documentation">
  ## 编写文档
</div>

每个新增新特性的拉取请求都必须附带完善的文档。
如果你想预览文档改动，可在[这里](https://github.com/ClickHouse/clickhouse-docs)的 README.md 文件中查看如何在本地构建文档页面的说明。
向 ClickHouse 添加新函数时，你可以参考下面的模板：

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
  ## 使用测试数据
</div>

开发 ClickHouse 时通常需要加载真实的数据集。
这对性能测试尤为重要。
我们专门准备了一组经过匿名化处理的网站分析数据。
此外，还需要约 3GB 的可用磁盘空间。

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

在 ClickHouse 客户端中：

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

导入数据：

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```