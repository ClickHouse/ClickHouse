---
description: 'Prerequisites and setup instructions for ClickHouse development'
sidebar_label: 'Prerequisites'
sidebar_position: 5
slug: /development/developer-instruction
title: 'Developer Prerequisites'
---

# Prerequisites

ClickHouse can be build on Linux, FreeBSD and macOS.
If you use Windows, you can still build ClickHouse in a virtual machine running Linux, e.g. [VirtualBox](https://www.virtualbox.org/) with Ubuntu.

## Create a Repository on GitHub {#create-a-repository-on-github}

To start developing for ClickHouse you will need a [GitHub](https://www.github.com/) account.
Please also generate an SSH key locally (if you don't have one already) and upload the public key to GitHub as this is a prerequisite for contributing patches.

Next, fork the [ClickHouse repository](https://github.com/ClickHouse/ClickHouse/) in your personal account by clicking the "fork" button in the upper right corner.

To contribute changes, e.g., a fix for an issue or a feature, first commit your changes to a branch in your fork, then create a "Pull Request" with the changes to the main repository.

For working with Git repositories, please install Git. For example, in Ubuntu, run:

```sh
sudo apt update
sudo apt install git
```

A Git cheatsheet can be found [here](https://education.github.com/git-cheat-sheet-education.pdf).
A detailed Git manual is [here](https://git-scm.com/book/en/v2).

## Clone the Repository to Your Development Machine {#clone-the-repository-to-your-development-machine}

First, download the source files to your working machine, i.e. clone the repository:

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

This command creates a directory `ClickHouse/` containing the source code, tests, and other files.
You can specify a custom directory for checkout after the URL, but it is important that this path does not contain whitespaces as this may break the build later on.

ClickHouse's Git repository uses submodules to pull in 3rd party libraries.
Submodules are not checked out by default.
You can either

- run `git clone` with option `--recurse-submodules`,

- if `git clone` is run without `--recurse-submodules`, run `git submodule update --init --jobs <N>` to checkout all submodules explicitly. (`<N>` can be set for example to `12` to parallelize the download.)

- if `git clone` is run without `--recurse-submodules` and you like to use [sparse](https://github.blog/2020-01-17-bring-your-monorepo-down-to-size-with-sparse-checkout/) and [shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/) submodule checkout to omit unneeded files and history in submodules to save space (ca. 5 GB instead of ca. 15 GB), run `./contrib/update-submodules.sh`. This alternative is used by CI but not recommended for local development as it makes working with submodules less convenient and slower.

To check the status of the Git submodules, run `git submodule status`.

If you get the following error message

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

the SSH keys for connecting to GitHub are missing.
These keys are normally located in `~/.ssh`.
For SSH keys to be accepted you need to upload them in GitHub's settings.

You can also clone the repository via HTTPS:

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

This, however, will not let you send your changes to the server.
You can still use it temporarily and add the SSH keys later replacing the remote address of the repository with `git remote` command.

You can also add original ClickHouse repo address to your local repository to pull updates from there:

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

After successfully running this command you will be able to pull updates from the main ClickHouse repo by running `git pull upstream master`.

:::tip
Please do not use verbatim `git push`, you may push to the wrong remote and/or the wrong branch.
It is better to specify the remote and branch names explicitly, e.g. `git push origin my_branch_name`.
:::

## Writing Code {#writing-code}

Below you can find some quick links which may be useful when writing code for ClickHouse:

- [ClickHouse Architecture](/development/architecture/).
- [Code style guide](/development/style/).
- [Third-party libraries](/development/contrib#adding-and-maintaining-third-party-libraries)
- [Writing tests](/development/tests/)
- [Open issues](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

### IDE {#ide}

[Visual Studio Code](https://code.visualstudio.com/) and [Neovim](https://neovim.io/) are two options that have worked well in the past for developing ClickHouse. If you are using VS Code, we recommend using the [clangd extension](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) to replace IntelliSense as it is much more performant.

[CLion](https://www.jetbrains.com/clion/) is another great alternative. However, it can be slower on larger projects like ClickHouse. A few things to keep in mind when using CLion:

- CLion creates a `build` path on its own and automatically selects `debug` for the build type
- It uses a version of CMake that is defined in CLion and not the one installed by you
- CLion will use `make` to run build tasks instead of `ninja` (this is normal behavior)

Other IDEs you can use are [Sublime Text](https://www.sublimetext.com/), [Qt Creator](https://www.qt.io/product/development-tools), or [Kate](https://kate-editor.org/).

## Create A Pull Request {#create-a-pull-request}

Navigate to your fork repository in GitHub's UI.
If you have been developing in a branch, you need to select that branch.
There will be a "Pull request" button located on the screen.
In essence, this means "create a request for accepting my changes into the main repository".

A pull request can be created even if the work is not completed yet.
In this case please put the word "WIP" (work in progress) at the beginning of the title, it can be changed later.
This is useful for cooperative reviewing and discussion of changes as well as for running all of the available tests.
It is important that you provide a brief description of your changes, it will later be used for generating release changelog.

Testing will commence as soon as ClickHouse employees label your PR with a tag "can be tested".
The results of some first checks (e.g. code style) will come in within several minutes.
Build check results will arrive within half an hour.
The main set of tests will report itself within an hour.

The system will prepare ClickHouse binary builds for your pull request individually.
To retrieve these builds click the "Details" link next to "Builds" entry in the list of checks.
There you will find direct links to the built .deb packages of ClickHouse which you can deploy even on your production servers (if you have no fear).

## Write Documentation {#write-documentation}

Every pull request which adds a new feature must come with proper documentation.
If you'd like to preview your documentation changes the instructions for how to build the documentation page locally are available in the README.md file [here](https://github.com/ClickHouse/clickhouse-docs).
When adding a new function to ClickHouse you can use the template below as a guide:

```markdown
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

Query:

\```sql
SELECT 'write your example query here';
\```

Response:

\```response
┌───────────────────────────────────┐
│ the result of the query           │
└───────────────────────────────────┘
\```
```

## Using Test Data {#using-test-data}

Developing ClickHouse often requires loading realistic datasets.
This is particularly important for performance testing.
We have a specially prepared set of anonymized data of web analytics.
It requires additionally some 3GB of free disk space.

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

In clickhouse-client:

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

Import the data:

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```
