---
sidebar_position: 61
sidebar_label: Getting Started
description: Prerequisites and an overview of how to build ClickHouse
---

# Getting Started Guide for Building ClickHouse

The building of ClickHouse is supported on Linux, FreeBSD and Mac OS X.

If you use Windows, you need to create a virtual machine with Ubuntu. To start working with a virtual machine please install VirtualBox. You can download Ubuntu from the website: https://www.ubuntu.com/#download. Please create a virtual machine from the downloaded image (you should reserve at least 4GB of RAM for it). To run a command-line terminal in Ubuntu, please locate a program containing the word “terminal” in its name (gnome-terminal, konsole etc.) or just press Ctrl+Alt+T.

ClickHouse cannot work or build on a 32-bit system. You should acquire access to a 64-bit system and you can continue reading.

## Creating a Repository on GitHub {#creating-a-repository-on-github}

To start working with ClickHouse repository you will need a GitHub account.

You probably already have one, but if you do not, please register at https://github.com. In case you do not have SSH keys, you should generate them and then upload them on GitHub. It is required for sending over your patches. It is also possible to use the same SSH keys that you use with any other SSH servers - probably you already have those.

Create a fork of ClickHouse repository. To do that please click on the “fork” button in the upper right corner at https://github.com/ClickHouse/ClickHouse. It will fork your own copy of ClickHouse/ClickHouse to your account.

The development process consists of first committing the intended changes into your fork of ClickHouse and then creating a “pull request” for these changes to be accepted into the main repository (ClickHouse/ClickHouse).

To work with git repositories, please install `git`.

To do that in Ubuntu you would run in the command line terminal:

    sudo apt update
    sudo apt install git

A brief manual on using Git can be found here: https://education.github.com/git-cheat-sheet-education.pdf.
For a detailed manual on Git see https://git-scm.com/book/en/v2.

## Cloning a Repository to Your Development Machine {#cloning-a-repository-to-your-development-machine}

Next, you need to download the source files onto your working machine. This is called “to clone a repository” because it creates a local copy of the repository on your working machine.

In the command line terminal run:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

Note: please, substitute *your_github_username* with what is appropriate!

This command will create a directory `ClickHouse` containing the working copy of the project.

It is important that the path to the working directory contains no whitespaces as it may lead to problems with running the build system.

Please note that ClickHouse repository uses `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` flag as in the example above. If the repository has been cloned without submodules, to download them you need to run the following:

    git submodule init
    git submodule update

You can check the status with the command: `git submodule status`.

If you get the following error message:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

It generally means that the SSH keys for connecting to GitHub are missing. These keys are normally located in `~/.ssh`. For SSH keys to be accepted you need to upload them in the settings section of GitHub UI.

You can also clone the repository via https protocol:

    git clone --recursive https://github.com/ClickHouse/ClickHouse.git

This, however, will not let you send your changes to the server. You can still use it temporarily and add the SSH keys later replacing the remote address of the repository with `git remote` command.

You can also add original ClickHouse repo address to your local repository to pull updates from there:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

After successfully running this command you will be able to pull updates from the main ClickHouse repo by running `git pull upstream master`.

### Working with Submodules {#working-with-submodules}

Working with submodules in git could be painful. Next commands will help to manage it:

    # ! each command accepts
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

The next commands would help you to reset all submodules to the initial state (!WARNING! - any changes inside will be deleted):

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

## Build System {#build-system}

ClickHouse uses CMake and Ninja for building.

CMake - a meta-build system that can generate Ninja files (build tasks).
Ninja - a smaller build system with a focus on the speed used to execute those cmake generated tasks.

To install on Ubuntu, Debian or Mint run `sudo apt install cmake ninja-build`.

On CentOS, RedHat run `sudo yum install cmake ninja-build`.

If you use Arch or Gentoo, you probably know it yourself how to install CMake.

## C++ Compiler {#c-compiler}

Compilers Clang starting from version 11 is supported for building ClickHouse.

Clang should be used instead of gcc. Though, our continuous integration (CI) platform runs checks for about a dozen of build combinations.

On Ubuntu/Debian you can use the automatic installation script (check [official webpage](https://apt.llvm.org/))

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

## The Building Process {#the-building-process}

Now that you are ready to build ClickHouse we recommend you to create a separate directory `build` inside `ClickHouse` that will contain all of the build artefacts:

    mkdir build
    cd build

You can have several different directories (build_release, build_debug, etc.) for different types of build.

While inside the `build` directory, configure your build by running CMake. Before the first run, you need to define environment variables that specify compiler.

    export CC=clang CXX=clang++
    cmake ..

If you installed clang using the automatic installation script above, also specify the version of clang installed in the first command, e.g. `export CC=clang-14 CXX=clang++-14`. The clang version will be in the script output.

The `CC` variable specifies the compiler for C (short for C Compiler), and `CXX` variable instructs which C++ compiler is to be used for building.

For a faster build, you can resort to the `debug` build type - a build with no optimizations. For that supply the following parameter `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

You can change the type of build by running this command in the `build` directory.

Run ninja to build:

    ninja clickhouse-server clickhouse-client

Only the required binaries are going to be built in this example.

If you require to build all the binaries (utilities and tests), you should run ninja with no parameters:

    ninja

Full build requires about 30GB of free disk space or 15GB to build the main binaries.

When a large amount of RAM is available on build machine you should limit the number of build tasks run in parallel with `-j` parameter:

    ninja -j 1 clickhouse-server clickhouse-client

On machines with 4GB of RAM, it is recommended to specify 1, for 8GB of RAM `-j 2` is recommended.

If you get the message: `ninja: error: loading 'build.ninja': No such file or directory`, it means that generating a build configuration has failed and you need to inspect the message above.

Upon the successful start of the building process, you’ll see the build progress - the number of processed tasks and the total number of tasks.

While building messages about protobuf files in libhdfs2 library like `libprotobuf WARNING` may show up. They affect nothing and are safe to be ignored.

Upon successful build you get an executable file `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

## Running the Built Executable of ClickHouse {#running-the-built-executable-of-clickhouse}

To run the server under the current user you need to navigate to `ClickHouse/programs/server/` (located outside of `build`) and run:

    ../../build/programs/clickhouse server

In this case, ClickHouse will use config files located in the current directory. You can run `clickhouse server` from any directory specifying the path to a config file as a command-line parameter `--config-file`.

To connect to ClickHouse with clickhouse-client in another terminal navigate to `ClickHouse/build/programs/` and run `./clickhouse client`.

If you get `Connection refused` message on Mac OS X or FreeBSD, try specifying host address 127.0.0.1:

    clickhouse client --host 127.0.0.1

You can replace the production version of ClickHouse binary installed in your system with your custom-built ClickHouse binary. To do that install ClickHouse on your machine following the instructions from the official website. Next, run the following:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

Note that `clickhouse-client`, `clickhouse-server` and others are symlinks to the commonly shared `clickhouse` binary.

You can also run your custom-built ClickHouse binary with the config file from the ClickHouse package installed on your system:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

## IDE (Integrated Development Environment) {#ide-integrated-development-environment}

If you do not know which IDE to use, we recommend that you use CLion. CLion is commercial software, but it offers 30 days free trial period. It is also free of charge for students. CLion can be used both on Linux and on Mac OS X.

KDevelop and QTCreator are other great alternatives of an IDE for developing ClickHouse. KDevelop comes in as a very handy IDE although unstable. If KDevelop crashes after a while upon opening project, you should click “Stop All” button as soon as it has opened the list of project’s files. After doing so KDevelop should be fine to work with.

As simple code editors, you can use Sublime Text or Visual Studio Code, or Kate (all of which are available on Linux).

Just in case, it is worth mentioning that CLion creates `build` path on its own, it also on its own selects `debug` for build type, for configuration it uses a version of CMake that is defined in CLion and not the one installed by you, and finally, CLion will use `make` to run build tasks instead of `ninja`. This is normal behaviour, just keep that in mind to avoid confusion.

## Writing Code {#writing-code}

The description of ClickHouse architecture can be found here: https://clickhouse.com/docs/en/development/architecture/

The Code Style Guide: https://clickhouse.com/docs/en/development/style/

Adding third-party libraries: https://clickhouse.com/docs/en/development/contrib/#adding-third-party-libraries

Writing tests: https://clickhouse.com/docs/en/development/tests/

List of tasks: https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3Ahacktoberfest

## Test Data {#test-data}

Developing ClickHouse often requires loading realistic datasets. It is particularly important for performance testing. We have a specially prepared set of anonymized data of web analytics. It requires additionally some 3GB of free disk space. Note that this data is not required to accomplish most of the development tasks.

    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client

    CREATE DATABASE IF NOT EXISTS test

    CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

    CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
    clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv

## Creating Pull Request {#creating-pull-request}

Navigate to your fork repository in GitHub’s UI. If you have been developing in a branch, you need to select that branch. There will be a “Pull request” button located on the screen. In essence, this means “create a request for accepting my changes into the main repository”.

A pull request can be created even if the work is not completed yet. In this case please put the word “WIP” (work in progress) at the beginning of the title, it can be changed later. This is useful for cooperative reviewing and discussion of changes as well as for running all of the available tests. It is important that you provide a brief description of your changes, it will later be used for generating release changelog.

Testing will commence as soon as ClickHouse employees label your PR with a tag “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

The system will prepare ClickHouse binary builds for your pull request individually. To retrieve these builds click the “Details” link next to “ClickHouse build check” entry in the list of checks. There you will find direct links to the built .deb packages of ClickHouse which you can deploy even on your production servers (if you have no fear).

Most probably some of the builds will fail at first times. This is due to the fact that we check builds both with gcc as well as with clang, with almost all of existing warnings (always with the `-Werror` flag) enabled for clang. On that same page, you can find all of the build logs so that you do not have to build ClickHouse in all of the possible ways.

## Faster builds for development: Split build configuration {#split-build}

ClickHouse is normally statically linked into a single static `clickhouse` binary with minimal dependencies. This is convenient for distribution, but it means that for every change the entire binary needs to be re-linked, which is slow and inconvenient for development. As an alternative, you can instead build dynamically linked shared libraries and separate binaries `clickhouse-server`, `clickhouse-client` etc., allowing for faster incremental builds. To use it, add the following flags to your `cmake` invocation:
```
-DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DCLICKHOUSE_SPLIT_BINARY=1
```

Note that the split build has several drawbacks:
* There is no single `clickhouse` binary, and you have to run `clickhouse-server`, `clickhouse-client`, etc.
* Risk of segfault if you run any of the programs while rebuilding the project.
* You cannot run the integration tests since they only work a single complete binary.
* You can't easily copy the binaries elsewhere. Instead of moving a single binary you'll need to copy all binaries and libraries.

If you are not interested in functionality provided by third-party libraries, you can further speed up the build using `cmake` options
```
-DENABLE_LIBRARIES=0 -DENABLE_EMBEDDED_COMPILER=0
```

In case of problems with any of the development options, you are on your own!
