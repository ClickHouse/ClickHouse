---
toc_priority: 61
toc_title: "Инструкция для разработчиков"
---

# Инструкция для разработчиков

Сборка ClickHouse поддерживается на Linux, FreeBSD, Mac OS X.

## Если вы используете Windows {#esli-vy-ispolzuete-windows}

Если вы используете Windows, вам потребуется создать виртуальную машину с Ubuntu. Для работы с виртуальной машиной, установите VirtualBox. Скачать Ubuntu можно на сайте: https://www.ubuntu.com/#download Создайте виртуальную машину из полученного образа. Выделите для неё не менее 4 GB оперативной памяти. Для запуска терминала в Ubuntu, найдите в меню программу со словом terminal (gnome-terminal, konsole или что-то в этом роде) или нажмите Ctrl+Alt+T.

## Если вы используете 32-битную систему {#esli-vy-ispolzuete-32-bitnuiu-sistemu}

ClickHouse не работает и не собирается на 32-битных системах. Получите доступ к 64-битной системе и продолжайте.

## Создание репозитория на GitHub {#sozdanie-repozitoriia-na-github}

Для работы с репозиторием ClickHouse, вам потребуется аккаунт на GitHub. Наверное, он у вас уже есть.

Если аккаунта нет - зарегистрируйтесь на https://github.com/. Создайте ssh ключи, если их нет, и загрузите публичные ключи на GitHub. Это потребуется для отправки изменений. Для работы с GitHub можно использовать такие же ssh ключи, как и для работы с другими ssh серверами - скорее всего, они уже у вас есть.

Создайте fork репозитория ClickHouse. Для этого, на странице https://github.com/ClickHouse/ClickHouse нажмите на кнопку «fork» в правом верхнем углу. Вы получите полную копию репозитория ClickHouse на своём аккаунте, которая называется «форк». Процесс разработки состоит в том, чтобы внести нужные изменения в свой форк репозитория, а затем создать «pull request» для принятия изменений в основной репозиторий.

Для работы с git репозиториями, установите `git`.

В Ubuntu выполните в терминале:

    sudo apt update
    sudo apt install git

Краткое руководство по использованию Git: https://education.github.com/git-cheat-sheet-education.pdf

Подробное руководство по использованию Git: https://git-scm.com/book/ru/v2

## Клонирование репозитория на рабочую машину {#klonirovanie-repozitoriia-na-rabochuiu-mashinu}

Затем вам потребуется загрузить исходники для работы на свой компьютер. Это называется «клонирование репозитория», потому что создаёт на вашем компьютере локальную копию репозитория, с которой вы будете работать.

Выполните в терминале:

    git clone git@github.com:your_github_username/ClickHouse.git --recursive
    cd ClickHouse

Замените слово `your_github_username` в команде для git на имя вашего аккаунта на GitHub.

Эта команда создаст директорию ClickHouse, содержащую рабочую копию проекта.

Необходимо, чтобы путь к рабочей копии не содержал пробелы в именах директорий. Это может привести к проблемам в работе системы сборки.

Обратите внимание, что репозиторий ClickHouse использует submodules. Так называются ссылки на дополнительные репозитории (например, внешние библиотеки, от которых зависит проект). Это значит, что при клонировании репозитория, следует указывать ключ `--recursive`, как в примере выше. Если репозиторий был клонирован без submodules, то для их скачивания, необходимо выполнить:

    git submodule init
    git submodule update

Проверить наличие submodules можно с помощью команды `git submodule status`.

Если вы получили сообщение об ошибке:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

Как правило это означает, что отсутствуют ssh ключи для соединения с GitHub. Ключи расположены в директории `~/.ssh`. В интерфейсе GitHub, в настройках, необходимо загрузить публичные ключи, чтобы он их понимал.

Вы также можете клонировать репозиторий по протоколу https:

    git clone https://github.com/ClickHouse/ClickHouse.git

Этот вариант не подходит для отправки изменений на сервер. Вы можете временно его использовать, а затем добавить ssh ключи и заменить адрес репозитория с помощью команды `git remote`.

Вы можете также добавить для своего локального репозитория адрес оригинального репозитория Яндекса, чтобы притягивать оттуда обновления:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

После этого, вы сможете добавлять в свой репозиторий обновления из репозитория Яндекса с помощью команды `git pull upstream master`.

### Работа с сабмодулями Git {#rabota-s-sabmoduliami-git}

Работа с сабмодулями git может быть достаточно болезненной. Следующие команды позволят содержать их в порядке:

    # ! Каждая команда принимает аргумент
    # Обновить URLs удалённого репозитория для каждого сабмодуля, используется относительно редко
    git submodule sync
    # Добавить новые сабмодули
    git submodule init
    # Обновить сабмодули до актуального состояния
    git submodule update
    # Две последние команды могут быть объединены вместе:
    git submodule update --init

Следующие команды помогут сбросить все сабмодули в изначальное состояние (!ВНИМАНИЕ! - все изменения в сабмодулях будут утеряны):

    # Synchronizes submodules' remote URL with .gitmodules
    # Обновить URLs удалённого репозитория для каждого сабмодуля
    git submodule sync
    # Обновить существующие модули и добавить отсутствующие
    git submodule update --init
    # Удалить все изменения в сабмодуле относительно HEAD
    git submodule foreach git reset --hard
    # Очистить игнорируемые файлы
    git submodule foreach git clean -xfd
    # Повторить последние 4 команды для каждого из сабмодулей
    git submodule foreach git submodule sync
    git submodule foreach git submodule update --init
    git submodule foreach git submodule foreach git reset --hard
    git submodule foreach git submodule foreach git clean -xfd

## Система сборки {#sistema-sborki}

ClickHouse использует систему сборки CMake и Ninja.

CMake - генератор задач сборки.
Ninja - система запуска сборочных задач.

Для установки на Ubuntu или Debian, Mint, выполните `sudo apt install cmake ninja-build`.

Для установки на CentOS, RedHat, выполните `sudo yum install cmake ninja-build`.

Если у вас Arch или Gentoo, то вы сами знаете, как установить CMake.

Для установки CMake и Ninja на Mac OS X, сначала установите Homebrew, а затем, с помощью него, установите всё остальное.

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

Проверьте версию CMake: `cmake --version`. Если версия меньше 3.12, то установите новую версию с сайта https://cmake.org/download/

## Необязательные внешние библиотеки {#neobiazatelnye-vneshnie-biblioteki}

ClickHouse использует для сборки некоторое количество внешних библиотек. Но ни одну из них не требуется отдельно устанавливать, так как они собираются вместе с ClickHouse, из исходников, которые расположены в submodules. Посмотреть набор этих библиотек можно в директории contrib.

## Компилятор C++ {#kompiliator-c}

В качестве компилятора C++ поддерживается Clang начиная с версии 11.

Впрочем, наша среда continuous integration проверяет около десятка вариантов сборки, включая gcc, но сборка с помощью gcc непригодна для использования в продакшене.

На Ubuntu и Debian вы можете использовать скрипт для автоматической установки (см. [официальный сайт](https://apt.llvm.org/))

```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

Сборка под Mac OS X поддерживается только для компилятора Clang. Чтобы установить его выполните `brew install llvm`

## Процесс сборки {#protsess-sborki}

Теперь вы готовы к сборке ClickHouse. Для размещения собранных файлов, рекомендуется создать отдельную директорию build внутри директории ClickHouse:

    mkdir build
    cd build

Вы можете иметь несколько разных директорий (build_release, build_debug) для разных вариантов сборки.

Находясь в директории build, выполните конфигурацию сборки с помощью CMake.
Перед первым запуском необходимо выставить переменные окружения, отвечающие за выбор компилятора.

    export CC=clang CXX=clang++
    cmake ..

Переменная CC отвечает за компилятор C (сокращение от слов C Compiler), переменная CXX отвечает за выбор компилятора C++ (символ X - это как плюс, но положенный набок, ради того, чтобы превратить его в букву). При получении ошибки типа `Could not find compiler set in environment variable CC: clang` необходимо указать в значениях для переменных CC и CXX явную версию компилятора, например, `clang-12` и `clang++-12`.

Для более быстрой сборки, можно использовать debug вариант - сборку без оптимизаций. Для этого, укажите параметр `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

В случае использования на разработческой машине старого HDD или SSD, а также при желании использовать меньше места для артефактов сборки можно использовать следующую команду:
```bash
cmake -DUSE_DEBUG_HELPERS=1 -DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DCLICKHOUSE_SPLIT_BINARY=1 ..
```
При этом надо учесть, что получаемые в результате сборки исполнимые файлы будут динамически слинкованы с библиотеками, и поэтому фактически станут непереносимыми на другие компьютеры (либо для этого нужно будет предпринять значительно больше усилий по сравнению со статической сборкой). Плюсом же в данном случае является значительно меньшее время сборки (это проявляется не на первой сборке, а на последующих, после внесения изменений в исходный код - тратится меньшее время на линковку по сравнению со статической сборкой) и значительно меньшее использование места на жёстком диске (экономия более, чем в 3 раза по сравнению со статической сборкой). Для целей разработки, когда планируются только отладочные запуски на том же компьютере, где осуществлялась сборка, это может быть наиболее удобным вариантом.

Вы можете изменить вариант сборки, выполнив новую команду в директории build.

Запустите ninja для сборки:

    ninja clickhouse-server clickhouse-client

В этом примере собираются только нужные в первую очередь программы.

Если вы хотите собрать все программы (утилиты и тесты), то запустите ninja без параметров:

    ninja

Для полной сборки требуется около 30 GB свободного места на диске или 15 GB для сборки только основных программ.

При наличии небольшого количества оперативной памяти на компьютере, следует ограничить количество параллельных задач с помощью параметра `-j`:

    ninja -j 1 clickhouse-server clickhouse-client

На машинах с 4 GB памяти, рекомендуется указывать значение 1, а если памяти до 8 GB, укажите значение 2.

Если вы получили сообщение `ninja: error: loading 'build.ninja': No such file or directory`, значит конфигурация сборки прошла с ошибкой и вам необходимо посмотреть на сообщение об ошибке выше.

В случае успешного запуска, вы увидите прогресс сборки - количество обработанных задач и общее количество задач.

В процессе сборки могут появится сообщения `libprotobuf WARNING` про protobuf файлы в библиотеке libhdfs2. Это не имеет значения.

В случае получения ошибок вида `error: variable 'y' set but not used [-Werror,-Wunused-but-set-variable]` ножно попробовать использовать другую версию компилятора сlang. Например, на момент написания данного текста описанная выше команда по установке clang для Ubuntu 20.04 по-умолчанию устанавливает clang-13, с которым возникает эта ошибка. Для решения проблемы можно установить clang-12 с помощью команд:
```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 12
```
И далее использовать именно его, указав соответствующую версию при установке переменных окружения CC и CXX перед вызовом cmake.

При успешной сборке, вы получите готовый исполняемый файл `ClickHouse/build/programs/clickhouse`:

    ls -l programs/clickhouse

## Запуск собранной версии ClickHouse {#zapusk-sobrannoi-versii-clickhouse}

Для запуска сервера из под текущего пользователя, с выводом логов в терминал и с использованием примеров конфигурационных файлов, расположенных в исходниках, перейдите в директорию `ClickHouse/programs/server/` (эта директория находится не в директории build) и выполните:

    ../../build/programs/clickhouse server

В этом случае, ClickHouse будет использовать конфигурационные файлы, расположенные в текущей директории. Вы можете запустить `clickhouse server` из любой директории, передав ему путь к конфигурационному файлу в аргументе командной строки `--config-file`.

Для подключения к ClickHouse с помощью clickhouse-client, в соседнем терминале, зайдите в директорию `ClickHouse/build/programs/` и выполните `./clickhouse client`.

Если вы получили сообщение `Connection refused` на Mac OS X или FreeBSD, то укажите для клиента 127.0.0.1 в качестве имени хоста:

    clickhouse client --host 127.0.0.1

Вы можете заменить собранным вами ClickHouse продакшен версию, установленную в системе. Для этого, установите ClickHouse на свою машину по инструкции с официального сайта. Затем выполните:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

Обратите внимание, что `clickhouse-client`, `clickhouse-server` и другие, являеются симлинками на общий бинарник `clickhouse`.

Также вы можете запустить собранный вами ClickHouse с конфигурационным файлом системного ClickHouse:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

## Среда разработки {#sreda-razrabotki}

Если вы не знаете, какую среду разработки использовать, то рекомендуется использовать CLion. CLion является платным ПО, но его можно использовать бесплатно в течение пробного периода. Также он бесплатен для учащихся. CLion можно использовать как под Linux, так и под Mac OS X.

Также в качестве среды разработки, вы можете использовать KDevelop или QTCreator. KDevelop - очень удобная, но нестабильная среда разработки. Если KDevelop вылетает через небольшое время после открытия проекта, вам следует нажать на кнопку «Stop All» как только он открыл список файлов проекта. После этого, KDevelop можно будет использовать.

В качестве простых редакторов кода можно использовать Sublime Text или Visual Studio Code или Kate (все варианты доступны под Linux).

На всякий случай заметим, что CLion самостоятельно создаёт свою build директорию, самостоятельно выбирает тип сборки debug по-умолчанию, для конфигурации использует встроенную в CLion версию CMake вместо установленного вами, а для запуска задач использует make вместо ninja (но при желании начиная с версии CLion 2019.3 EAP можно настроить использование ninja, см. подробнее [тут](https://blog.jetbrains.com/clion/2019/10/clion-2019-3-eap-ninja-cmake-generators/)). Это нормально, просто имейте это ввиду, чтобы не возникало путаницы.

## Написание кода {#napisanie-koda}

Описание архитектуры ClickHouse: https://clickhouse.com/docs/ru/development/architecture/

Стиль кода: https://clickhouse.com/docs/ru/development/style/

Рекомендации по добавлению сторонних библиотек и поддержанию в них пользовательских изменений: https://clickhouse.com/docs/ru/development/contrib/#adding-third-party-libraries

Разработка тестов: https://clickhouse.com/docs/ru/development/tests/

Список задач: https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22

## Тестовые данные {#testovye-dannye}

Разработка ClickHouse часто требует загрузки реалистичных наборов данных. Особенно это важно для тестирования производительности. Специально для вас мы подготовили набор данных, представляющий собой анонимизированные данные Яндекс.Метрики. Загрузка этих данных потребует ещё 3 GB места на диске. Для выполнения большинства задач разработки, загружать эти данные не обязательно.

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

## Создание Pull Request {#sozdanie-pull-request}

Откройте свой форк репозитория в интерфейсе GitHub. Если вы вели разработку в бранче, выберите этот бранч. На странице будет доступна кнопка «Pull request». По сути, это означает «создать заявку на принятие моих изменений в основной репозиторий».

Pull request можно создать, даже если работа над задачей ещё не завершена. В этом случае, добавьте в его название слово «WIP» (work in progress). Название можно будет изменить позже. Это полезно для совместного просмотра и обсуждения изменений, а также для запуска всех имеющихся тестов. Введите краткое описание изменений - впоследствии, оно будет использовано для релизных changelog.

Тесты будут запущены, как только сотрудники Яндекса поставят для pull request тег «Can be tested». Результаты первых проверок (стиль кода) появятся уже через несколько минут. Результаты сборки появятся примерно через пол часа. Результаты основного набора тестов будут доступны в пределах часа.

Система подготовит сборки ClickHouse специально для вашего pull request. Для их получения, нажмите на ссылку «Details» у проверки «Clickhouse build check». Там вы сможете найти прямые ссылки на собранные .deb пакеты ClickHouse, которые, при желании, вы даже сможете установить на свои продакшен серверы (если не страшно).

Вероятнее всего, часть сборок не будет успешной с первого раза. Ведь мы проверяем сборку кода и gcc и clang, а при сборке с помощью clang включаются почти все существующие в природе warnings (всегда с флагом `-Werror`). На той же странице, вы сможете найти логи сборки - вам не обязательно самому собирать ClickHouse всеми возможными способами.
