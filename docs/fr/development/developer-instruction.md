---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "Le D\xE9butant Clickhouse Developer Instruction"
---

La construction de ClickHouse est prise en charge sous Linux, FreeBSD et Mac OS X.

# Si Vous Utilisez Windows {#if-you-use-windows}

Si vous utilisez Windows, vous devez créer une machine virtuelle avec Ubuntu. Pour commencer à travailler avec une machine virtuelle, installez VirtualBox. Vous pouvez télécharger Ubuntu sur le site: https://www.ubuntu.com/#download. veuillez créer une machine virtuelle à partir de l'image téléchargée (vous devez réserver au moins 4 Go de RAM pour cela). Pour exécuter un terminal de ligne de commande dans Ubuntu, recherchez un programme contenant le mot “terminal” dans son nom (gnome-terminal, konsole etc.) ou appuyez simplement sur Ctrl + Alt + T.

# Si vous utilisez un système 32 bits {#if-you-use-a-32-bit-system}

ClickHouse ne peut pas fonctionner ou construire sur un système 32 bits. Vous devez acquérir l'accès à un système 64 bits et vous pouvez continuer la lecture.

# Création d'un référentiel sur GitHub {#creating-a-repository-on-github}

Pour commencer à travailler avec clickhouse repository, vous aurez besoin d'un compte GitHub.

Vous en avez probablement déjà un, mais si vous ne le faites pas, veuillez vous inscrire à https://github.com. dans le cas où vous n'avez pas de clés SSH, vous devez les générer, puis les télécharger sur GitHub. Il est nécessaire pour l'envoi de vos correctifs. Il est également possible d'utiliser les mêmes clés SSH que vous utilisez avec d'autres serveurs SSH - vous les avez probablement déjà.

Créer un fork de clickhouse repository. Pour ce faire, cliquez sur l' “fork” bouton dans le coin supérieur droit à https://github.com/ClickHouse/ClickHouse. il fourche votre propre copie de ClickHouse / ClickHouse à votre compte.

Le processus de développement consiste d'abord à valider les modifications prévues dans votre fork de ClickHouse, puis à créer un “pull request” pour que ces modifications soient acceptées dans le référentiel principal (ClickHouse/ClickHouse).

Pour travailler avec les dépôts git, veuillez installer `git`.

Pour ce faire dans Ubuntu vous exécutez dans le terminal de ligne de commande:

    sudo apt update
    sudo apt install git

Un bref manuel sur l'utilisation de Git peut être trouvé ici: https://education.github.com/git-cheat-sheet-education.pdf.
Pour un manuel détaillé sur Git voir https://git-scm.com/book/en/v2.

# Clonage D'un référentiel sur votre machine de développement {#cloning-a-repository-to-your-development-machine}

Ensuite, vous devez télécharger les fichiers source sur votre machine de travail. Ceci est appelé “to clone a repository” parce qu'il crée une copie locale du dépôt sur votre machine de travail.

Dans le terminal de ligne de commande exécuter:

    git clone --recursive git@github.com:your_github_username/ClickHouse.git
    cd ClickHouse

Remarque: Veuillez remplacer *your_github_username* avec ce qui est approprié!

Cette commande va créer un répertoire `ClickHouse` contenant la copie de travail du projet.

Il est important que le chemin d'accès au répertoire de travail ne contienne aucun espace, car cela peut entraîner des problèmes lors de l'exécution du système de construction.

Veuillez noter que clickhouse repository utilise `submodules`. That is what the references to additional repositories are called (i.e. external libraries on which the project depends). It means that when cloning the repository you need to specify the `--recursive` drapeau comme dans l'exemple ci-dessus. Si le dépôt a été cloné sans submodules, pour télécharger, vous devez exécuter les opérations suivantes:

    git submodule init
    git submodule update

Vous pouvez vérifier l'état avec la commande: `git submodule status`.

Si vous obtenez le message d'erreur suivantes:

    Permission denied (publickey).
    fatal: Could not read from remote repository.

    Please make sure you have the correct access rights
    and the repository exists.

Cela signifie généralement que les clés SSH pour la connexion à GitHub sont manquantes. Ces clés sont normalement situés dans `~/.ssh`. Pour que les clés SSH soient acceptées, vous devez les télécharger dans la section Paramètres de L'interface utilisateur GitHub.

Vous pouvez également cloner le référentiel via le protocole https:

    git clone https://github.com/ClickHouse/ClickHouse.git

Ceci, cependant, ne vous permettra pas d'envoyer vos modifications sur le serveur. Vous pouvez toujours l'utiliser temporairement et ajouter les clés SSH plus tard en remplaçant l'adresse distante du référentiel par `git remote` commande.

Vous pouvez également ajouter l'adresse du dépôt clickhouse original à votre référentiel local pour extraire les mises à jour à partir de là:

    git remote add upstream git@github.com:ClickHouse/ClickHouse.git

Après avoir exécuté avec succès cette commande vous serez en mesure de tirer les mises à jour du repo clickhouse principal en exécutant `git pull upstream master`.

## Travailler avec des Submodules {#working-with-submodules}

Travailler avec des sous-modules dans git pourrait être douloureux. Prochaines commandes aidera à gérer:

    # ! each command accepts --recursive
    # Update remote URLs for submodules. Barely rare case
    git submodule sync
    # Add new submodules
    git submodule init
    # Update existing submodules to the current state
    git submodule update
    # Two last commands could be merged together
    git submodule update --init

Les commandes suivantes vous aideront à réinitialiser tous les sous-modules à l'état initial (!AVERTISSEMENT! - tout changement à l'intérieur sera supprimé):

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

# Système De Construction {#build-system}

ClickHouse utilise CMake et Ninja pour la construction.

CMake - un système de méta-construction qui peut générer des fichiers Ninja (tâches de construction).
Ninja - un système de construction plus petit avec un accent sur la vitesse utilisée pour exécuter ces tâches générées cmake.

Pour installer sur Ubuntu, Debian ou mint run `sudo apt install cmake ninja-build`.

Sur CentOS, RedHat run `sudo yum install cmake ninja-build`.

Si vous utilisez Arch ou Gentoo, vous savez probablement vous - même comment installer CMake.

Pour installer CMake et Ninja sur Mac OS X installez D'abord Homebrew puis installez tout le reste via brew:

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew install cmake ninja

Ensuite, vérifiez la version de CMake: `cmake --version`. Si elle est inférieure à 3.3, vous devez installer une version plus récente du site web: https://cmake.org/download/.

# Bibliothèques Externes Facultatives {#optional-external-libraries}

ClickHouse utilise plusieurs bibliothèques externes pour la construction. Tous n'ont pas besoin d'être installés séparément car ils sont construits avec ClickHouse à partir des sources situées dans les sous-modules. Vous pouvez vérifier la liste dans `contrib`.

# Compilateur C++  {#c-compiler}

Les compilateurs GCC à partir de la version 10 et Clang version 8 ou supérieure sont pris en charge pour construire ClickHouse.

Les builds officiels de Yandex utilisent actuellement GCC car ils génèrent du code machine de performances légèrement meilleures (ce qui donne une différence allant jusqu'à plusieurs pour cent selon nos benchmarks). Et Clang est plus pratique pour le développement habituellement. Cependant, notre plate-forme d'intégration continue (CI) vérifie environ une douzaine de combinaisons de construction.

Pour installer GCC sur Ubuntu Exécutez: `sudo apt install gcc g++`

Vérifiez la version de gcc: `gcc --version`. Si elle est inférieure à 10, suivez les instructions ici: https://clickhouse.tech/docs/fr/development/build/#install-gcc-10.

Mac OS X build est pris en charge uniquement pour Clang. Il suffit d'exécuter `brew install llvm`

Si vous décidez d'utiliser Clang, vous pouvez également installer `libc++` et `lld` si vous savez ce que c'est. Utiliser `ccache` est également recommandé.

# Le Processus De Construction {#the-building-process}

Maintenant que vous êtes prêt à construire ClickHouse nous vous conseillons de créer un répertoire séparé `build` à l'intérieur de `ClickHouse` qui contiendra tous les artefacts de construction:

    mkdir build
    cd build

Vous pouvez avoir plusieurs répertoires différents (build_release, build_debug, etc.) pour les différents types de construction.

Tandis qu'à l'intérieur de la `build` répertoire, configurez votre build en exécutant CMake. Avant la première exécution, vous devez définir des variables d'environnement qui spécifient le compilateur (compilateur gcc version 10 dans cet exemple).

Linux:

    export CC=gcc-10 CXX=g++-10
    cmake ..

Mac OS X:

    export CC=clang CXX=clang++
    cmake ..

Le `CC` variable spécifie le compilateur pour C (abréviation de compilateur C), et `CXX` variable indique quel compilateur C++ doit être utilisé pour la construction.

Pour une version plus rapide, vous pouvez recourir à l' `debug` build type - Une construction sans optimisations. Pour cela fournissez le paramètre suivant `-D CMAKE_BUILD_TYPE=Debug`:

    cmake -D CMAKE_BUILD_TYPE=Debug ..

Vous pouvez modifier le type de génération en exécutant cette commande dans le `build` répertoire.

Exécutez ninja pour construire:

    ninja clickhouse-server clickhouse-client

Seules les binaires vont être construites dans cet exemple.

Si vous avez besoin de construire tous les binaires (utilitaires et tests), vous devez exécuter ninja sans paramètres:

    ninja

La construction complète nécessite environ 30 Go d'espace disque libre ou 15 Go pour construire les binaires principaux.

Lorsqu'une grande quantité de RAM est disponible sur la machine de construction vous devez limiter le nombre de tâches de construction exécutées en parallèle avec `-j` param:

    ninja -j 1 clickhouse-server clickhouse-client

Sur les machines avec 4 Go de RAM, il est recommandé de spécifier 1, pour 8 Go de RAM `-j 2` est recommandé.

Si vous recevez le message: `ninja: error: loading 'build.ninja': No such file or directory`, cela signifie que la génération d'une configuration de construction a échoué et que vous devez inspecter le message ci-dessus.

Après le démarrage réussi du processus de construction, vous verrez la progression de la construction - le nombre de tâches traitées et le nombre total de tâches.

Lors de la construction de messages sur les fichiers protobuf dans la bibliothèque libhdfs2 comme `libprotobuf WARNING` peuvent apparaître. Ils touchent rien et sont sûrs d'être ignoré.

Lors de la construction, vous obtenez un fichier exécutable `ClickHouse/<build_dir>/programs/clickhouse`:

    ls -l programs/clickhouse

# Exécution de L'exécutable construit de ClickHouse {#running-the-built-executable-of-clickhouse}

Pour exécuter le serveur sous l'utilisateur actuel vous devez naviguer vers `ClickHouse/programs/server/` (situé à l'extérieur de `build` et les exécuter:

    ../../build/programs/clickhouse server

Dans ce cas, ClickHouse utilisera les fichiers de configuration situés dans le répertoire courant. Vous pouvez l'exécuter `clickhouse server` depuis n'importe quel répertoire spécifiant le chemin d'accès à un fichier de configuration en tant que paramètre de ligne de commande `--config-file`.

Pour vous connecter à ClickHouse avec clickhouse-client dans un autre terminal, accédez à `ClickHouse/build/programs/` et exécuter `./clickhouse client`.

Si vous obtenez `Connection refused` message sur Mac OS X ou FreeBSD, essayez de spécifier l'adresse hôte 127.0.0.1:

    clickhouse client --host 127.0.0.1

Vous pouvez remplacer la version de production de clickhouse binary installée dans votre système par votre clickhouse Binary sur mesure. Pour ce faire, installez ClickHouse sur votre machine en suivant les instructions du site officiel. Ensuite, exécutez ce qui suit:

    sudo service clickhouse-server stop
    sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
    sudo service clickhouse-server start

Notez que `clickhouse-client`, `clickhouse-server` et d'autres sont des liens symboliques à la commune `clickhouse` binaire.

Vous pouvez également exécuter votre binaire ClickHouse personnalisé avec le fichier de configuration du package clickhouse installé sur votre système:

    sudo service clickhouse-server stop
    sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml

# IDE (environnement de développement intégré) {#ide-integrated-development-environment}

Si vous ne savez pas quel IDE utiliser, nous vous recommandons D'utiliser CLion. CLion est un logiciel commercial, mais il offre une période d'essai gratuite de 30 jours. Il est également gratuit pour les étudiants. CLion peut être utilisé à la fois sur Linux et sur Mac OS X.

KDevelop et QTCreator sont d'autres grandes alternatives D'un IDE pour développer ClickHouse. KDevelop est un IDE très pratique bien qu'instable. Si KDevelop se bloque après un certain temps lors de l'ouverture du projet, vous devez cliquer sur “Stop All” bouton dès qu'il a ouvert la liste des fichiers du projet. Après cela, KDevelop devrait être bien pour travailler avec.

En tant qu'éditeurs de code simples, vous pouvez utiliser Sublime Text ou Visual Studio Code, ou Kate (qui sont tous disponibles sur Linux).

Juste au cas où, il convient de mentionner que CLion crée `build` chemin sur son propre, il aussi sur son propre sélectionne `debug` pour le type de construction, pour la configuration, il utilise une version de CMake définie dans CLion et non celle installée par vous, et enfin, CLion utilisera `make` pour exécuter construire des tâches au lieu de `ninja`. C'est un comportement normal, gardez cela à l'esprit pour éviter toute confusion.

# L'Écriture De Code {#writing-code}

La description de l'architecture ClickHouse peut être trouvée ici: https://clickhouse.tech/docs/fr/développement/architecture/

Le code Style Guide: https://clickhouse.tech/docs/fr/développement/style/

Rédaction de tests: https://clickhouse.tech/docs/fr/développement/tests/

Liste des tâches: https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22

# Des Données De Test {#test-data}

Le développement de ClickHouse nécessite souvent le chargement d'ensembles de données réalistes. Il est particulièrement important pour les tests de performance. Nous avons un ensemble spécialement préparé de données anonymisées de Yandex.Metrica. Il nécessite en outre quelques 3 Go d'espace disque libre. Notez que ces données ne sont pas requises pour accomplir la plupart des tâches de développement.

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

# La Création De Pull Request {#creating-pull-request}

Accédez à votre référentiel fork dans L'interface utilisateur de GitHub. Si vous avez développé dans une branche, vous devez sélectionner cette branche. Il y aura un “Pull request” bouton situé sur l'écran. En substance, cela signifie “create a request for accepting my changes into the main repository”.

Une demande d'extraction peuvent être créés, même si le travail n'est pas encore terminée. Dans ce cas veuillez mettre le mot “WIP” (travaux en cours) au début du titre, il peut être modifié plus tard. Ceci est utile pour l'examen coopératif et la discussion des changements ainsi que pour l'exécution de tous les tests disponibles. Il est important que vous fournissiez une brève description de vos modifications, il sera ensuite utilisé pour générer des journaux de modifications de version.

Les tests commenceront dès que les employés de Yandex étiqueteront votre PR avec une étiquette “can be tested”. The results of some first checks (e.g. code style) will come in within several minutes. Build check results will arrive within half an hour. And the main set of tests will report itself within an hour.

Le système préparera les builds binaires ClickHouse pour votre demande de tirage individuellement. Pour récupérer ces versions cliquez sur le “Details” lien à côté “ClickHouse build check” entrée dans la liste de vérifications. Vous y trouverez des liens directs vers les construit .paquets deb de ClickHouse que vous pouvez déployer même sur vos serveurs de production (si vous n'avez pas peur).

Très probablement, certaines des constructions échoueront à la première fois. Cela est dû au fait que nous avons vérifier s'appuie à la fois avec gcc, ainsi qu'avec clang, pratiquement tous les avertissements existants (toujours avec le `-Werror` drapeau) activé pour clang. Sur cette même page, vous pouvez trouver tous les journaux de construction afin que vous n'ayez pas à construire ClickHouse de toutes les manières possibles.
