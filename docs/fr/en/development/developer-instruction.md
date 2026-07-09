---
description: 'Prérequis et instructions de configuration pour le développement de ClickHouse'
sidebar_label: 'Prérequis'
sidebar_position: 5
slug: /development/developer-instruction
title: 'Prérequis pour les développeurs'
doc_type: 'guide'
---

ClickHouse peut être compilé sous Linux, FreeBSD et macOS.
Si vous utilisez Windows, vous pouvez tout de même compiler ClickHouse dans une machine virtuelle sous Linux, par exemple [VirtualBox](https://www.virtualbox.org/) avec Ubuntu.

<div id="create-a-repository-on-github">
  ## Créer un dépôt sur GitHub
</div>

Pour commencer à développer pour ClickHouse, vous aurez besoin d&#39;un compte [GitHub](https://www.github.com/).
Veuillez également générer une clé SSH localement (si vous n&#39;en avez pas déjà une) et téléverser la clé publique sur GitHub, car il s&#39;agit d&#39;un prérequis pour soumettre des correctifs.

Ensuite, créez un fork du [dépôt ClickHouse](https://github.com/ClickHouse/ClickHouse/) dans votre compte personnel en cliquant sur le bouton &quot;fork&quot; dans le coin supérieur droit.

Pour contribuer des modifications, par exemple un correctif pour une issue ou une fonctionnalité, commencez par valider vos modifications sur une branche de votre fork, puis créez une &quot;Pull Request&quot; vers le dépôt principal.

Pour travailler avec des dépôts Git, veuillez installer Git. Par exemple, sur Ubuntu, exécutez :

```sh
sudo apt update
sudo apt install git
```

Un aide-mémoire Git est disponible [ici](https://education.github.com/git-cheat-sheet-education.pdf).
Un manuel Git détaillé est disponible [ici](https://git-scm.com/book/en/v2).

<div id="clone-the-repository-to-your-development-machine">
  ## Clonez le dépôt sur votre machine de développement
</div>

Commencez par télécharger les fichiers sources sur votre poste de travail, c’est-à-dire en clonant le dépôt :

```sh
git clone git@github.com:your_github_username/ClickHouse.git  # replace the placeholder with your GitHub user name
cd ClickHouse
```

Cette commande crée un répertoire `ClickHouse/` contenant le code source, les tests et d’autres fichiers.
Vous pouvez spécifier un répertoire de destination personnalisé après l’URL, mais il est important que ce chemin ne contienne pas d’espaces, car cela pourrait faire échouer le build par la suite.

Le dépôt Git de ClickHouse utilise des sous-modules pour inclure des bibliothèques tierces.
Les sous-modules ne sont pas extraits par défaut.
Vous pouvez soit

* exécuter `git clone` avec l’option `--recurse-submodules`,

* si `git clone` est exécuté sans `--recurse-submodules`, exécuter `git submodule update --init --jobs <N>` pour extraire explicitement tous les sous-modules. (`<N>` peut par exemple être défini sur `12` pour paralléliser le téléchargement.)

* si `git clone` est exécuté sans `--recurse-submodules` et que vous souhaitez utiliser une extraction superficielle ([shallow](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/)) des sous-modules pour omettre leur historique et économiser de l’espace, exécutez `./contrib/update-submodules.sh`. Cette alternative est utilisée par l’intégration continue, mais n’est pas recommandée pour le développement local, car elle rend l’utilisation des sous-modules moins pratique et plus lente.

Pour vérifier l’état des sous-modules Git, exécutez `git submodule status`.

Si vous obtenez le message d’erreur suivant

```bash
Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

les clés SSH nécessaires pour vous connecter à GitHub sont absentes.
Ces clés se trouvent normalement dans `~/.ssh`.
Pour que les clés SSH soient acceptées, vous devez les téléverser dans les paramètres de GitHub.

Vous pouvez également cloner le dépôt via HTTPS :

```sh
git clone https://github.com/ClickHouse/ClickHouse.git
```

Cela ne vous permettra toutefois pas d’envoyer vos modifications au serveur.
Vous pouvez néanmoins l’utiliser temporairement et ajouter les clés SSH plus tard en remplaçant l’adresse distante du dépôt à l’aide de la commande `git remote`.

Vous pouvez également ajouter l’adresse du dépôt ClickHouse d’origine à votre dépôt local pour récupérer les mises à jour depuis ce dépôt :

```sh
git remote add upstream git@github.com:ClickHouse/ClickHouse.git
```

Après avoir exécuté cette commande avec succès, vous pourrez récupérer les mises à jour du dépôt principal de ClickHouse en lançant `git pull upstream master`.

:::tip
N’utilisez pas `git push` tel quel, car vous pourriez pousser vers le mauvais dépôt distant et/ou la mauvaise branche.
Mieux vaut préciser explicitement les noms du dépôt distant et de la branche, par exemple `git push origin my_branch_name`.
:::

<div id="writing-code">
  ## Écrire du code
</div>

Vous trouverez ci-dessous quelques liens utiles pour écrire du code pour ClickHouse :

* [Architecture de ClickHouse](/fr/development/architecture/).
* [Guide de style du code](/fr/development/style/).
* [Bibliothèques tierces](/fr/development/contrib#adding-and-maintaining-third-party-libraries)
* [Écriture de tests](/fr/development/tests/)
* [Issues ouvertes](https://github.com/ClickHouse/ClickHouse/issues?q=is%3Aopen+is%3Aissue+label%3A%22easy+task%22)

<div id="ide">
  ### IDE
</div>

[Visual Studio Code](https://code.visualstudio.com/) et [Neovim](https://neovim.io/) sont deux options qui ont déjà fait leurs preuves pour développer ClickHouse. Si vous utilisez VS Code, nous vous recommandons d’utiliser l’[extension clangd](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) à la place d’IntelliSense, car elle est bien plus performante.

[CLion](https://www.jetbrains.com/clion/) est une autre excellente option. Cependant, il peut être plus lent sur des projets de grande taille comme ClickHouse. Voici quelques points à garder à l’esprit lorsque vous utilisez CLion :

* CLion crée lui-même un répertoire `build` et sélectionne automatiquement `debug` comme type de build
* Il utilise une version de CMake définie dans CLion, et non celle que vous avez installée
* CLion utilise `make` pour exécuter les tâches de build au lieu de `ninja` (c’est le comportement normal)

Vous pouvez également utiliser d’autres IDE comme [Sublime Text](https://www.sublimetext.com/), [Qt Creator](https://www.qt.io/product/development-tools) ou [Kate](https://kate-editor.org/).

<div id="create-a-pull-request">
  ## Créer une pull request
</div>

Accédez au dépôt de votre fork dans l&#39;interface GitHub.
Si vous avez travaillé dans une branche, vous devez sélectionner cette branche.
Un bouton &quot;Pull request&quot; sera affiché à l&#39;écran.
En pratique, cela signifie « créer une demande pour faire accepter mes modifications dans le dépôt principal ».

Une pull request peut être créée même si le travail n&#39;est pas encore terminé.
Dans ce cas, veuillez ajouter le mot &quot;WIP&quot; (work in progress) au début du titre ; vous pourrez le modifier plus tard.
C&#39;est utile pour la revue collaborative et la discussion des modifications, ainsi que pour exécuter tous les tests disponibles.
Il est important de fournir une brève description de vos modifications, car elle sera ensuite utilisée pour générer le changelog de la release.

Les tests démarreront dès que des employés de ClickHouse auront ajouté à votre PR le tag &quot;can be tested&quot;.
Les résultats de certaines premières vérifications (par ex. le style du code) arriveront en quelques minutes.
Les résultats des vérifications de build arriveront en moins d&#39;une demi-heure.
Le principal ensemble de tests remontera ses résultats dans l&#39;heure.

Le système préparera des builds binaires de ClickHouse spécifiquement pour votre pull request.
Pour récupérer ces builds, cliquez sur le lien &quot;Details&quot; à côté de l&#39;entrée &quot;Builds&quot; dans la liste des vérifications.
Vous y trouverez des liens directs vers les paquets .deb de ClickHouse générés, que vous pouvez déployer même sur vos serveurs de production (si cela ne vous fait pas peur).

<div id="write-documentation">
  ## Rédiger la documentation
</div>

Chaque pull request qui ajoute une nouvelle fonctionnalité doit s’accompagner d’une documentation adéquate.
Si vous souhaitez prévisualiser vos modifications de la documentation, les instructions expliquant comment générer la page de documentation en local sont disponibles dans le fichier README.md [ici](https://github.com/ClickHouse/clickhouse-docs).
Lorsque vous ajoutez une nouvelle fonction à ClickHouse, vous pouvez utiliser le modèle ci-dessous comme référence :

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
  ## Utiliser des données de test
</div>

Le développement de ClickHouse nécessite souvent de charger des jeux de données réalistes.
C’est particulièrement important pour les tests de performance.
Nous disposons d’un jeu de données d’analyse d’audience web anonymisées, spécialement préparé à cet effet.
Cela nécessite par ailleurs environ 3 Go d’espace disque libre.

```sh
    sudo apt install wget xz-utils

    wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
    wget https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz

    xz -v -d hits_v1.tsv.xz
    xz -v -d visits_v1.tsv.xz

    clickhouse-client
```

Dans clickhouse-client :

```sql
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.hits ( WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);

CREATE TABLE test.visits ( CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,  Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,  EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),  RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,  SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,  UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,  FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,  Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),  Params Array(String),  `Goals.ID` Array(UInt32),  `Goals.Serial` Array(UInt32),  `Goals.EventTime` Array(DateTime),  `Goals.Price` Array(Int64),  `Goals.OrderID` Array(String),  `Goals.CurrencyID` Array(UInt32),  WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,  ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,  ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,  ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,  ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,  ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,  PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  `TraficSource.ID` Array(Int8),  `TraficSource.SearchEngineID` Array(UInt16),  `TraficSource.AdvEngineID` Array(UInt8),  `TraficSource.PlaceID` Array(UInt16),  `TraficSource.SocialSourceNetworkID` Array(UInt8),  `TraficSource.Domain` Array(String),  `TraficSource.SearchPhrase` Array(String),  `TraficSource.SocialSourcePage` Array(String),  Attendance FixedString(16),  CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,  StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,  OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,  UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,  `ParsedParams.Key1` Array(String),  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array(String),  `ParsedParams.ValueDouble` Array(Float64),  `Market.Type` Array(UInt8),  `Market.GoalID` Array(UInt32),  `Market.OrderID` Array(String),  `Market.OrderPrice` Array(Int64),  `Market.PP` Array(UInt32),  `Market.DirectPlaceID` Array(UInt32),  `Market.DirectOrderID` Array(UInt32),  `Market.DirectBannerID` Array(UInt32),  `Market.GoodID` Array(String),  `Market.GoodName` Array(String),  `Market.GoodQuantity` Array(Int32),  `Market.GoodPrice` Array(Int64),  IslandID FixedString(16)) ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID);

```

Importez les données :

```bash
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.hits FORMAT TSV" < hits_v1.tsv
clickhouse-client --max_insert_block_size 100000 --query "INSERT INTO test.visits FORMAT TSV" < visits_v1.tsv
```