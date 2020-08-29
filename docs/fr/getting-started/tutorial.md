---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 12
toc_title: Tutoriel
---

# Tutoriel ClickHouse {#clickhouse-tutorial}

## À quoi S'attendre de ce tutoriel? {#what-to-expect-from-this-tutorial}

En parcourant ce tutoriel, vous apprendrez à configurer un cluster ClickHouse simple. Ce sera petit, mais tolérant aux pannes et évolutif. Ensuite, nous utiliserons l'un des exemples de jeux de données pour le remplir de données et exécuter des requêtes de démonstration.

## Configuration De Noeud Unique {#single-node-setup}

Pour retarder les complexités d'un environnement distribué, nous allons commencer par déployer ClickHouse sur un seul serveur ou une machine virtuelle. ClickHouse est généralement installé à partir de [deb](install.md#install-from-deb-packages) ou [tr / min](install.md#from-rpm-packages) les paquets, mais il y a [alternative](install.md#from-docker-image) pour les systèmes d'exploitation qui ne sont pas les soutenir.

Par exemple, vous avez choisi `deb` paquets et exécutés:

``` bash
{% include 'install/deb.sh' %}
```

Quelles sont les paquets installés:

-   `clickhouse-client` package contient [clickhouse-client](../interfaces/cli.md) application, client interactif de console de ClickHouse.
-   `clickhouse-common` paquet contient un fichier exécutable ClickHouse.
-   `clickhouse-server` package contient des fichiers de configuration pour exécuter ClickHouse en tant que serveur.

Les fichiers de configuration du serveur sont `/etc/clickhouse-server/`. Avant d'aller plus loin, notez le `<path>` élément `config.xml`. Path détermine l'emplacement pour le stockage des données, il doit donc être situé sur le volume avec une grande capacité de disque; la valeur par défaut est `/var/lib/clickhouse/`. Si vous souhaitez ajuster la configuration, il n'est pas pratique de modifier directement `config.xml` fichier, considérant qu'il pourrait obtenir réécrit sur les futures mises à jour du progiciel. La façon recommandée de remplacer les éléments de configuration est de créer [fichiers dans config.d: répertoire](../operations/configuration-files.md) qui servent de “patches” config.XML.

Comme vous l'avez peut-être remarqué, `clickhouse-server` n'est pas lancé automatiquement après l'installation du paquet. Il ne sera pas redémarré automatiquement après les mises à jour, non plus. La façon dont vous démarrez le serveur dépend de votre système d'initialisation, généralement, c'est:

``` bash
sudo service clickhouse-server start
```

ou

``` bash
sudo /etc/init.d/clickhouse-server start
```

L'emplacement par défaut pour les journaux du serveur est `/var/log/clickhouse-server/`. Le serveur est prêt à gérer les connexions client une fois `Ready for connections` message.

Une fois l' `clickhouse-server` est opérationnel, nous pouvons utiliser `clickhouse-client` pour se connecter au serveur et effectuer des tests de requêtes comme `SELECT "Hello, world!";`.

<details markdown="1">

<summary>Conseils rapides pour clickhouse-client</summary>

Mode interactif:

``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Activer les requêtes multilignes:

``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Exécuter des requêtes en mode batch:

``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
clickhouse-client <<< 'SELECT 1'
```

Insérer des données à partir d'un fichier au format spécifié:

``` bash
clickhouse-client --query='INSERT INTO table VALUES' < data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' < data.tsv
```

</details>

## Importer Un Échantillon De Données {#import-sample-dataset}

Maintenant, il est temps de remplir notre serveur ClickHouse avec quelques exemples de données. Dans ce tutoriel, nous allons utiliser les données anonymisées de Yandex.Metrica, le premier service qui exécute ClickHouse en production avant de devenir open-source (plus à ce sujet dans [section d'histoire](../introduction/history.md)). Il y a [plusieurs façons d'importer Yandex.Metrica dataset](example-datasets/metrica.md), et pour le bien du tutoriel, nous irons avec le plus réaliste.

### Télécharger et extraire les données de la Table {#download-and-extract-table-data}

``` bash
curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv
```

Les fichiers extraits ont une taille d'environ 10 Go.

### Créer Des Tables {#create-tables}

Comme dans la plupart des systèmes de gestion de bases de données, clickhouse regroupe logiquement les tables en “databases”. Il y a un `default` base de données, mais nous allons en créer une nouvelle nommée `tutorial`:

``` bash
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"
```

La syntaxe pour créer des tables est beaucoup plus compliquée par rapport aux bases de données (voir [référence](../sql-reference/statements/create.md). En général `CREATE TABLE` déclaration doit spécifier trois choses clés:

1.  Nom de la table à créer.
2.  Table schema, i.e. list of columns and their [types de données](../sql-reference/data-types/index.md).
3.  [Tableau moteur](../engines/table-engines/index.md) et ses paramètres, qui détermine tous les détails sur la façon dont les requêtes à cette table seront physiquement exécutées.

Yandex.Metrica est un service d'analyse web, et l'exemple de jeu de données ne couvre pas toutes ses fonctionnalités, il n'y a donc que deux tables à créer:

-   `hits` est un tableau avec chaque action effectuée par tous les utilisateurs sur tous les sites couverts par le service.
-   `visits` est une table qui contient des sessions pré-construites au lieu d'actions individuelles.

Voyons et exécutons les vraies requêtes create table pour ces tables:

``` sql
CREATE TABLE tutorial.hits_v1
(
    `WatchID` UInt64,
    `JavaEnable` UInt8,
    `Title` String,
    `GoodEvent` Int16,
    `EventTime` DateTime,
    `EventDate` Date,
    `CounterID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RegionID` UInt32,
    `UserID` UInt64,
    `CounterClass` Int8,
    `OS` UInt8,
    `UserAgent` UInt8,
    `URL` String,
    `Referer` String,
    `URLDomain` String,
    `RefererDomain` String,
    `Refresh` UInt8,
    `IsRobot` UInt8,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `FlashMinor2` String,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` FixedString(2),
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `MobilePhone` UInt8,
    `MobilePhoneModel` String,
    `Params` String,
    `IPNetworkID` UInt32,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `IsArtifical` UInt8,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `ClientTimeZone` Int16,
    `ClientEventTime` DateTime,
    `SilverlightVersion1` UInt8,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion3` UInt32,
    `SilverlightVersion4` UInt16,
    `PageCharset` String,
    `CodeVersion` UInt32,
    `IsLink` UInt8,
    `IsDownload` UInt8,
    `IsNotBounce` UInt8,
    `FUniqID` UInt64,
    `HID` UInt32,
    `IsOldCounter` UInt8,
    `IsEvent` UInt8,
    `IsParameter` UInt8,
    `DontCountHits` UInt8,
    `WithHash` UInt8,
    `HitColor` FixedString(1),
    `UTCEventTime` DateTime,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `WindowName` Int32,
    `OpenerName` Int32,
    `HistoryLength` Int16,
    `BrowserLanguage` FixedString(2),
    `BrowserCountry` FixedString(2),
    `SocialNetwork` String,
    `SocialAction` String,
    `HTTPError` UInt16,
    `SendTiming` Int32,
    `DNSTiming` Int32,
    `ConnectTiming` Int32,
    `ResponseStartTiming` Int32,
    `ResponseEndTiming` Int32,
    `FetchTiming` Int32,
    `RedirectTiming` Int32,
    `DOMInteractiveTiming` Int32,
    `DOMContentLoadedTiming` Int32,
    `DOMCompleteTiming` Int32,
    `LoadEventStartTiming` Int32,
    `LoadEventEndTiming` Int32,
    `NSToDOMContentLoadedTiming` Int32,
    `FirstPaintTiming` Int32,
    `RedirectCount` Int8,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `ParamPrice` Int64,
    `ParamOrderID` String,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `GoalsReached` Array(UInt32),
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `RefererHash` UInt64,
    `URLHash` UInt64,
    `CLID` UInt32,
    `YCLID` UInt64,
    `ShareService` String,
    `ShareURL` String,
    `ShareTitle` String,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `IslandID` FixedString(16),
    `RequestNum` UInt32,
    `RequestTry` UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

``` sql
CREATE TABLE tutorial.visits_v1
(
    `CounterID` UInt32,
    `StartDate` Date,
    `Sign` Int8,
    `IsNew` UInt8,
    `VisitID` UInt64,
    `UserID` UInt64,
    `StartTime` DateTime,
    `Duration` UInt32,
    `UTCStartTime` DateTime,
    `PageViews` Int32,
    `Hits` Int32,
    `IsBounce` UInt8,
    `Referer` String,
    `StartURL` String,
    `RefererDomain` String,
    `StartURLDomain` String,
    `EndURL` String,
    `LinkURL` String,
    `IsDownload` UInt8,
    `TraficSourceID` Int8,
    `SearchEngineID` UInt16,
    `SearchPhrase` String,
    `AdvEngineID` UInt8,
    `PlaceID` Int32,
    `RefererCategories` Array(UInt16),
    `URLCategories` Array(UInt16),
    `URLRegions` Array(UInt32),
    `RefererRegions` Array(UInt32),
    `IsYandex` UInt8,
    `GoalReachesDepth` Int32,
    `GoalReachesURL` Int32,
    `GoalReachesAny` Int32,
    `SocialSourceNetworkID` UInt8,
    `SocialSourcePage` String,
    `MobilePhoneModel` String,
    `ClientEventTime` DateTime,
    `RegionID` UInt32,
    `ClientIP` UInt32,
    `ClientIP6` FixedString(16),
    `RemoteIP` UInt32,
    `RemoteIP6` FixedString(16),
    `IPNetworkID` UInt32,
    `SilverlightVersion3` UInt32,
    `CodeVersion` UInt32,
    `ResolutionWidth` UInt16,
    `ResolutionHeight` UInt16,
    `UserAgentMajor` UInt16,
    `UserAgentMinor` UInt16,
    `WindowClientWidth` UInt16,
    `WindowClientHeight` UInt16,
    `SilverlightVersion2` UInt8,
    `SilverlightVersion4` UInt16,
    `FlashVersion3` UInt16,
    `FlashVersion4` UInt16,
    `ClientTimeZone` Int16,
    `OS` UInt8,
    `UserAgent` UInt8,
    `ResolutionDepth` UInt8,
    `FlashMajor` UInt8,
    `FlashMinor` UInt8,
    `NetMajor` UInt8,
    `NetMinor` UInt8,
    `MobilePhone` UInt8,
    `SilverlightVersion1` UInt8,
    `Age` UInt8,
    `Sex` UInt8,
    `Income` UInt8,
    `JavaEnable` UInt8,
    `CookieEnable` UInt8,
    `JavascriptEnable` UInt8,
    `IsMobile` UInt8,
    `BrowserLanguage` UInt16,
    `BrowserCountry` UInt16,
    `Interests` UInt16,
    `Robotness` UInt8,
    `GeneralInterests` Array(UInt16),
    `Params` Array(String),
    `Goals` Nested(
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32),
    `WatchIDs` Array(UInt64),
    `ParamSumPrice` Int64,
    `ParamCurrency` FixedString(3),
    `ParamCurrencyID` UInt16,
    `ClickLogID` UInt64,
    `ClickEventID` Int32,
    `ClickGoodEvent` Int32,
    `ClickEventTime` DateTime,
    `ClickPriorityID` Int32,
    `ClickPhraseID` Int32,
    `ClickPageID` Int32,
    `ClickPlaceID` Int32,
    `ClickTypeID` Int32,
    `ClickResourceID` Int32,
    `ClickCost` UInt32,
    `ClickClientIP` UInt32,
    `ClickDomainID` UInt32,
    `ClickURL` String,
    `ClickAttempt` UInt8,
    `ClickOrderID` UInt32,
    `ClickBannerID` UInt32,
    `ClickMarketCategoryID` UInt32,
    `ClickMarketPP` UInt32,
    `ClickMarketCategoryName` String,
    `ClickMarketPPName` String,
    `ClickAWAPSCampaignName` String,
    `ClickPageName` String,
    `ClickTargetType` UInt16,
    `ClickTargetPhraseID` UInt64,
    `ClickContextType` UInt8,
    `ClickSelectType` Int8,
    `ClickOptions` String,
    `ClickGroupBannerID` Int32,
    `OpenstatServiceName` String,
    `OpenstatCampaignID` String,
    `OpenstatAdID` String,
    `OpenstatSourceID` String,
    `UTMSource` String,
    `UTMMedium` String,
    `UTMCampaign` String,
    `UTMContent` String,
    `UTMTerm` String,
    `FromTag` String,
    `HasGCLID` UInt8,
    `FirstVisit` DateTime,
    `PredLastVisit` Date,
    `LastVisit` Date,
    `TotalVisits` UInt32,
    `TraficSource` Nested(
        ID Int8,
        SearchEngineID UInt16,
        AdvEngineID UInt8,
        PlaceID UInt16,
        SocialSourceNetworkID UInt8,
        Domain String,
        SearchPhrase String,
        SocialSourcePage String),
    `Attendance` FixedString(16),
    `CLID` UInt32,
    `YCLID` UInt64,
    `NormalizedRefererHash` UInt64,
    `SearchPhraseHash` UInt64,
    `RefererDomainHash` UInt64,
    `NormalizedStartURLHash` UInt64,
    `StartURLDomainHash` UInt64,
    `NormalizedEndURLHash` UInt64,
    `TopLevelDomain` UInt64,
    `URLScheme` UInt64,
    `OpenstatServiceNameHash` UInt64,
    `OpenstatCampaignIDHash` UInt64,
    `OpenstatAdIDHash` UInt64,
    `OpenstatSourceIDHash` UInt64,
    `UTMSourceHash` UInt64,
    `UTMMediumHash` UInt64,
    `UTMCampaignHash` UInt64,
    `UTMContentHash` UInt64,
    `UTMTermHash` UInt64,
    `FromHash` UInt64,
    `WebVisorEnabled` UInt8,
    `WebVisorActivity` UInt32,
    `ParsedParams` Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    `Market` Nested(
        Type UInt8,
        GoalID UInt32,
        OrderID String,
        OrderPrice Int64,
        PP UInt32,
        DirectPlaceID UInt32,
        DirectOrderID UInt32,
        DirectBannerID UInt32,
        GoodID String,
        GoodName String,
        GoodQuantity Int32,
        GoodPrice Int64),
    `IslandID` FixedString(16)
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192
```

Vous pouvez exécuter ces requêtes en utilisant le mode interactif de `clickhouse-client` (lancez - le simplement dans un terminal sans spécifier une requête à l'avance) ou essayez-en [interface de rechange](../interfaces/index.md) Si tu veux.

Comme nous pouvons le voir, `hits_v1` utilise la [moteur MergeTree de base](../engines/table-engines/mergetree-family/mergetree.md) tandis que le `visits_v1` utilise la [Effondrer](../engines/table-engines/mergetree-family/collapsingmergetree.md) variante.

### Importer Des Données {#import-data}

L'importation de données vers ClickHouse se fait via [INSERT INTO](../sql-reference/statements/insert-into.md) requête comme dans de nombreuses autres bases de données SQL. Toutefois, les données sont généralement fournies dans l'une des [formats de sérialisation pris en charge](../interfaces/formats.md) plutôt `VALUES` clause (qui est également pris en charge).

Les fichiers que nous avons téléchargés plus tôt sont au format séparé par des onglets, alors voici comment les importer via le client console:

``` bash
clickhouse-client --query "INSERT INTO tutorial.hits_v1 FORMAT TSV" --max_insert_block_size=100000 < hits_v1.tsv
clickhouse-client --query "INSERT INTO tutorial.visits_v1 FORMAT TSV" --max_insert_block_size=100000 < visits_v1.tsv
```

ClickHouse a beaucoup de [les paramètres de tune](../operations/settings/index.md) et une façon de Les spécifier dans le client console est via des arguments, comme nous pouvons le voir avec `--max_insert_block_size`. La façon la plus simple de comprendre quels paramètres sont disponibles, que signifient-ils et quelles sont les valeurs par défaut est d'interroger le `system.settings` table:

``` sql
SELECT name, value, changed, description
FROM system.settings
WHERE name LIKE '%max_insert_b%'
FORMAT TSV

max_insert_block_size    1048576    0    "The maximum block size for insertion, if we control the creation of blocks for insertion."
```

Optionnellement, vous pouvez [OPTIMIZE](../sql-reference/statements/misc.md#misc_operations-optimize) les tables après l'importation. Les Tables configurées avec un moteur de MergeTree-family font toujours des fusions de parties de données en arrière-plan pour optimiser le stockage des données (ou au moins vérifier si cela a du sens). Ces requêtes forcent le moteur de table à optimiser le stockage dès maintenant au lieu d'un certain temps plus tard:

``` bash
clickhouse-client --query "OPTIMIZE TABLE tutorial.hits_v1 FINAL"
clickhouse-client --query "OPTIMIZE TABLE tutorial.visits_v1 FINAL"
```

Ces requêtes démarrent une opération intensive D'E/S et de CPU, donc si la table reçoit systématiquement de nouvelles données, il est préférable de la laisser seule et de laisser les fusions s'exécuter en arrière-plan.

Maintenant, nous pouvons vérifier si l'importation de table a réussi:

``` bash
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.hits_v1"
clickhouse-client --query "SELECT COUNT(*) FROM tutorial.visits_v1"
```

## Exemple De Requêtes {#example-queries}

``` sql
SELECT
    StartURL AS URL,
    AVG(Duration) AS AvgDuration
FROM tutorial.visits_v1
WHERE StartDate BETWEEN '2014-03-23' AND '2014-03-30'
GROUP BY URL
ORDER BY AvgDuration DESC
LIMIT 10
```

``` sql
SELECT
    sum(Sign) AS visits,
    sumIf(Sign, has(Goals.ID, 1105530)) AS goal_visits,
    (100. * goal_visits) / visits AS goal_percent
FROM tutorial.visits_v1
WHERE (CounterID = 912887) AND (toYYYYMM(StartDate) = 201403) AND (domain(StartURL) = 'yandex.ru')
```

## Déploiement De Cluster {#cluster-deployment}

Clickhouse cluster est un cluster homogène. Étapes pour configurer:

1.  Installer clickhouse server sur toutes les machines du cluster
2.  Configurer les configurations de cluster dans les fichiers de configuration
3.  Créer des tables locales sur chaque instance
4.  Créer un [Distribué table](../engines/table-engines/special/distributed.md)

[Distribué table](../engines/table-engines/special/distributed.md) est en fait une sorte de “view” aux tables locales du cluster ClickHouse. SELECT query from a distributed table s'exécute à l'aide des ressources de tous les fragments du cluster. Vous pouvez spécifier des configurations pour plusieurs clusters et créer plusieurs tables distribuées fournissant des vues à différents clusters.

Exemple de configuration pour un cluster avec trois fragments, une réplique chacun:

``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```

Pour plus de démonstration, créons une nouvelle table locale avec le même `CREATE TABLE` la requête que nous avons utilisé pour `hits_v1`, mais nom de table différent:

``` sql
CREATE TABLE tutorial.hits_local (...) ENGINE = MergeTree() ...
```

Création d'une table distribuée fournissant une vue dans les tables locales du cluster:

``` sql
CREATE TABLE tutorial.hits_all AS tutorial.hits_local
ENGINE = Distributed(perftest_3shards_1replicas, tutorial, hits_local, rand());
```

Une pratique courante consiste à créer des tables distribuées similaires sur toutes les machines du cluster. Il permet d'exécuter des requêtes distribuées sur n'importe quelle machine du cluster. Il existe également une autre option pour créer une table distribuée temporaire pour une requête SELECT donnée en utilisant [distant](../sql-reference/table-functions/remote.md) table de fonction.

Passons à l'exécution de [INSERT SELECT](../sql-reference/statements/insert-into.md) dans les Distribué table la table à plusieurs serveurs.

``` sql
INSERT INTO tutorial.hits_all SELECT * FROM tutorial.hits_v1;
```

!!! warning "Avis"
    Cette approche ne convient pas au sharding de grandes tables. Il y a un outil séparé [clickhouse-copieur](../operations/utilities/clickhouse-copier.md) cela peut re-fragmenter de grandes tables arbitraires.

Comme vous pouvez vous y attendre, les requêtes lourdes de calcul s'exécutent N fois plus vite si elles utilisent 3 serveurs au lieu d'un.

Dans ce cas, nous avons utilisé un cluster avec 3 fragments, et chacun contient une seule réplique.

Pour assurer la résilience dans un environnement de production, nous recommandons que chaque fragment contienne 2-3 répliques réparties entre plusieurs zones de disponibilité ou centres de données (ou au moins des racks). Notez que ClickHouse prend en charge un nombre illimité de répliques.

Exemple de configuration pour un cluster d'un fragment contenant trois répliques:

``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```

Pour activer la réplication native [ZooKeeper](http://zookeeper.apache.org/) est requis. ClickHouse s'occupe de la cohérence des données sur toutes les répliques et exécute automatiquement la procédure de restauration après l'échec. Il est recommandé de déployer le cluster ZooKeeper sur des serveurs séparés (où aucun autre processus, y compris ClickHouse, n'est en cours d'exécution).

!!! note "Note"
    ZooKeeper est pas une exigence stricte: dans certains cas simples, vous pouvez dupliquer les données par écrit dans tous les réplicas de votre code d'application. Cette approche est **pas** recommandé, dans ce cas, ClickHouse ne sera pas en mesure de garantir la cohérence des données sur toutes les répliques. Ainsi, il devient la responsabilité de votre application.

Les emplacements ZooKeeper sont spécifiés dans le fichier de configuration:

``` xml
<zookeeper>
    <node>
        <host>zoo01.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.yandex.ru</host>
        <port>2181</port>
    </node>
</zookeeper>
```

En outre, nous devons définir des macros pour identifier chaque fragment et chaque réplique utilisés lors de la création de la table:

``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```

S'il n'y a pas de répliques pour le moment lors de la création de la table répliquée, une nouvelle première réplique est instanciée. S'il existe déjà des répliques en direct, la nouvelle réplique clone les données de celles existantes. Vous avez la possibilité de créer toutes les tables répliquées d'abord, et ensuite insérer les données. Une autre option consiste à créer des répliques et à en ajouter d'autres Après ou pendant l'insertion des données.

``` sql
CREATE TABLE tutorial.hits_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/hits',
    '{replica}'
)
...
```

Ici, nous utilisons [ReplicatedMergeTree](../engines/table-engines/mergetree-family/replication.md) tableau moteur. Dans les paramètres, nous spécifions le chemin Zookeeper contenant des identificateurs de fragments et de répliques.

``` sql
INSERT INTO tutorial.hits_replica SELECT * FROM tutorial.hits_local;
```

La réplication fonctionne en mode multi-maître. Les données peuvent être chargées dans n'importe quel réplica, et le système les synchronise ensuite automatiquement avec d'autres instances. La réplication est asynchrone, donc à un moment donné, toutes les répliques ne peuvent pas contenir de données récemment insérées. Au moins une réplique devrait être en place pour permettre l'ingestion de données. D'autres synchroniseront les données et répareront la cohérence une fois qu'ils redeviendront actifs. Notez que cette approche permet une faible possibilité de perte de données récemment insérées.

[Article Original](https://clickhouse.tech/docs/en/getting_started/tutorial/) <!--hide-->
