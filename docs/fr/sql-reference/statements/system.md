---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: SYSTEM
---

# SYSTÈME de Requêtes {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Recharge tous les dictionnaires qui ont déjà été chargés avec succès.
Par défaut, les dictionnaires sont chargés paresseusement (voir [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), donc au lieu d'être chargés automatiquement au démarrage, ils sont initialisés lors du premier accès via la fonction dictGet ou sélectionnez dans les tables avec ENGINE = Dictionary . Le `SYSTEM RELOAD DICTIONARIES` query recharge ces dictionnaires (chargés).
Retourne toujours `Ok.` quel que soit le résultat de la mise à jour du dictionnaire.

## Recharger le dictionnaire Dictionary_name {#query_language-system-reload-dictionary}

Recharge complètement un dictionnaire `dictionary_name`, quel que soit l'état du dictionnaire (LOADED / NOT_LOADED / FAILED).
Retourne toujours `Ok.` quel que soit le résultat de la mise à jour du dictionnaire.
L'état du dictionnaire peut être vérifié en interrogeant le `system.dictionaries` table.

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Réinitialise le cache DNS interne de ClickHouse. Parfois (pour les anciennes versions de ClickHouse), il est nécessaire d'utiliser cette commande lors de la modification de l'infrastructure (modification de l'adresse IP d'un autre serveur ClickHouse ou du serveur utilisé par les dictionnaires).

Pour une gestion du cache plus pratique (automatique), voir paramètres disable_internal_dns_cache, dns_cache_update_period.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

Réinitialise le cache de marque. Utilisé dans le développement de ClickHouse et des tests de performance.

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

Recharge la configuration de ClickHouse. Utilisé lorsque la configuration est stockée dans ZooKeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Normalement ferme ClickHouse (comme `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Annule le processus de ClickHouse (comme `kill -9 {$ pid_clickhouse-server}`)

## Gestion Des Tables Distribuées {#query-language-system-distributed}

ClickHouse peut gérer [distribué](../../engines/table-engines/special/distributed.md) table. Lorsqu'un utilisateur insère des données dans ces tables, ClickHouse crée d'abord une file d'attente des données qui doivent être envoyées aux nœuds de cluster, puis l'envoie de manière asynchrone. Vous pouvez gérer le traitement des files d'attente avec [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), et [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) requête. Vous pouvez également insérer de manière synchrone des données distribuées avec `insert_distributed_sync` paramètre.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Désactive la distribution de données en arrière-plan lors de l'insertion de données dans des tables distribuées.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Force ClickHouse à envoyer des données aux nœuds de cluster de manière synchrone. Si des nœuds ne sont pas disponibles, ClickHouse lève une exception et arrête l'exécution de la requête. Vous pouvez réessayer la requête jusqu'à ce qu'elle réussisse, ce qui se produira lorsque tous les nœuds seront de nouveau en ligne.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Active la distribution de données en arrière-plan lors de l'insertion de données dans des tables distribuées.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

Offre la possibilité d'arrêter les fusions d'arrière-plan pour les tables de la famille MergeTree:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "Note"
    `DETACH / ATTACH` table va commencer les fusions d'arrière-plan pour la table même dans le cas où les fusions ont été arrêtées pour toutes les tables MergeTree auparavant.

### START MERGES {#query_language-system-start-merges}

Offre la possibilité de démarrer des fusions en arrière-plan pour les tables de la famille MergeTree:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[Article Original](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
