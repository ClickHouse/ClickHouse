---
description: 'Les tables utilisant le moteur Distributed ne stockent aucune donnée en
  propre, mais permettent le traitement distribué des requêtes sur plusieurs
  serveurs. La lecture est automatiquement parallélisée. Lors de la lecture,
  les index des tables sur les serveurs distants sont utilisés, s’ils existent.'
sidebar_label: 'Distributed'
sidebar_position: 10
slug: /engines/table-engines/special/distributed
title: 'Moteur de table Distributed'
doc_type: 'reference'
---

:::warning Moteur Distributed dans Cloud
Pour créer un moteur de table Distributed dans ClickHouse Cloud, vous pouvez utiliser les fonctions de table [`remote` et `remoteSecure`](../../../sql-reference/table-functions/remote).
La syntaxe `Distributed(...)` ne peut pas être utilisée dans ClickHouse Cloud.
:::

Les tables utilisant le moteur Distributed ne stockent aucune donnée en propre, mais permettent le traitement distribué des requêtes sur plusieurs serveurs.
La lecture est automatiquement parallélisée. Lors de la lecture, les index des tables sur les serveurs distants sont utilisés, s’ils existent.

<div id="distributed-creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

<div id="distributed-from-a-table">
  ### À partir d’une table
</div>

Lorsque la table `Distributed` pointe vers une table sur le serveur actuel, vous pouvez reprendre le schéma de cette table :

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

<div id="distributed-parameters">
  ### Paramètres de Distributed
</div>

| Paramètre                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`                 | Le nom du cluster dans le fichier de configuration du serveur                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `database`                | Le nom d&#39;une base de données distante                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `table`                   | Le nom d&#39;une table distante                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `sharding_key` (Optional) | La clé de sharding. <br /> La spécification de `sharding_key` est nécessaire dans les cas suivants : <ul><li>Pour les `INSERTs` dans une table distribuée (car le moteur de table a besoin de `sharding_key` pour déterminer comment répartir les données). Toutefois, si le paramètre `insert_distributed_one_random_shard` est activé, les `INSERTs` n&#39;ont pas besoin de clé de sharding.</li><li>Pour une utilisation avec `optimize_skip_unused_shards`, car `sharding_key` est nécessaire pour déterminer quels shards doivent être interrogés</li></ul> |
| `policy_name` (Optional)  | Le nom de la stratégie ; il sera utilisé pour stocker les fichiers temporaires pour l&#39;envoi en arrière-plan                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

**Voir aussi**

* le paramètre [distributed&#95;foreground&#95;insert](../../../operations/settings/settings.md#distributed_foreground_insert)
* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) pour les exemples

<div id="distributed-parameters">
  ### Paramètres de Distributed
</div>

| Paramètre                                  | Description                                                                                                                                                                                                                                                                                                                                                                                           | Valeur par défaut              |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `fsync_after_insert`                       | Exécute `fsync` sur les données du fichier après un `insert` en arrière-plan vers Distributed. Garantit que l’OS a écrit l’intégralité des données insérées dans un fichier **sur le disque du nœud initiateur**.                                                                                                                                                                                     | `false`                        |
| `fsync_directories`                        | Exécute `fsync` sur les répertoires. Garantit que l’OS a actualisé les métadonnées du répertoire après les opérations liées aux `insert` en arrière-plan sur une table Distributed (après l’insert, après l’envoi des données au shard, etc.).                                                                                                                                                        | `false`                        |
| `skip_unavailable_shards`                  | Si true, ClickHouse ignore silencieusement les shards indisponibles. Le comportement de ce paramètre est contrôlé par le paramètre `skip_unavailable_shards_mode`.                                                                                                                                                                                                                                    | `false`                        |
| `skip_unavailable_shards_mode`             | Contrôle les exceptions provenant d’un shard distant qui sont ignorées lorsque `skip_unavailable_shards` est activé : `unavailable` ignore uniquement les erreurs de connexion ; `unavailable_or_table_missing` ignore également une table ou une database manquante ; `unavailable_or_exception_before_processing` ignore également toute exception reçue avant que le shard ne renvoie des données. | `unavailable_or_table_missing` |
| `bytes_to_throw_insert`                    | Si plus de ce nombre d’octets compressés sont en attente pour un `INSERT` en arrière-plan, une exception sera levée. `0` - ne pas lever d’exception.                                                                                                                                                                                                                                                  | `0`                            |
| `bytes_to_delay_insert`                    | Si plus de ce nombre d’octets compressés sont en attente pour un `INSERT` en arrière-plan, la query sera retardée. `0` - ne pas retarder.                                                                                                                                                                                                                                                             | `0`                            |
| `max_delay_to_insert`                      | Délai maximal d’insertion des données dans une table Distributed, en secondes, s’il y a beaucoup d’octets en attente d’envoi en arrière-plan.                                                                                                                                                                                                                                                         | `60`                           |
| `background_insert_batch`                  | Identique à [`distributed_background_insert_batch`](../../../operations/settings/settings.md#distributed_background_insert_batch)                                                                                                                                                                                                                                                                     | `0`                            |
| `background_insert_split_batch_on_failure` | Identique à [`distributed_background_insert_split_batch_on_failure`](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure)                                                                                                                                                                                                                                   | `0`                            |
| `background_insert_sleep_time_ms`          | Identique à [`distributed_background_insert_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms)                                                                                                                                                                                                                                                     | `0`                            |
| `background_insert_max_sleep_time_ms`      | Identique à [`distributed_background_insert_max_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms)                                                                                                                                                                                                                                             | `0`                            |
| `flush_on_detach`                          | Vide les données vers les nœuds distants lors de `DETACH`/`DROP`/de l’arrêt du server.                                                                                                                                                                                                                                                                                                                | `true`                         |

:::note
**Paramètres de durabilité** (`fsync_...`) :

* Affectent uniquement les `INSERT` en arrière-plan (c.-à-d. `distributed_foreground_insert=false`) lorsque les données sont d’abord stockées sur le disque du nœud initiateur puis envoyées en arrière-plan aux shards.
* Peuvent réduire considérablement les performances de `INSERT`
* Affectent l’écriture des données stockées dans le dossier de la table distribuée sur le **nœud qui a accepté votre insert**. Si vous avez besoin de garanties d’écriture des données dans les tables MergeTree sous-jacentes, consultez les paramètres de durabilité (`...fsync...`) dans `system.merge_tree_settings`

Pour les **paramètres de limite d’insert** (`..._insert`), voir aussi :

* le paramètre [`distributed_foreground_insert`](../../../operations/settings/settings.md#distributed_foreground_insert)
* le paramètre [`prefer_localhost_replica`](/fr/operations/settings/settings#prefer_localhost_replica)
* `bytes_to_throw_insert` est traité avant `bytes_to_delay_insert`, vous ne devez donc pas lui attribuer une valeur inférieure à `bytes_to_delay_insert`
  :::

**Exemple**

```sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

Les données seront lues depuis tous les serveurs du cluster `logs`, à partir de la table `default.hits` présente sur chaque serveur du cluster. Les données ne sont pas seulement lues : elles sont aussi partiellement traitées sur les serveurs distants (dans la mesure du possible). Par exemple, pour une requête avec `GROUP BY`, les données seront agrégées sur les serveurs distants, et les états intermédiaires des fonctions d’agrégation seront envoyés au serveur à l’origine de la requête. Les données y seront ensuite agrégées davantage.

Au lieu du nom de la base de données, vous pouvez utiliser une expression constante qui renvoie une chaîne de caractères. Par exemple : `currentDatabase()`.

<div id="distributed-clusters">
  ## Clusters
</div>

Les clusters sont configurés dans le [fichier de configuration du serveur](../../../operations/configuration-files.md) :

```xml
<remote_servers>
    <logs>
        <!-- Inter-server per-cluster secret for Distributed queries
             default: no secret (no authentication will be performed)

             If set, then Distributed queries will be validated on shards, so at least:
             - such cluster should exist on the shard,
             - such cluster should have the same secret.

             And also (and which is more important), the initial_user will
             be used as current user for the query.
        -->
        <!-- <secret></secret> -->
        
        <!-- Optional. Whether distributed DDL queries (ON CLUSTER clause) are allowed for this cluster. Default: true (allowed). -->        
        <!-- <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries> -->
        
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. The shard name.  Must be non-empty and unique among shards in the cluster. If not specified, will be empty. -->
            <name>shard_01</name>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- Optional. Priority of the replica for load balancing (see also load_balancing setting). Default: 1 (less value has more priority). -->
                <priority>1</priority>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <name>shard_02</name>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

Ici, un cluster nommé `logs` est défini. Il se compose de deux shards, chacun contenant deux répliques. Les shards correspondent aux serveurs qui contiennent différentes parties des données (pour lire toutes les données, vous devez accéder à tous les shards). Les répliques sont des serveurs dupliqués (pour lire toutes les données, vous pouvez accéder aux données de n’importe laquelle des répliques).

Les noms de cluster ne doivent pas contenir de points.

Les paramètres `host`, `port` et, facultativement, `user`, `password`, `secure`, `compression`, `bind_host` sont spécifiés pour chaque serveur :

| Paramètre     | Description                                                                                                                                                                                                                                                                                                                                                                                   | Valeur par défaut |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `host`        | L’adresse du serveur distant. Vous pouvez utiliser soit le nom de domaine, soit une adresse IPv4 ou IPv6. Si vous spécifiez un nom de domaine, le serveur effectue une requête DNS à son démarrage, et le résultat est conservé tant que le serveur est en fonctionnement. Si la requête DNS échoue, le serveur ne démarre pas. Si vous modifiez l’enregistrement DNS, redémarrez le serveur. | -                 |
| `port`        | Le port TCP utilisé pour les échanges de messages (`tcp_port` dans la config, généralement défini sur 9000). À ne pas confondre avec `http_port`.                                                                                                                                                                                                                                             | -                 |
| `user`        | Nom de l’utilisateur permettant de se connecter à un serveur distant. Cet utilisateur doit disposer des droits d’accès nécessaires pour se connecter au serveur spécifié. L’accès est configuré dans le fichier `users.xml`. Pour plus d’informations, consultez la section [droits d’accès](../../../guides/sre/user-management/index.md).                                                   | `default`         |
| `password`    | Le mot de passe utilisé pour se connecter à un serveur distant (non masqué).                                                                                                                                                                                                                                                                                                                  | &#39;&#39;        |
| `secure`      | Indique s’il faut utiliser une connexion SSL/TLS sécurisée. Cela nécessite généralement aussi de spécifier le port (le port sécurisé par défaut est `9440`). Le serveur doit écouter sur `<tcp_port_secure>9440</tcp_port_secure>` et être configuré avec les certificats appropriés.                                                                                                         | `false`           |
| `compression` | Active la compression des données.                                                                                                                                                                                                                                                                                                                                                            | `true`            |
| `bind_host`   | L’adresse source à utiliser lors de la connexion au serveur distant depuis ce nœud. Seules les adresses IPv4 sont prises en charge. Conçu pour des cas de déploiement avancés où il est nécessaire de définir l’adresse IP source utilisée par les requêtes distribuées ClickHouse.                                                                                                           | -                 |

Lors de la spécification des répliques, l’une des répliques disponibles sera sélectionnée pour chacun des shards lors de la lecture. Vous pouvez configurer l’algorithme de répartition de charge (c’est-à-dire la préférence quant à la réplique à utiliser) — voir le paramètre [load&#95;balancing](../../../operations/settings/settings.md#load_balancing). Si la connexion au serveur n’est pas établie, une tentative de connexion sera effectuée avec un délai d’attente court. Si la connexion échoue, la réplique suivante sera sélectionnée, et ainsi de suite pour toutes les répliques. Si la tentative de connexion échoue pour toutes les répliques, elle sera répétée de la même manière, plusieurs fois. Cela améliore la résilience, mais n’assure pas une tolérance aux pannes complète : un serveur distant peut accepter la connexion, mais ne pas fonctionner, ou fonctionner de manière dégradée.

Vous pouvez ne spécifier qu’un seul shard (dans ce cas, le traitement de la requête devrait être qualifié de remote plutôt que de distribué) ou un nombre quelconque de shards. Dans chaque shard, vous pouvez spécifier d’une à un nombre quelconque de répliques. Vous pouvez spécifier un nombre différent de répliques pour chaque shard.

Vous pouvez spécifier autant de clusters que vous le souhaitez dans la configuration.

Pour afficher vos clusters, utilisez la table `system.clusters`.

Le moteur `Distributed` permet de travailler avec un cluster comme avec un serveur local. Cependant, la configuration du cluster ne peut pas être définie dynamiquement ; elle doit être configurée dans le fichier de configuration du serveur. En général, tous les serveurs d’un cluster ont la même configuration de cluster (bien que cela ne soit pas obligatoire). Les clusters du fichier de configuration sont mis à jour à la volée, sans redémarrage du serveur.

Si vous devez envoyer une requête à un ensemble inconnu de shards et de répliques à chaque fois, vous n’avez pas besoin de créer une table `Distributed` — utilisez plutôt la fonction de table `remote`. Voir la section [Fonctions de table](../../../sql-reference/table-functions/index.md).

<div id="distributed-writing-data">
  ## Écriture des données
</div>

Il existe deux méthodes pour écrire des données dans un cluster :

Premièrement, vous pouvez définir sur quels serveurs écrire quelles données et effectuer l&#39;écriture directement sur chaque shard. Autrement dit, exécuter directement des instructions `INSERT` sur les tables distantes du cluster vers lesquelles pointe la table `Distributed`. C&#39;est la solution la plus flexible, car vous pouvez utiliser n&#39;importe quel schéma de sharding, même complexe en raison des contraintes du domaine métier. C&#39;est aussi la solution la plus efficace, puisque les données peuvent être écrites sur différents shards de façon totalement indépendante.

Deuxièmement, vous pouvez exécuter des instructions `INSERT` sur une table `Distributed`. Dans ce cas, la table répartit elle-même les données insérées entre les serveurs. Pour écrire dans une table `Distributed`, le paramètre `sharding_key` doit être configuré (sauf s&#39;il n&#39;y a qu&#39;un seul shard).

Chaque shard peut avoir un `<weight>` défini dans le fichier de configuration. Par défaut, le poids est `1`. Les données sont réparties entre les shards proportionnellement à leur poids. Tous les poids des shards sont additionnés, puis le poids de chaque shard est divisé par le total afin de déterminer sa proportion. Par exemple, s&#39;il y a deux shards et que le premier a un poids de 1 tandis que le second a un poids de 2, un tiers (1 / 3) des lignes insérées sera envoyé au premier et deux tiers (2 / 3) au second.

Chaque shard peut avoir le paramètre `internal_replication` défini dans le fichier de configuration. Si ce paramètre est défini sur `true`, l&#39;opération d&#39;écriture sélectionne la première réplique saine et y écrit les données. Utilisez ce mode si les tables sous-jacentes à la table `Distributed` sont des tables répliquées (par exemple, n&#39;importe lequel des moteurs de table `Replicated*MergeTree`). L&#39;une des répliques de la table recevra l&#39;écriture, puis celle-ci sera automatiquement répliquée vers les autres répliques.

Si `internal_replication` est défini sur `false` (valeur par défaut), les données sont écrites sur toutes les répliques. Dans ce cas, la table `Distributed` réplique elle-même les données. Cette approche est moins bonne que l&#39;utilisation de tables répliquées, car la cohérence des répliques n&#39;est pas vérifiée et, avec le temps, elles contiendront des données légèrement différentes.

Pour sélectionner le shard auquel une ligne de données est envoyée, l&#39;expression de sharding est analysée, puis on prend le reste de sa division par le poids total des shards. La ligne est envoyée au shard correspondant au demi-intervalle des restes allant de `prev_weights` à `prev_weights + weight`, où `prev_weights` est le poids total des shards ayant le plus petit numéro, et `weight` est le poids de ce shard. Par exemple, s&#39;il y a deux shards et que le premier a un poids de 9 tandis que le second a un poids de 10, la ligne sera envoyée au premier shard pour les restes de l&#39;intervalle [0, 9), et au second pour les restes de l&#39;intervalle [9, 19).

L&#39;expression de sharding peut être n&#39;importe quelle expression composée de constantes et de colonnes de table qui renvoie un entier. Par exemple, vous pouvez utiliser l&#39;expression `rand()` pour une distribution aléatoire des données, ou `UserID` pour une distribution selon le reste de la division de l&#39;identifiant utilisateur (les données d&#39;un même utilisateur résideront alors sur un seul shard, ce qui simplifie l&#39;exécution de `IN` et `JOIN` par utilisateur). Si l&#39;une des colonnes n&#39;est pas répartie de manière suffisamment uniforme, vous pouvez l&#39;encapsuler dans une fonction de hachage, par exemple `intHash64(UserID)`.

Un simple reste de division constitue une solution limitée pour le sharding et n&#39;est pas toujours approprié. Cela fonctionne pour des volumes de données moyens à grands (des dizaines de serveurs), mais pas pour de très grands volumes de données (des centaines de serveurs ou plus). Dans ce dernier cas, utilisez le schéma de sharding requis par le domaine métier plutôt que des entrées dans des tables `Distributed`.

Vous devez vous préoccuper du schéma de sharding dans les cas suivants :

* Des requêtes nécessitent de joindre des données (`IN` ou `JOIN`) selon une clé spécifique. Si les données sont shardées selon cette clé, vous pouvez utiliser `IN` ou `JOIN` local au lieu de `GLOBAL IN` ou `GLOBAL JOIN`, ce qui est bien plus efficace.
* Un grand nombre de serveurs est utilisé (des centaines ou plus) avec un grand nombre de petites requêtes, par exemple des requêtes portant sur les données de clients individuels (par exemple, des sites web, des annonceurs ou des partenaires). Pour éviter que les petites requêtes n&#39;affectent l&#39;ensemble du cluster, il est judicieux de placer les données d&#39;un même client sur un seul shard. Vous pouvez aussi mettre en place un sharding à deux niveaux : diviser l&#39;ensemble du cluster en « couches », où une couche peut se composer de plusieurs shards. Les données d&#39;un même client sont placées sur une seule couche, mais des shards peuvent être ajoutés à une couche selon les besoins, et les données y sont réparties aléatoirement. Des tables `Distributed` sont créées pour chaque couche, et une table distribuée partagée unique est créée pour les requêtes globales.

Les données sont écrites en arrière-plan. Lorsqu’elles sont insérées dans la table, le bloc de données est simplement écrit sur le système de fichiers local. Les données sont envoyées aux serveurs distants en arrière-plan dès que possible. La fréquence d’envoi des données est gérée par les paramètres [distributed&#95;background&#95;insert&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) et [distributed&#95;background&#95;insert&#95;max&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms). Le moteur `Distributed` envoie séparément chaque fichier contenant des données insérées, mais vous pouvez activer l’envoi par lots des fichiers avec le paramètre [distributed&#95;background&#95;insert&#95;batch](../../../operations/settings/settings.md#distributed_background_insert_batch). Ce paramètre améliore les performances du cluster en utilisant plus efficacement les ressources du serveur local et du réseau. Vous devez vérifier que les données ont bien été envoyées en consultant la liste des fichiers (données en attente d’envoi) dans le répertoire de la table : `/var/lib/clickhouse/data/database/table/`. Le nombre de threads exécutant les tâches en arrière-plan peut être défini avec le paramètre [background&#95;distributed&#95;schedule&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size).

Si le serveur n’était plus disponible ou a subi un redémarrage brutal (par exemple, en raison d’une panne matérielle) après un `INSERT` dans une table `Distributed`, les données insérées peuvent être perdues. Si une part de données endommagée est détectée dans le répertoire de la table, elle est déplacée vers le sous-répertoire `broken` et n’est plus utilisée.

<div id="distributed-reading-data">
  ## Lecture des données
</div>

Lorsqu&#39;une table `Distributed` est interrogée, les requêtes `SELECT` sont envoyées à tous les shards et fonctionnent quelle que soit la manière dont les données sont réparties entre eux (elles peuvent l’être de façon totalement aléatoire). Lorsque vous ajoutez un nouveau shard, vous n’avez pas besoin d’y transférer les anciennes données. Vous pouvez plutôt y écrire de nouvelles données en lui attribuant un poids plus élevé ; les données seront alors réparties de manière légèrement inégale, mais les requêtes continueront de fonctionner correctement et efficacement.

Lorsque l’option `max_parallel_replicas` est activée, le traitement des requêtes est parallélisé sur toutes les répliques au sein d’un même shard. Pour plus d’informations, consultez la section [max&#95;parallel&#95;replicas](../../../operations/settings/settings.md#max_parallel_replicas).

Pour en savoir plus sur le traitement des requêtes distribuées `in` et `global in`, consultez [cette](/fr/sql-reference/operators/in#distributed-subqueries) documentation.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

<div id="_shard_num">
  #### _Shard_num
</div>

`_shard_num` — Contient la valeur `shard_num` de la table `system.clusters`. Type : [UInt32](../../../sql-reference/data-types/int-uint.md).

:::note
Étant donné que les fonctions de table [`remote`](../../../sql-reference/table-functions/remote.md) et [`cluster](../../../sql-reference/table-functions/cluster.md) créent en interne une table Distributed temporaire, `&#95;shard&#95;num&#96; y est également disponible.
:::

**Voir aussi**

* Description des [colonnes virtuelles](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* Paramètre [`background_distributed_schedule_pool_size`](/fr/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size)
* Fonctions [`shardNum()`](../../../sql-reference/functions/other-functions.md#shardNum) et [`shardCount()`](../../../sql-reference/functions/other-functions.md#shardCount)