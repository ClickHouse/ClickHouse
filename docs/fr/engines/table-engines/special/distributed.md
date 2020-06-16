---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: "Distribu\xE9"
---

# Distribué {#distributed}

**Les Tables avec moteur distribué ne stockent aucune donnée par elles mêmes**, mais autoriser le traitement des requêtes distribuées sur plusieurs serveurs.
La lecture est automatiquement parallélisée. Lors d'une lecture, les index de table sur les serveurs distants sont utilisés, s'il y en a.

Le moteur distribué accepte les paramètres:

-   le nom du cluster dans le fichier de configuration du serveur

-   le nom d'une base de données distante

-   le nom d'une table distante

-   (en option) sharding clé

-   (éventuellement) nom de la stratégie, il sera utilisé pour stocker des fichiers temporaires pour l'envoi asynchrone

    Voir aussi:

    -   `insert_distributed_sync` paramètre
    -   [MergeTree](../mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) pour les exemples

Exemple:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

Les données seront lues à partir de tous les serveurs ‘logs’ cluster, à partir de la valeur par défaut.hits table située sur chaque serveur du cluster.
Les données ne sont pas seulement lues mais sont partiellement traitées sur les serveurs distants (dans la mesure du possible).
Par exemple, pour une requête avec GROUP BY, les données seront agrégées sur des serveurs distants et les états intermédiaires des fonctions d'agrégation seront envoyés au serveur demandeur. Ensuite, les données seront plus agrégées.

Au lieu du nom de la base de données, vous pouvez utiliser une expression constante qui renvoie une chaîne. Par exemple: currentDatabase().

logs – The cluster name in the server's config file.

Les Clusters sont définis comme ceci:

``` xml
<remote_servers>
    <logs>
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
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

Ici un cluster est défini avec le nom ‘logs’ qui se compose de deux fragments, dont chacune contient deux répliques.
Les partitions se réfèrent aux serveurs qui contiennent différentes parties des données (pour lire toutes les données, vous devez accéder à tous les partitions).
Les répliques sont des serveurs de duplication (afin de lire toutes les données, vous pouvez accéder aux données sur l'une des répliques).

Les noms de Cluster ne doivent pas contenir de points.

Paramètre `host`, `port` et , éventuellement, `user`, `password`, `secure`, `compression` sont spécifiés pour chaque serveur:
- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server doesn't start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (‘tcp\_port’ dans la configuration, généralement définie sur 9000). Ne le confondez pas avec http\_port.
- `user` – Name of the user for connecting to a remote server. Default value: default. This user must have access to connect to the specified server. Access is configured in the users.xml file. For more information, see the section [Les droits d'accès](../../../operations/access-rights.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` - Utilisez ssl pour la connexion, généralement vous devez également définir `port` = 9440. Le serveur doit écouter `<tcp_port_secure>9440</tcp_port_secure>` et avoir des certificats corrects.
- `compression` - Utiliser la compression de données. Valeur par défaut: true.

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [équilibrage](../../../operations/settings/settings.md#settings-load_balancing) paramètre.
Si la connexion avec le serveur n'est pas établie, il y aura une tentative de connexion avec un court délai. Si la connexion échoue, la réplique suivante sera sélectionnée, et ainsi de suite pour toutes les répliques. Si la tentative de connexion a échoué pour toutes les répliques, la tentative sera répété de la même façon, plusieurs fois.
Cela fonctionne en faveur de la résilience, mais ne fournit pas de tolérance aux pannes complète: un serveur distant peut accepter la connexion, mais peut ne pas fonctionner ou fonctionner mal.

Vous pouvez spécifier un seul des fragments (dans ce cas, le traitement de la requête doit être appelé distant, plutôt que distribué) ou jusqu'à un nombre quelconque de fragments. Dans chaque fragment, vous pouvez spécifier un nombre de répliques. Vous pouvez spécifier un nombre différent de répliques pour chaque fragment.

Vous pouvez spécifier autant de clusters que vous souhaitez dans la configuration.

Pour afficher vos clusters, utilisez ‘system.clusters’ table.

Le moteur distribué permet de travailler avec un cluster comme un serveur local. Cependant, le cluster est inextensible: vous devez écrire sa configuration dans le fichier de configuration du serveur (encore mieux, pour tous les serveurs du cluster).

The Distributed engine requires writing clusters to the config file. Clusters from the config file are updated on the fly, without restarting the server. If you need to send a query to an unknown set of shards and replicas each time, you don't need to create a Distributed table – use the ‘remote’ fonction de table à la place. Voir la section [Les fonctions de Table](../../../sql-reference/table-functions/index.md).

Il existe deux méthodes pour écrire des données dans un cluster:

Tout d'abord, vous pouvez définir les serveurs d'écrire les données à et effectuer l'écriture directement sur chaque fragment. En d'autres termes, effectuez INSERT dans les tables que la table distribuée “looks at”. C'est la solution la plus flexible car vous pouvez utiliser n'importe quel schéma de sharding, qui pourrait être non trivial en raison des exigences du sujet. C'est également la solution la plus optimale puisque les données peuvent être écrites sur différents fragments de manière complètement indépendante.

Deuxièmement, vous pouvez effectuer INSERT dans une table distribuée. Dans ce cas, la table distribuera les données insérées sur les serveurs eux-mêmes. Pour écrire dans une table distribuée, elle doit avoir un jeu de clés de sharding (le dernier paramètre). De plus, s'il n'y a qu'un seul fragment, l'opération d'écriture fonctionne sans spécifier la clé de sharding, car cela ne signifie rien dans ce cas.

Chaque fragment peut avoir un poids défini dans le fichier de configuration. Par défaut, le poids est égal à un. Les données sont réparties entre les fragments dans la quantité proportionnelle au poids des fragments. Par exemple, si il y a deux tessons et le premier a un poids de 9 tandis que la seconde a un poids de 10, le premier sera envoyé 9 / 19 parties de lignes, et le second sera envoyé 10 / 19.

Chaque fragment peut avoir le ‘internal\_replication’ paramètre défini dans le fichier de configuration.

Si ce paramètre est défini à ‘true’, l'opération d'écriture sélectionne le premier saine réplique et écrit les données. Utilisez cette option si le tableau Distribué “looks at” tables répliquées. En d'autres termes, si la table où les données seront écrites va répliquer elle-même.

Si elle est définie sur ‘false’ (par défaut), les données sont écrites dans toutes les répliques. En substance, cela signifie que la table distribuée réplique les données elle-même. C'est pire que d'utiliser des tables répliquées, car la cohérence des répliques n'est pas vérifiée et, au fil du temps, elles contiendront des données légèrement différentes.

Pour sélectionner le fragment auquel une ligne de données est envoyée, l'expression de sharding est analysée et son reste est extrait de la diviser par le poids total des fragments. La ligne est envoyée au fragment qui correspond au demi-intervalle des restes de ‘prev\_weight’ de ‘prev\_weights + weight’, où ‘prev\_weights’ c'est le poids total des tessons avec le plus petit nombre, et ‘weight’ est le poids de cet éclat. Par exemple, s'il y a deux fragments, et que le premier a un poids de 9 tandis que le second a un poids de 10, la ligne sera envoyée au premier fragment pour les restes de la plage \[0, 9), et au second pour les restes de la plage \[9, 19).

L'expression de sharding peut être n'importe quelle expression de constantes et de colonnes de table qui renvoie un entier. Par exemple, vous pouvez utiliser l'expression ‘rand()’ pour la distribution aléatoire des données, ou ‘UserID’ pour la distribution par le reste de la division de L'ID de l'utilisateur (alors les données d'un seul utilisateur résideront sur un seul fragment, ce qui simplifie l'exécution et la jointure par les utilisateurs). Si l'une des colonnes n'est pas assez répartie uniformément, vous pouvez l'envelopper dans une fonction de hachage: intHash64 (UserID).

Un simple rappel de la division est une solution limitée pour le sharding et n'est pas toujours approprié. Cela fonctionne pour des volumes de données moyens et importants (des dizaines de serveurs), mais pas pour des volumes de données très importants (des centaines de serveurs ou plus). Dans ce dernier cas, utilisez le schéma de répartition requis par le domaine, plutôt que d'utiliser des entrées dans des tableaux distribués.

SELECT queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you don't have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

Vous devriez être préoccupé par le système de sharding dans les cas suivants:

-   Les requêtes sont utilisées qui nécessitent des données de jointure (IN ou JOIN) par une clé spécifique. Si les données sont partagées par cette clé, vous pouvez utiliser local in ou JOIN au lieu de GLOBAL IN ou global JOIN, ce qui est beaucoup plus efficace.
-   Un grand nombre de serveurs est utilisé (des centaines ou plus) avec un grand nombre de petites requêtes (requêtes de clients individuels - sites Web, annonceurs ou partenaires). Pour que les petites requêtes n'affectent pas l'ensemble du cluster, il est logique de localiser les données d'un seul client sur un seul fragment. Alternativement, comme nous l'avons fait dans Yandex.Metrica, vous pouvez configurer le sharding à deux niveaux: divisez le cluster entier en “layers”, où une couche peut être constituée de plusieurs éclats. Les données d'un seul client sont situées sur une seule couche, mais des fragments peuvent être ajoutés à une couche si nécessaire, et les données sont distribuées aléatoirement à l'intérieur de celles-ci. Des tables distribuées sont créées pour chaque couche et une seule table distribuée partagée est créée pour les requêtes globales.

Les données sont écrites de manière asynchrone. Lorsqu'il est inséré dans la table, le bloc de données est simplement écrit dans le système de fichiers local. Les données sont envoyées aux serveurs distants en arrière-plan dès que possible. La période d'envoi des données est gérée par [distributed\_directory\_monitor\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms) et [distributed\_directory\_monitor\_max\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) paramètre. Le `Distributed` moteur envoie chaque fichier de données insérées séparément, mais vous pouvez activer le lot envoi de fichiers avec l' [distributed\_directory\_monitor\_batch\_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts) paramètre. Ce paramètre améliore les performances du cluster en utilisant mieux les ressources réseau et serveur local. Vous devriez vérifier si les données sont envoyées avec succès en vérifiant la liste des fichiers (données en attente d'envoi) dans le répertoire de la table: `/var/lib/clickhouse/data/database/table/`.

Si le serveur a cessé d'exister ou a subi un redémarrage Brutal (par exemple, après une panne de périphérique) après une insertion dans une table distribuée, les données insérées peuvent être perdues. Si une partie de données endommagée est détectée dans le répertoire de la table, elle est transférée ‘broken’ sous-répertoire et n'est plus utilisé.

Lorsque l'option max\_parallel\_replicas est activée, le traitement des requêtes est parallélisé entre toutes les répliques d'un seul fragment. Pour plus d'informations, consultez la section [max\_parallel\_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas).

## Les Colonnes Virtuelles {#virtual-columns}

-   `_shard_num` — Contains the `shard_num` (de `system.clusters`). Type: [UInt32](../../../sql-reference/data-types/int-uint.md).

!!! note "Note"
    Depuis [`remote`](../../../sql-reference/table-functions/remote.md)/`cluster` les fonctions de table créent en interne une instance temporaire du même moteur distribué, `_shard_num` est disponible là-bas aussi.

**Voir Aussi**

-   [Les colonnes virtuelles](index.md#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/distributed/) <!--hide-->
