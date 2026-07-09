---
description: 'Documentation sur Cluster Discovery dans ClickHouse'
sidebar_label: 'Cluster Discovery'
slug: /operations/cluster-discovery
title: 'Cluster Discovery'
doc_type: 'guide'
---

<div id="overview">
  ## Vue d’ensemble
</div>

La fonctionnalité Cluster Discovery de ClickHouse simplifie la configuration du cluster en permettant aux nœuds de se détecter et de s’enregistrer automatiquement, sans qu’il soit nécessaire de les définir explicitement dans les fichiers de configuration. Cela est particulièrement utile lorsque la définition manuelle de chaque nœud devient fastidieuse.

:::note

Cluster Discovery est une fonctionnalité expérimentale et peut être modifiée ou supprimée dans les versions futures.
Pour l’activer, incluez le paramètre `allow_experimental_cluster_discovery` dans votre fichier de configuration :

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## Configuration des serveurs distants
</div>

<div id="traditional-manual-configuration">
  ### Configuration manuelle traditionnelle
</div>

Traditionnellement, dans ClickHouse, chaque shard et chaque réplique du cluster devaient être renseignés manuellement dans la configuration :

```xml
<remote_servers>
    <cluster_name>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_name>
</remote_servers>

```

<div id="using-cluster-discovery">
  ### Utilisation de Cluster Discovery
</div>

Avec Cluster Discovery, au lieu de définir explicitement chaque nœud, il suffit d’indiquer un chemin dans ZooKeeper. Tous les nœuds qui s’enregistrent sous ce chemin dans ZooKeeper sont automatiquement découverts et ajoutés au cluster.

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # Optional configuration parameters: -->

            <!-- ## Authentication credentials to access all other nodes in cluster: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### Alternatively to password, interserver secret may be used: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## Shard for current node (see below): -->
            <!-- <shard>1</shard> -->

            <!-- ## Observer mode (see below): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

Si vous souhaitez spécifier un numéro de shard pour un nœud donné, vous pouvez inclure la balise `<shard>` dans la section `<discovery>` :

pour `node1` et `node2` :

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

pour `node3` et `node4` :

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### Mode observateur
</div>

Les nœuds configurés en mode observateur ne s’enregistrent pas comme répliques.
Ils se contentent d’observer et de détecter les autres répliques actives du cluster sans y participer activement.
Pour activer le mode observateur, incluez la balise `<observer/>` dans la section `<discovery>` :

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### Découverte des clusters
</div>

Il peut parfois être nécessaire d’ajouter et de supprimer non seulement des hôtes au sein des clusters, mais aussi les clusters eux-mêmes. Vous pouvez utiliser le nœud `<multicluster_root_path>` avec un chemin racine pour plusieurs clusters :

```xml
<remote_servers>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

Dans ce cas, lorsqu’un autre hôte s’enregistre avec le chemin `/clickhouse/discovery/some_new_cluster`, un cluster nommé `some_new_cluster` sera ajouté.

Vous pouvez utiliser les deux fonctionnalités simultanément : l’hôte peut s’enregistrer dans le cluster `my_cluster` et découvrir d’autres clusters :

```xml
<remote_servers>
    <my_cluster>
        <discovery>
            <path>/clickhouse/discovery/my_cluster</path>
        </discovery>
    </my_cluster>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

Limites :

* Vous ne pouvez pas utiliser simultanément `<path>` et `<multicluster_root_path>` dans le même sous-arbre `remote_servers`.
* `<multicluster_root_path>` ne peut être utilisé qu’avec `<observer/>`.
* La dernière partie du path provenant de Keeper est utilisée comme nom du cluster, tandis que lors de l’enregistrement, le nom est tiré de la balise XML.

<div id="use-cases-and-limitations">
  ## Cas d’usage et limites
</div>

Lorsque des nœuds sont ajoutés ou supprimés du chemin ZooKeeper spécifié, ils sont automatiquement détectés ou retirés du cluster, sans qu’il soit nécessaire de modifier la configuration ni de redémarrer le serveur.

Cependant, ces modifications n’affectent que la configuration du cluster, et non les données ni les bases de données et tables existantes.

Prenons l’exemple suivant d’un cluster de 3 nœuds :

```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```sql
SELECT * EXCEPT (default_database, errors_count, slowdowns_count, estimated_recovery_time, database_shard_name, database_replica_name)
FROM system.clusters WHERE cluster = 'default';

┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

```sql
CREATE TABLE event_table ON CLUSTER default (event_time DateTime, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/event_table', '{replica}')
ORDER BY event_time PARTITION BY toYYYYMM(event_time);

INSERT INTO event_table ...
```

Ensuite, nous ajoutons un nouveau nœud au cluster en démarrant un nouveau nœud avec la même entrée dans la section `remote_servers` d’un fichier de configuration :

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

Le quatrième nœud fait partie du cluster, mais la table `event_table` n&#39;existe toujours que sur les trois premiers nœuds :

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

Si vous souhaitez que les tables soient répliquées sur tous les nœuds, vous pouvez utiliser le moteur de base de données [Replicated](../engines/database-engines/replicated.md) à la place de Cluster Discovery.