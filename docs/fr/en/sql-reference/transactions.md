---
description: 'Page décrivant la prise en charge des transactions (ACID) dans ClickHouse'
slug: /guides/developer/transactional
title: 'Prise en charge des transactions (ACID)'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # Prise en charge des transactions (ACID)
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## Cas 1 : INSERT dans une partition d’une table de la famille MergeTree*
</div>

Ceci est transactionnel (ACID) si les lignes insérées sont compactées et insérées sous la forme d’un seul bloc (voir les notes) :

* Atomicité : un INSERT réussit ou est rejeté dans son ensemble : si une confirmation est envoyée au client, alors toutes les lignes ont été insérées ; si une erreur est envoyée au client, alors aucune ligne n’a été insérée.
* Cohérence : si aucune contrainte de table n’est violée, alors toutes les lignes d’un INSERT sont insérées et l’INSERT réussit ; si des contraintes sont violées, alors aucune ligne n’est insérée.
* Isolation : des clients concurrents observent un instantané cohérent de la table — son état tel qu’il était avant la tentative d’INSERT, ou après la réussite de l’INSERT ; aucun état partiel n’est visible. Les clients à l’intérieur d’une autre transaction bénéficient d’une [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation), tandis que les clients en dehors d’une transaction ont un niveau d’isolation [read uncommitted](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted).
* Durabilité : un INSERT réussi est écrit dans le système de fichiers avant qu’une réponse ne soit renvoyée au client, sur une seule réplique ou sur plusieurs répliques (contrôlé par le paramètre `insert_quorum`), et ClickHouse peut demander à l’OS de synchroniser les données du système de fichiers sur le support de stockage (contrôlé par le paramètre `fsync_after_insert`).
* Un INSERT dans plusieurs tables avec une seule instruction est possible si des vues matérialisées sont impliquées (l’INSERT du client vise une table à laquelle des vues matérialisées sont associées).

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## Cas 2 : INSERT dans plusieurs partitions d&#39;une table de la famille MergeTree*
</div>

Identique au cas 1 ci-dessus, avec cette précision :

* Si la table comporte de nombreuses partitions et que l&#39;INSERT en couvre plusieurs, alors l&#39;insertion dans chaque partition constitue une transaction distincte

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## Cas 3 : INSERT dans une table distribuée de la famille MergeTree*
</div>

Identique au cas 1 ci-dessus, avec cette précision :

* L’INSERT dans une table Distributed n’est pas transactionnel dans son ensemble, tandis que l’insertion dans chaque segment l’est

<div id="case-4-using-a-buffer-table">
  ## Cas 4 : utilisation d’une table Buffer
</div>

* l’insertion dans les tables Buffer n’est ni atomique, ni isolée, ni cohérente, ni durable

<div id="case-5-using-async_insert">
  ## Cas 5 : Utilisation d’`async_insert`
</div>

Comme pour le cas 1 ci-dessus, avec cette précision :

* l’atomicité est garantie même si `async_insert` est activé et que `wait_for_async_insert` est défini sur 1 (valeur par défaut), mais si `wait_for_async_insert` est défini sur 0, alors l’atomicité n’est pas garantie.

<div id="notes">
  ## Remarques
</div>

* les lignes insérées depuis le client dans un certain format de données sont regroupées en un seul bloc lorsque :
  * le format d&#39;insertion est orienté lignes (comme CSV, TSV, Values, JSONEachRow, etc.) et que les données contiennent moins de `max_insert_block_size` lignes (~1 000 000 par défaut) ou moins de `min_chunk_bytes_for_parallel_parsing` octets (10 MB par défaut) si l&#39;analyse syntaxique parallèle est utilisée (activée par défaut)
  * le format d&#39;insertion est orienté colonnes (comme Native, Parquet, ORC, etc.) et que les données ne contiennent qu&#39;un seul bloc de données
* la taille du bloc inséré peut en général dépendre de nombreux paramètres (par exemple : `max_block_size`, `max_insert_block_size`, `min_insert_block_size_rows`, `min_insert_block_size_bytes`, `preferred_block_size_bytes`, etc.)
* si le client n&#39;a pas reçu de réponse du serveur, il ne sait pas si la transaction a réussi et peut répéter la transaction en utilisant les propriétés d&#39;insertion exactly-once
* ClickHouse utilise [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) avec [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) en interne pour les transactions concurrentes
* toutes les propriétés ACID restent valides même en cas d&#39;arrêt forcé ou de panne du serveur
* pour garantir la durabilité des insertions dans une configuration typique, il faut soit activer insert&#95;quorum sur différentes AZ, soit activer fsync
* la « cohérence » au sens d&#39;ACID ne couvre pas la sémantique des systèmes distribués ; voir https://jepsen.io/consistency ; celle-ci est contrôlée par d&#39;autres paramètres (select&#95;sequential&#95;consistency)
* cette explication ne couvre pas la nouvelle fonctionnalité de transactions, qui permet d&#39;avoir des transactions complètes sur plusieurs tables, vues matérialisées, pour plusieurs SELECT, etc. (voir la section suivante sur Transactions, Commit et Rollback)

<div id="transactions-commit-and-rollback">
  ## Transactions, commit et rollback
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

En plus des fonctionnalités décrites au début de ce document, ClickHouse prend en charge, à titre expérimental, les transactions, les commits et les rollbacks.

<div id="requirements">
  ### Prérequis
</div>

* Déployez ClickHouse Keeper ou ZooKeeper pour assurer le suivi des transactions
* DB atomic uniquement (par défaut)
* Uniquement le moteur de table MergeTree non répliqué
* Activez la prise en charge expérimentale des transactions en ajoutant ce paramètre dans `config.d/transactions.xml` :
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### Remarques
</div>

* Il s’agit d’une fonctionnalité expérimentale et des modifications sont à prévoir.
* Si une exception survient pendant une transaction, vous ne pouvez pas commit la transaction. Cela inclut toutes les exceptions, y compris les exceptions `UNKNOWN_FUNCTION` causées par des fautes de frappe.
* Les transactions imbriquées ne sont pas prises en charge ; terminez la transaction en cours et démarrez-en une nouvelle

<div id="configuration">
  ### Configuration
</div>

Ces exemples utilisent un serveur ClickHouse à nœud unique avec ClickHouse Keeper activé.

<div id="enable-experimental-transaction-support">
  #### Activer la prise en charge expérimentale des transactions
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### Configuration de base pour un nœud unique de serveur ClickHouse avec ClickHouse Keeper activé
</div>

:::note
Consultez la documentation sur le [déploiement](/fr/deployment-guides/terminology.md) pour plus de détails sur le déploiement du serveur ClickHouse et d’un quorum adéquat de nœuds ClickHouse Keeper. La configuration présentée ici est fournie à titre expérimental.
:::

```xml title=/etc/clickhouse-server/config.d/config.xml
<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <display_name>node 1</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <zookeeper>
        <node>
            <host>clickhouse-01</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse-keeper-01</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

<div id="example">
  ### Exemple
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### Vérifiez que les transactions expérimentales sont activées
</div>

Exécutez un `BEGIN TRANSACTION` ou un `START TRANSACTION`, suivi d’un `ROLLBACK`, pour vérifier que les transactions expérimentales sont activées et que ClickHouse Keeper l’est également, puisqu’il sert à suivre les transactions.

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
Si vous voyez l’erreur suivante, vérifiez votre fichier de configuration afin de vous assurer que `allow_experimental_transactions` est défini sur `1` (ou sur toute valeur autre que `0` ou `false`).

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

Vous pouvez également vérifier ClickHouse Keeper avec la commande suivante

```bash
echo ruok | nc localhost 9181
```

ClickHouse Keeper doit répondre par `imok`.
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### Créer une table de test
</div>

:::tip
La création de tables n&#39;est pas transactionnelle. Exécutez cette requête DDL en dehors d&#39;une transaction.
:::

```sql
CREATE TABLE mergetree_table
(
    `n` Int64
)
ENGINE = MergeTree
ORDER BY n
```

```response
Ok.
```

<div id="begin-a-transaction-and-insert-a-row">
  #### Démarrer une transaction et insérer une ligne
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (10)
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 10 │
└────┘
```

:::note
Vous pouvez interroger la table au sein d’une transaction et constater que la ligne a bien été insérée, même si elle n’a pas encore été validée.
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### Rollback la transaction et interrogez de nouveau la table
</div>

Vérifiez que la transaction a bien été rollback :

```sql
ROLLBACK
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

<div id="complete-a-transaction-and-query-the-table-again">
  #### Finaliser une transaction et interroger à nouveau la table
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (42)
```

```response
Ok.
```

```sql
COMMIT
```

```response
Ok. Elapsed: 0.002 sec.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 42 │
└────┘
```

<div id="transactions-introspection">
  ### Introspection des transactions
</div>

Vous pouvez inspecter les transactions en interrogeant la table `system.transactions`, mais notez que vous ne pouvez pas interroger cette
table depuis une session en cours de transaction. Ouvrez une deuxième session `clickhouse client` pour interroger cette table.

```sql
SELECT *
FROM system.transactions
FORMAT Vertical
```

```response
Row 1:
──────
tid:         (33,61,'51e60bce-6b82-4732-9e1d-b40705ae9ab8')
tid_hash:    11240433987908122467
elapsed:     210.017820947
is_readonly: 1
state:       RUNNING
```

<div id="more-details">
  ## Plus de détails
</div>

Consultez cette [méta-issue](https://github.com/ClickHouse/ClickHouse/issues/48794) pour accéder à des tests beaucoup plus complets et suivre l’avancement.