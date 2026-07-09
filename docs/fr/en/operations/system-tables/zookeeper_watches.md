---
description: 'Table système affichant les watches ZooKeeper actuellement actifs enregistrés sur
  ce serveur ClickHouse.'
keywords: ['table système', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'référence'
---

<div id="description">
  ## Description
</div>

Affiche les [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) actuellement actifs enregistrés par ce serveur ClickHouse sur des nœuds ZooKeeper (y compris les ZooKeeper auxiliaires). Chaque ligne correspond à un watch.

<div id="columns">
  ## Colonnes
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — Nom de la connexion ZooKeeper (`default` pour la connexion principale ou le nom auxiliaire).
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Heure de création de la watch.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Heure de création de la watch avec une précision à la microseconde.
* `path` ([String](../../sql-reference/data-types/string.md)) — Chemin ZooKeeper surveillé.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — ID de session de la connexion ayant enregistré la watch.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID de la requête ayant créé la watch.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — Type de la requête ayant créé la watch.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type de watch. Valeurs possibles :
  * `Children` — watch des modifications de la liste des nœuds enfants (définie par les opérations `List`).
  * `Exists` — watch de la création ou de la suppression d’un nœud.
  * `Data` — watch des modifications des données d’un nœud (définie par les opérations `Get`).

Exemple :

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**Voir aussi**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [Guide de ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)