---
description: 'Documentation du DDL distribué'
sidebar_label: 'DDL distribué'
sidebar_position: 3
slug: /sql-reference/distributed-ddl
title: 'Requêtes DDL distribuées (clause ON CLUSTER)'
doc_type: 'reference'
---

Par défaut, les requêtes `CREATE`, `DROP`, `ALTER` et `RENAME` n’affectent que le serveur actuel sur lequel elles sont exécutées. Dans un environnement en cluster, il est possible d’exécuter ces requêtes de manière distribuée à l’aide de la clause `ON CLUSTER`.

Par exemple, la requête suivante crée la table `all_hits` `Distributed` sur chaque hôte du `cluster` :

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Pour exécuter correctement ces requêtes, chaque hôte doit avoir la même définition du cluster (pour simplifier la synchronisation des configurations, vous pouvez utiliser des substitutions depuis ZooKeeper). Ils doivent également se connecter aux serveurs ZooKeeper.

La version locale de la requête finira par être exécutée sur chaque hôte du cluster, même si certains hôtes ne sont pas disponibles pour le moment.

:::important
L&#39;ordre d&#39;exécution des requêtes sur un même hôte est garanti.
:::