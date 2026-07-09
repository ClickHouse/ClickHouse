---
description: 'Documentation de CREATE DATABASE'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'référence'
---

Crée une nouvelle base de données.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## Clauses
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

Si la base de données `db_name` existe déjà, ClickHouse ne crée pas de nouvelle base de données et :

* ne lève pas d’exception si la clause est spécifiée.
* lève une exception si la clause n’est pas spécifiée.

<div id="on-cluster">
  ### ON CLUSTER
</div>

ClickHouse crée la base de données `db_name` sur tous les serveurs du cluster spécifié. Pour plus de détails, consultez l’article [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="engine">
  ### MOTEUR
</div>

Par défaut, ClickHouse utilise son propre [moteur](../../../engines/database-engines/atomic.md) de base de données [Atomic](../../../engines/database-engines/atomic.md). Il existe également [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md), [SQLite](../../../engines/database-engines/sqlite.md).

<div id="comment">
  ### COMMENT
</div>

Vous pouvez ajouter un commentaire à la base de données au moment de sa création.

Les commentaires sont pris en charge pour tous les moteurs de base de données.

**Syntaxe**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Exemple**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### Paramètres
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

Lorsqu’il est activé, les tables ne sont pas entièrement chargées au démarrage de la base de données. À la place, un proxy léger est créé pour chaque table, et le véritable moteur de table est matérialisé au premier accès. Cela réduit le temps de démarrage et l’utilisation de la mémoire pour les bases de données comportant de nombreuses tables, dont seul un sous-ensemble est réellement interrogé.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

S’applique aux moteurs de base de données qui stockent les métadonnées des tables sur disque (par ex. `Atomic`, `Ordinary`). Les vues, les vues matérialisées, les dictionnaires et les tables basées sur des fonctions de table sont toujours chargés de manière anticipée, indépendamment de ce paramètre.

**Quand l’utiliser :** Ce paramètre est utile pour les bases de données comportant un grand nombre de tables (des centaines ou des milliers), dont seul un sous-ensemble fait l’objet de requêtes actives. Il réduit le temps de démarrage du serveur et l’utilisation de la mémoire en reportant au premier accès la création des objets de moteur de table, l’analyse des parties de données et l’initialisation des threads d’arrière-plan.

**Impact sur `system.tables` :**

* Avant qu’une table ne soit accédée, `system.tables` affiche son moteur comme `TableProxy`. Après le premier accès, il affiche le nom réel du moteur (par ex. `MergeTree`).
* Des colonnes comme `total_rows` et `total_bytes` renvoient `NULL` pour les tables non chargées, car le stockage réel n’a pas encore été créé.

**Interaction avec les opérations DDL :**

* `SELECT`, `INSERT`, `ALTER`, `DROP` déclenchent de manière transparente le chargement du véritable moteur de table lors de la première utilisation.
* `RENAME TABLE` fonctionne sans déclencher de chargement.
* Une fois qu’une table est chargée, elle le reste pendant toute la durée de vie du processus serveur.

**Limitations :**

* Les outils de surveillance qui s’appuient sur les métadonnées de `system.tables` (par ex. `total_rows`, `engine`) peuvent voir des informations incomplètes pour les tables non chargées.
* La première requête sur une table non chargée entraîne un coût de chargement ponctuel (analyse de l’instruction `CREATE TABLE` stockée et initialisation du moteur).

Valeur par défaut : `0` (désactivé).