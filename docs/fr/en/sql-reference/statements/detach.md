---
description: 'Documentation de l’instruction DETACH'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'Instruction DETACH'
doc_type: 'référence'
---

Fait &quot;oublier&quot; au serveur l’existence d’une table, d’une vue matérialisée, d’un dictionnaire ou d’une base de données.

**Syntaxe**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

Le détachement ne supprime ni les données ni les métadonnées d’une table, d’une vue matérialisée, d’un dictionnaire ou d’une base de données. Si une entité n’a pas été détachée `PERMANENTLY`, au prochain démarrage du serveur, celui-ci lira les métadonnées et rattachera de nouveau la table/vue/le dictionnaire/la base de données. Si une entité a été détachée `PERMANENTLY`, il n’y aura pas de rattachement automatique.

Qu’une table, un dictionnaire ou une base de données ait été détaché(e) définitivement ou non, vous pouvez, dans les deux cas, les rattacher à l’aide de la requête [ATTACH](../../sql-reference/statements/attach.md).
Les tables de logs système peuvent également être rattachées (par ex. `query_log`, `text_log`, etc.). Les autres tables système ne peuvent pas être rattachées. Au prochain démarrage du serveur, celui-ci rattachera de nouveau ces tables.

`ATTACH MATERIALIZED VIEW` ne fonctionne pas avec la syntaxe courte (sans `SELECT`), mais vous pouvez rattacher la vue à l’aide de la requête `ATTACH TABLE`.

Notez que vous ne pouvez pas détacher définitivement une table déjà détachée (temporairement). En revanche, vous pouvez la rattacher, puis la détacher de nouveau définitivement.

De plus, vous ne pouvez pas [DROP](../../sql-reference/statements/drop.md#drop-table) la table détachée, ni exécuter [CREATE TABLE](../../sql-reference/statements/create/table.md) avec le même nom qu’une table détachée définitivement, ni la remplacer par une autre table avec la requête [RENAME TABLE](../../sql-reference/statements/rename.md).

Le modificateur `SYNC` exécute l’action sans délai.

**Exemple**

Création d’une table :

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Détacher la table :

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
Dans ClickHouse Cloud, les utilisateurs doivent utiliser la clause `PERMANENTLY`, par exemple `DETACH TABLE <table> PERMANENTLY`. Si cette clause n&#39;est pas utilisée, les tables seront de nouveau rattachées au redémarrage du cluster, par exemple lors des mises à niveau.
:::

**Voir aussi**

* [Vue matérialisée](/fr/sql-reference/statements/create/view#materialized-view)
* [Dictionnaires](./create/dictionary/overview.md)