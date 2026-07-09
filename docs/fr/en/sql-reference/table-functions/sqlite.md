---
description: 'Permet d’exécuter des requêtes sur des données stockées dans une base de données SQLite.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

Permet d’exécuter des requêtes sur des données stockées dans une base de données [SQLite](../../engines/database-engines/sqlite.md).

<div id="syntax">
  ## Syntaxe
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## Arguments
</div>

* `db_path` — Chemin d’accès à un fichier contenant une base de données SQLite. [String](../../sql-reference/data-types/string.md).
* `table_name` — Nom d’une table dans la base de données SQLite, ou requête transmise telle quelle à SQLite (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valeur renvoyée
</div>

* Un objet de type table avec les mêmes colonnes que la table `SQLite` d’origine.

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, le deuxième argument peut être une requête `SELECT` transmise telle quelle à SQLite. La structure de la table obtenue est inférée à partir du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Une telle table est en lecture seule : les opérations `INSERT` n&#39;y sont pas autorisées. La même syntaxe est prise en charge par le moteur de table [`SQLite`](/fr/engines/table-engines/integrations/sqlite).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse, puis re-sérialisée avant d&#39;être envoyée à SQLite. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe propre à SQLite que ClickHouse n&#39;analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé tel quel à SQLite.

Tout `WHERE`, `LIMIT`, toute agrégation, etc. externe de la requête ClickHouse englobante n&#39;est **pas** poussé dans la requête transmise — cela est appliqué dans ClickHouse après la récupération de l&#39;intégralité du résultat de la requête. Pour restreindre les données lues depuis SQLite, placez le filtre dans la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être poussé est rejeté avec une exception au lieu d&#39;être appliqué localement.
:::

<div id="example">
  ## Exemple
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## Voir aussi
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) comme moteur de table
* [Moteur de base de données SQLite](../../engines/database-engines/sqlite.md) — section sur la prise en charge des types de données