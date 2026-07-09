---
description: 'Le moteur `Merge` (à ne pas confondre avec `MergeTree`) ne stocke pas
  lui-même les données, mais permet de lire simultanément à partir d''un nombre quelconque d''autres tables.'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Moteur de table Merge'
doc_type: 'reference'
---

Le moteur `Merge` (à ne pas confondre avec `MergeTree`) ne stocke pas lui-même les données, mais permet de lire simultanément à partir d&#39;un nombre quelconque d&#39;autres tables.

La lecture est automatiquement parallélisée. L&#39;écriture dans cette table n&#39;est pas prise en charge. Lors de la lecture, les index des tables effectivement lues sont utilisés, s&#39;ils existent.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## Paramètres du moteur
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — Valeurs possibles :

* nom de base de données,
  * expression constante qui renvoie une chaîne contenant un nom de base de données, par exemple `currentDatabase()`,
  * `REGEXP(expression)`, où `expression` est une expression régulière correspondant aux noms de bases de données.

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — Une expression régulière permettant de faire correspondre les noms des tables dans la ou les DB spécifiées.

Expressions régulières — [re2](https://github.com/google/re2) (prend en charge un sous-ensemble de PCRE), sensible à la casse.
Voir les notes sur l’échappement des symboles dans les expressions régulières dans la section « match ».

<div id="usage">
  ## Utilisation
</div>

Lors de la sélection des tables à lire, la table `Merge` elle-même n&#39;est pas prise en compte, même si elle correspond à la regex. Cela permet d&#39;éviter les boucles.
Il est possible de créer deux tables `Merge` qui tenteront indéfiniment de lire les données l&#39;une de l&#39;autre, mais ce n&#39;est pas une bonne idée.

La façon la plus courante d&#39;utiliser le moteur `Merge` consiste à manipuler un grand nombre de tables `TinyLog` comme s&#39;il s&#39;agissait d&#39;une seule table.

<div id="examples">
  ## Exemples
</div>

**Exemple 1**

Prenons deux bases de données `ABC_corporate_site` et `ABC_store`. La table `all_visitors` contiendra les identifiants des tables `visitors` des deux bases de données.

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**Exemple 2**

Supposons que vous ayez une ancienne table `WatchLog_old` et que vous décidiez de modifier le partitionnement sans déplacer les données vers une nouvelle table `WatchLog_new`, tout en devant consulter les données des deux tables.

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_table` — Nom de la table à partir de laquelle les données ont été lues. Type : [String](../../../sql-reference/data-types/string.md).

  Si vous appliquez un filtre sur `_table` (par exemple `WHERE _table='xyz'`), seules les tables qui satisfont la condition de filtrage sont lues.

* `_database` — Contient le nom de la base de données à partir de laquelle les données ont été lues. Type : [String](../../../sql-reference/data-types/string.md).

**Voir aussi**

* [Colonnes virtuelles](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* fonction de table [merge](../../../sql-reference/table-functions/merge.md)