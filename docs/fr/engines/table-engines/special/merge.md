---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: Fusionner
---

# Fusionner {#merge}

Le `Merge` moteur (à ne pas confondre avec `MergeTree`) ne stocke pas les données elles-mêmes, mais permet la lecture de n'importe quel nombre d'autres tables simultanément.
La lecture est automatiquement parallélisée. L'écriture dans une table n'est pas prise en charge. Lors de la lecture, les index des tables en cours de lecture sont utilisés, s'ils existent.
Le `Merge` engine accepte les paramètres: le nom de la base de données et une expression régulière pour les tables.

Exemple:

``` sql
Merge(hits, '^WatchLog')
```

Les données seront lues à partir des tableaux du `hits` base de données dont les noms correspondent à l'expression régulière ‘`^WatchLog`’.

Au lieu du nom de la base de données, vous pouvez utiliser une expression constante qui renvoie une chaîne. Exemple, `currentDatabase()`.

Regular expressions — [re2](https://github.com/google/re2) (prend en charge un sous-ensemble de PCRE), sensible à la casse.
Voir les notes sur les symboles d'échappement dans les expressions régulières “match” section.

Lors de la sélection des tables à lire, le `Merge` le tableau lui-même ne sera pas choisie, même si elle correspond à l'expression rationnelle. C'est pour éviter les boucles.
Il est possible de créer deux `Merge` des tables qui essaieront sans cesse de lire les données des autres, mais ce n'est pas une bonne idée.

L'utilisation traditionnelle de la `Merge` moteur pour travailler avec un grand nombre de `TinyLog` les tables comme si avec une seule table.

Exemple 2:

Disons que vous avez une ancienne table (WatchLog_old) et que vous avez décidé de changer de partitionnement sans déplacer les données vers une nouvelle table (WatchLog_new) et que vous devez voir les données des deux tables.

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## Les Colonnes Virtuelles {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [Chaîne](../../../sql-reference/data-types/string.md).

    Vous pouvez définir les conditions constantes sur `_table` dans le `WHERE/PREWHERE` clause (par exemple, `WHERE _table='xyz'`). Dans ce cas l'opération de lecture est effectuée uniquement pour les tables où la condition sur `_table` est satisfaite, pour le `_table` colonne agit comme un index.

**Voir Aussi**

-   [Les colonnes virtuelles](index.md#table_engines-virtual_columns)

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->
