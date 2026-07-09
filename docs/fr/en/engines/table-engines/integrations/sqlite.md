---
description: 'Le moteur permet d’importer et d’exporter des données vers SQLite, et d’interroger directement des tables SQLite depuis ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'Moteur de table SQLite'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # Moteur de table SQLite
</div>

<CloudNotSupportedBadge />

Ce moteur permet d&#39;importer et d&#39;exporter des données vers SQLite, et prend également en charge l&#39;exécution directe de requêtes sur des tables SQLite depuis ClickHouse.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Paramètres du moteur**

* `db_path` — Chemin vers le fichier SQLite contenant une base de données.
* `table` — Nom d’une table dans la base de données SQLite, ou requête transmise telle quelle à SQLite (voir [Utilisation d’une requête à la place d’un nom de table](#passing-a-query)).

<div id="passing-a-query">
  ## Utilisation d’une requête à la place d’un nom de table
</div>

Au lieu d’un nom de table, l’argument `table` peut être une requête `SELECT` transmise telle quelle à SQLite. La structure de la table est déduite du résultat de la requête. La requête peut être écrite soit sous forme de sous-requête, soit encapsulée dans la fonction `query` :

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Une telle table est en lecture seule : il n&#39;est pas permis d&#39;y faire des `INSERT`. La même syntaxe est prise en charge par la fonction de table [`sqlite`](/fr/sql-reference/table-functions/sqlite).

:::note
La forme de sous-requête `(SELECT ...)` est analysée par ClickHouse, puis re-sérialisée avant d&#39;être envoyée à SQLite. Elle doit donc être valide en ClickHouse SQL. Pour transmettre une syntaxe spécifique à SQLite que ClickHouse n&#39;analyse pas, utilisez la forme `query('...')`, dont le texte est envoyé tel quel à SQLite.

Tout `WHERE`, `LIMIT`, toute agrégation, etc. externe de la requête ClickHouse englobante n&#39;est **pas** poussé vers le bas dans la requête transmise — il est appliqué dans ClickHouse après récupération de l&#39;intégralité du résultat de la requête. Pour restreindre les données lues depuis SQLite, placez le filtre dans la requête transmise. Avec [`external_table_strict_query = 1`](/fr/operations/settings/settings#external_table_strict_query), un filtre externe qui ne peut pas être poussé vers le bas est rejeté avec une exception au lieu d&#39;être appliqué localement.
:::

<div id="data-types-support">
  ## Prise en charge des types de données
</div>

Lorsque vous spécifiez explicitement les types de colonnes ClickHouse dans la définition de la table, les types ClickHouse suivants peuvent être interprétés à partir de colonnes SQLite TEXT :

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* Tous les types entiers ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

Voir le [moteur de base de données SQLite](../../../engines/database-engines/sqlite.md#data_types-support) pour la correspondance de types par défaut.

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

Affiche une requête qui crée la table SQLite :

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Renvoie les données de la table :

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**Voir aussi**

* moteur [SQLite](../../../engines/database-engines/sqlite.md)
* fonction de table [sqlite](../../../sql-reference/table-functions/sqlite.md)