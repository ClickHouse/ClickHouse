---
description: 'Permet de se connecter à une base de données SQLite et d’effectuer des requêtes `INSERT` et `SELECT`
  pour échanger des données entre ClickHouse et SQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

Permet de se connecter à une base de données [SQLite](https://www.sqlite.org/index.html) et d’effectuer des requêtes `INSERT` et `SELECT` pour échanger des données entre ClickHouse et SQLite.

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Paramètres du moteur**

* `db_path` — Chemin d’accès à un fichier contenant une base de données SQLite.

<div id="data_types-support">
  ## Prise en charge des types de données
</div>

Le tableau ci-dessous montre la correspondance de types par défaut lorsque ClickHouse déduit automatiquement le schéma à partir de SQLite :

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

Lorsque vous définissez explicitement une table avec des types ClickHouse spécifiques à l&#39;aide du [moteur de table SQLite](../../engines/table-engines/integrations/sqlite.md), les types ClickHouse suivants peuvent être interprétés à partir de colonnes SQLite TEXT :

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* Tous les types entiers ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite utilise un typage dynamique, et ses fonctions d&#39;accès aux types effectuent automatiquement des conversions de type. Par exemple, la lecture d&#39;une colonne TEXT comme un entier renverra 0 si le texte ne peut pas être interprété comme un nombre. Cela signifie que si une table ClickHouse est définie avec un type différent de celui de la colonne SQLite sous-jacente, les valeurs peuvent être converties silencieusement au lieu de provoquer une erreur.

<div id="specifics-and-recommendations">
  ## Spécificités et recommandations
</div>

SQLite stocke l’intégralité de la base de données (définitions, tables, index et données proprement dites) dans un seul fichier multiplateforme sur une machine hôte. Lors de l’écriture, SQLite verrouille l’ensemble du fichier de base de données ; les opérations d’écriture sont donc effectuées de manière séquentielle. Les opérations de lecture peuvent, elles, être exécutées en parallèle.
SQLite ne nécessite ni gestion de service (comme des scripts de démarrage) ni contrôle d’accès reposant sur `GRANT` et des mots de passe. Le contrôle d’accès est assuré au moyen des permissions du système de fichiers appliquées au fichier de base de données lui-même.

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

Base de données dans ClickHouse, connectée à SQLite :

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Affiche les tables :

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

Insertion de données dans une table SQLite à partir d’une table ClickHouse :

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```