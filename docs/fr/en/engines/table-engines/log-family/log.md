---
description: 'Documentation du moteur Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Moteur de table Log'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Moteur de table Log
</div>

<CloudNotSupportedBadge />

Ce moteur appartient à la famille des moteurs `Log`. Consultez l’article [Famille de moteurs Log](../../../engines/table-engines/log-family/index.md) pour connaître les propriétés communes des moteurs `Log` et leurs différences.

`Log` se distingue de [TinyLog](../../../engines/table-engines/log-family/tinylog.md) par la présence d’un petit fichier de « mark » aux côtés des fichiers de colonnes. Ces marks sont écrites pour chaque bloc de données et contiennent des offset indiquant où commencer la lecture du fichier afin d’ignorer le nombre spécifié de lignes. Cela permet de lire les données de la table à l’aide de plusieurs threads.
En cas d’accès concurrent aux données, les opérations de lecture peuvent être effectuées simultanément, tandis que les opérations d’écriture bloquent les lectures ainsi qu’entre elles.
Le moteur `Log` ne prend pas en charge les index. De même, si l’écriture dans une table échoue, la table est corrompue et sa lecture renvoie une erreur. Le moteur `Log` convient aux données temporaires, aux tables à écriture unique, ainsi qu’aux tests ou aux démonstrations.

<div id="table_engines-log-creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

Consultez la description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

<div id="table_engines-log-writing-the-data">
  ## Écriture des données
</div>

Le moteur `Log` stocke efficacement les données en écrivant chaque colonne dans un fichier distinct. Pour chaque table, le moteur `Log` écrit les fichiers suivants dans le chemin de stockage spécifié :

* `<column>.bin` : un fichier de données pour chaque colonne, contenant les données sérialisées et compressées.
  `__marks.mrk` : un fichier de marks, qui stocke les offsets et le nombre de lignes pour chaque bloc de données inséré. Les marks permettent d’optimiser l’exécution des requêtes en autorisant le moteur à ignorer les blocs de données non pertinents lors des lectures.

<div id="writing-process">
  ### Processus d’écriture
</div>

Lorsque des données sont écrites dans une table `Log` :

1. Les données sont sérialisées et compressées en blocs.
2. Pour chaque colonne, les données compressées sont ajoutées à la suite dans leur fichier `<column>.bin` respectif.
3. Les entrées correspondantes sont ajoutées au fichier `__marks.mrk` afin d’enregistrer le décalage et le nombre de lignes des données nouvellement insérées.

<div id="table_engines-log-reading-the-data">
  ## Lecture des données
</div>

Le fichier de marks permet à ClickHouse de paralléliser la lecture des données. Cela signifie qu&#39;une requête `SELECT` renvoie les lignes dans un ordre imprévisible. Utilisez la clause `ORDER BY` pour trier les lignes.

<div id="table_engines-log-example-of-use">
  ## Exemple d’utilisation
</div>

Création d’une table :

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

Insertion de données :

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Nous avons utilisé deux requêtes `INSERT` pour créer deux blocs de données dans les fichiers `<column>.bin`.

ClickHouse utilise plusieurs threads pour lire les données. Chaque thread lit un bloc de données distinct et renvoie indépendamment les lignes obtenues dès qu’il a terminé. Par conséquent, l’ordre des blocs de lignes en sortie peut ne pas correspondre à l’ordre de ces mêmes blocs en entrée. Par exemple :

```sql
SELECT * FROM log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Tri des résultats (ordre croissant par défaut) :

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```