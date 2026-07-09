---
description: 'Documentation du moteur de table StripeLog'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'Moteur de table StripeLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # Moteur de table StripeLog
</div>

<CloudNotSupportedBadge />

Ce moteur appartient à la famille des log engines. Consultez les propriétés communes des log engines ainsi que leurs différences dans l’article [Famille des moteurs Log](../../../engines/table-engines/log-family/index.md).

Utilisez ce moteur lorsque vous devez écrire dans un grand nombre de tables contenant peu de données (moins d’un million de lignes). Par exemple, cette table peut servir à stocker des batches de données entrants en vue de leur transformation lorsqu’un traitement atomique est nécessaire. 100k instances de ce type de table sont possibles sur un serveur ClickHouse. Ce moteur de table est à privilégier par rapport à [Log](./log.md) lorsqu’un grand nombre de tables est requis. En contrepartie, les performances en lecture sont moindres.

<div id="table_engines-stripelog-creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Consultez la description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

<div id="table_engines-stripelog-writing-the-data">
  ## Écriture des données
</div>

Le moteur `StripeLog` stocke toutes les colonnes dans un seul fichier. Pour chaque requête `INSERT`, ClickHouse ajoute le bloc de données à la fin du fichier de la table, en écrivant les colonnes une à une.

Pour chaque table, ClickHouse écrit les fichiers suivants :

* `data.bin` — Fichier de données.
* `index.mrk` — Fichier contenant les marques. Les marques contiennent les décalages de chaque colonne de chaque bloc de données inséré.

Le moteur `StripeLog` ne prend pas en charge les opérations `ALTER UPDATE` et `ALTER DELETE`.

<div id="table_engines-stripelog-reading-the-data">
  ## Lecture des données
</div>

Le fichier de marques permet à ClickHouse de paralléliser la lecture des données. Cela signifie qu&#39;une requête `SELECT` renvoie les lignes dans un ordre non déterminé. Utilisez la clause `ORDER BY` pour trier les lignes.

<div id="table_engines-stripelog-example-of-use">
  ## Exemple d’utilisation
</div>

Création d’une table :

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Insertion de données :

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Nous avons utilisé deux requêtes `INSERT` pour créer deux blocs de données dans le fichier `data.bin`.

ClickHouse utilise plusieurs threads pour lire les données. Chaque thread lit un bloc de données distinct et renvoie les lignes obtenues indépendamment dès qu’il a terminé. Par conséquent, dans la plupart des cas, l’ordre des blocs de lignes en sortie ne correspond pas à l’ordre de ces mêmes blocs en entrée. Par exemple :

```sql
SELECT * FROM stripe_log_table
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
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```