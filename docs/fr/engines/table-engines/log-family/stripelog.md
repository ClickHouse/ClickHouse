---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: StripeLog
---

# Stripelog {#stripelog}

Ce moteur appartient à la famille des moteurs en rondins. Voir les propriétés communes des moteurs de journal et leurs différences dans le [Famille De Moteurs En Rondins](index.md) article.

Utilisez ce moteur dans des scénarios lorsque vous devez écrire de nombreuses tables avec une petite quantité de données (moins de 1 million de lignes).

## Création d'une Table {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Voir la description détaillée de la [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) requête.

## L'écriture des Données {#table_engines-stripelog-writing-the-data}

Le `StripeLog` moteur stocke toutes les colonnes dans un fichier. Pour chaque `INSERT` requête, ClickHouse ajoute le bloc de données à la fin d'un fichier de table, en écrivant des colonnes une par une.

Pour chaque table ClickHouse écrit les fichiers:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

Le `StripeLog` moteur ne prend pas en charge la `ALTER UPDATE` et `ALTER DELETE` opérations.

## La lecture des Données {#table_engines-stripelog-reading-the-data}

Le fichier avec des marques permet à ClickHouse de paralléliser la lecture des données. Cela signifie qu'une `SELECT` la requête renvoie des lignes dans un ordre imprévisible. L'utilisation de la `ORDER BY` clause pour trier les lignes.

## Exemple D'utilisation {#table_engines-stripelog-example-of-use}

Création d'une table:

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Insertion de données:

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Nous avons utilisé deux `INSERT` requêtes pour créer deux blocs de données `data.bin` fichier.

ClickHouse utilise plusieurs threads lors de la sélection des données. Chaque thread lit un bloc de données séparé et renvoie les lignes résultantes indépendamment à la fin. En conséquence, l'ordre des blocs de lignes dans le résultat ne correspond pas à l'ordre des mêmes blocs dans l'entrée, dans la plupart des cas. Exemple:

``` sql
SELECT * FROM stripe_log_table
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Trier les résultats (ordre croissant par défaut):

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
