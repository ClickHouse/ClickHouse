---
description: 'Documentation sur le moteur de table TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'Moteur de table TinyLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # Moteur de table TinyLog
</div>

<CloudNotSupportedBadge />

Ce moteur appartient à la famille des moteurs Log. Consultez [la famille de moteurs Log](../../../engines/table-engines/log-family/index.md) pour connaître les propriétés communes des moteurs Log ainsi que leurs différences.

Ce moteur de table est généralement utilisé selon la méthode d’écriture unique : écrire les données une seule fois, puis les lire autant de fois que nécessaire. Par exemple, vous pouvez utiliser des tables de type `TinyLog` pour des données intermédiaires traitées par petits lots. Notez qu’il est inefficace de stocker des données dans un grand nombre de petites tables.

Les requêtes sont exécutées dans un seul flux. En d’autres termes, ce moteur est destiné à des tables relativement petites (jusqu’à environ 1 000 000 de lignes). Il est judicieux d’utiliser ce moteur de table si vous avez de nombreuses petites tables, car il est plus simple que le moteur [Log](../../../engines/table-engines/log-family/log.md) (moins de fichiers doivent être ouverts).

<div id="characteristics">
  ## Caractéristiques
</div>

* **Structure plus simple** : Contrairement au moteur Log, TinyLog n&#39;utilise pas de fichiers de marks. Cela réduit la complexité, mais limite aussi les optimisations de performances pour les grands ensembles de données.
* **Requêtes en flux unique** : Les requêtes sur les tables TinyLog sont exécutées dans un seul flux, ce qui le rend adapté à des tables relativement petites, généralement jusqu&#39;à 1 000 000 de lignes.
* **Efficace pour les petites tables** : La simplicité du moteur TinyLog en fait un bon choix pour gérer de nombreuses petites tables, car il nécessite moins d&#39;opérations sur les fichiers que le moteur Log.

Contrairement au moteur Log, TinyLog n&#39;utilise pas de fichiers de marks. Cela réduit la complexité, mais limite aussi les optimisations de performances pour les ensembles de données plus volumineux.

<div id="table_engines-tinylog-creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

Consultez la description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

<div id="table_engines-tinylog-writing-the-data">
  ## Écriture des données
</div>

Le moteur `TinyLog` stocke toutes les colonnes dans un seul fichier. Pour chaque requête `INSERT`, ClickHouse ajoute le bloc de données à la fin du fichier de la table, en écrivant les colonnes une par une.

Pour chaque table, ClickHouse écrit les fichiers suivants :

* `<column>.bin` : un fichier de données pour chaque colonne, contenant les données sérialisées et compressées.

Le moteur `TinyLog` ne prend pas en charge les opérations `ALTER UPDATE` et `ALTER DELETE`.

<div id="table_engines-tinylog-example-of-use">
  ## Exemple d’utilisation
</div>

Création d’une table :

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

Insertion de données :

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Nous avons utilisé deux requêtes `INSERT` pour créer deux blocs de données dans les fichiers `<column>.bin`.

ClickHouse utilise un seul flux pour sélectionner les données. Par conséquent, l’ordre des blocs de lignes dans le résultat correspond à l’ordre de ces mêmes blocs en entrée. Par exemple :

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```