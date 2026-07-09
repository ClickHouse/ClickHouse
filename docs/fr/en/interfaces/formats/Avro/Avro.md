---
alias: []
description: 'Documentation sur le format Avro'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

[Apache Avro](https://avro.apache.org/) est un format de sérialisation orienté ligne qui utilise un codage binaire pour traiter les données efficacement. Le format `Avro` prend en charge la lecture et l’écriture de [fichiers de données Avro](https://avro.apache.org/docs/current/specification/#object-container-files). Ce format suppose des messages auto-descriptifs intégrant un schéma. Si vous utilisez Avro avec un registre de schémas, reportez-vous au format [`AvroConfluent`](./AvroConfluent.md).

<div id="data-type-mapping">
  ## Correspondance des types de données
</div>

<DataTypeMapping />

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                  | Description                                                                                                                                                                                                  | Valeur par défaut |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------- |
| `input_format_avro_allow_missing_fields`   | Indique s’il faut utiliser une valeur par défaut au lieu de renvoyer une erreur lorsqu’un champ est introuvable dans le schéma.                                                                              | `0`               |
| `input_format_avro_null_as_default`        | Indique s’il faut utiliser une valeur par défaut au lieu de renvoyer une erreur lors de l’insertion d’une valeur `null` dans une colonne non nullable.                                                       | `0`               |
| `output_format_avro_codec`                 | Algorithme de compression des fichiers de sortie Avro. Valeurs possibles : `null`, `deflate`, `snappy`, `zstd`.                                                                                              |                   |
| `output_format_avro_sync_interval`         | Fréquence des marqueurs de synchronisation dans les fichiers Avro (en octets).                                                                                                                               | `16384`           |
| `output_format_avro_string_column_pattern` | Expression régulière permettant d’identifier les colonnes `String` pour le mappage vers le type de chaîne Avro. Par défaut, les colonnes ClickHouse de type `String` sont écrites avec le type Avro `bytes`. |                   |
| `output_format_avro_rows_in_file`          | Nombre maximal de lignes par fichier de sortie Avro. Lorsque cette limite est atteinte, un nouveau fichier est créé (si le système de stockage prend en charge le fractionnement des fichiers).              | `1`               |

<div id="examples">
  ## Exemples
</div>

<div id="reading-avro-data">
  ### Lecture de données Avro
</div>

Pour lire des données à partir d’un fichier Avro dans une table ClickHouse :

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

Le schéma racine du fichier Avro importé doit être de type `record`.

Pour établir la correspondance entre les colonnes de la table et les champs du schéma Avro, ClickHouse compare leurs noms.
Cette comparaison est sensible à la casse, et les champs inutilisés sont ignorés.

Les types de données des colonnes de la table ClickHouse peuvent différer de ceux des champs correspondants de la donnée Avro insérée. Lors de l&#39;insertion des données, ClickHouse interprète les types de données conformément au tableau ci-dessus, puis [convertit](/fr/sql-reference/functions/type-conversion-functions#CAST) les données dans le type de colonne correspondant.

Lors de l&#39;importation des données, si un champ est introuvable dans le schéma et que le paramètre [`input_format_avro_allow_missing_fields`](/fr/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) est activé, la valeur par défaut est utilisée au lieu de générer une erreur.

<div id="writing-avro-data">
  ### Écriture de données Avro
</div>

Pour écrire des données d’une table ClickHouse dans un fichier Avro :

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Les noms de colonnes doivent :

* Commencer par `[A-Za-z_]`
* Être suivis uniquement de caractères `[A-Za-z0-9_]`

La compression de sortie et l’intervalle de synchronisation des fichiers Avro peuvent être configurés à l’aide des paramètres [`output_format_avro_codec`](/fr/operations/settings/settings-formats.md/#output_format_avro_codec) et [`output_format_avro_sync_interval`](/fr/operations/settings/settings-formats.md/#output_format_avro_sync_interval), respectivement.

<div id="inferring-the-avro-schema">
  ### Déduire le schéma Avro
</div>

À l’aide de la fonction ClickHouse [`DESCRIBE`](/fr/sql-reference/statements/describe-table), vous pouvez afficher rapidement le format déduit d’un fichier Avro, comme dans l’exemple suivant.
Cet exemple inclut l’URL d’un fichier Avro accessible au public dans le bucket public S3 de ClickHouse :

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```