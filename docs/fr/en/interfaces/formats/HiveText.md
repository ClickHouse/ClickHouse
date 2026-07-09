---
alias: []
description: 'Documentation sur le format HiveText'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'référence'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

`HiveText` lit le format de sérialisation texte utilisé par les tables [Apache Hive](https://hive.apache.org/)
(format produit par le `LazySimpleSerDe` de Hive). Il s&#39;agit d&#39;un format texte délimité,
similaire à [`CSV`](/fr/interfaces/formats/CSV), dans lequel les champs sont
séparés par le délimiteur Hive `\x01` (Ctrl-A) par défaut. Le délimiteur de champ est
configurable via [`input_format_hive_text_fields_delimiter`](#format-settings).

`HiveText` est un format d&#39;entrée uniquement. Les données n&#39;ont pas de ligne d&#39;en-tête : les valeurs sont
associées par position aux colonnes de la table de destination, de sorte que les noms et types de colonnes
sont pris de la table (ou d&#39;une structure fournie explicitement)
plutôt que déduits des données. Lors de la lecture, ClickHouse analyse les
dates et heures en mode best-effort (voir [`date_time_input_format`](/fr/operations/settings/formats#date_time_input_format)),
remplit les champs de fin omis avec les valeurs par défaut des colonnes et ignore les champs qu&#39;il ne
reconnaît pas.

À l&#39;intérieur d&#39;un champ, les valeurs sont analysées selon les mêmes règles d&#39;échappement que `CSV`,
et non avec les délimiteurs imbriqués de Hive. En particulier, une colonne de type
[`Array`](/fr/sql-reference/data-types/array) est lue à partir de la
représentation entre crochets (par exemple, `"['a','b','c']"`), et non à partir de valeurs séparées par
le délimiteur de collection Hive `\x02`.

:::note Les paramètres de délimiteur imbriqué n&#39;ont aucun effet
Les paramètres [`input_format_hive_text_collection_items_delimiter`](#format-settings) et
[`input_format_hive_text_map_keys_delimiter`](#format-settings) sont
acceptés pour des raisons de compatibilité, mais ne sont actuellement pas utilisés lors de l&#39;analyse syntaxique.
:::

Par défaut, les lignes peuvent comporter un nombre variable de champs (voir
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)) :
les lignes contenant moins de champs que la table voient les colonnes manquantes remplies avec
des valeurs par défaut, et les lignes comportant des champs de fin supplémentaires voient ces champs ignorés.

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

Les exemples ci-dessous redéfinissent le délimiteur de champ par défaut en utilisant une virgule (`,`) via
[`input_format_hive_text_fields_delimiter`](#format-settings), afin de rendre les fichiers d&#39;entrée
plus faciles à lire.

<div id="reading-data">
  ### Lecture d’un fichier HiveText
</div>

Soit un fichier `hive_data.txt` avec des champs séparés par des virgules :

```text title="hive_data.txt"
1,3
3,5,9
```

Nous créons une table qui définit les noms et les types des colonnes, puis nous y insérons le fichier
avec `FORMAT HiveText` :

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

Notez que la première ligne, `1,3`, ne comporte que deux champs, donc la colonne manquante `c`
est remplie par sa valeur par défaut `0`.

<div id="variable-number-of-columns">
  ### Nombre variable de colonnes
</div>

Avec le paramètre par défaut `input_format_hive_text_allow_variable_number_of_columns = 1`,
les lignes qui comportent plus de champs que de colonnes dans la table voient simplement les champs
supplémentaires en fin de ligne ignorés :

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

Définir plutôt `input_format_hive_text_allow_variable_number_of_columns = 0`
impose un nombre strict de champs, et une ligne comportant moins de champs que la table
déclenche une exception d’analyse syntaxique.

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                 | Description                                                                                                                                                               | Par défaut |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `input_format_hive_text_fields_delimiter`                 | Délimiteur entre les champs dans Hive Text File                                                                                                                           | `\x01`     |
| `input_format_hive_text_collection_items_delimiter`       | Délimiteur entre les éléments d’une collection (Array ou Map) dans Hive Text File. Accepté, mais actuellement non utilisé lors de l’analyse syntaxique.                   | `\x02`     |
| `input_format_hive_text_map_keys_delimiter`               | Délimiteur entre une paire clé/valeur d’une Map dans Hive Text File. Accepté, mais actuellement non utilisé lors de l’analyse syntaxique.                                 | `\x03`     |
| `input_format_hive_text_allow_variable_number_of_columns` | Ignore les colonnes supplémentaires dans l’entrée Hive Text (si le fichier contient plus de colonnes que prévu) et traite les champs absents comme des valeurs par défaut | `1`        |