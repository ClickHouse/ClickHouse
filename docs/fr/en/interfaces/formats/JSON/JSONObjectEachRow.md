---
alias: []
description: 'Documentation sur le format JSONObjectEachRow'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Dans ce format, toutes les données sont représentées sous la forme d’un unique objet JSON, chaque ligne correspondant à un champ distinct de cet objet, à l’instar du format [`JSONEachRow`](./JSONEachRow.md).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="basic-example">
  ### Exemple simple
</div>

Soit le JSON suivant :

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

Pour utiliser un nom d’objet comme valeur de colonne, vous pouvez utiliser le paramètre spécial [`format_json_object_each_row_column_for_object_name`](/fr/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name).
La valeur de ce paramètre correspond au nom d’une colonne, utilisée comme clé JSON pour une ligne dans l’objet obtenu.

<div id="output">
  #### Sortie
</div>

Supposons que nous ayons la table `test` avec deux colonnes :

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Affichons-le au format `JSONObjectEachRow` et utilisons le paramètre `format_json_object_each_row_column_for_object_name` :

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### Entrée
</div>

Supposons que nous ayons stocké la sortie de l’exemple précédent dans un fichier nommé `data.json` :

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Cela fonctionne également pour l’inférence de schéma :

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### Insertion de données
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse permet :

* Les paires clé-valeur dans l’objet peuvent être dans n’importe quel ordre.
* D’omettre certaines valeurs.

ClickHouse ignore les espaces entre les éléments et les virgules après les objets. Vous pouvez placer tous les objets sur une seule ligne. Il n’est pas nécessaire de les séparer par des sauts de ligne.

<div id="omitted-values-processing">
  #### Traitement des valeurs omises
</div>

ClickHouse remplace les valeurs omises par les valeurs par défaut des [types de données](/fr/sql-reference/data-types/index.md) correspondants.

Si `DEFAULT expr` est spécifié, ClickHouse applique des règles de substitution différentes selon le paramètre [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/fr/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields).

Considérez la table suivante :

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* Si `input_format_defaults_for_omitted_fields = 0`, la valeur par défaut de `x` et de `a` est `0` (qui est la valeur par défaut du type de données `UInt32`).
* Si `input_format_defaults_for_omitted_fields = 1`, la valeur par défaut de `x` est `0`, mais celle de `a` est `x * 2`.

:::note
Lors de l’insertion de données avec `input_format_defaults_for_omitted_fields = 1`, ClickHouse consomme davantage de ressources de calcul que lors d’une insertion avec `input_format_defaults_for_omitted_fields = 0`.
:::

<div id="json-selecting-data">
  ### Sélection de données
</div>

Prenons la table `UserActivity` comme exemple :

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La requête `SELECT * FROM UserActivity FORMAT JSONEachRow` renvoie :

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Contrairement au format [JSON](/fr/interfaces/formats/JSON), il n’y a aucune substitution des séquences UTF-8 non valides. Les valeurs sont échappées de la même manière qu’en `JSON`.

:::info
N’importe quelle suite d’octets peut être renvoyée dans les chaînes. Utilisez le format [`JSONEachRow`](./JSONEachRow.md) si vous êtes certain que les données de la table peuvent être mises en forme en JSON sans perte d’information.
:::

<div id="jsoneachrow-nested">
  ### Utilisation des structures Nested
</div>

Si vous avez une table avec des colonnes du type de données [`Nested`](/fr/sql-reference/data-types/nested-data-structures/index.md), vous pouvez insérer des données JSON ayant la même structure. Activez cette fonctionnalité à l’aide du paramètre [input&#95;format&#95;import&#95;nested&#95;json](/fr/operations/settings/settings-formats.md/#input_format_import_nested_json).

Par exemple, prenez la table suivante :

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Comme vous pouvez le voir dans la description du type de données `Nested`, ClickHouse traite chaque composant de la structure imbriquée comme une colonne distincte (`n.s` et `n.i` dans notre table). Vous pouvez insérer des données de la manière suivante :

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Pour insérer des données sous forme d’objet JSON hiérarchique, définissez [`input_format_import_nested_json=1`](/fr/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

En l’absence de ce paramètre, ClickHouse lève une exception.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                                                                                                                                    | Description                                                                                                                                                                                            | Par défaut | Notes                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`input_format_import_nested_json`](/fr/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | faire correspondre des données JSON imbriquées à des tables imbriquées (fonctionne avec le format JSONEachRow).                                                                                        | `false`    |                                                                                                                                                                                                        |
| [`input_format_json_read_bools_as_numbers`](/fr/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | autoriser l’analyse des booléens comme des nombres dans les formats d’entrée JSON.                                                                                                                     | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_read_bools_as_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | autorise l&#39;interprétation des booléens sous forme de chaînes dans les formats d&#39;entrée JSON.                                                                                                   | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_read_numbers_as_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | autorise l&#39;interprétation des nombres sous forme de chaînes dans les formats d&#39;entrée JSON.                                                                                                    | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_read_arrays_as_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | autorise l&#39;interprétation des tableaux JSON sous forme de chaînes dans les formats d&#39;entrée JSON.                                                                                              | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_read_objects_as_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | permet d’analyser les objets JSON comme des chaînes dans les formats d’entrée JSON.                                                                                                                    | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_named_tuples_as_objects`](/fr/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | analyse les colonnes de tuples nommés comme des objets JSON.                                                                                                                                           | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_try_infer_numbers_from_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | essaie d’inférer des nombres à partir de champs de type chaîne lors de l’inférence de schéma.                                                                                                          | `false`    |                                                                                                                                                                                                        |
| [`input_format_json_try_infer_named_tuples_from_objects`](/fr/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | essayer d’inférer un tuple nommé à partir d’objets JSON lors de l’inférence du schéma.                                                                                                                 | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_infer_incomplete_types_as_strings`](/fr/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | utiliser le type String pour les clés qui ne contiennent que des NULL ou des objets/tableaux vides lors de l’inférence du schéma dans les formats d’entrée JSON.                                       | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/fr/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | insérer des valeurs par défaut pour les éléments manquants dans un objet JSON lors de l’analyse d’un tuple nommé.                                                                                      | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/fr/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | ignorer les clés inconnues dans l’objet JSON pour les tuples nommés.                                                                                                                                   | `false`    |                                                                                                                                                                                                        |
| [`input_format_json_compact_allow_variable_number_of_columns`](/fr/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | autoriser un nombre variable de colonnes au format JSONCompact/JSONCompactEachRow, ignorer les colonnes supplémentaires et utiliser les valeurs par défaut pour les colonnes manquantes.               | `false`    |                                                                                                                                                                                                        |
| [`input_format_json_throw_on_bad_escape_sequence`](/fr/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | lever une exception si la chaîne JSON contient une séquence d’échappement invalide. Si cette option est désactivée, les séquences d’échappement invalides resteront telles quelles dans les données.   | `true`     |                                                                                                                                                                                                        |
| [`input_format_json_empty_as_default`](/fr/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | traite les champs vides de l’entrée JSON comme des valeurs par défaut.                                                                                                                                 | `false`.   | Pour les expressions par défaut complexes, il faut également activer [`input_format_defaults_for_omitted_fields`](/fr/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [`output_format_json_quote_64bit_integers`](/fr/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | contrôle la mise entre guillemets des entiers 64 bits dans le format de sortie JSON.                                                                                                                   | `true`     |                                                                                                                                                                                                        |
| [`output_format_json_quote_64bit_floats`](/fr/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | contrôle la mise entre guillemets des nombres à virgule flottante 64 bits dans le format de sortie JSON.                                                                                               | `false`    |                                                                                                                                                                                                        |
| [`output_format_json_quote_denormals`](/fr/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | active les sorties &#39;+nan&#39;, &#39;-nan&#39;, &#39;+inf&#39;, &#39;-inf&#39; au format de sortie JSON.                                                                                            | `false`    |                                                                                                                                                                                                        |
| [`output_format_json_quote_decimals`](/fr/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | contrôle la mise entre guillemets des nombres décimaux au format de sortie JSON.                                                                                                                       | `false`    |                                                                                                                                                                                                        |
| [`output_format_json_escape_forward_slashes`](/fr/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | contrôle l’échappement des barres obliques dans les sorties de chaînes au format de sortie JSON.                                                                                                       | `true`     |                                                                                                                                                                                                        |
| [`output_format_json_named_tuples_as_objects`](/fr/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | sérialise les colonnes de tuples nommés en objets JSON.                                                                                                                                                | `true`     |                                                                                                                                                                                                        |
| [`output_format_json_array_of_rows`](/fr/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | produit un tableau JSON de toutes les lignes au format JSONEachRow(Compact).                                                                                                                           | `false`    |                                                                                                                                                                                                        |
| [`output_format_json_validate_utf8`](/fr/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | active la validation des séquences UTF-8 dans les formats de sortie JSON (notez que cela n’a pas d’incidence sur les formats JSON/JSONCompact/JSONColumnsWithMetadata, qui valident toujours l’UTF-8). | `false`    |                                                                                                                                                                                                        |