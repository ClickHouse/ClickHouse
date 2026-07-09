---
alias: []
description: 'Documentation du format CSV'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'référence'
---

<div id="description">
  ## Description
</div>

Format CSV ([RFC](https://tools.ietf.org/html/rfc4180)).
Lors du formatage, les lignes sont placées entre guillemets doubles. Un guillemet double à l&#39;intérieur d&#39;une chaîne est représenté par deux guillemets doubles consécutifs.
Il n&#39;y a pas d&#39;autres règles d&#39;échappement des caractères.

* Les dates et les dates-heures sont placées entre guillemets doubles.
* Les nombres sont affichés sans guillemets.
* Les valeurs sont séparées par un caractère délimiteur, qui est `,` par défaut. Ce caractère est défini par le paramètre [format&#95;csv&#95;delimiter](/fr/operations/settings/settings-formats.md/#format_csv_delimiter).
* Les lignes sont séparées par le saut de ligne Unix (LF).
* Les tableaux sont sérialisés en CSV comme suit :
  * d&#39;abord, le tableau est sérialisé en chaîne, comme dans le format TabSeparated
  * la chaîne obtenue est ensuite écrite en CSV entre guillemets doubles.
* Les tuples au format CSV sont sérialisés en colonnes distinctes (c&#39;est-à-dire que leur imbrication dans le tuple est perdue).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
Par défaut, le délimiteur est `,`
Consultez le paramètre [format&#95;csv&#95;delimiter](/fr/operations/settings/settings-formats.md/#format_csv_delimiter) pour plus d’informations.
:::

Lors de l’analyse, toutes les valeurs peuvent être analysées avec ou sans guillemets. Les guillemets doubles comme simples sont pris en charge.

Les lignes peuvent également être présentées sans guillemets. Dans ce cas, elles sont analysées jusqu’au caractère délimiteur ou au saut de ligne (CR ou LF).
Cependant, contrairement à la RFC, lors de l’analyse de lignes sans guillemets, les espaces et tabulations en début et en fin de ligne sont ignorés.
Les types de fin de ligne pris en charge sont les suivants : Unix (LF), Windows (CR LF) et Mac OS Classic (CR LF).

`NULL` est formaté selon le paramètre [format&#95;csv&#95;null&#95;representation](/fr/operations/settings/settings-formats.md/#format_csv_null_representation) (la valeur par défaut est `\N`).

Dans les données d’entrée, les valeurs `ENUM` peuvent être représentées sous forme de noms ou d’identifiants.
Nous essayons d’abord de faire correspondre la valeur d’entrée au nom de l’`ENUM`.
En cas d’échec, si la valeur d’entrée est un nombre, nous essayons de faire correspondre ce nombre à l’identifiant de l’`ENUM`.
Si les données d’entrée ne contiennent que des identifiants d’`ENUM`, il est recommandé d’activer le paramètre [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/fr/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) afin d’optimiser l’analyse des `ENUM`.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                                                                                                                                                | Description                                                                                                                                                           | Par défaut | Notes                                                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/fr/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | le caractère considéré comme délimiteur dans les données CSV.                                                                                                         | `,`        |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/fr/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | autoriser les chaînes entre apostrophes.                                                                                                                              | `true`     |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/fr/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | autoriser les chaînes entre guillemets doubles.                                                                                                                       | `true`     |                                                                                                                                                                                                                           |
| [format&#95;csv&#95;null&#95;representation](/fr/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | représentation personnalisée de NULL au format CSV.                                                                                                                   | `\N`       |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/fr/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | traiter les champs vides dans l&#39;entrée CSV comme des valeurs par défaut.                                                                                          | `true`     | Pour les expressions par défaut complexes, [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/fr/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) doit également être activé. |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/fr/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | traiter les valeurs enum insérées dans les formats CSV comme des indices d&#39;enum.                                                                                  | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/fr/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | utiliser certains ajustements et heuristiques pour inférer le schéma au format CSV. Si cette option est désactivée, tous les champs seront inférés comme des Strings. | `true`     |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/fr/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | lors de la lecture d&#39;un Array depuis un CSV, s&#39;attendre à ce que ses éléments aient été sérialisés en CSV imbriqué puis placés dans une chaîne.               | `false`    |                                                                                                                                                                                                                           |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/fr/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | si cette option est définie sur true, la fin de ligne du output format CSV sera `\r\n` au lieu de `\n`.                                                               | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/fr/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | ignorer le nombre de lignes spécifié au début des données.                                                                                                            | `0`        |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;detect&#95;header](/fr/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | détecter automatiquement l&#39;en-tête avec les noms et les types au format CSV.                                                                                      | `true`     |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/fr/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | ignorer les lignes vides de fin de fichier.                                                                                                                           | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/fr/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | supprimer les espaces et les tabulations dans les chaînes CSV non entourées de guillemets.                                                                            | `true`     |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/fr/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | autoriser l&#39;utilisation d&#39;espaces ou de tabulations comme délimiteurs de champs dans les chaînes CSV.                                                         | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/fr/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | autoriser un nombre variable de colonnes au format CSV, ignorer les colonnes supplémentaires et utiliser des valeurs par défaut pour les colonnes manquantes.         | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/fr/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | autoriser la définition d&#39;une valeur par défaut pour une colonne lorsque la désérialisation d&#39;un champ CSV échoue à cause d&#39;une valeur invalide.          | `false`    |                                                                                                                                                                                                                           |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/fr/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | essayer d&#39;inférer les nombres à partir des champs de type chaîne lors de l&#39;inférence de schéma.                                                               | `false`    |                                                                                                                                                                                                                           |