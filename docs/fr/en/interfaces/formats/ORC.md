---
alias: []
description: 'Documentation sur le format ORC'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

[Apache ORC](https://orc.apache.org/) est un format de stockage en colonnes largement utilisé dans l’écosystème [Hadoop](https://hadoop.apache.org/).

<div id="data-types-matching-orc">
  ## Correspondance des types de données
</div>

Le tableau ci-dessous compare les types de données ORC pris en charge et les [types de données](/fr/sql-reference/data-types/index.md) ClickHouse correspondants dans les requêtes `INSERT` et `SELECT`.

| Type de données ORC (`INSERT`)        | Type de données ClickHouse                                                                        | Type de données ORC (`SELECT`) |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------ |
| `Boolean`                             | [UInt8](/fr/sql-reference/data-types/int-uint.md)                                                    | `Boolean`                      |
| `Tinyint`                             | [Int8/UInt8](/fr/sql-reference/data-types/int-uint.md)/[Enum8](/fr/sql-reference/data-types/enum.md)    | `Tinyint`                      |
| `Smallint`                            | [Int16/UInt16](/fr/sql-reference/data-types/int-uint.md)/[Enum16](/fr/sql-reference/data-types/enum.md) | `Smallint`                     |
| `Int`                                 | [Int32/UInt32](/fr/sql-reference/data-types/int-uint.md)                                             | `Int`                          |
| `Bigint`                              | [Int64/UInt32](/fr/sql-reference/data-types/int-uint.md)                                             | `Bigint`                       |
| `Float`                               | [Float32](/fr/sql-reference/data-types/float.md)                                                     | `Float`                        |
| `Double`                              | [Float64](/fr/sql-reference/data-types/float.md)                                                     | `Double`                       |
| `Decimal`                             | [Decimal](/fr/sql-reference/data-types/decimal.md)                                                   | `Decimal`                      |
| `Date`                                | [Date32](/fr/sql-reference/data-types/date32.md)                                                     | `Date`                         |
| `Timestamp`                           | [DateTime64](/fr/sql-reference/data-types/datetime64.md)                                             | `Timestamp`                    |
| `String`, `Char`, `Varchar`, `Binary` | [String](/fr/sql-reference/data-types/string.md)                                                     | `Binary`                       |
| `List`                                | [Array](/fr/sql-reference/data-types/array.md)                                                       | `List`                         |
| `Struct`                              | [Tuple](/fr/sql-reference/data-types/tuple.md)                                                       | `Struct`                       |
| `Map`                                 | [Map](/fr/sql-reference/data-types/map.md)                                                           | `Map`                          |
| `Int`                                 | [IPv4](/fr/sql-reference/data-types/int-uint.md)                                                     | `Int`                          |
| `Binary`                              | [IPv6](/fr/sql-reference/data-types/ipv6.md)                                                         | `Binary`                       |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/fr/sql-reference/data-types/int-uint.md)                            | `Binary`                       |
| `Binary`                              | [Decimal256](/fr/sql-reference/data-types/decimal.md)                                                | `Binary`                       |

* Les autres types ne sont pas pris en charge.
* Les `Array` peuvent être imbriqués et accepter une valeur de type `Nullable` comme argument. Les types `Tuple` et `Map` peuvent également être imbriqués.
* Les types de données des colonnes d&#39;une table ClickHouse ne doivent pas nécessairement correspondre aux champs de données ORC associés. Lors de l&#39;insertion des données, ClickHouse interprète les types de données conformément au tableau ci-dessus, puis [effectue un transtypage](/fr/sql-reference/functions/type-conversion-functions#CAST) des données vers le type défini pour la colonne de la table ClickHouse.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="inserting-data">
  ### Insertion de données
</div>

À l’aide d’un fichier ORC nommé `football.orc` et contenant les données suivantes :

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insérez les données :

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### Lire des données
</div>

Lisez des données au format `ORC` :

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC est un format binaire qui ne s’affiche pas de manière lisible dans un terminal. Utilisez `INTO OUTFILE` pour écrire des fichiers ORC.
:::

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                                                                                                                                                            | Description                                                                                                                      | Par défaut |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| [`output_format_arrow_string_as_string`](/fr/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Utiliser le type String d’Arrow au lieu de Binary pour les colonnes de type String.                                              | `false`    |
| [`output_format_orc_compression_method`](/fr/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Méthode de compression utilisée pour le format ORC de sortie. Valeur par défaut                                                  | `none`     |
| [`input_format_arrow_case_insensitive_column_matching`](/fr/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Ignorer la casse lors de la correspondance entre les colonnes Arrow et les colonnes ClickHouse.                                  | `false`    |
| [`input_format_arrow_allow_missing_columns`](/fr/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Autoriser les colonnes manquantes lors de la lecture des données Arrow.                                                          | `false`    |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/fr/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Autoriser l’ignorance des colonnes dont les types ne sont pas pris en charge lors de l’inférence de schéma pour le format Arrow. | `false`    |

Pour échanger des données avec Hadoop, vous pouvez utiliser le [moteur de table HDFS](/fr/engines/table-engines/integrations/hdfs.md).