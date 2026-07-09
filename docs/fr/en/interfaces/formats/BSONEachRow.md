---
alias: []
description: 'Documentation du format BSONEachRow'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `BSONEachRow` analyse les données comme une séquence de documents Binary JSON (BSON), sans aucun séparateur entre eux.
Chaque ligne est représentée sous la forme d’un document unique, et chaque colonne sous la forme d’un champ unique du document BSON, le nom de la colonne servant de clé.

<div id="data-types-matching">
  ## Correspondance des types de données
</div>

En sortie, la correspondance suivante est utilisée entre les types ClickHouse et les types BSON :

| Type ClickHouse                                                                                       | Type BSON                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| [Bool](/fr/sql-reference/data-types/boolean.md)                                                          | `\x08` booléen                                                                                                                             |
| [Int8/UInt8](/fr/sql-reference/data-types/int-uint.md)/[Enum8](/fr/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                               |
| [Int16/UInt16](/fr/sql-reference/data-types/int-uint.md)/[Enum16](/fr/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                               |
| [Int32](/fr/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                               |
| [UInt32](/fr/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                               |
| [Int64/UInt64](/fr/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                               |
| [Float32/Float64](/fr/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                              |
| [Date](/fr/sql-reference/data-types/date.md)/[Date32](/fr/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                               |
| [DateTime](/fr/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                               |
| [DateTime64](/fr/sql-reference/data-types/datetime64.md)                                                 | `\x09` datetime                                                                                                                            |
| [Decimal32](/fr/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                               |
| [Decimal64](/fr/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                               |
| [Decimal128](/fr/sql-reference/data-types/decimal.md)                                                    | `\x05` binaire, `\x00` sous-type binaire, taille = 16                                                                                      |
| [Decimal256](/fr/sql-reference/data-types/decimal.md)                                                    | `\x05` binaire, `\x00` sous-type binaire, taille = 32                                                                                      |
| [Int128/UInt128](/fr/sql-reference/data-types/int-uint.md)                                               | `\x05` binaire, `\x00` sous-type binaire, taille = 16                                                                                      |
| [Int256/UInt256](/fr/sql-reference/data-types/int-uint.md)                                               | `\x05` binaire, `\x00` sous-type binaire, taille = 32                                                                                      |
| [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md) | `\x05` binaire, `\x00` sous-type binaire ou \x02 chaîne si le paramètre output&#95;format&#95;bson&#95;string&#95;as&#95;string est activé |
| [UUID](/fr/sql-reference/data-types/uuid.md)                                                             | `\x05` binaire, `\x04` sous-type uuid, taille = 16                                                                                         |
| [Array](/fr/sql-reference/data-types/array.md)                                                           | `\x04` tableau                                                                                                                             |
| [Tuple](/fr/sql-reference/data-types/tuple.md)                                                           | `\x04` tableau                                                                                                                             |
| [Named Tuple](/fr/sql-reference/data-types/tuple.md)                                                     | `\x03` document                                                                                                                            |
| [Map](/fr/sql-reference/data-types/map.md)                                                               | `\x03` document                                                                                                                            |
| [IPv4](/fr/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                               |
| [IPv6](/fr/sql-reference/data-types/ipv6.md)                                                             | `\x05` binaire, `\x00` sous-type binaire                                                                                                   |

En entrée, la correspondance suivante est utilisée entre les types BSON et les types ClickHouse :

| Type BSON                                       | Type ClickHouse                                                                                                                                                                                     |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                                   | [Float32/Float64](/fr/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` chaîne                                   | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` document                                 | [Map](/fr/sql-reference/data-types/map.md)/[Named Tuple](/fr/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` tableau                                  | [Array](/fr/sql-reference/data-types/array.md)/[Tuple](/fr/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` binaire, `\x00` sous-type binaire        | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)/[IPv6](/fr/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` binaire, `\x02` ancien sous-type binaire | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` binaire, `\x03` ancien sous-type UUID    | [UUID](/fr/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` binaire, `\x04` sous-type UUID           | [UUID](/fr/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                                 | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` booléen                                  | [Bool](/fr/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` datetime                                 | [DateTime64](/fr/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` valeur NULL                              | [NULL](/fr/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` code JavaScript                          | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` symbole                                  | [String](/fr/sql-reference/data-types/string.md)/[FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                                    | [Int32/UInt32](/fr/sql-reference/data-types/int-uint.md)/[Decimal32](/fr/sql-reference/data-types/decimal.md)/[IPv4](/fr/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/fr/sql-reference/data-types/enum.md) |
| `\x12` int64                                    | [Int64/UInt64](/fr/sql-reference/data-types/int-uint.md)/[Decimal64](/fr/sql-reference/data-types/decimal.md)/[DateTime64](/fr/sql-reference/data-types/datetime64.md)                                       |

Les autres types BSON ne sont pas pris en charge. De plus, ce format convertit entre différents types d&#39;entiers.
Par exemple, il est possible d&#39;insérer une valeur BSON `int32` dans ClickHouse en tant que [`UInt8`](../../sql-reference/data-types/int-uint.md).

Les grands entiers et les nombres décimaux tels que `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` peuvent être interprétés à partir d&#39;une valeur BSON Binary avec le sous-type binaire `\x00`.
Dans ce cas, le format vérifie que la taille des données binaires est égale à celle de la valeur attendue.

:::note
Ce format ne fonctionne pas correctement sur les plateformes big-endian.
:::

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="inserting-data">
  ### Insertion de données
</div>

À l’aide d’un fichier BSON nommé `football.bson` et contenant les données suivantes :

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
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### Lecture des données
</div>

Lisez les données au format `BSONEachRow` :

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON est un format binaire qui ne s’affiche pas sous une forme lisible par l’humain dans le terminal. Utilisez `INTO OUTFILE` pour générer des fichiers BSON.
:::

<div id="format-settings">
  ## Paramètres du format
</div>

| Paramètre                                                                                                                                                                                             | Description                                                                                                                           | Par défaut |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Utiliser le type String BSON au lieu de binaire pour les colonne de type String.                                                       | `false`    |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Autoriser l’omission des colonnes dont les types ne sont pas pris en charge lors de l’inférence du schéma pour le format BSONEachRow. | `false`    |