---
alias: []
description: 'Documentation sur le format Arrow'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

[Apache Arrow](https://arrow.apache.org/) propose deux formats de stockage colonnaire intégrés.
ClickHouse prend en charge les opérations de lecture et d&#39;écriture pour ces formats.
`Arrow` est le format « fichier » d&#39;Apache Arrow, conçu pour un accès aléatoire en mémoire.

<div id="data-types-matching">
  ## Correspondance des types de données
</div>

Le tableau ci-dessous indique les types de données pris en charge et leur correspondance avec les [types de données](/fr/sql-reference/data-types/index.md) de ClickHouse dans les requêtes `INSERT` et `SELECT`.

| Type de données Arrow (`INSERT`)        | Type de données ClickHouse                                                                                            | Type de données Arrow (`SELECT`) |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `BOOL`                                  | [Bool](/fr/sql-reference/data-types/boolean.md)                                                                          | `BOOL`                           |
| `UINT8`, `BOOL`                         | [UInt8](/fr/sql-reference/data-types/int-uint.md)                                                                        | `UINT8`                          |
| `INT8`                                  | [Int8](/fr/sql-reference/data-types/int-uint.md)/[Enum8](/fr/sql-reference/data-types/enum.md)                              | `INT8`                           |
| `UINT16`                                | [UInt16](/fr/sql-reference/data-types/int-uint.md)                                                                       | `UINT16`                         |
| `INT16`                                 | [Int16](/fr/sql-reference/data-types/int-uint.md)/[Enum16](/fr/sql-reference/data-types/enum.md)                            | `INT16`                          |
| `UINT32`                                | [UInt32](/fr/sql-reference/data-types/int-uint.md)                                                                       | `UINT32`                         |
| `INT32`                                 | [Int32](/fr/sql-reference/data-types/int-uint.md)                                                                        | `INT32`                          |
| `UINT64`                                | [UInt64](/fr/sql-reference/data-types/int-uint.md)                                                                       | `UINT64`                         |
| `INT64`                                 | [Int64](/fr/sql-reference/data-types/int-uint.md)                                                                        | `INT64`                          |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/fr/sql-reference/data-types/float.md)                                                                         | `FLOAT32`                        |
| `DOUBLE`                                | [Float64](/fr/sql-reference/data-types/float.md)                                                                         | `FLOAT64`                        |
| `DATE32`                                | [Date32](/fr/sql-reference/data-types/date32.md)                                                                         | `UINT16`                         |
| `DATE64`                                | [DateTime](/fr/sql-reference/data-types/datetime.md)                                                                     | `UINT32`                         |
| `TIMESTAMP`                             | [DateTime64](/fr/sql-reference/data-types/datetime64.md)                                                                 | `TIMESTAMP`                      |
| `TIME32`, `TIME64`                      | [Time64](/fr/sql-reference/data-types/time64.md)                                                                         | `TIME32`, `TIME64`               |
| `STRING`, `BINARY`                      | [String](/fr/sql-reference/data-types/string.md)                                                                         | `BINARY`                         |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/fr/sql-reference/data-types/fixedstring.md)                                                               | `FIXED_SIZE_BINARY`              |
| `DECIMAL`                               | [Decimal](/fr/sql-reference/data-types/decimal.md)                                                                       | `DECIMAL`                        |
| `DECIMAL256`                            | [Decimal256](/fr/sql-reference/data-types/decimal.md)                                                                    | `DECIMAL256`                     |
| `LIST`                                  | [Array](/fr/sql-reference/data-types/array.md)                                                                           | `LIST`                           |
| `STRUCT`                                | [Tuple](/fr/sql-reference/data-types/tuple.md)                                                                           | `STRUCT`                         |
| `MAP`                                   | [Map](/fr/sql-reference/data-types/map.md)                                                                               | `MAP`                            |
| `UINT32`                                | [IPv4](/fr/sql-reference/data-types/ipv4.md)                                                                             | `UINT32`                         |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/fr/sql-reference/data-types/ipv6.md)                                                                             | `FIXED_SIZE_BINARY`              |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/fr/sql-reference/data-types/int-uint.md)                                                | `FIXED_SIZE_BINARY`              |
| `DURATION`                              | [Interval](/fr/sql-reference/data-types/special-data-types/interval.md) (Nanoseconde/Microseconde/Milliseconde/Seconde)  | `DURATION`                       |
| `INT64`                                 | [Interval](/fr/sql-reference/data-types/special-data-types/interval.md) (Minute/Heure/Jour/Semaine/Mois/Trimestre/Année) | `INT64`                          |

Les `Array` peuvent être imbriqués et prendre une valeur de type `Nullable` comme argument. Les types `Tuple` et `Map` peuvent également être imbriqués.

Le type `DICTIONARY` est pris en charge pour les requêtes `INSERT` et, pour les requêtes `SELECT`, il existe un paramètre [`output_format_arrow_low_cardinality_as_dictionary`](/fr/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) qui permet d’exporter le type [LowCardinality](/fr/sql-reference/data-types/lowcardinality.md) sous forme de type `DICTIONARY`. Notez qu’il peut y avoir des valeurs inutilisées dans le dictionnaire `LowCardinality`, ce qui peut entraîner des valeurs inutilisées dans le `DICTIONARY` Arrow en sortie.

Types de données Arrow non pris en charge :

* `JSON`
* `ENUM`.

Les types de données des colonnes d’une table ClickHouse ne doivent pas nécessairement correspondre aux champs de données Arrow correspondants. Lors de l’insertion de données, ClickHouse interprète les types de données conformément au tableau ci-dessus, puis [convertit](/fr/sql-reference/functions/type-conversion-functions#CAST) les données vers le type de données défini pour la colonne de la table ClickHouse.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Dans l’exemple ci-dessous, nous utilisons le jeu de données `forex` disponible dans le
[ClickHouse SQL playground](https://sql.clickhouse.com).

<div id="selecting-data">
  ### Sélection des données
</div>

Nous sélectionnons une journée de taux de change `EUR/USD` dans le Playground et l&#39;enregistrons
dans un fichier local `forex_eurusd.arrow`. Nous interrogeons le Playground via l&#39;interface HTTP,
où l&#39;hôte est `sql-clickhouse.clickhouse.com` et l&#39;utilisateur est
`demo` (qui n&#39;a pas de mot de passe) :

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### Lecture du fichier
</div>

Nous pouvons maintenant relire le fichier Arrow local avec
[`clickhouse-local`](/fr/operations/utilities/clickhouse-local) en utilisant la
fonction de table [`file`](/fr/sql-reference/table-functions/file). Le fichier est
auto-descriptif, le format `Arrow` en déduit donc automatiquement le schéma :

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### Insertion de données
</div>

Pour charger un fichier Arrow dans une table ClickHouse, envoyez-le à `clickhouse-client`
avec `FORMAT Arrow` :

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                                    | Description                                                                                                                                                                                                                                                               | Valeur par défaut |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `input_format_arrow_allow_missing_columns`                                   | Autoriser les colonnes manquantes lors de la lecture des formats d&#39;entrée Arrow                                                                                                                                                                                       | `1`               |
| `input_format_arrow_case_insensitive_column_matching`                        | Ignorer la casse lors de la correspondance entre les colonnes Arrow et les colonnes CH.                                                                                                                                                                                   | `0`               |
| `input_format_arrow_import_nested`                                           | Paramètre obsolète, sans effet                                                                                                                                                                                                                                            | `0`               |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Ignorer les colonnes dont les types ne sont pas pris en charge lors de l&#39;inférence de schéma du format Arrow                                                                                                                                                          | `0`               |
| `input_format_arrow_use_native_reader`                                       | Utiliser le lecteur natif de ClickHouse pour les formats `Arrow` et `ArrowStream` au lieu de la bibliothèque Apache Arrow. Définir sur `0` pour utiliser le lecteur de la bibliothèque Apache Arrow.                                                                      | `1`               |
| `output_format_arrow_compression_method`                                     | Méthode de compression du format de sortie Arrow. Codecs pris en charge : lz4&#95;frame, zstd, none (non compressé)                                                                                                                                                       | `lz4_frame`       |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | Utiliser le type Arrow FIXED&#95;SIZE&#95;BINARY à la place de Binary pour les colonnes FixedString.                                                                                                                                                                      | `1`               |
| `output_format_arrow_low_cardinality_as_dictionary`                          | Activer la sortie du type LowCardinality sous forme de type Arrow Dictionary                                                                                                                                                                                              | `0`               |
| `output_format_arrow_string_as_string`                                       | Utiliser le type Arrow String à la place de Binary pour les colonne de type String                                                                                                                                                                                        | `1`               |
| `output_format_arrow_unsupported_types_as_binary`                            | Produire sous forme de données binaires brutes un type qui n&#39;a pas d&#39;équivalent Arrow (par ex. `BFloat16`, `AggregateFunction`). Si false, ce type déclenche une exception. S&#39;applique à la fois au lecteur natif et à celui de la bibliothèque Apache Arrow. | `1`               |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Toujours utiliser des entiers 64 bits pour les indices du dictionnaire au format Arrow                                                                                                                                                                                    | `0`               |
| `output_format_arrow_use_native_writer`                                      | Utiliser le module d&#39;écriture natif de ClickHouse pour les formats `Arrow` et `ArrowStream` au lieu de la bibliothèque Apache Arrow. Définir sur `0` pour utiliser le module d&#39;écriture de la bibliothèque Apache Arrow.                                          | `1`               |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Utiliser des entiers signés pour les indices du dictionnaire au format Arrow                                                                                                                                                                                              | `1`               |