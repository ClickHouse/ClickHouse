---
description: 'Représente le contenu des fichiers d’index et de marks des tables MergeTree.
  Peut être utilisé pour l’introspection.'
sidebar_label: 'mergeTreeIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeIndex
title: 'mergeTreeIndex'
doc_type: 'reference'
---

Représente le contenu des fichiers d’index et de marks des tables MergeTree. Peut être utilisé pour l’introspection.

<div id="syntax">
  ## Syntaxe
</div>

```sql
mergeTreeIndex(database, table [, with_marks = true] [, with_minmax = true])
```

<div id="arguments">
  ## Arguments
</div>

| Argument      | Description                                                                  |
| ------------- | ---------------------------------------------------------------------------- |
| `database`    | Le nom de la base de données à partir de laquelle lire l’index et les marks. |
| `table`       | Le nom de la table à partir de laquelle lire l’index et les marks.           |
| `with_marks`  | Indique s’il faut inclure les colonnes de marks dans le résultat.            |
| `with_minmax` | Indique s’il faut inclure l’index min-max dans le résultat.                  |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet table avec des colonnes contenant les valeurs de l’index primaire et de l’index min-max (s’ils sont activés) de la table source, des colonnes contenant les valeurs des marks (s’ils sont activés) pour tous les fichiers possibles dans les data parts de la table source, ainsi que des virtual columns :

* `part_name` - Le nom de la data part.
* `mark_number` - Le numéro de la mark actuelle dans la data part.
* `rows_in_granule` - Le nombre de rows dans le granule actuel.

La colonne des marks peut contenir la valeur `(NULL, NULL)` lorsque la colonne est absente de la data part ou que les marks de l’un de ses substreams ne sont pas écrites (par exemple, dans les compact parts).

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

```sql
CREATE TABLE test_table
(
    `id` UInt64,
    `n` UInt64,
    `arr` Array(UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 8;

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(5);

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(10, 10);
```

```sql
SELECT * FROM mergeTreeIndex(currentDatabase(), test_table, with_marks = true);
```

```text
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark──┬─arr.size0.mark─┬─arr.mark─┐
│ all_1_1_0 │           0 │               3 │  0 │ (0,0)   │ (42,0)  │ (NULL,NULL)    │ (84,0)   │
│ all_1_1_0 │           1 │               2 │  3 │ (133,0) │ (172,0) │ (NULL,NULL)    │ (211,0)  │
│ all_1_1_0 │           2 │               0 │  4 │ (271,0) │ (271,0) │ (NULL,NULL)    │ (271,0)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴─────────┴────────────────┴──────────┘
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark─┬─arr.size0.mark─┬─arr.mark─┐
│ all_2_2_0 │           0 │               3 │ 10 │ (0,0)   │ (0,0)  │ (0,0)          │ (0,0)    │
│ all_2_2_0 │           1 │               3 │ 13 │ (0,24)  │ (0,24) │ (0,24)         │ (0,24)   │
│ all_2_2_0 │           2 │               3 │ 16 │ (0,48)  │ (0,48) │ (0,48)         │ (0,80)   │
│ all_2_2_0 │           3 │               1 │ 19 │ (0,72)  │ (0,72) │ (0,72)         │ (0,128)  │
│ all_2_2_0 │           4 │               0 │ 19 │ (0,80)  │ (0,80) │ (0,80)         │ (0,160)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴────────┴────────────────┴──────────┘
```

```sql
DESCRIBE mergeTreeIndex(currentDatabase(), test_table, with_marks = true) SETTINGS describe_compact_output = 1;
```

```text
┌─name────────────┬─type─────────────────────────────────────────────────────────────────────────────────────────────┐
│ part_name       │ String                                                                                           │
│ mark_number     │ UInt64                                                                                           │
│ rows_in_granule │ UInt64                                                                                           │
│ id              │ UInt64                                                                                           │
│ id.mark         │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ n.mark          │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.size0.mark  │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.mark        │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
└─────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘
```