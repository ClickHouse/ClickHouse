---
description: 'Représente le dictionnaire d’un index textuel dans une table MergeTree.
  Il peut être utilisé pour l’introspection.'
sidebar_label: 'mergeTreeTextIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeTextIndex
title: 'mergeTreeTextIndex'
doc_type: 'reference'
---

Représente le dictionnaire d’un index textuel dans des tables MergeTree.
Renvoie les tokens avec les métadonnées associées à leur liste de postings.
Il peut être utilisé pour l’introspection.

<div id="syntax">
  ## Syntaxe
</div>

```sql
mergeTreeTextIndex(database, table, index_name)
```

<div id="arguments">
  ## Arguments
</div>

| Argument     | Description                                                              |
| ------------ | ------------------------------------------------------------------------ |
| `database`   | Le nom de la base de données à partir de laquelle lire l’index textuel. |
| `table`      | Le nom de la table à partir de laquelle lire l’index textuel.           |
| `index_name` | L’index textuel à lire.                                                 |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet de type table avec les tokens et les métadonnées de leurs listes de postings.

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

```sql title="Query"
CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concatWithSeparator(' ', 'apple', 'banana') FROM numbers(500);
INSERT INTO tab SELECT 500 + number, concatWithSeparator(' ', 'cherry', 'date') FROM numbers(500);

SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s);
```

```text title="Response"
   ┌─part_name─┬─token──┬─dictionary_compression─┬─cardinality─┬─num_posting_blocks─┬─has_embedded_postings─┬─has_raw_postings─┬─has_compressed_postings─┐
1. │ all_1_1_0 │ apple  │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
2. │ all_1_1_0 │ banana │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
3. │ all_2_2_0 │ cherry │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
4. │ all_2_2_0 │ date   │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
   └───────────┴────────┴────────────────────────┴─────────────┴────────────────────┴───────────────────────┴──────────────────┴─────────────────────────┘
```