---
description: 'Representa el diccionario de un índice de texto en una tabla MergeTree.
  Puede utilizarse para tareas de introspección.'
sidebar_label: 'mergeTreeTextIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeTextIndex
title: 'mergeTreeTextIndex'
doc_type: 'reference'
---

Representa el diccionario de un índice de texto en tablas MergeTree.
Devuelve tokens con los metadatos de su posting list.
Puede utilizarse para tareas de introspección.

<div id="syntax">
  ## Sintaxis
</div>

```sql
mergeTreeTextIndex(database, table, index_name)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento    | Descripción                                                        |
| ------------ | ------------------------------------------------------------------ |
| `database`   | El nombre de la base de datos de la que se lee el índice de texto. |
| `table`      | El nombre de la tabla de la que se lee el índice de texto.         |
| `index_name` | El índice de texto del que se lee.                                 |

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tabla con tokens y sus metadatos de posting list.

<div id="usage-example">
  ## Ejemplo de uso
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