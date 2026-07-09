---
description: 'Representa o dicionário de um índice de texto em uma tabela MergeTree.
  Pode ser usada para introspecção.'
sidebar_label: 'mergeTreeTextIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeTextIndex
title: 'mergeTreeTextIndex'
doc_type: 'reference'
---

Representa o dicionário de um índice de texto em tabelas MergeTree.
Retorna tokens com os metadados da posting list.
Pode ser usada para introspecção.

<div id="syntax">
  ## Sintaxe
</div>

```sql
mergeTreeTextIndex(database, table, index_name)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento    | Descrição                                                              |
| ------------ | ---------------------------------------------------------------------- |
| `database`   | O nome do banco de dados a partir do qual o índice de texto será lido. |
| `table`      | O nome da tabela a partir da qual o índice de texto será lido.         |
| `index_name` | O índice de texto que será lido.                                       |

<div id="returned_value">
  ## Valor retornado
</div>

Um objeto do tipo tabela com tokens e seus metadados de posting list.

<div id="usage-example">
  ## Exemplo de uso
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