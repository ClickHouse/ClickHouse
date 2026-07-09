---
description: 'Представляет словарь текстового индекса в таблице семейства MergeTree.
  Может использоваться для интроспекции.'
sidebar_label: 'mergeTreeTextIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeTextIndex
title: 'mergeTreeTextIndex'
doc_type: 'reference'
---

Представляет словарь текстового индекса в таблицах семейства MergeTree.
Возвращает токены с метаданными их списков вхождений.
Может использоваться для интроспекции.

<div id="syntax">
  ## Синтаксис
</div>

```sql
mergeTreeTextIndex(database, table, index_name)
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент     | Описание                                                  |
| ------------ | --------------------------------------------------------- |
| `database`   | Имя базы данных, из которой считывается текстовый индекс. |
| `table`      | Имя таблицы, из которой считывается текстовый индекс.     |
| `index_name` | Текстовый индекс, из которого считываются данные.         |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Объект table, содержащий токены и метаданные их списков вхождений.

<div id="usage-example">
  ## Пример использования
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