---
alias: []
description: 'Documentation for the RowBinaryWithNamesAndTypesAndDefaults format'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to the [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) format, but with an extra byte before each cell that indicates whether the column's `DEFAULT` value should be used — exactly like in the [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md) format. This combination supports schema-evolving `INSERT`s: the writer can omit columns from the header (they receive the target column's `DEFAULT`) and, for any column it does send, it can mark individual cells as "use the column's `DEFAULT`" without conflating that with `NULL`.

This format is input only.

## Wire format {#wire-format}

The header is identical to [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md):

1. A `VarUInt` with the number of columns `N`.
2. `N` length-prefixed `String`s with column names.
3. `N` column types — either textual names or compact binary encoding, controlled by the `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format` settings.

After the header, each row consists of `N` cells. For each cell:

- A single `UInt8` marker byte.
  - `0x01` — use the target column's `DEFAULT` expression. No value bytes follow.
  - `0x00` — a value follows, serialized via the column type's `RowBinary` serializer. For `Nullable(T)` the value bytes start with the `Nullable` null byte (`0` for non-null, `1` for NULL), then the inner value if non-null.

## Defaults vs NULL {#defaults-vs-null}

The per-cell default marker and `Nullable`'s built-in null byte are independent. A `Nullable(UInt32) DEFAULT 42` column can be sent three different ways per row:

| Bytes      | Meaning                                                |
|------------|--------------------------------------------------------|
| `01`       | Use `DEFAULT 42`.                                      |
| `00 01`    | Value path, then `NULL` via the `Nullable` type.       |
| `00 00 …`  | Value path, then a non-null inner value.               |

## Schema evolution {#schema-evolution}

| Case                                                                             | Behavior                                                                                                                                                                                |
|----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Column missing from the file's header entirely                                   | Filled in the target via `insertDefaultsForNotSeenColumns`; gated by `defaults_for_omitted_fields`.                                                                                     |
| Column present in the header, cell marker `0x01`                                 | `insertDefault` per row.                                                                                                                                                                |
| Column present in the header, cell marker `0x00`                                 | Value is parsed normally.                                                                                                                                                               |
| Extra column in the header, not in the target table                              | Silently dropped when `input_format_skip_unknown_fields = 1` (the marker is consumed first; if `0x01`, nothing else; if `0x00`, the typed value is parsed and discarded).               |

## Example usage {#example-usage}

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

- The header carries one column named `x` of type `Nullable(UInt32)`.
- The single cell uses marker `0x01`, meaning "use `DEFAULT 42`".

## Format settings {#format-settings}

<RowBinaryFormatSettings/>
