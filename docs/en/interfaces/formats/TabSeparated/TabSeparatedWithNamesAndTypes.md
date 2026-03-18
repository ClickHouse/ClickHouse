---
description: 'Documentation for the TabSeparatedWithNamesAndTypes format'
keywords: ['TabSeparatedWithNamesAndTypes']
slug: /interfaces/formats/TabSeparatedWithNamesAndTypes
title: 'TabSeparatedWithNamesAndTypes'
---

| Input | Output | Alias                                          |
|-------|--------|------------------------------------------------|
|     ✔    |     ✔     | `TSVWithNamesAndTypes`, `RawWithNamesAndTypes` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written to the first row, while the column types are in the second row.

:::note
- If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns in the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
- If setting [`input_format_with_types_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## Example Usage {#example-usage}

## Format Settings {#format-settings}