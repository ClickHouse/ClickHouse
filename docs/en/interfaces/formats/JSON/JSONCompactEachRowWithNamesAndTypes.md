---
alias: []
description: 'Documentation for the JSONCompactEachRowWithNamesAndTypes format'
input_format: true
keywords: ['JSONCompactEachRowWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/JSONCompactEachRowWithNamesAndTypes
title: 'JSONCompactEachRowWithNamesAndTypes'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints two header rows with column names and types, similar to the [TabSeparatedWithNamesAndTypes](../TabSeparated/TabSeparatedWithNamesAndTypes.md) format.

## Example Usage {#example-usage}

## Format Settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [`input_format_with_types_use_header`](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::