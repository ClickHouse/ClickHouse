---
title : JSONCompactEachRowWithNames
slug: /interfaces/formats/JSONCompactEachRowWithNames
keywords : [JSONCompactEachRowWithNames]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |


## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints the header row with column names, similar to the [`TabSeparatedWithNames`](../TabSeparated/TabSeparatedWithNames.md) format.


## Example Usage {#example-usage}

## Format Settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/docs/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/docs/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::
