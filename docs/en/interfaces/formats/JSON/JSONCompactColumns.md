---
title : JSONCompactColumns
slug : /en/interfaces/formats/JSONCompactColumns
keywords : [JSONCompactColumns]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

In this format, all data is represented as a single JSON Array.

:::note
The `JSONCompactColumns` output format buffers all data in memory to output it as a single block which can lead to high memory consumption.
:::

## Example Usage

```json
[
	[42, 43, 44],
	["hello", "hello", "hello"],
	[[0,1], [0,1,2], [0,1,2,3]]
]
```

Columns that are not present in the block will be filled with default values (you can use [`input_format_defaults_for_omitted_fields`](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)

## Format Settings

