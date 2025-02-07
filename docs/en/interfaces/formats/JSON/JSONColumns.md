---
title : JSONColumns
slug : /en/interfaces/formats/JSONColumns
keywords : [JSONColumns]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

:::tip
The output of the JSONColumns* formats provides the ClickHouse field name and then the content of each row in the table for that field;
visually, the data is rotated 90 degrees to the left.
:::

In this format, all data is represented as a single JSON Object.

:::note
The `JSONColumns` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example Usage

Example:

```json
{
	"num": [42, 43, 44],
	"str": ["hello", "hello", "hello"],
	"arr": [[0,1], [0,1,2], [0,1,2,3]]
}
```

## Format Settings

During import, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Columns that are not present in the block will be filled with default values (you can use the [`input_format_defaults_for_omitted_fields`](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)