---
alias: []
description: 'Documentation for the One format'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
---


| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `One` format is a special input format that doesn't read any data from file, and returns only one row with column of type [`UInt8`](../../sql-reference/data-types/int-uint.md), name `dummy` and value `0` (like the `system.one` table).
Can be used with virtual columns `_file/_path`  to list all files without reading actual data.

## Example Usage {#example-usage}

Example:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

## Format Settings {#format-settings}