---
title : One
slug : /en/interfaces/formats/One
keywords : [One]
---

## Description

Special input format that doesn't read any data from file and returns only one row with column of type `UInt8`, name `dummy` and value `0` (like `system.one` table).
Can be used with virtual columns `_file/_path`  to list all files without reading actual data.

## Example Usage

Example:

Query:
```sql
SELECT _file FROM file('path/to/files/data*', One);
```

Result:
```text
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

## Format Settings