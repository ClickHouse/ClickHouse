---
description: 'Documentation for LS statement'
sidebar_label: 'LS'
sidebar_position: 38
slug: /sql-reference/statements/ls
title: 'LS'
---

# LS Statement

Lists files in the current working directory accessible to ClickHouse.

## Syntax

```sql title="Syntax"
LS [INTO OUTFILE filename] [FORMAT format]
```

The `LS` statement returns a list of files that ClickHouse can access in the current directory.

This statement is equivalent to the query:

```sql
SELECT _file FROM file('*', 'One')
```

## Examples

List all files in the current directory:

```sql title="Query"
ls
```

```text title="Response"
┌─_file────────┐
│ data1.csv    │
│ data2.json   │
│ config.xml   │
└──────────────┘
```

You can use different output formats:

```sql title="Query"
LS FORMAT Vertical
```

```text title="Response"
Row 1:
──────
_file: data1.csv

Row 2:
──────
_file: data2.json

Row 3:
──────
_file: config.xml
```

Export the file list to a file:

```sql title="Query"
LS INTO OUTFILE 'file_list.txt' FORMAT TabSeparated
```
