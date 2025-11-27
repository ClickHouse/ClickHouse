---
alias: []
description: 'Documentation for the Vertical format'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.
[`NULL`](/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

## Example Usage {#example-usage}

Example:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Rows are not escaped in Vertical format:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

## Format Settings {#format-settings}

