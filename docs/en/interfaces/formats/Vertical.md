---
alias: []
description: 'Documentation for the Vertical format'
input_format: false
keywords: ['Vertical']
output_format: true
slug: /interfaces/formats/Vertical
title: 'Vertical'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.

Note that [`NULL`](/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ` to make it easier to distinguish between the string value `NULL` and no value. JSON columns will be pretty printed, and `NULL` is output as `null`, because it is a valid JSON value and easily distinguishable from `"null"`.

## Example usage {#example-usage}

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

## Format settings {#format-settings}
