---
alias: []
description: 'Documentation for the Values format'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Values` format prints every row in brackets. 

- Rows are separated by commas without a comma after the last row. 
- The values inside the brackets are also comma-separated. 
- Numbers are output in a decimal format without quotes. 
- Arrays are output in `[]`.
- Strings, dates, and dates with times are output in quotes. 
- Escaping rules and parsing are similar to the [TabSeparated](TabSeparated/TabSeparated.md) format.

During formatting, extra spaces aren't inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). 
[`NULL`](/sql-reference/syntax.md) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in the `Values` format: 
- single quotes
- backslashes

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

The `Values` format is what `INSERT` uses, so any `INSERT ... VALUES` statement
is already using it. The `FORMAT Values` clause can be stated explicitly, and the
rows can be supplied from a stream or a file. Each row is a bracketed,
comma-separated tuple, with the tuples themselves separated by commas:

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

### Using expressions on input {#using-expressions}

Unlike most input formats, `Values` can evaluate SQL expressions in each field
rather than only accepting literals. This is controlled by
[`input_format_values_interpret_expressions`](#format-settings) (enabled by
default): when a field cannot be read by the fast streaming parser, ClickHouse
falls back to the SQL parser and interprets the field as an expression.

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

### Selecting data {#selecting-data}

The `Values` format can also be used to format query results. Numbers are
written without quotes, arrays in `[]`, and strings and dates in single quotes;
single quotes and backslashes inside strings are escaped with a backslash, and
[`NULL`](/sql-reference/syntax.md) is written as `NULL`:

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

## Format settings {#format-settings}

| Setting                                                                                                                                                     | Description                                                                                                                                                                                   | Default |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.                                                                               | `true`  |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows. | `true`  |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.                                                       | `true`  |
