---
alias: []
description: 'Documentation for the Pretty format'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

The `Pretty` format outputs data as Unicode-art tables, 
using ANSI-escape sequences for displaying colors in the terminal.
A full grid of the table is drawn, and each row occupies two lines in the terminal.
Each result block is output as a separate table. 
This is necessary so that blocks can be output without buffering results (buffering would be necessary to pre-calculate the visible width of all the values).

[NULL](/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

## Example Usage {#example-usage}

Example (shown for the [`PrettyCompact`](./PrettyCompact.md) format):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Rows are not escaped in any of the `Pretty` formats. The following example is shown for the [`PrettyCompact`](./PrettyCompact.md) format:

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

To avoid dumping too much data to the terminal, only the first `10,000` rows are printed. 
If the number of rows is greater than or equal to `10,000`, the message "Showed first 10 000" is printed.

:::note
This format is only appropriate for outputting a query result, but not for parsing data.
:::

The Pretty format supports outputting total values (when using `WITH TOTALS`) and extremes (when 'extremes' is set to 1). 
In these cases, total values and extreme values are output after the main data, in separate tables. 
This is shown in the following example which uses the [`PrettyCompact`](./PrettyCompact.md) format:

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## Format Settings {#format-settings}

<PrettyFormatSettings/>

