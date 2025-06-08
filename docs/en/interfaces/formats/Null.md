---
alias: []
description: 'Documentation for the Null format'
input_format: false
keywords: ['Null', 'format']
output_format: true
slug: /interfaces/formats/Null
title: 'Null'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

In the `Null` format - nothing is output. 
This may at first sound strange, but it's important to note that despite outputting nothing, the query is still processed, 
and when using the command-line client, data is transmitted to the client. 

:::tip
The `Null` format can be useful for performance testing.
:::

## Example Usage {#example-usage}

Connect to `play.clickhouse.com` with clickhouse client:

```bash
clickhouse client --secure --host play.clickhouse.com --user explorer
```

Run the following the query:

```sql title="Query"
SELECT town
FROM uk_price_paid
LIMIT 1000
FORMAT `Null`
```

```response title="Response"
0 rows in set. Elapsed: 0.002 sec. Processed 1.00 thousand rows, 2.00 KB (506.97 thousand rows/s., 1.01 MB/s.)
Peak memory usage: 297.74 KiB.
```

Note how 1000 rows were processed, but 0 rows were output in the result set.

## Format Settings {#format-settings}
