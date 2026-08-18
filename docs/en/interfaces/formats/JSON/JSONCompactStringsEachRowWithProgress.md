---
alias: []
description: 'Documentation for the JSONCompactStringsEachRowWithProgress format'
input_format: true
keywords: ['JSONCompactStringsEachRowWithProgress']
output_format: true
slug: /interfaces/formats/JSONCompactStringsEachRowWithProgress
title: 'JSONCompactStringsEachRowWithProgress'
doc_type: 'reference'
---

| Input | Output  | Alias  |
|-------|---------|--------|
| ✗     | ✔       |        |

## Description {#description}

Similar to [`JSONCompactEachRowWithProgress`](/interfaces/formats/JSONCompactEachRowWithProgress), but all values are converted to strings.
This is useful when you need consistent string representation of all data types.

Key features:
- Same structure as `JSONCompactEachRowWithProgress`
- All values are represented as strings (numbers, arrays, etc. are all quoted strings)
- Includes progress updates, totals, and exception handling
- Useful for clients that prefer or require string-based data

## Example usage {#example-usage}

### Inserting data {#inserting-data}

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactStringsEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":["[-8]", "46848.5225", "('2064-06-11 14:00:36.578','b06f4fa1-22ff-f84f-a1b7-a5807d983ae6')"]}
{"row":["[-76]", "-85331.598", "('2038-06-16 04:10:27.271','2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c')"]}
{"row":["[-32]", "-31470.8994", "('2027-07-18 16:58:34.654','1cdbae4c-ceb2-1337-b954-b175f5efbef8')"]}
{"row":["[-116]", "32104.097", "('1979-04-27 21:51:53.321','66903704-3c83-8f8a-648a-da4ac1ffa9fc')"]}
{"row":["[]", "2427.6614", "('1980-04-24 11:30:35.487','fee19be8-0f46-149b-ed98-43e7455ce2b2')"]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"191151"}}
{"rows_before_limit_at_least":5}
```

## Format settings {#format-settings}
