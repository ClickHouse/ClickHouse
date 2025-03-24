---
description: 'Parses data from arguments according to specified input format. If structure argument is not specified, it''s extracted from the data.'
slug: /sql-reference/table-functions/format
sidebar_position: 65
sidebar_label: 'format'
title: 'format'
---

# format Table Function

Parses data from arguments according to specified input format. If structure argument is not specified, it's extracted from the data.

**Syntax**

```sql
format(format_name, [structure], data)
```

**Parameters**

- `format_name` — The [format](/sql-reference/formats) of the data.
- `structure` - Structure of the table. Optional. Format 'column1_name column1_type, column2_name column2_type, ...'.
- `data` — String literal or constant expression that returns a string containing data in specified format

**Returned value**

A table with data parsed from `data` argument according to specified format and specified or extracted structure.

**Examples**

Without `structure` argument:

**Query:**
```sql
SELECT * FROM format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

**Result:**

```response
┌───b─┬─a─────┐
│ 111 │ Hello │
│ 123 │ World │
│ 112 │ Hello │
│ 124 │ World │
└─────┴───────┘
```

**Query:**
```sql
DESC format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

**Result:**

```response
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ b    │ Nullable(Float64) │              │                    │         │                  │                │
│ a    │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

With `structure` argument:

**Query:**
```sql
SELECT * FROM format(JSONEachRow, 'a String, b UInt32',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

**Result:**
```response
┌─a─────┬───b─┐
│ Hello │ 111 │
│ World │ 123 │
│ Hello │ 112 │
│ World │ 124 │
└───────┴─────┘
```

**See Also**

- [Formats](../../interfaces/formats.md)
