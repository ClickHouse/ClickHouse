---
slug: /zh/sql-reference/table-functions/format
sidebar_position: 56
sidebar_label: format
---

# format

Extracts table structure from data and parses it according to specified input format.

**Syntax**

``` sql
format(format_name, data)
```

**Parameters**

-   `format_name` — The [format](/sql-reference/formats) of the data.
-   `data` — String literal or constant expression that returns a string containing data in specified format

**Returned value**

A table with data parsed from `data` argument according specified format and extracted schema.

**Examples**

**Query:**
``` sql
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

**See Also**

-   [Formats](../../interfaces/formats.md)
