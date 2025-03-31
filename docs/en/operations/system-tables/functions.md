---
slug: /en/operations/system-tables/functions
---
# functions

Contains information about normal and aggregate functions.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) – The name of the function.
- `is_aggregate` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether the function is an aggregate function.
- `case_insensitive`, ([UInt8](../../sql-reference/data-types/int-uint.md)) - Whether the function name can be used case-insensitively.
- `alias_to`, ([String](../../sql-reference/data-types/string.md)) - The original function name, if the function name is an alias.
- `create_query`, ([String](../../sql-reference/data-types/enum.md)) - Unused.
- `origin`, ([Enum8](../../sql-reference/data-types/string.md)) - Unused.
- `description`, ([String](../../sql-reference/data-types/string.md)) - A high-level description what the function does.
- `syntax`, ([String](../../sql-reference/data-types/string.md)) - Signature of the function.
- `arguments`, ([String](../../sql-reference/data-types/string.md)) - What arguments does the function take.
- `returned_value`, ([String](../../sql-reference/data-types/string.md)) - What does the function return.
- `examples`, ([String](../../sql-reference/data-types/string.md)) - Example usage of the function.
- `categories`, ([String](../../sql-reference/data-types/string.md)) - The category of the function.

**Example**

```sql
 SELECT name, is_aggregate, is_deterministic, case_insensitive, alias_to FROM system.functions LIMIT 5;
```

```text
┌─name─────────────────────┬─is_aggregate─┬─is_deterministic─┬─case_insensitive─┬─alias_to─┐
│ BLAKE3                   │            0 │                1 │                0 │          │
│ sipHash128Reference      │            0 │                1 │                0 │          │
│ mapExtractKeyLike        │            0 │                1 │                0 │          │
│ sipHash128ReferenceKeyed │            0 │                1 │                0 │          │
│ mapPartialSort           │            0 │                1 │                0 │          │
└──────────────────────────┴──────────────┴──────────────────┴──────────────────┴──────────┘

5 rows in set. Elapsed: 0.002 sec.
```
