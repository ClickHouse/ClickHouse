---
slug: /en/operations/system-tables/errors
---
# errors

Contains error codes with the number of times they have been triggered.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — name of the error (`errorCodeToName`).
- `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — code number of the error.
- `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — the number of times this error happened.
- `last_error_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — the time when the last error happened.
- `last_error_message` ([String](../../sql-reference/data-types/string.md)) — message for the last error.
- `last_error_trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — A [stack trace](https://en.wikipedia.org/wiki/Stack_trace) that represents a list of physical addresses where the called methods are stored.
- `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — remote exception (i.e. received during one of the distributed queries).

:::note
Counters for some errors may increase during successful query execution. It's not recommended to use this table for server monitoring purposes unless you are sure that corresponding error can not be a false positive.
:::

**Example**

``` sql
SELECT name, code, value
FROM system.errors
WHERE value > 0
ORDER BY code ASC
LIMIT 1

┌─name─────────────┬─code─┬─value─┐
│ CANNOT_OPEN_FILE │   76 │     1 │
└──────────────────┴──────┴───────┘
```

``` sql
WITH arrayMap(x -> demangle(addressToSymbol(x)), last_error_trace) AS all
SELECT name, arrayStringConcat(all, '\n') AS res
FROM system.errors
LIMIT 1
SETTINGS allow_introspection_functions=1\G
```
