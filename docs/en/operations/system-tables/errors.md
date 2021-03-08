# system.errors {#system_tables-errors}

Contains error codes with the number of times they have been triggered.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — name of the error (`errorCodeToName`).
-   `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — code number of the error.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — the number of times this error has been happened.
-   `last_error_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — time when the last error happened.
-   `last_error_message` ([String](../../sql-reference/data-types/string.md)) — message for the last error.
-   `last_error_stacktrace` ([String](../../sql-reference/data-types/string.md)) — stacktrace for the last error.
-   `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — remote exception (i.e. received during one of the distributed query).

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
