# system.errors {#system_tables-errors}

Contains error codes with number of times they have been triggered.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — name of the error (`errorCodeToName`).
-   `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — code number of the error.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) - number of times this error has been happened.

**Example**

``` sql
SELECT *
FROM system.errors
WHERE value > 0
ORDER BY code ASC
LIMIT 1

┌─name─────────────┬─code─┬─value─┐
│ CANNOT_OPEN_FILE │   76 │     1 │
└──────────────────┴──────┴───────┘
```
