# system.errors {#system_tables-errors}

Содержит коды ошибок с указанием количества срабатываний.

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — название ошибки (`errorCodeToName`).
-   `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — номер кода ошибки.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество ошибок.

**Пример**

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
