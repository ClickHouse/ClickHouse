# system.errors {#system_tables-errors}

包含错误代码和它们被触发的次数.

列信息:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 错误名称 (`errorCodeToName`).
-   `code` ([Int32](../../sql-reference/data-types/int-uint.md)) — 错误码.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 发生此错误的次数.
-   `last_error_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 上一次错误发生的时间.
-   `last_error_message` ([String](../../sql-reference/data-types/string.md)) — 最后一个错误的消息.
-   `last_error_trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — 一个[堆栈跟踪](https://en.wikipedia.org/wiki/Stack_trace), 它表示存储被调用方法的物理地址列表.
-   `remote` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 远程异常(即在一个分布式查询期间接收的).

**示例**

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
