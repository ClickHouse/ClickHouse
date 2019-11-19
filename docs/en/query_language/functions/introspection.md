# Introspection Functions

You can use functions described in this chapter to introspect ELF and DWARF for query profiling. These functions are slow and may impose security considerations.

For proper operation of introspection functions:

- Install the `clickhouse-common-static-dbg` package.
- Set the [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) to 1.


## addressToLine {#addresstoline}

Shows the filename and the number of a string in ClickHouse sources, using the debug information installed by the `clickhouse-common-static-dbg` package.

**Syntax**

```sql
addressToLine(address_of_binary_instruction)
```

**Parameters**

- `address_of_binary_instruction` ([UInt64](../../data_types/int_uint.md)) — Address of code instruction that have been executed at the moment of sampling.

**Returned value**

- Source code filename and the number of string in this file.

Type: [String](../../data_types/string.md).

**Example**

Enabling introspection functions:

```sql
SET allow_introspection_functions=1
```

Selecting the first string from the `trace_log` system table:

```sql
SELECT 
    *,
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines  
FROM system.trace_log 
LIMIT 1 
\G
```

The [arrayMap](higher_order_functions.md#higher_order_functions-array-map) function allows to process each individual element of the `trace` array by the `addressToLine` function. The result of this processing you see in the `trace_source_code_lines` column of output.

```text
Row 1:
──────
event_date:              2019-11-19
event_time:              2019-11-19 18:57:23
revision:                54429
timer_type:              Real
thread_number:           48
query_id:                421b6855-1858-45a5-8f37-f383409d6d72
trace:                   [140658411141617,94784174532828,94784076370703,94784076372094,94784076361020,94784175007680,140658411116251,140658403895439]
trace_source_code_lines: /lib/x86_64-linux-gnu/libpthread-2.27.so
/usr/lib/debug/usr/bin/clickhouse
/build/obj-x86_64-linux-gnu/../dbms/src/Common/ThreadPool.cpp:199
/build/obj-x86_64-linux-gnu/../dbms/src/Common/ThreadPool.h:155
/usr/include/c++/9/bits/atomic_base.h:551
/usr/lib/debug/usr/bin/clickhouse
/lib/x86_64-linux-gnu/libpthread-2.27.so
/build/glibc-OTsEL5/glibc-2.27/misc/../sysdeps/unix/sysv/linux/x86_64/clone.S:97
```
