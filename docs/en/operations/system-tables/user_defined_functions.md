---
description: 'System table containing loading status and configuration metadata for User-Defined Functions (UDFs).'
keywords: ['system table', 'user_defined_functions', 'udf', 'executable']
slug: /operations/system-tables/user_defined_functions
title: 'system.user_defined_functions'
doc_type: 'reference'
---

# system.user_defined_functions

Contains loading status, error information, and configuration metadata for [User-Defined Functions (UDFs)](/sql-reference/functions/udf.md).

Columns:

**Loading Status**

-   `name` ([String](/sql-reference/data-types/string.md)) — UDF name.
-   `load_status` ([Enum8](/sql-reference/data-types/enum.md)) — Loading status: `Success` (UDF loaded and ready), `Failed` (UDF failed to load).
-   `loading_error_message` ([String](/sql-reference/data-types/string.md)) — Detailed error message when loading failed. Empty if loaded successfully.
-   `last_successful_update_time` ([Nullable(DateTime)](/sql-reference/data-types/datetime.md)) — Timestamp of the last successful load. `NULL` if never succeeded.
-   `loading_duration_ms` ([UInt64](/sql-reference/data-types/int-uint.md)) — Time spent loading the UDF, in milliseconds.

**UDF Configuration**

-   `type` ([Enum8](/sql-reference/data-types/enum.md)) — UDF type: `executable` (single process per block) or `executable_pool` (persistent process pool).
-   `command` ([String](/sql-reference/data-types/string.md)) — Script or command to execute, including arguments.
-   `format` ([String](/sql-reference/data-types/string.md)) — Data format for I/O (e.g., `TabSeparated`, `JSONEachRow`).
-   `return_type` ([String](/sql-reference/data-types/string.md)) — Function return type (e.g., `String`, `UInt64`).
-   `return_name` ([String](/sql-reference/data-types/string.md)) — Optional return value identifier. Empty if not configured.
-   `argument_types` ([Array(String)](/sql-reference/data-types/array.md)) — Array of argument types.
-   `argument_names` ([Array(String)](/sql-reference/data-types/array.md)) — Array of argument names. Empty strings for unnamed arguments.

**Execution Parameters**

-   `max_command_execution_time` ([UInt64](/sql-reference/data-types/int-uint.md)) — Maximum seconds to process a data block. Only for `executable_pool` type.
-   `command_termination_timeout` ([UInt64](/sql-reference/data-types/int-uint.md)) — Seconds before sending SIGTERM to command process.
-   `command_read_timeout` ([UInt64](/sql-reference/data-types/int-uint.md)) — Milliseconds for reading from command stdout.
-   `command_write_timeout` ([UInt64](/sql-reference/data-types/int-uint.md)) — Milliseconds for writing to command stdin.
-   `pool_size` ([UInt64](/sql-reference/data-types/int-uint.md)) — Number of process instances in pool. Only for `executable_pool` type.
-   `send_chunk_header` ([UInt8](/sql-reference/data-types/int-uint.md)) — Whether to send row count before each data chunk (1 = true, 0 = false).
-   `execute_direct` ([UInt8](/sql-reference/data-types/int-uint.md)) — Whether to execute command directly (1) or via `/bin/bash` (0).
-   `lifetime` ([UInt64](/sql-reference/data-types/int-uint.md)) — Reload interval in seconds. 0 means reload is disabled.
-   `deterministic` ([UInt8](/sql-reference/data-types/int-uint.md)) — Whether function returns the same result for same arguments (1 = true, 0 = false).

**Example**

View all UDFs and their loading status:

```sql
SELECT
    name,
    load_status,
    type,
    command,
    return_type,
    argument_types
FROM system.user_defined_functions
FORMAT Vertical;
```

```response
Row 1:
──────
name:           my_sum_udf
load_status:    Success
type:           executable
command:        /var/lib/clickhouse/user_scripts/sum.py
return_type:    UInt64
argument_types: ['UInt64','UInt64']
```

Find failed UDFs:

```sql
SELECT
    name,
    loading_error_message
FROM system.user_defined_functions
WHERE load_status = 'Failed';
```

**See Also**

-   [User-Defined Functions](/sql-reference/functions/udf.md) — How to create and configure UDFs.
