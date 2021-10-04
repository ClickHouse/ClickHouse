---
toc_priority: 47
toc_title: Executable/ExecutablePool
---

# Executable and ExecutablePool Table Engines {#table_engines-executable}

`Executable` and `ExecutablePool` engines allow executing a custom script and passing data into this script. This data comes as a result of queries, executed in ClickHouse. 

`Executable` and `ExecutablePool` engines run a [pool_size](../../../operations/settings/settings.md#pool_size) number of processes. Each process has [max_command_execution_time](../../../operations/settings/settings.md#max_command_execution_time) seconds for processing the incoming data. After processing all the data, the engine has [command_termination_timeout](../../../operations/settings/settings.md#command_termination_timeout) seconds to shutdown.

## Creating a Table {#creating-a-table}

``` sql
ENGINE = Executable(script_line, format, query)
ENGINE = ExecutablePool(script_line, format, query1[, query2, ...])
```

**Engine Parameters**

- `script_line` - An execution line for a script. Can include not only script name, but also arguments, passed to the script. [String](../../../sql-reference/data-types/string.md).
- `format` - [Format](../../../interfaces/formats.md) of data, passed to the executed script. [String](../../../sql-reference/data-types/string.md).
- `query(n)` - One or more queries, which results are passed to the executed script. Optional parameter. [String](../../../sql-reference/data-types/string.md).


## Usage Examples {#usage-example}

**1. Executable engine, data passed in a query**

Consider the script `input_loop.sh`, that should be executed:

```bash
#!/bin/bash

while read read_data; do printf "Key $read_data\n"; done
```

The data is passed to the script via the query:

```sql
CREATE TABLE table1 (value String) ENGINE = Executable('input_loop.sh', 'TabSeparated', (SELECT 1));
SELECT * FROM table1;
```

Result:

```text
Key 1
```

**2. Executable engine, data passed in a script argument**

Consider the script `input_arg.sh`, that should be executed:

```bash
#!/bin/bash

echo "Key $1"
```

The data is passed to the script via an argument, no query needed:

```sql
CREATE TABLE table2 (value String) ENGINE = Executable('input_arg.sh 1', 'TabSeparated');
SELECT * FROM table2;
```

Result:

```text
Key 1
```

**3. ExecutablePool engine example**

Consider the script `input_process.sh`, that should be executed:

```bash
#!/bin/bash

read -t 250 -u 4 read_data_from_4_fd;
read -t 250 -u 3 read_data_from_3_fd;
read -t 250 read_data_from_0_df;

printf "Key $read_data_from_4_fd\n";
printf "Key $read_data_from_3_fd\n";
printf "Key $read_data_from_0_df\n";
```

The data is passed to the script via the query:

``` sql
CREATE TABLE table3 (value String) ENGINE = ExecutablePool('input_process.sh', 'TabSeparated', (SELECT 1), (SELECT 2), (SELECT 3));
SELECT * FROM table3;
```

Result:

```text
Key 3
Key 2
Key 1
```

**See Also**

-   [Executable](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-executable) and [Executable Pool](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-executable_pool) dictionaries
-   [executable](../../../sql-reference/table-functions/executable.md) function
-   [pool_size](../../../operations/settings/settings.md#pool_size) setting
-   [max_command_execution_time](../../../operations/settings/settings.md#max_command_execution_time) setting
-   [command_termination_timeout](../../../operations/settings/settings.md#command_termination_timeout) setting
-   [send_chunk_header](../../../operations/settings/settings.md#send_chunk_header) setting