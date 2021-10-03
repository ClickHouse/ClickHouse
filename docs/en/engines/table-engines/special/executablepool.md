---
toc_priority: 47
toc_title: ExecutablePool
---

# ExecutablePool Table Engine {#table_engines-executablepool}

ExecutablePool engine allows executing a custom script and passing data into this script. This data comes as a result of queries, executed in ClickHouse. 

ExecutablePool runs a [pool_size](../../../operations/settings/settings.md#pool_size) number of processes. Each process has [max_command_execution_time](../../../operations/settings/settings.md#max_command_execution_time) seconds for processing the incoming data. After processing all the data, the engine has [command_termination_timeout](../../../operations/settings/settings.md#command_termination_timeout) seconds to shutdown.

## Creating a Table {#creating-a-table}

``` sql
ENGINE = ExecutablePool(script, format, process1[, process2, ...])
```

**Engine Parameters**

- script - Name of a script to execute. [String](../../../sql-reference/data-types/string.md).
- format - [Format](../../../interfaces/formats.md) of data, passed to the executed script. [String](../../../sql-reference/data-types/string.md).
- process(n) - One or more queries, which results are passed to the executed script. [String](../../../sql-reference/data-types/string.md).

## Virtual columns {#virtual-columns} 

List and virtual columns with description, if they exist.

## Usage Example {#usage-example}

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
CREATE TABLE result_table (value String) ENGINE = ExecutablePool('input_process.sh', 'TabSeparated', (SELECT 1), (SELECT 2), (SELECT 3));
SELECT * from result_table;
```

Result:

```text
Key 3\nKey 2\nKey 1\n
```

**See Also**

-   [Executable Pool](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-executable_pool) dictionary
-   [pool_size](../../../operations/settings/settings.md#pool_size) setting
-   [max_command_execution_time](../../../operations/settings/settings.md#max_command_execution_time) setting
-   [command_termination_timeout](../../../operations/settings/settings.md#command_termination_timeout) setting