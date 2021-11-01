---
toc_priority: 47
toc_title: Executable/ExecutablePool
---

# Executable and ExecutablePool Table Engines {#table_engines-executable}

`Executable` and `ExecutablePool` engines allow to create a new table that is built by streaming data through a custom script.  

When you define an `Executable` or `ExecutablePool` table, you specify a script to execute and also a query to specify the records to be processed by the script. ClickHouse looks for your custom scripts in the `user_scripts` folder (on Linux it is typically the `/var/lib/clickhouse/user_scripts` folder). 

`Executable` engine creates one process, and `ExecutablePool` - a [pool_size](../../../operations/settings/settings.md#pool_size) number of processes, and reuses them during queries. Each process has [max_command_execution_time](../../../operations/settings/settings.md#max_command_execution_time) seconds for processing the incoming data. After processing all the data, the engine has [command_termination_timeout](../../../operations/settings/settings.md#command_termination_timeout) seconds to shutdown.

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

Consider the script `input_loop.sh`, that adds the word `Key` to the input strings:

```bash
#!/bin/bash
while read read_data; do printf "Key $read_data\n"; done
```

The `key_str` table is built by streaming the data from the `SELECT 1` query to the `input_loop.sh` script:

```sql
CREATE TABLE key_str (value String) ENGINE = Executable('input_loop.sh', 'TabSeparated', (SELECT 1));
SELECT * FROM key_str;
```

Result:

```text
Key 1
```

**2. Executable engine, data passed in a script argument**

Consider the script `input_arg.sh`, that adds the word `Key` to the input argument:

```bash
#!/bin/bash
echo "Key $1"
```

The `key_arg` table is built by passing the data to the `input_arg.sh` script via an argument, no query needed:

```sql
CREATE TABLE key_arg (value String) ENGINE = Executable('input_arg.sh 1', 'TabSeparated');
SELECT * FROM key_arg;
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

The `keys_list` table is built by streaming the data from the list of queries to the `input_process.sh` script:

``` sql
CREATE TABLE keys_list (value String) ENGINE = ExecutablePool('input_process.sh', 'TabSeparated', (SELECT 1), (SELECT 2), (SELECT 3));
SELECT * FROM keys_list;
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