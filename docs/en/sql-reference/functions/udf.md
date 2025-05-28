---
description: 'Documentation for UDFs User Defined Functions'
sidebar_label: 'UDF'
sidebar_position: 15
slug: /sql-reference/functions/udf
title: 'UDFs User Defined Functions'
---

import PrivatePreviewBadge from '@theme/badges/PrivatePreviewBadge';

# UDFs User Defined Functions

## Executable User Defined Functions {#executable-user-defined-functions}

<PrivatePreviewBadge/>

:::note
This feature is supported in private preview in ClickHouse Cloud. Please contact ClickHouse Support at https://clickhouse.cloud/support to access.
:::

ClickHouse can call any external executable program or script to process data.

The configuration of executable user defined functions can be located in one or more xml-files. The path to the configuration is specified in the [user_defined_executable_functions_config](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config) parameter.

A function configuration contains the following settings:

- `name` - a function name.
- `command` - script name to execute or command if `execute_direct` is false.
- `argument` - argument description with the `type`, and optional `name` of an argument. Each argument is described in a separate setting. Specifying name is necessary if argument names are part of serialization for user defined function format like [Native](/interfaces/formats/Native) or [JSONEachRow](/interfaces/formats/JSONEachRow). Default argument name value is `c` + argument_number.
- `format` - a [format](../../interfaces/formats.md) in which arguments are passed to the command.
- `return_type` - the type of a returned value.
- `return_name` - name of returned value. Specifying return name is necessary if return name is part of serialization for user defined function format like [Native](../../interfaces/formats.md#native) or [JSONEachRow](/interfaces/formats/JSONEachRow). Optional. Default value is `result`.
- `type` - an executable type. If `type` is set to `executable` then single command is started. If it is set to `executable_pool` then a pool of commands is created.
- `max_command_execution_time` - maximum execution time in seconds for processing block of data. This setting is valid for `executable_pool` commands only. Optional. Default value is `10`.
- `command_termination_timeout` - time in seconds during which a command should finish after its pipe is closed. After that time `SIGTERM` is sent to the process executing the command. Optional. Default value is `10`.
- `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
- `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
- `pool_size` - the size of a command pool. Optional. Default value is `16`.
- `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.
- `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional parameter.
- `lifetime` - the reload interval of a function in seconds. If it is set to `0` then the function is not reloaded. Default value is `0`. Optional parameter.
- `deterministic` - if the function is deterministic (returns the same result for the same input). Default value is `false`. Optional parameter.

The command must read arguments from `STDIN` and must output the result to `STDOUT`. The command must process arguments iteratively. That is after processing a chunk of arguments it must wait for the next chunk.

### Examples {#examples}

**Inline script**

Creating `test_function_sum` manually specifying `execute_direct` to `0` using XML configuration.
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_sum</name>
        <return_type>UInt64</return_type>
        <argument>
            <type>UInt64</type>
            <name>lhs</name>
        </argument>
        <argument>
            <type>UInt64</type>
            <name>rhs</name>
        </argument>
        <format>TabSeparated</format>
        <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
        <execute_direct>0</execute_direct>
        <deterministic>true</deterministic>
    </function>
</functions>
```

Query:

```sql
SELECT test_function_sum(2, 2);
```

Result:

```text
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

**Python script**

Reads a value from `STDIN` and returns it as a string:

Creating `test_function` using XML configuration.
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_python</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
            <name>value</name>
        </argument>
        <format>TabSeparated</format>
        <command>test_function.py</command>
    </function>
</functions>
```

Script file inside `user_scripts` folder `test_function.py` (`/var/lib/clickhouse/user_scripts/test_function.py` with default path settings).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

Query:

```sql
SELECT test_function_python(toUInt64(2));
```

Result:

```text
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

Read two values from `STDIN` and returns their sum as a JSON object:

Creating `test_function_sum_json` with named arguments and format [JSONEachRow](../../interfaces/formats.md#jsoneachrow) using XML configuration.
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_function_sum_json</name>
        <return_type>UInt64</return_type>
        <return_name>result_name</return_name>
        <argument>
            <type>UInt64</type>
            <name>argument_1</name>
        </argument>
        <argument>
            <type>UInt64</type>
            <name>argument_2</name>
        </argument>
        <format>JSONEachRow</format>
        <command>test_function_sum_json.py</command>
    </function>
</functions>
```

Script file inside `user_scripts` folder `test_function_sum_json.py` (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` with default path settings).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

Query:

```sql
SELECT test_function_sum_json(2, 2);
```

Result:

```text
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

Use parameters in `command` setting:

Executable user defined functions can take constant parameters configured in `command` setting (works only for user defined functions with `executable` type). It also requires the `execute_direct` option (to ensure no shell argument expansion vulnerability).
File `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` with default path settings).
```xml
<functions>
    <function>
        <type>executable</type>
        <execute_direct>true</execute_direct>
        <name>test_function_parameter_python</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
        </argument>
        <format>TabSeparated</format>
        <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
    </function>
</functions>
```

Script file inside `user_scripts` folder `test_function_parameter_python.py` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` with default path settings).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

Query:

```sql
SELECT test_function_parameter_python(1)(2);
```

Result:

```text
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

**Shell script**

Shell script that multiplies each value by 2:

Executable user defined functions can be used with shell script.
File `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` with default path settings).
```xml
<functions>
    <function>
        <type>executable</type>
        <name>test_shell</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt8</type>
            <name>value</name>
        </argument>
        <format>TabSeparated</format>
        <command>test_shell.sh</command>
    </function>
</functions>
```

Script file inside `user_scripts` folder `test_shell.sh` (`/var/lib/clickhouse/user_scripts/test_shell.sh` with default path settings).

```bash
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

Query:

```sql
SELECT test_shell(number) FROM numbers(10);
```

Result:

```text
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```


### Error Handling {#error-handling}

Some functions might throw an exception if the data is invalid. In this case, the query is canceled and an error text is returned to the client. For distributed processing, when an exception occurs on one of the servers, the other servers also attempt to abort the query.

### Evaluation of Argument Expressions {#evaluation-of-argument-expressions}

In almost all programming languages, one of the arguments might not be evaluated for certain operators. This is usually the operators `&&`, `||`, and `?:`.
But in ClickHouse, arguments of functions (operators) are always evaluated. This is because entire parts of columns are evaluated at once, instead of calculating each row separately.

### Performing Functions for Distributed Query Processing {#performing-functions-for-distributed-query-processing}

For distributed query processing, as many stages of query processing as possible are performed on remote servers, and the rest of the stages (merging intermediate results and everything after that) are performed on the requestor server.

This means that functions can be performed on different servers.
For example, in the query `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

- if a `distributed_table` has at least two shards, the functions 'g' and 'h' are performed on remote servers, and the function 'f' is performed on the requestor server.
- if a `distributed_table` has only one shard, all the 'f', 'g', and 'h' functions are performed on this shard's server.

The result of a function usually does not depend on which server it is performed on. However, sometimes this is important.
For example, functions that work with dictionaries use the dictionary that exists on the server they are running on.
Another example is the `hostName` function, which returns the name of the server it is running on in order to make `GROUP BY` by servers in a `SELECT` query.

If a function in a query is performed on the requestor server, but you need to perform it on remote servers, you can wrap it in an 'any' aggregate function or add it to a key in `GROUP BY`.

## SQL User Defined Functions {#sql-user-defined-functions}

Custom functions from lambda expressions can be created using the [CREATE FUNCTION](../statements/create/function.md) statement. To delete these functions use the [DROP FUNCTION](../statements/drop.md#drop-function) statement.

## Related Content {#related-content}

### [User-defined functions in ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs) {#user-defined-functions-in-clickhouse-cloud}
