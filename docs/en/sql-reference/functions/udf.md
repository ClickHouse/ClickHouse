---
description: 'Documentation for User Defined Functions (UDFs)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'User Defined Functions (UDFs)'
doc_type: 'reference'
---

import PrivatePreviewBadge from '@theme/badges/PrivatePreviewBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# UDFs User Defined Functions

ClickHouse supports several types of user defined functions (UDFs):

- [Executable UDFs](#executable-user-defined-functions) start an external program or script (Python, Bash, etc.) and stream blocks of data to it over STDIN / STDOUT. Use them to integrate existing code or tooling without recompiling ClickHouse. They have higher per‑call overhead compared to in‑process options and are best for heavier logic or where a different runtime is required.
- [SQL UDFs](#sql-user-defined-functions) are defined with `CREATE FUNCTION` purely in SQL. They are inlined/expanded into the query plan (no process boundary), making them lightweight and ideal for reusing expression logic or simplifying complex calculated columns.
- [Experimental WebAssembly UDFs](#webassembly-user-defined-functions) run code compiled to WebAssembly inside a sandbox within the server process. They offer lower per‑call overhead than external executables with better isolation than native extensions, making them suitable for custom algorithms written in languages that can target WASM (e.g. C/C++/Rust).

## Executable User Defined Functions {#executable-user-defined-functions}

<PrivatePreviewBadge/>

:::note
This feature is supported in private preview in ClickHouse Cloud.
Please contact ClickHouse Support at https://clickhouse.cloud/support to access.
:::

ClickHouse can call any external executable program or script to process data.

The configuration of executable user defined functions can be located in one or more xml-files.
The path to the configuration is specified in the [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config) parameter.

A function configuration contains the following settings:

| Parameter                     | Description                                                                                                                                                                                                                                                                                                                                                                                   | Required  | Default Value         |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------|
| `name`                        | A function name                                                                                                                                                                                                                                                                                                                                                                               | Yes       | -                     |
| `command`                     | Script name to execute or command if `execute_direct` is false                                                                                                                                                                                                                                                                                                                                | Yes       | -                     |
| `argument`                    | Argument description with the `type`, and optional `name` of an argument. Each argument is described in a separate setting. Specifying name is necessary if argument names are part of serialization for user defined function format like [Native](/interfaces/formats/Native) or [JSONEachRow](/interfaces/formats/JSONEachRow)                                                             | Yes       | `c` + argument_number |
| `format`                      | A [format](../../interfaces/formats.md) in which arguments are passed to the command. The command output is expected to use the same format too                                                                                                                                                                                                                                               | Yes       | -                     |
| `return_type`                 | The type of a returned value                                                                                                                                                                                                                                                                                                                                                                  | Yes       | -                     |
| `return_name`                 | Name of returned value. Specifying return name is necessary if return name is part of serialization for user defined function format like [Native](/interfaces/formats/Native) or [JSONEachRow](/interfaces/formats/JSONEachRow)                                                                                                                                                      | Optional  | `result`              |
| `type`                        | An executable type. If `type` is set to `executable` then single command is started. If it is set to `executable_pool` then a pool of commands is created                                                                                                                                                                                                                                     | Yes       | -                     |
| `max_command_execution_time`  | Maximum execution time in seconds for processing block of data. This setting is valid for `executable_pool` commands only                                                                                                                                                                                                                                                                     | Optional  | `10`                  |
| `command_termination_timeout` | Time in seconds during which a command should finish after its pipe is closed. After that time `SIGTERM` is sent to the process executing the command                                                                                                                                                                                                                                         | Optional  | `10`                  |
| `command_read_timeout`        | Timeout for reading data from command stdout in milliseconds                                                                                                                                                                                                                                                                                                                                  | Optional  | `10000`               |
| `command_write_timeout`       | Timeout for writing data to command stdin in milliseconds                                                                                                                                                                                                                                                                                                                                     | Optional  | `10000`               |
| `pool_size`                   | The size of a command pool                                                                                                                                                                                                                                                                                                                                                                    | Optional  | `16`                  |
| `send_chunk_header`           | Controls whether to send row count before sending a chunk of data to process                                                                                                                                                                                                                                                                                                                  | Optional  | `false`               |
| `execute_direct`              | If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c` | Optional  | `1`                   |
| `lifetime`                    | The reload interval of a function in seconds. If it is set to `0` then the function is not reloaded                                                                                                                                                                                                                                                                                           | Optional  | `0`                   |
| `deterministic`               | If the function is deterministic (returns the same result for the same input)                                                                                                                                                                                                                                                                                                                 | Optional  | `false`               |

The command must read arguments from `STDIN` and must output the result to `STDOUT`. The command must process arguments iteratively. That is after processing a chunk of arguments it must wait for the next chunk.

## Executable User Defined Functions {#executable-user-defined-functions}

## Examples {#examples}

### UDF from inline script {#udf-inline}

Create `test_function_sum` manually specifying `execute_direct` to `0` using either XML or YAML configuration.

<Tabs>
  <TabItem value="XML" label="XML" default>
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).

```xml title="/etc/clickhouse-server/test_function.xml"
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
  </TabItem>
  <TabItem value="YAML" label="YAML">

File `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` with default path settings).

```yml title="/etc/clickhouse-server/test_function.yaml"
functions:
  type: executable
  name: test_function_sum
  return_type: UInt64
  argument:
    - type: UInt64
      name: lhs
    - type: UInt64
      name: rhs
  format: TabSeparated
  command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
  execute_direct: 0
  deterministic: true
```
  </TabItem>
</Tabs>

<br/>

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

### UDF from Python script {#udf-python}

In this example we create a UDF which reads a value from `STDIN` and returns it as a string.

Create `test_function` using either XML OR YAML configuration.

<Tabs>
  <TabItem value="XML" label="XML" default>
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).
```xml title="/etc/clickhouse-server/test_function.xml"
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
  </TabItem>
  <TabItem value="YAML" label="YAML">
File `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` with default path settings).
```yml title="/etc/clickhouse-server/test_function.yaml"
functions:
  type: executable
  name: test_function_python
  return_type: String
  argument:
    - type: UInt64
      name: value
  format: TabSeparated
  command: test_function.py
```
  </TabItem>
</Tabs>

<br/>

Create a script file `test_function.py` inside `user_scripts` folder (`/var/lib/clickhouse/user_scripts/test_function.py` with default path settings).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

### Read two values from `STDIN` and return their sum as a JSON object {#udf-stdin}

Create `test_function_sum_json` with named arguments and format [JSONEachRow](/interfaces/formats/JSONEachRow) using either XML or YAML configuration.

<Tabs>
  <TabItem value="XML" label="XML" default>
File `test_function.xml` (`/etc/clickhouse-server/test_function.xml` with default path settings).
```xml title="/etc/clickhouse-server/test_function.xml"
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
  </TabItem>
  <TabItem value="YAML" label="YAML">
File `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` with default path settings).
```yml title="/etc/clickhouse-server/test_function.yaml"
functions:
  type: executable
  name: test_function_sum_json
  return_type: UInt64
  return_name: result_name
  argument:
    - type: UInt64
      name: argument_1
    - type: UInt64
      name: argument_2
  format: JSONEachRow
  command: test_function_sum_json.py
```
  </TabItem>
</Tabs>

<br/>

Create script file `test_function_sum_json.py` inside the `user_scripts` folder (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` with default path settings).

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

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

### Use parameters in `command` setting {#udf-parameters-in-command}

Executable user defined functions can take constant parameters configured in `command` setting (this works only for user defined functions with `executable` type).
It also requires the `execute_direct` option to ensure no shell argument expansion vulnerability.

<Tabs>
  <TabItem value="XML" label="XML" default>
File `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` with default path settings).
```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
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
  </TabItem>
  <TabItem value="YAML" label="YAML">
File `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` with default path settings).
```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
functions:
  type: executable
  execute_direct: true
  name: test_function_parameter_python
  return_type: String
  argument:
    - type: UInt64
  format: TabSeparated
  command: test_function_parameter_python.py {test_parameter:UInt64}
```
  </TabItem>
</Tabs>

<br/>

Create script file `test_function_parameter_python.py` inside the `user_scripts` folder (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` with default path settings).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

### UDF from shell script {#udf-shell-script}

In this example, we create a shell script that multiplies each value by 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
File `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` with default path settings).
```xml title="/etc/clickhouse-server/test_function_shell.xml"
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
  </TabItem>
  <TabItem value="YAML" label="YAML">
File `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` with default path settings).
```yml title="/etc/clickhouse-server/test_function_shell.yaml"
functions:
  type: executable
  name: test_shell
  return_type: String
  argument:
    - type: UInt8
      name: value
  format: TabSeparated
  command: test_shell.sh
```
  </TabItem>
</Tabs>

<br/>

Create a script file `test_shell.sh` inside the `user_scripts` folder (`/var/lib/clickhouse/user_scripts/test_shell.sh` with default path settings).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
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

## Error Handling {#error-handling}

Some functions might throw an exception if the data is invalid.
In this case, the query is canceled and an error text is returned to the client.
For distributed processing, when an exception occurs on one of the servers, the other servers also attempt to abort the query.

## Evaluation of Argument Expressions {#evaluation-of-argument-expressions}

In almost all programming languages, one of the arguments might not be evaluated for certain operators.
This is usually the operators `&&`, `||`, and `?:`.
In ClickHouse, arguments of functions (operators) are always evaluated.
This is because entire parts of columns are evaluated at once, instead of calculating each row separately.

## Performing Functions for Distributed Query Processing {#performing-functions-for-distributed-query-processing}

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

## WebAssembly User Defined Functions {#webassembly-user-defined-functions}

<CloudNotSupportedBadge/>
<ExperimentalBadge/>

WebAssembly User Defined Functions (WASM UDFs) allow you to run custom code compiled to WebAssembly inside the ClickHouse server process.

### Quick Start

Enable experimental WebAssembly support in your ClickHouse configuration:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

Insert your compiled WASM module into the system table:

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Create a function using your WASM module:

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

Use the function in your queries:

```sql
SELECT my_function(10, 20);
```

### More Information

Refer to the documentation on [WebAssembly User Defined Functions](wasm_udf.md) for more details.

## Related Content {#related-content}
- [User-defined functions in ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
