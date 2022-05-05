---
sidebar_position: 32
sidebar_label: Functions
---

# Functions

There are at least\* two types of functions - regular functions (they are just called “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function does not depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

In this section we discuss regular functions. For aggregate functions, see the section “Aggregate functions”.

\* - There is a third type of function that the ‘arrayJoin’ function belongs to; table functions can also be mentioned separately.\*

## Strong Typing {#strong-typing}

In contrast to standard SQL, ClickHouse has strong typing. In other words, it does not make implicit conversions between types. Each function works for a specific set of types. This means that sometimes you need to use type conversion functions.

## Common Subexpression Elimination {#common-subexpression-elimination}

All expressions in a query that have the same AST (the same record or same result of syntactic parsing) are considered to have identical values. Such expressions are concatenated and executed once. Identical subqueries are also eliminated this way.

## Types of Results {#types-of-results}

All functions return a single return as the result (not several values, and not zero values). The type of result is usually defined only by the types of arguments, not by the values. Exceptions are the tupleElement function (the a.N operator), and the toFixedString function.

## Constants {#constants}

For simplicity, certain functions can only work with constants for some arguments. For example, the right argument of the LIKE operator must be a constant.
Almost all functions return a constant for constant arguments. The exception is functions that generate random numbers.
The ‘now’ function returns different values for queries that were run at different times, but the result is considered a constant, since constancy is only important within a single query.
A constant expression is also considered a constant (for example, the right half of the LIKE operator can be constructed from multiple constants).

Functions can be implemented in different ways for constant and non-constant arguments (different code is executed). But the results for a constant and for a true column containing only the same value should match each other.

## NULL Processing {#null-processing}

Functions have the following behaviors:

-   If at least one of the arguments of the function is `NULL`, the function result is also `NULL`.
-   Special behavior that is specified individually in the description of each function. In the ClickHouse source code, these functions have `UseDefaultImplementationForNulls=false`.

## Constancy {#constancy}

Functions can’t change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## Higher-order functions, `->` operator and lambda(params, expr) function {#higher-order-functions}

Higher-order functions can only accept lambda functions as their functional argument. To pass a lambda function to a higher-order function use `->` operator. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

Examples:

```
x -> 2 * x
str -> str != Referer
```

A lambda function that accepts multiple arguments can also be passed to a higher-order function. In this case, the higher-order function is passed several arrays of identical length that these arguments will correspond to.

For some functions the first argument (the lambda function) can be omitted. In this case, identical mapping is assumed.

## SQL User Defined Functions {#user-defined-functions}

Custom functions from lambda expressions can be created using the [CREATE FUNCTION](../statements/create/function.md) statement. To delete these functions use the [DROP FUNCTION](../statements/drop.md#drop-function) statement.

## Executable User Defined Functions {#executable-user-defined-functions}
ClickHouse can call any external executable program or script to process data.

The configuration of executable user defined functions can be located in one or more xml-files. The path to the configuration is specified in the [user_defined_executable_functions_config](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_defined_executable_functions_config) parameter.

A function configuration contains the following settings:

-   `name` - a function name.
-   `command` - script name to execute or command if `execute_direct` is false.
-   `argument` - argument description with the `type`, and optional `name` of an argument. Each argument is described in a separate setting. Specifying name is necessary if argument names are part of serialization for user defined function format like [Native](../../interfaces/formats.md#native) or [JSONEachRow](../../interfaces/formats.md#jsoneachrow). Default argument name value is `c` + argument_number.
-   `format` - a [format](../../interfaces/formats.md) in which arguments are passed to the command.
-   `return_type` - the type of a returned value.
-   `return_name` - name of retuned value. Specifying return name is necessary if return name is part of serialization for user defined function format like [Native](../../interfaces/formats.md#native) or [JSONEachRow](../../interfaces/formats.md#jsoneachrow). Optional. Default value is `result`.
-   `type` - an executable type. If `type` is set to `executable` then single command is started. If it is set to `executable_pool` then a pool of commands is created.
-   `max_command_execution_time` - maximum execution time in seconds for processing block of data. This setting is valid for `executable_pool` commands only. Optional. Default value is `10`.
-   `command_termination_timeout` - time in seconds during which a command should finish after its pipe is closed. After that time `SIGTERM` is sent to the process executing the command. Optional. Default value is `10`.
-   `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
-   `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
-   `pool_size` - the size of a command pool. Optional. Default value is `16`.
-   `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.
-   `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional parameter.
-   `lifetime` - the reload interval of a function in seconds. If it is set to `0` then the function is not reloaded. Default value is `0`. Optional parameter.

The command must read arguments from `STDIN` and must output the result to `STDOUT`. The command must process arguments iteratively. That is after processing a chunk of arguments it must wait for the next chunk.

**Example**

Creating `test_function` using XML configuration.
File test_function.xml.
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

Script file inside `user_scripts` folder `test_function.py`.

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

Query:

``` sql
SELECT test_function_python(toUInt64(2));
```

Result:

``` text
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

Creating `test_function_sum` manually specifying `execute_direct` to `0` using XML configuration.
File test_function.xml.
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
    </function>
</functions>
```

Query:

``` sql
SELECT test_function_sum(2, 2);
```

Result:

``` text
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

Creating `test_function_sum_json` with named arguments and format [JSONEachRow](../../interfaces/formats.md#jsoneachrow) using XML configuration.
File test_function.xml.
```xml
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
```

Script file inside `user_scripts` folder `test_function_sum_json.py`.

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

``` sql
SELECT test_function_sum_json(2, 2);
```

Result:

``` text
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

## Error Handling {#error-handling}

Some functions might throw an exception if the data is invalid. In this case, the query is canceled and an error text is returned to the client. For distributed processing, when an exception occurs on one of the servers, the other servers also attempt to abort the query.

## Evaluation of Argument Expressions {#evaluation-of-argument-expressions}

In almost all programming languages, one of the arguments might not be evaluated for certain operators. This is usually the operators `&&`, `||`, and `?:`.
But in ClickHouse, arguments of functions (operators) are always evaluated. This is because entire parts of columns are evaluated at once, instead of calculating each row separately.

## Performing Functions for Distributed Query Processing {#performing-functions-for-distributed-query-processing}

For distributed query processing, as many stages of query processing as possible are performed on remote servers, and the rest of the stages (merging intermediate results and everything after that) are performed on the requestor server.

This means that functions can be performed on different servers.
For example, in the query `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   if a `distributed_table` has at least two shards, the functions ‘g’ and ‘h’ are performed on remote servers, and the function ‘f’ is performed on the requestor server.
-   if a `distributed_table` has only one shard, all the ‘f’, ‘g’, and ‘h’ functions are performed on this shard’s server.

The result of a function usually does not depend on which server it is performed on. However, sometimes this is important.
For example, functions that work with dictionaries use the dictionary that exists on the server they are running on.
Another example is the `hostName` function, which returns the name of the server it is running on in order to make `GROUP BY` by servers in a `SELECT` query.

If a function in a query is performed on the requestor server, but you need to perform it on remote servers, you can wrap it in an ‘any’ aggregate function or add it to a key in `GROUP BY`.

