---
description: '用户自定义函数（UDFs）文档'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: '用户自定义函数（UDFs）'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # UDFs 用户自定义函数
</div>

ClickHouse 支持多种类型的用户自定义函数 (UDFs) ：

* [可执行 UDFs](#executable-user-defined-functions) 会启动外部程序或脚本 (Python、Bash 等) ，并通过 STDIN / STDOUT 以流式方式向其传输数据块。借助它们，你可以在无需重新编译 ClickHouse 的情况下集成现有代码或工具。与进程内方案相比，它们的单次调用开销更高，更适合较重的逻辑，或需要不同运行时的场景。
* [SQL UDFs](#sql-user-defined-functions) 完全通过 SQL 中的 `CREATE FUNCTION` 来定义。它们会被内联/展开到查询计划中 (不存在进程边界) ，因此非常轻量，适合复用表达式逻辑或简化复杂的计算列。
* [Experimental WebAssembly UDFs](#webassembly-user-defined-functions) 在服务器进程内的沙箱中运行编译为 WebAssembly 的代码。与外部可执行程序相比，它们的单次调用开销更低；与原生扩展相比，它们的隔离性更好，因此适合用可编译为 WASM 的语言 (例如 C/C++/Rust) 编写自定义算法。
* [Experimental 基于驱动的可执行 UDFs](#driver-based-executable-user-defined-functions) 允许由运维人员提供的“驱动”将 `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'` 中提供的代码片段在函数创建时转换为可执行 UDF (例如通过编译) 。它们构建在可执行 UDFs 之上，并且需要服务器端的 驱动 配置。

<div id="executable-user-defined-functions">
  ## 可执行用户自定义函数
</div>

<BetaBadge />

:::note
在 ClickHouse Cloud 中，可执行 UDF 目前处于 Public Beta 阶段，并通过 Cloud Console UI 创建。有关 Cloud 特有的工作流程，请参见[Cloud 中的用户自定义函数](/zh/cloud/features/user-defined-functions)。
:::

ClickHouse 可以调用任何外部可执行程序或脚本来处理数据。

可执行用户自定义函数的配置可以位于一个或多个 XML 文件中。
配置路径由 [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config) 参数指定。

函数配置包含以下设置：

| Parameter                     | Description                                                                                                                                                                                                                                                                               | Required | Default Value             |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------- |
| `name`                        | 函数名称                                                                                                                                                                                                                                                                                      | 是        | -                         |
| `command`                     | 要执行的脚本名称；如果 `execute_direct` 为 false，则表示要执行的命令                                                                                                                                                                                                                                            | 是        | -                         |
| `argument`                    | 参数描述，包含参数的 `type`，以及可选的参数 `name`。每个参数都在单独的设置中进行描述。如果参数名是用户自定义函数序列化格式的一部分 (例如 [Native](/zh/interfaces/formats/Native) 或 [JSONEachRow](/zh/interfaces/formats/JSONEachRow)) ，则必须指定名称                                                                                                              | 是        | `c` + argument&#95;number |
| `format`                      | 向命令传递参数时使用的[格式](../../interfaces/formats.md)。命令输出也应使用相同的格式                                                                                                                                                                                                                                | 是        | -                         |
| `return_type`                 | 返回值的类型                                                                                                                                                                                                                                                                                    | 是        | -                         |
| `return_name`                 | 返回值的名称。如果返回值名称是用户自定义函数序列化格式的一部分 (例如 [Native](/zh/interfaces/formats/Native) 或 [JSONEachRow](/zh/interfaces/formats/JSONEachRow)) ，则必须指定返回值名称                                                                                                                                                    | 可选       | `result`                  |
| `type`                        | 可执行类型。如果 `type` 设置为 `executable`，则启动单个命令；如果设置为 `executable_pool`，则创建命令池                                                                                                                                                                                                                   | 是        | -                         |
| `max_command_execution_time`  | 处理数据块的最大执行时间 (秒) 。此设置仅对 `executable_pool` 命令有效                                                                                                                                                                                                                                            | 可选       | `10`                      |
| `command_termination_timeout` | 管道关闭后，命令应在此时间内完成执行 (秒) 。超过该时间后，将向执行该命令的进程发送 `SIGTERM`                                                                                                                                                                                                                                     | 可选       | `10`                      |
| `command_read_timeout`        | 从命令 stdout 读取数据的超时时间 (毫秒)                                                                                                                                                                                                                                                                 | 可选       | `10000`                   |
| `command_write_timeout`       | 向命令 stdin 写入数据的超时时间 (毫秒)                                                                                                                                                                                                                                                                  | 可选       | `10000`                   |
| `pool_size`                   | 命令池的大小                                                                                                                                                                                                                                                                                    | 可选       | `16`                      |
| `send_chunk_header`           | 控制在向进程发送一块数据前，是否先发送行数                                                                                                                                                                                                                                                                     | 可选       | `false`                   |
| `execute_direct`              | 如果 `execute_direct` = `1`，则会在由 [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path) 指定的 user&#95;scripts 文件夹中查找 `command`。可以使用空白分隔符指定额外的脚本参数。例如：`script_name arg1 arg2`。如果 `execute_direct` = `0`，则 `command` 会作为参数传递给 `bin/sh -c` | 可选       | `1`                       |
| `lifetime`                    | 函数的重新加载间隔 (秒) 。如果设置为 `0`，则不会重新加载该函数                                                                                                                                                                                                                                                       | 可选       | `0`                       |
| `deterministic`               | 函数是否为确定性的 (相同输入始终返回相同结果)                                                                                                                                                                                                                                                                  | 可选       | `false`                   |
| `stderr_reaction`             | 如何处理命令的 stderr 输出。取值包括：`none` (忽略) 、`log` (立即记录所有 stderr 输出) 、`log_first` (退出后记录前 4 KiB) 、`log_last` (退出后记录后 4 KiB) 、`throw` (一旦有任何 stderr 输出立即抛出异常) 。当使用 `log_first` 或 `log_last` 且退出码非零时，stderr 内容会包含在异常消息中                                                                             | 可选       | `log_last`                |
| `check_exit_code`             | 如果为 true，ClickHouse 将检查命令的退出码。非零退出码会导致抛出异常                                                                                                                                                                                                                                                | 可选       | `true`                    |

该命令必须从 `STDIN` 读取参数，并将结果输出到 `STDOUT`。该命令必须以迭代方式处理参数。也就是说，处理完一块参数后，它必须等待下一块。

<div id="executable-user-defined-functions">
  ## 可执行用户自定义函数
</div>

<div id="examples">
  ## 示例
</div>

<div id="udf-inline">
  ### 由内联脚本创建的 UDF
</div>

通过 XML 或 YAML 配置手动创建 `test_function_sum`，并将 `execute_direct` 指定为 `0`。

<Tabs>
  <TabItem value="XML" label="XML" default>
    文件 `test_function.xml` (默认路径设置下位于 `/etc/clickhouse-server/test_function.xml`) 。

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
    文件 `test_function.yaml` (默认路径设置下位于 `/etc/clickhouse-server/test_function.yaml`) 。

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

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### 基于 Python 脚本的 UDF
</div>

在本示例中，我们将创建一个 UDF：它从 `STDIN` 读取一个值，并以字符串形式返回。

使用 XML 或 YAML 配置创建 `test_function`。

<Tabs>
  <TabItem value="XML" label="XML" default>
    文件 `test_function.xml` (默认路径设置下位于 `/etc/clickhouse-server/test_function.xml`) 。

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
    文件 `test_function.yaml` (默认路径设置下位于 `/etc/clickhouse-server/test_function.yaml`) 。

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

<br />

在 `user_scripts` 文件夹中创建脚本文件 `test_function.py` (默认路径设置下位于 `/var/lib/clickhouse/user_scripts/test_function.py`) 。

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

<div id="udf-stdin">
  ### 从 `STDIN` 读取两个值，并将它们的和作为 JSON 对象返回
</div>

使用 XML 或 YAML 配置创建 `test_function_sum_json`，并指定命名参数和 [JSONEachRow](/zh/interfaces/formats/JSONEachRow) 格式。

<Tabs>
  <TabItem value="XML" label="XML" default>
    文件 `test_function.xml` (在默认路径设置下为 `/etc/clickhouse-server/test_function.xml`) 。

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
    文件 `test_function.yaml` (在默认路径设置下为 `/etc/clickhouse-server/test_function.yaml`) 。

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

<br />

在 `user_scripts` 文件夹中创建脚本文件 `test_function_sum_json.py` (在默认路径设置下为 `/var/lib/clickhouse/user_scripts/test_function_sum_json.py`) 。

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

<div id="udf-parameters-in-command">
  ### 在 `command` 设置中使用参数
</div>

可执行用户自定义函数可以接收在 `command` 设置中配置的常量参数 (仅适用于 `executable` 类型的用户自定义函数) 。
此外，还需要启用 `execute_direct` 选项，以避免 shell 参数展开漏洞。

<Tabs>
  <TabItem value="XML" label="XML" default>
    文件 `test_function_parameter_python.xml` (在默认路径设置下为 `/etc/clickhouse-server/test_function_parameter_python.xml`) 。

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
    文件 `test_function_parameter_python.yaml` (在默认路径设置下为 `/etc/clickhouse-server/test_function_parameter_python.yaml`) 。

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

<br />

在 `user_scripts` 文件夹中创建脚本文件 `test_function_parameter_python.py` (在默认路径设置下为 `/var/lib/clickhouse/user_scripts/test_function_parameter_python.py`) 。

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

<div id="udf-shell-script">
  ### 基于 shell 脚本的 UDF
</div>

在此示例中，我们将创建一个 shell 脚本，将每个值乘以 2。

<Tabs>
  <TabItem value="XML" label="XML" default>
    文件 `test_function_shell.xml` (默认路径设置下为 `/etc/clickhouse-server/test_function_shell.xml`) 。

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
    文件 `test_function_shell.yaml` (默认路径设置下为 `/etc/clickhouse-server/test_function_shell.yaml`) 。

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

<br />

在 `user_scripts` 文件夹中创建脚本文件 `test_shell.sh` (默认路径设置下为 `/var/lib/clickhouse/user_scripts/test_shell.sh`) 。

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

<div id="error-handling">
  ## 错误处理
</div>

如果数据无效，某些函数可能会抛出异常。
在这种情况下，查询会被取消，并向客户端返回错误信息。
对于分布式处理，当某台服务器上发生异常时，其他服务器也会尝试中止该查询。

<div id="evaluation-of-argument-expressions">
  ## 参数表达式的求值
</div>

在几乎所有编程语言中，对于某些运算符，某个参数可能不会被求值。
通常是运算符 `&&`、`||` 和 `?:`。
在 ClickHouse 中，函数 (运算符) 的参数始终都会被求值。
这是因为系统会一次性对整块列数据进行求值，而不是逐行分别计算。

<div id="performing-functions-for-distributed-query-processing">
  ## 在分布式查询处理中执行函数
</div>

在分布式查询处理中，会尽可能将更多的查询处理阶段放在远程服务器上执行，其余阶段 (合并中间结果以及之后的所有操作) 则在请求方服务器上执行。

这意味着，函数可能会在不同的服务器上执行。
例如，在查询 `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),` 中：

* 如果 `distributed_table` 至少有两个分片，则函数 &#39;g&#39; 和 &#39;h&#39; 在远程服务器上执行，而函数 &#39;f&#39; 在请求方服务器上执行。
* 如果 `distributed_table` 只有一个分片，则 &#39;f&#39;、&#39;g&#39; 和 &#39;h&#39; 这几个函数都会在该分片所在的服务器上执行。

函数的结果通常与它在哪台服务器上执行无关。不过，有时这一点很重要。
例如，使用字典的函数会使用其所在服务器上的字典。
另一个例子是 `hostName` 函数，它会返回其所在服务器的名称，以便在 `SELECT` 查询中按服务器进行 `GROUP BY`。

如果查询中的某个函数是在请求方服务器上执行的，但你需要让它在远程服务器上执行，可以将其包装在 &#39;any&#39; 聚合函数中，或者将其添加到 `GROUP BY` 的键中。

<div id="sql-user-defined-functions">
  ## SQL 用户自定义函数
</div>

可以使用 [CREATE FUNCTION](../statements/create/function.md) 语句创建基于 lambda 表达式的自定义函数。要删除这些函数，请使用 [DROP FUNCTION](../statements/drop.md#drop-function) 语句。

<div id="webassembly-user-defined-functions">
  ## WebAssembly 用户自定义函数
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

WebAssembly 用户自定义函数 (WASM UDF) 允许您在 ClickHouse 服务器进程中运行编译为 WebAssembly 的自定义代码。

<div id="quick-start">
  ### 快速入门
</div>

在 ClickHouse 配置中启用 Experimental WebAssembly 支持：

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

将已编译好的 WASM 模块插入系统表：

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

使用您的 WASM 模块创建函数：

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

在查询中使用此函数：

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### 更多信息
</div>

更多详情，请参阅 [WebAssembly 用户自定义函数](wasm_udf.md) 文档。

<div id="driver-based-executable-user-defined-functions">
  ## 基于驱动的可执行用户自定义函数
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
这是一项 Experimental 功能，可能会在未来的发行版中发生不向后兼容的变更。可通过服务器级设置 [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers) 启用。
:::

*驱动*是由运维方提供的一种适配器，用于将用户代码片段转换为可运行的[可执行 UDF](#executable-user-defined-functions)。当使用 `ENGINE = DriverName(...)` 创建函数时，ClickHouse 会运行该驱动的 `create_command`，并向其传递函数签名和代码主体；驱动会对主体进行编译或其他处理，然后输出一份可执行 UDF 配置，供 ClickHouse 存储和加载。

这样一来，管理员就能为用户提供一种安全且受限的方式，让他们用任意语言定义函数 (例如在沙箱容器中编译的 C) ，而无需授予他们访问服务器配置文件或文件系统的权限。可用驱动的集合完全由运维方控制。

<div id="enabling-drivers">
  ### 启用驱动
</div>

基于驱动的可执行 UDF 默认处于禁用状态。要启用它们，请执行以下操作：

1. 在服务器配置中启用 Experimental 开关：

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. 将 [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) 指向一个或多个驱动配置文件 (支持 glob) ，并可选设置 [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path)，即用于存储生成的可执行 UDF 配置的目录：

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

驱动 registry 会在服务器启动时加载，并在 `SYSTEM RELOAD CONFIG` 时刷新，因此无需重启服务器即可添加、修改或移除驱动。

<div id="driver-configuration">
  ### 驱动配置
</div>

驱动由一个以 `<driver>` 元素为顶层元素的 XML (或 YAML) 文件描述。支持以下字段：

| 字段                 | 描述                                                                                           | 必填 |
| ------------------ | -------------------------------------------------------------------------------------------- | -- |
| `name`             | 驱动名称，用于 `CREATE FUNCTION ... ENGINE = <name>(...)`。                                          | 是  |
| `create_command`   | 用于根据代码片段创建 UDF 时调用的程序路径。相对路径会相对于驱动配置文件解析。                                                    | 是  |
| `drop_command`     | 删除基于此驱动的函数时调用的程序路径。                                                                          | 否  |
| `engine_arguments` | 声明 `ENGINE = DriverName(...)` 中允许使用的参数。每个子元素都是一个参数名；`<required>true</required>` 子元素表示该参数为必填。 | 否  |
| `env`              | 调用驱动命令时导出的环境变量。                                                                              | 否  |

驱动配置示例：

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### 驱动程序调用约定
</div>

运行 `CREATE FUNCTION` 时，会调用 `create_command`，并设置已配置的 `env` 变量，同时传入以下参数：

* `--name <function_name>`
* `--return <return_type>` (如果存在 `RETURNS` 子句)
* `--args <signature>` (如果存在 `ARGUMENTS` 子句`），其中签名是已声明的参数列表，例如 `x UInt8, y DateTime&#96;
* 对于在 `ENGINE = DriverName(key = value)` 中提供的每个已声明引擎参数，传入 `--<key> <value>`

用户代码主体 (即 `AS` 之后的文本) 会发送到该命令的标准输入。该命令必须将 可执行 UDF 的配置打印到标准输出。格式会自动检测：以 `<` 开头的输出会被视为 XML，否则视为 YAML。生成配置中定义的函数名必须与正在创建的名称一致。如果 `create_command` 以非零退出状态结束，则该语句会失败，并抛出包含退出码和驱动程序标准错误的异常。

如果存在 `drop_command`，则在删除函数时也会以相同方式调用它 (但不会通过 stdin 传入代码主体) 。

<div id="creating-a-function-with-a-driver">
  ### 创建 FUNCTION
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

ClickHouse 运行 驱动 的 `create_command`，将生成的配置写入 [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path)，现有的 可执行 UDF 加载器随后会自动读取该配置。之后，这个函数就可以像调用其他函数一样调用。

<div id="dropping-a-function-with-a-driver">
  ### 删除函数
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

`DROP FUNCTION` 会调用驱动的 `drop_command` (如果存在) ，删除生成的动态配置以及每个函数对应的工作目录，重新加载 可执行 UDF 加载器，并删除已持久化的查询。

<div id="driver-persistence-and-restart">
  ### 持久化与重启
</div>

源查询会以 `ATTACH FUNCTION ...` 语句的形式持久化到用户定义的 SQL 对象目录中，因此该函数在服务器重启后仍然可用。启动时，会直接加载 [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) 中生成的配置，而不会重新运行 驱动。如果某个已持久化的 `ATTACH FUNCTION` 没有对应的已生成配置 (例如动态目录丢失) ，则会重新运行 驱动 以重新创建该配置。

<div id="driver-limitations">
  ### 限制
</div>

* 该功能为 Experimental，且受 `allow_experimental_executable_udf_drivers` 控制。
* 基于驱动程序的函数不支持复制型用户自定义函数存储 (`ON CLUSTER` 和 `<user_defined_zookeeper_path>`) ，因为只有发起的查询会被复制，生成的制品不会被复制。
* 对已备份的基于驱动程序的函数执行 `RESTORE` 时，会保留查询，但不会重新运行驱动程序；生成的配置会在后续的重启恢复过程中被 materialized。

<div id="example-c-drivers">
  ### 示例 C 驱动程序
</div>

源码树在 `programs/server/user_defined_executable_function_drivers_config.d/` 下提供了用于编译和运行 C 函数体的概念验证驱动。它们仅作为示例，**不会随软件包安装**：

* `DockerC` - 在经过沙箱隔离的 Docker 容器中编译并运行代码 (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`，外加内存/CPU/PID 限制) ，并生成一个 `executable_pool` UDF。
* `GVisorC` - 一种变体，在 [gVisor](https://gvisor.dev/) `runsc` runtime 下运行编译后的 binary。
* `UnsafeC` - 直接在主机上编译并运行代码，不使用沙箱。顾名思义，它不提供任何隔离，仅适用于受信任环境和测试。

这些示例驱动程序可作为起点；在将它们开放给不受信任的用户之前，请先根据你的环境审查并加固沙箱机制。

<div id="related-content">
  ## 相关内容
</div>

* [ClickHouse Cloud 中的用户自定义函数](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)