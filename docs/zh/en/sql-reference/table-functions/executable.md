---
description: '`executable` 表函数基于用户在将行输出到 **stdout** 的脚本中定义的用户自定义函数（UDF）的输出创建一个表。'
keywords: ['udf', '用户自定义函数', 'ClickHouse', 'executable', '表', '函数']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'reference'
---

`executable` 表函数基于用户在将行输出到 **stdout** 的脚本中定义的用户自定义函数 (UDF) 的输出创建一个表。可执行脚本存储在 `users_scripts` 目录中，并且可以从任何来源读取数据。请确保 ClickHouse server 具备运行该可执行脚本所需的全部软件包。例如，如果它是一个 Python 脚本，请确保 server 已安装所需的 Python 软件包。

你还可以选择包含一个或多个输入查询，将其结果流式传输到 **stdin** 供脚本读取。

:::note
普通 UDF 函数与 `executable` 表函数和 `Executable` 表引擎之间的一个关键区别在于，普通 UDF 函数不能改变行数。例如，如果输入是 100 行，那么结果也必须返回 100 行。使用 `executable` 表函数或 `Executable` 表引擎时，脚本可以执行你所需的任何数据转换，包括复杂聚合。
:::

<div id="syntax">
  ## 语法
</div>

`executable` 表函数需要三个参数，并接受可选的输入查询列表：

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`：脚本的文件名，保存在 `user_scripts` 文件夹中 (即 `user_scripts_path` 设置的默认文件夹) 
* `format`：生成的表的 format
* `structure`：生成的表的 schema
* `input_query`：可选查询 (或 collection 或 queries) ，其结果会通过 **stdin** 传递给脚本

:::note
如果你会使用相同的输入查询反复调用同一个脚本，建议考虑使用 [`Executable` 表引擎](../../engines/table-engines/special/executable.md)。
:::

下面的 Python 脚本名为 `generate_random.py`，保存在 `user_scripts` 文件夹中。它读取一个数字 `i`，并输出 `i` 个随机字符串，每个字符串前都有一个数字，二者之间以制表符分隔：

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

运行该脚本，生成 10 个随机字符串：

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

返回结果如下：

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## 设置
</div>

* `send_chunk_header` - 控制在向进程发送一块数据之前，是否先发送行数。默认值为 `false`。
* `pool_size` — 池大小。如果将 `0` 指定为 `pool_size`，则池大小不受限制。默认值为 `16`。
* `max_command_execution_time` — 处理数据块时，可执行脚本命令的最大执行时间。以秒为单位指定。默认值为 10。
* `command_termination_timeout` — 可执行脚本应包含主读写循环。表函数销毁后，管道会关闭；在 ClickHouse 向子进程发送 SIGTERM 信号之前，可执行文件有 `command_termination_timeout` 秒的时间来关闭。以秒为单位指定。默认值为 10。
* `command_read_timeout` - 以毫秒为单位，从命令的 `stdout` 读取数据的超时时间。默认值为 10000。
* `command_write_timeout` - 以毫秒为单位，向命令的 `stdin` 写入数据的超时时间。默认值为 10000。

<div id="passing-query-results-to-a-script">
  ## 将查询结果传递给脚本
</div>

请务必参阅 `Executable` 表引擎中关于[如何将查询结果传递给脚本](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script)的示例。下面介绍如何使用 `executable` 表函数执行该示例中的同一个脚本：

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```