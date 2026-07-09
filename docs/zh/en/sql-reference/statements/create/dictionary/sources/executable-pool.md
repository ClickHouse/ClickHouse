---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: '可执行程序池字典源'
sidebar_position: 4
sidebar_label: '可执行程序池'
description: '在 ClickHouse 中将可执行程序池配置为字典源。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Executable pool 允许从进程池加载数据。
该源不适用于需要从源加载全部数据的字典布局。

如果字典[存储](../layouts/#storing-dictionaries-in-memory)时使用以下任一布局，Executable pool 即可工作：

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

Executable pool 会使用指定命令启动一个进程池，并让这些进程持续运行直到退出。程序应在 STDIN 可用时从中读取数据，并将结果输出到 STDOUT。它可以等待 STDIN 上的下一个数据块。ClickHouse 在处理完一个数据块后不会关闭 STDIN，而是在需要时再通过管道传输另一段数据。可执行脚本应能够适应这种数据处理方式——它应轮询 STDIN，并尽早将数据刷新到 STDOUT。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

设置字段：

| 设置                            | 说明                                                                                                                                                                                                                                                                                            |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | 可执行文件的绝对路径，或文件名 (如果程序所在目录已写入 `PATH`) 。                                                                                                                                                                                                                                                        |
| `format`                      | 文件格式。支持[格式](/zh/sql-reference/formats)中描述的所有格式。                                                                                                                                                                                                                                                  |
| `pool_size`                   | 池大小。如果将 `0` 指定为 `pool_size`，则池大小不受限制。默认值为 `16`。                                                                                                                                                                                                                                               |
| `command_termination_timeout` | 可执行脚本应包含主读写循环。字典销毁后，管道会关闭，可执行文件会在 ClickHouse 向子进程发送 SIGTERM 信号之前获得 `command_termination_timeout` 秒的关闭时间。以秒为单位。默认值为 `10`。可选。                                                                                                                                                                   |
| `max_command_execution_time`  | 可执行脚本命令处理数据块时的最大执行时间。以秒为单位。默认值为 `10`。可选。                                                                                                                                                                                                                                                      |
| `command_read_timeout`        | 从命令的 stdout 读取数据的超时时间，单位为毫秒。默认值为 `10000`。可选。                                                                                                                                                                                                                                                  |
| `command_write_timeout`       | 向命令的 stdin 写入数据的超时时间，单位为毫秒。默认值为 `10000`。可选。                                                                                                                                                                                                                                                   |
| `implicit_key`                | 可执行源文件可以只返回值，与请求键的对应关系由结果中行的顺序隐式确定。默认值为 `false`。可选。                                                                                                                                                                                                                                           |
| `execute_direct`              | 如果 `execute_direct` = `1`，则会在由 [user&#95;scripts&#95;path](/zh/operations/server-configuration-parameters/settings#user_scripts_path) 指定的 user&#95;scripts 文件夹中查找 `command`。可使用空白分隔符指定额外的脚本参数。例如：`script_name arg1 arg2`。如果 `execute_direct` = `0`，则 `command` 会作为参数传递给 `bin/sh -c`。默认值为 `1`。可选。 |
| `send_chunk_header`           | 控制在向进程发送一段数据前是否先发送行数。默认值为 `false`。可选。                                                                                                                                                                                                                                                         |

该字典源只能通过 XML 配置进行配置。通过 DDL 创建带有 executable 源的字典已被禁用，否则数据库用户将能够在 ClickHouse 节点上执行任意 binary。