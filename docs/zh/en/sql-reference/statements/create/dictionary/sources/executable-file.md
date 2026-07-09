---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: '可执行文件字典源'
sidebar_position: 3
sidebar_label: '可执行文件'
description: '将可执行文件配置为 ClickHouse 中的字典源。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

使用可执行文件的方式取决于[字典在内存中的存储布局](../layouts/)。如果字典采用 `cache` 和 `complex_key_cache` 存储，ClickHouse 会通过向可执行文件的 STDIN 发送请求来获取所需的键。否则，ClickHouse 会启动该可执行文件，并将其输出视为字典数据。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

设置字段：

| Setting                       | Description                                                                                                                                                                                                                                                                                   |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | 可执行文件的绝对路径，或文件名 (如果命令所在目录在 `PATH` 中) 。                                                                                                                                                                                                                                                        |
| `format`                      | 文件格式。支持 [Formats](/zh/sql-reference/formats) 中描述的所有格式。                                                                                                                                                                                                                                           |
| `command_termination_timeout` | 可执行脚本应包含一个主读写循环。字典销毁后，管道会关闭，可执行文件会有 `command_termination_timeout` 秒的时间自行退出，之后 ClickHouse 会向子进程发送 SIGTERM 信号。该值以秒为单位指定。默认值为 `10`。可选。                                                                                                                                                           |
| `command_read_timeout`        | 从命令 stdout 读取数据的超时时间 (以毫秒为单位) 。默认值为 `10000`。可选。                                                                                                                                                                                                                                               |
| `command_write_timeout`       | 向命令 stdin 写入数据的超时时间 (以毫秒为单位) 。默认值为 `10000`。可选。                                                                                                                                                                                                                                                |
| `implicit_key`                | 可执行源文件可以只返回值，与所请求键的对应关系由结果中行的顺序隐式确定。默认值为 `false`。                                                                                                                                                                                                                                             |
| `execute_direct`              | 如果 `execute_direct` = `1`，则会在 [user&#95;scripts&#95;path](/zh/operations/server-configuration-parameters/settings#user_scripts_path) 指定的 user&#95;scripts 文件夹中查找 `command`。可使用空白分隔符指定额外的脚本参数。示例：`script_name arg1 arg2`。如果 `execute_direct` = `0`，则会将 `command` 作为参数传递给 `bin/sh -c`。默认值为 `0`。可选。 |
| `send_chunk_header`           | 控制在向进程发送一个数据块之前，是否先发送行数。默认值为 `false`。可选。                                                                                                                                                                                                                                                      |

该字典源只能通过 XML 配置进行配置。通过 DDL 创建使用可执行源的字典已被禁用；否则，数据库用户将能够在 ClickHouse 节点上执行任意二进制文件。