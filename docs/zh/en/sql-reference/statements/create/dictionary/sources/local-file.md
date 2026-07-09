---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: '本地文件字典源'
sidebar_position: 2
sidebar_label: '本地文件'
description: '将本地文件配置为 ClickHouse 的字典源。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

本地文件源会从本地文件系统中的文件加载字典数据。这适用于较小且静态的查找表，这类表可以以 TSV、CSV 或任何其他[支持的格式](/zh/sql-reference/formats)的平面文件形式存储。

设置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

设置字段：

| 设置       | 说明                                           |
| -------- | -------------------------------------------- |
| `path`   | 文件的绝对路径。                                     |
| `format` | 文件格式。支持[格式](/zh/sql-reference/formats)中介绍的所有格式。 |

当通过 DDL 命令 (`CREATE DICTIONARY ...`) 创建 source 为 `FILE` 的字典时，源文件必须位于 `user_files` 目录中，以防止数据库用户访问 ClickHouse 节点上的任意文件。

**另请参见**

* [字典函数](/zh/sql-reference/table-functions/dictionary)