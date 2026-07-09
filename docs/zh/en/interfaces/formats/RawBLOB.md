---
description: 'RawBLOB 格式文档'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## 说明
</div>

`RawBLOB` 格式会将所有输入数据读取为一个单一值。它只能解析仅包含一个 [`String`](/zh/sql-reference/data-types/string.md) 类型字段或类似类型字段的表。
结果会以不带分隔符和转义的二进制格式输出。如果输出的值不止一个，这种格式就会产生歧义，因而无法再将数据读回。

<div id="raw-formats-comparison">
  ### 原始格式对比
</div>

下面比较 `RawBLOB` 和 [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md) 格式。

`RawBLOB`：

* 数据以二进制格式输出，不进行转义；
* 值之间没有分隔符；
* 每个值末尾都没有换行符。

`TabSeparatedRaw`：

* 数据输出时不进行转义；
* 每行包含以制表符分隔的值；
* 每一行最后一个值后都有一个换行符。

下面比较 `RawBLOB` 和 [RowBinary](./RowBinary/RowBinary.md) 格式。

`RawBLOB`：

* String 字段输出时不带长度前缀。

`RowBinary`：

* String 字段表示为 varint 格式的长度 (无符号 [LEB128] (https://en.wikipedia.org/wiki/LEB128)) ，后跟字符串的字节。

当向 `RawBLOB` 输入传入空数据时，ClickHouse 会抛出异常：

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## 示例用法
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## 格式设置
</div>
