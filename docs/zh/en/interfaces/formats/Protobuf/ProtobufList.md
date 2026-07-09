---
alias: []
description: 'ProtobufList 格式文档'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

`ProtobufList` 格式与 [`Protobuf`](./Protobuf.md) 格式类似，但其中的行表示为一组子消息序列，这些子消息包含在一个名称固定为 &quot;Envelope&quot; 的消息中。

<div id="example-usage">
  ## 示例用法
</div>

例如：

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

其中，`schemafile.proto` 文件内容如下：

```capnp title="schemafile.proto"
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

`format_schema` 中指定的消息类型会先尝试解析为顶层 `Envelope` 消息内部的嵌套类型。如果在那里没有找到匹配项——无论是因为 schema 中没有 `Envelope` 消息，还是因为 `Envelope` 中不包含名称与请求名称相符的消息——则直接使用同名的顶层消息。

<div id="format-settings">
  ## 格式设置
</div>
