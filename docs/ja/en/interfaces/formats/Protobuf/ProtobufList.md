---
alias: []
description: 'ProtobufList フォーマットに関するドキュメント'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

`ProtobufList` フォーマットは [`Protobuf`](./Protobuf.md) フォーマットに似ていますが、行は &quot;Envelope&quot; という固定名のメッセージに含まれるサブメッセージの並びとして表現されます。

<div id="example-usage">
  ## 使用例
</div>

たとえば、

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

ファイル `schemafile.proto` の内容は次のとおりです:

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

`format_schema` で指定されたメッセージ型は、まず最上位の `Envelope` メッセージ内のネストされた型として探されます。そこで一致が見つからない場合、つまりスキーマに `Envelope` メッセージが存在しない場合、または `Envelope` に要求された名前のメッセージが含まれていない場合は、その名前を持つ最上位メッセージが直接使用されます。

<div id="format-settings">
  ## フォーマット設定
</div>
