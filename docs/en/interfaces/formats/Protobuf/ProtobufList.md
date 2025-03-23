---
title : ProtobufList
slug: /interfaces/formats/ProtobufList
keywords : [ProtobufList]
input_format: true
output_format: true
alias: []
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

The `ProtobufList` format is similar to the [`Protobuf`](./Protobuf.md) format but rows are represented as a sequence of sub-messages contained in a message with a fixed name of "Envelope".

## Example Usage

For example:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

Where the file `schemafile.proto` looks like this:

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

## Format Settings