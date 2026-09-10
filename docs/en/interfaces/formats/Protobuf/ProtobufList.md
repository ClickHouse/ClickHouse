---
alias: []
description: 'Documentation for the ProtobufList format'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `ProtobufList` format is similar to the [`Protobuf`](./Protobuf.md) format but rows are represented as a sequence of sub-messages contained in a message with a fixed name of "Envelope".

## Example usage {#example-usage}

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

The message type specified in `format_schema` is resolved by first looking for it as a nested type inside a top-level `Envelope` message. If no match is found there — either because the schema has no `Envelope` message, or the `Envelope` does not contain a message with the requested name — the top-level message with that name is used directly.

## Format settings {#format-settings}