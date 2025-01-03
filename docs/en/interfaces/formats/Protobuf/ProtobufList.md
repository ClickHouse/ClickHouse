---
title : ProtobufList
slug : /en/interfaces/formats/ProtobufList
keywords : [ProtobufList]
---

## Description

Similar to Protobuf but rows are represented as a sequence of sub-messages contained in a message with fixed name "Envelope".

## Example Usage

Usage example:

``` sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

where the file `schemafile.proto` looks like this:

``` capnp
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