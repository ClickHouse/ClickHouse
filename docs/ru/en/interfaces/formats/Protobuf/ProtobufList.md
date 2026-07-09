---
alias: []
description: 'Документация по формату ProtobufList'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `ProtobufList` похож на формат [`Protobuf`](./Protobuf.md), но строки в нём представлены в виде последовательности вложенных сообщений, содержащихся в сообщении с фиксированным именем &quot;Envelope&quot;.

<div id="example-usage">
  ## Пример использования
</div>

Например:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

При этом файл `schemafile.proto` выглядит так:

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

Тип сообщения, указанный в `format_schema`, определяется следующим образом: сначала система ищет его как вложенный тип внутри сообщения верхнего уровня `Envelope`. Если совпадение там не найдено — либо потому, что в схеме нет сообщения `Envelope`, либо потому, что `Envelope` не содержит сообщения с запрошенным именем, — напрямую используется сообщение верхнего уровня с этим именем.

<div id="format-settings">
  ## Настройки формата
</div>
