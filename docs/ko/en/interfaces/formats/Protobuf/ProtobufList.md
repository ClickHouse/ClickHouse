---
alias: []
description: 'ProtobufList 포맷 문서'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`ProtobufList` 포맷은 [`Protobuf`](./Protobuf.md) 포맷과 유사하지만, 행은 &quot;Envelope&quot;라는 고정된 이름의 메시지에 포함된 하위 메시지들의 시퀀스로 표현됩니다.

<div id="example-usage">
  ## 사용 예시
</div>

예를 들어:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

`schemafile.proto` 파일의 내용은 다음과 같습니다:

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

`format_schema`에 지정된 메시지 유형은 먼저 최상위 `Envelope` 메시지 안의 중첩 유형으로 찾아 해석됩니다. 여기서 일치하는 항목을 찾지 못하면 — 스키마에 `Envelope` 메시지가 없거나 `Envelope`에 요청한 이름의 메시지가 없는 경우 — 해당 이름의 최상위 메시지를 직접 사용합니다.

<div id="format-settings">
  ## 포맷 설정
</div>
