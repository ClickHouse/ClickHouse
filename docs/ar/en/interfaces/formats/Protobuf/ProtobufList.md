---
alias: []
description: 'توثيق تنسيق ProtobufList'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| الإدخال | الناتج | الاسم المستعار |
| ------- | ------ | -------------- |
| ✔       | ✔      |                |

<div id="description">
  ## الوصف
</div>

يشبه تنسيق `ProtobufList` التنسيق [`Protobuf`](./Protobuf.md)، لكن تُمثَّل الصفوف كتسلسل من الرسائل الفرعية المضمَّنة داخل رسالة ذات اسم ثابت هو &quot;Envelope&quot;.

<div id="example-usage">
  ## مثال للاستخدام
</div>

على سبيل المثال:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

حيث يكون الملف `schemafile.proto` على النحو التالي:

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

يُحلَّل نوع الرسالة المحدَّد في `format_schema` عبر البحث عنه أولًا باعتباره نوعًا متداخلًا داخل رسالة `Envelope` من المستوى الأعلى. وإذا لم يُعثر هناك على تطابق — إما لأن المخطط لا يحتوي على رسالة `Envelope`، أو لأن `Envelope` لا تتضمن رسالة بالاسم المطلوب — فتُستخدم مباشرةً الرسالة من المستوى الأعلى التي تحمل هذا الاسم.

<div id="format-settings">
  ## إعدادات التنسيق
</div>
