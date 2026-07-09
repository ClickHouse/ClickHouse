---
alias: []
description: 'وثائق CapnProto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| الإدخال | الناتج | الاسم المستعار |
| ------- | ------ | -------------- |
| ✔       | ✔      |                |

<div id="description">
  ## الوصف
</div>

تنسيق `CapnProto` هو تنسيق رسائل ثنائي يشبه تنسيق [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) و[Thrift](https://en.wikipedia.org/wiki/Apache_Thrift)، لكنه يختلف عن [JSON](./JSON/JSON.md) و[MessagePack](https://msgpack.org/).
تكون رسائل CapnProto محددة الأنواع بدقة وليست ذاتية الوصف، ما يعني أنها تحتاج إلى وصف مخطط خارجي. ويُطبَّق هذا المخطط عند الحاجة ويُخزَّن مؤقتًا لكل استعلام.

انظر أيضًا إلى [Format Schema](/ar/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## مطابقة أنواع البيانات
</div>

يوضح الجدول أدناه أنواع البيانات المدعومة وكيف تتوافق مع [أنواع البيانات](/ar/sql-reference/data-types/index.md) في ClickHouse ضمن استعلامات `INSERT` و`SELECT`.

| نوع بيانات CapnProto (`INSERT`)                      | نوع بيانات ClickHouse                                                                                                                                  | نوع بيانات CapnProto (`SELECT`)                      |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/ar/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/ar/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/ar/sql-reference/data-types/int-uint.md), [Date](/ar/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/ar/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/ar/sql-reference/data-types/int-uint.md), [DateTime](/ar/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/ar/sql-reference/data-types/int-uint.md), [Decimal32](/ar/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/ar/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/ar/sql-reference/data-types/int-uint.md), [DateTime64](/ar/sql-reference/data-types/datetime.md), [Decimal64](/ar/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/ar/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/ar/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/ar/sql-reference/data-types/string.md), [FixedString](/ar/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/ar/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/ar/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/ar/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/ar/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/ar/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/ar/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/ar/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/ar/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/ar/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* يمكن تحويل أنواع الأعداد الصحيحة بعضها إلى بعض أثناء الإدخال والإخراج.
* للعمل مع `Enum` بتنسيق CapnProto، استخدم الإعداد [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/ar/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode).
* يمكن أن يكون النوع `Array` متداخلًا، ويمكن أن يأخذ قيمة من النوع `Nullable` كوسيطة. كما يمكن أيضًا أن يكون النوعان `Tuple` و`Map` متداخلين.

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### إدراج البيانات والاستعلام عنها
</div>

يمكنك إدراج بيانات CapnProto من ملف في جدول ClickHouse باستخدام الأمر التالي:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

حيث يكون `schema.capnp` على النحو التالي:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

يمكنك تحديد البيانات من جدول في ClickHouse وحفظها في ملف بتنسيق `CapnProto` باستخدام الأمر التالي:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### استخدام المخطط المُولَّد تلقائيًا
</div>

إذا لم يكن لديك مخطط `CapnProto` خارجي لبياناتك، فلا يزال بإمكانك إخراج البيانات أو إدخالها بتنسيق `CapnProto` باستخدام مخطط مُولَّد تلقائيًا.

على سبيل المثال:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

في هذه الحالة، سينشئ ClickHouse تلقائيًا مخطط CapnProto استنادًا إلى بنية الجدول باستخدام الدالة [structureToCapnProtoSchema](/ar/sql-reference/functions/other-functions.md#structureToCapnProtoSchema)، وسيستخدم هذا المخطط لتسلسل البيانات بتنسيق CapnProto.

يمكنك أيضًا قراءة ملف CapnProto باستخدام مخطط مُولَّد تلقائيًا (وفي هذه الحالة يجب أن يكون الملف قد أُنشئ باستخدام المخطط نفسه):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

يكون الإعداد [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) مفعّلًا افتراضيًا، ويُستخدم إذا لم يتم تعيين [`format_schema`](/ar/interfaces/formats#formatschema).

يمكنك أيضًا حفظ المخطط مُولَّد تلقائيًا في ملف أثناء عمليات الإدخال/الإخراج باستخدام الإعداد [`output_format_schema`](/ar/operations/settings/formats#output_format_schema).

على سبيل المثال:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

في هذه الحالة، سيُحفَظ مخطط `CapnProto` المُولَّد تلقائيًا في الملف `path/to/schema/schema.capnp`.