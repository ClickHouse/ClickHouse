---
alias: []
description: 'توثيق صيغة Protobuf'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| المدخل | المخرج | الاسم المستعار |
| ------ | ------ | -------------- |
| ✔      | ✔      |                |

<div id="description">
  ## الوصف
</div>

تنسيق `Protobuf` هو تنسيق [Protocol Buffers](https://protobuf.dev/).

يتطلب هذا التنسيق مخطط تنسيق خارجيًا، ويُخزَّن مؤقتًا بين الاستعلامات.

يدعم ClickHouse ما يلي:

* كلًا من الصيغتين `proto2` و`proto3`.
* الحقول `Repeated`/`optional`/`required`.

للعثور على المطابقة بين أعمدة الجدول وحقول نوع رسالة Protocol Buffers، يقارن ClickHouse بين أسمائها.
وهذه المقارنة غير حساسة لحالة الأحرف، ويُتعامل مع المحرفين `_` (شرطة سفلية) و`.` (نقطة) على أنهما متكافئان.
إذا اختلف نوع العمود عن نوع الحقل في رسالة Protocol Buffers، فسيُطبَّق التحويل اللازم.

الرسائل المتداخلة مدعومة. على سبيل المثال، بالنسبة إلى الحقل `z` في نوع الرسالة التالي:

```capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

يحاول ClickHouse العثور على عمود باسم `x.y.z` (أو `x_y_z` أو `X.y_Z` وما إلى ذلك).

تُعدّ الرسائل المتداخلة مناسبة لإدخال [بنية بيانات متداخلة](/ar/sql-reference/data-types/nested-data-structures/index.md) أو إخراجها.

لا تُطبَّق القيم الافتراضية المعرَّفة في مخطط Protobuf، مثل المخطط التالي، بل تُستخدَم [القيم الافتراضية للجدول](/ar/sql-reference/statements/create/table#default_values) بدلًا منها:

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

إذا كانت الرسالة تحتوي على [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) وكان `input_format_protobuf_oneof_presence` مضبوطًا، فإن ClickHouse يملأ العمود الذي يحدد أيّ حقل من oneof كان موجودًا.

```capnp
syntax = "proto3";

message StringOrString {
  oneof string_oneof {
    string string1 = 1;
    string string2 = 42;
  }
}
```

```sql
CREATE TABLE string_or_string ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string
```

```text
   ┌─────────┬─────────┬──────────────┐
   │ string1 │ string2 │ string_oneof │
   ├─────────┼─────────┼──────────────┤
1. │         │ string2 │ world        │
   ├─────────┼─────────┼──────────────┤
2. │ string1 │         │ hello        │
   └─────────┴─────────┴──────────────┘
```

يجب أن يكون اسم العمود الذي يشير إلى وجود القيمة مطابقًا لاسم `oneof`.
الرسائل المتداخلة مدعومة (راجع  [basic-examples](#basic-examples)). كما أن الرسائل الفارغة مدعومة أيضًا.
الأنواع المسموح بها هي Int8 وUInt8 وInt16 وUInt16 وInt32 وUInt32 وInt64 وUInt64 وEnum وEnum8 أو Enum16.
يجب أن يحتوي Enum (وكذلك Enum8 أو Enum16) على جميع الوسوم الممكنة لـ `oneof`، بالإضافة إلى 0 للإشارة إلى عدم الوجود، ولا يهم التمثيل النصي.

يكون الإعداد [`input_format_protobuf_oneof_presence`](/ar/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) معطّلًا افتراضيًا

يقوم ClickHouse بقراءة رسائل protobuf وكتابتها بتنسيق `length-delimited`.
وهذا يعني أنه يجب كتابة طول كل رسالة قبلها على هيئة [عدد صحيح متغيّر الطول (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

<div id="example-usage">
  ## مثال للاستخدام
</div>

<div id="basic-examples">
  ### قراءة البيانات وكتابتها
</div>

:::note ملفات المثال
الملفات المستخدمة في هذا المثال متاحة في [مستودع الأمثلة](https://github.com/ClickHouse/formats/ProtoBuf)
:::

في هذا المثال، سنقرأ بعض البيانات من الملف `protobuf_message.bin` إلى جدول في ClickHouse. ثم سنكتبها
مرة أخرى إلى ملف باسم `protobuf_message_from_clickhouse.bin` باستخدام تنسيق `Protobuf`.

لنفترض وجود الملف `schemafile.proto`:

```capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

<details>
  <summary>إنشاء الملف الثنائي</summary>

  إذا كنت تعرف بالفعل كيفية تسلسل البيانات وإلغاء تسلسلها بتنسيق `Protobuf`، يمكنك تخطي هذه الخطوة.

  سنستخدم بايثون لتسلسل بعض البيانات إلى `protobuf_message.bin` وقراءتها في ClickHouse.
  إذا كنت تريد استخدام لغة أخرى، فراجع أيضًا: [&quot;كيفية قراءة/كتابة رسائل Protobuf المحددة بالطول في اللغات الشائعة&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

  شغّل الأمر التالي لإنشاء ملف بايثون باسم `schemafile_pb2.py` في
  الدليل نفسه الذي يوجد فيه `schemafile.proto`. يحتوي هذا الملف على فئات بايثون
  التي تمثل رسالة `UserData` في Protobuf:

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  أنشئ الآن ملف بايثون جديدًا باسم `generate_protobuf_data.py`، في الدليل نفسه
  الذي يوجد فيه `schemafile_pb2.py`. الصق فيه الشيفرة التالية:

  ```python
  import schemafile_pb2  # وحدة أنشأها 'protoc'
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # استيراد مُرمِّز varint الداخلي

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      ينشئ رسالة UserData في Protobuf ويملؤها بالبيانات.
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # البيانات الخاصة بمستخدمي المثال لدينا
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # افتح الملف الثنائي في وضع الكتابة الثنائية ('wb')
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # أنشئ مثيل رسالة Protobuf للمستخدم الحالي
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # سلّسل الرسالة
          serialized_data = message.SerializeToString()

          # احصل على طول البيانات المتسلسلة
          message_length = len(serialized_data)

          # استخدم _VarintBytes الداخلي من مكتبة Protobuf لترميز الطول
          length_prefix = _VarintBytes(message_length)

          # اكتب بادئة الطول
          f.write(length_prefix)
          # اكتب بيانات الرسالة المتسلسلة
          f.write(serialized_data)

  print(f"Protobuf messages (length-delimited) written to {output_filename}")

  # --- اختياري: التحقق (إعادة القراءة والطباعة) ---
  # عند إعادة القراءة، سنستخدم أيضًا مفكك ترميز Protobuf الداخلي لـ varints.
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- Verifying by reading back ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # اقرأ الملف بالكامل في مخزن مؤقت لتسهيل فك ترميز varint
      n = 0
      while n < len(buf):
          # فك ترميز بادئة طول varint
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # استخرج بيانات الرسالة
          message_data = buf[n:n+msg_len]
          n += msg_len

          # حلّل الرسالة
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  شغّل الآن البرنامج النصي من سطر الأوامر. ويُوصى بتشغيله من
  بيئة بايثون افتراضية، على سبيل المثال باستخدام `uv`:

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  ستحتاج إلى تثبيت مكتبات بايثون التالية:

  ```bash
  uv pip install --upgrade protobuf
  ```

  شغّل البرنامج النصي لإنشاء الملف الثنائي:

  ```bash
  python generate_protobuf_data.py
  ```
</details>

أنشئ جدول ClickHouse يطابق المخطط:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

أدرِج البيانات في الجدول عبر سطر الأوامر:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

يمكنك أيضًا إعادة كتابة البيانات إلى ملف ثنائي باستخدام تنسيق `Protobuf`:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

باستخدام مخطط Protobuf لديك، يمكنك الآن إلغاء تسلسل البيانات التي كتبها ClickHouse إلى الملف `protobuf_message_from_clickhouse.bin`.

<div id="basic-examples-cloud">
  ### قراءة البيانات وكتابتها باستخدام ClickHouse Cloud
</div>

مع ClickHouse Cloud، لا يمكنك تحميل ملف مخطط Protobuf. ومع ذلك، يمكنك استخدام الإعداد `format_protobuf_schema`
لتحديد المخطط ضمن الاستعلام. في هذا المثال، نوضح كيفية قراءة البيانات المُسلسلة من جهازك المحلي
وإدراجها في جدول في ClickHouse Cloud.

وكما في المثال السابق، أنشئ الجدول وفقًا للمخطط المحدد في مخطط Protobuf الخاص بك في ClickHouse Cloud:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

يحدّد الإعداد `format_schema_source` مصدر الإعداد `format_schema`

القيم الممكنة:

* &#39;file&#39; (الافتراضي): غير مدعوم في Cloud
* &#39;string&#39;: تكون `format_schema` هي المحتوى الحرفي للمخطط.
* &#39;query&#39;: تكون `format_schema` استعلامًا لجلب المخطط.

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

لإدراج البيانات في ClickHouse Cloud مع تحديد المخطط على هيئة سلسلة نصية، شغّل:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

اعرض البيانات المُدخلة في الجدول:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="format-schema-source-query">
  ### `format_schema_source='query'`
</div>

يمكنك أيضًا تخزين مخطط Protobuf في جدول.

أنشئ جدولًا على ClickHouse Cloud لإدراج البيانات فيه:

```sql
CREATE TABLE testing.protobuf_schema (
  schema String
)
ENGINE = MergeTree()
ORDER BY tuple();
```

```sql
INSERT INTO testing.protobuf_schema VALUES ('syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};');
```

أدرِج البيانات في ClickHouse Cloud، مع تحديد المخطط عبر استعلام للتشغيل:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

اعرض البيانات المُدرجة في الجدول:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### استخدام مخطط مُولَّد تلقائيًا
</div>

إذا لم يكن لديك مخطط Protobuf خارجي لبياناتك، فلا يزال بإمكانك إخراج البيانات وإدخالها بتنسيق Protobuf
باستخدام مخطط مُولَّد تلقائيًا. لهذا الغرض، استخدم الإعداد `format_protobuf_use_autogenerated_schema`.

على سبيل المثال:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

في هذه الحالة، سيُنشئ ClickHouse مخطط Protobuf تلقائيًا وفقًا لبنية الجدول باستخدام الدالة
[`structureToProtobufSchema`](/ar/sql-reference/functions/other-functions#structureToProtobufSchema). ثم سيستخدم هذا المخطط لتسلسل البيانات بتنسيق Protobuf.

يمكنك أيضًا قراءة ملف Protobuf باستخدام مخطط مُولَّد تلقائيًا. في هذه الحالة، من الضروري أن يكون الملف قد أُنشئ باستخدام المخطط نفسه:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

الإعداد [`format_protobuf_use_autogenerated_schema`](/ar/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) مُفعَّل افتراضيًا، ويُطبَّق إذا لم يتم تعيين [`format_schema`](/ar/operations/settings/formats#format_schema).

يمكنك أيضًا حفظ المخطط المُولَّد تلقائيًا في الملف أثناء الإدخال/الإخراج باستخدام الإعداد [`output_format_schema`](/ar/operations/settings/formats#output_format_schema). على سبيل المثال:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

في هذه الحالة، سيُحفَظ مخطط Protobuf المُولَّد تلقائيًا في الملف `path/to/schema/schema.capnp`.

<div id="drop-protobuf-cache">
  ### حذف ذاكرة التخزين المؤقت لـ Protobuf
</div>

لإعادة تحميل مخطط Protobuf المُحمَّل من [`format_schema_path`](/ar/operations/server-configuration-parameters/settings.md/#format_schema_path)، استخدم تعليمة [`SYSTEM DROP ... FORMAT CACHE`](/ar/sql-reference/statements/system.md/#system-drop-schema-format).

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```