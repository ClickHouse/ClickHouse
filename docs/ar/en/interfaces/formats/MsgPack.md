---
alias: []
description: 'توثيق تنسيق MsgPack'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'مرجع'
---

| إدخال | إخراج | اسم بديل |
| ----- | ----- | -------- |
| ✔     | ✔     |          |

<div id="description">
  ## الوصف
</div>

يدعم ClickHouse قراءة ملفات بيانات [MessagePack](https://msgpack.org/) وكتابتها.

<div id="data-types-matching">
  ## مطابقة أنواع البيانات
</div>

| نوع بيانات MessagePack (`INSERT`)                                  | نوع بيانات ClickHouse                                                                       | نوع بيانات MessagePack (`SELECT`)  |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/ar/sql-reference/data-types/int-uint.md)                                            | `uint N`                           |
| `int N`, `negative fixint`                                         | [`IntN`](/ar/sql-reference/data-types/int-uint.md)                                             | `int N`                            |
| `bool`                                                             | [`UInt8`](/ar/sql-reference/data-types/int-uint.md)                                            | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/ar/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/ar/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [`Float32`](/ar/sql-reference/data-types/float.md)                                             | `float 32`                         |
| `float 64`                                                         | [`Float64`](/ar/sql-reference/data-types/float.md)                                             | `float 64`                         |
| `uint 16`                                                          | [`Date`](/ar/sql-reference/data-types/date.md)                                                 | `uint 16`                          |
| `int 32`                                                           | [`Date32`](/ar/sql-reference/data-types/date32.md)                                             | `int 32`                           |
| `uint 32`                                                          | [`DateTime`](/ar/sql-reference/data-types/datetime.md)                                         | `uint 32`                          |
| `uint 64`                                                          | [`DateTime64`](/ar/sql-reference/data-types/datetime.md)                                       | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/ar/sql-reference/data-types/array.md)/[`Tuple`](/ar/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/ar/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`       |
| `uint 32`                                                          | [`IPv4`](/ar/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                          |
| `bin 8`                                                            | [`String`](/ar/sql-reference/data-types/string.md)                                             | `bin 8`                            |
| `int 8`                                                            | [`Enum8`](/ar/sql-reference/data-types/enum.md)                                                | `int 8`                            |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/ar/sql-reference/data-types/int-uint.md)                            | `bin 8`                            |
| `int 32`                                                           | [`Decimal32`](/ar/sql-reference/data-types/decimal.md)                                         | `int 32`                           |
| `int 64`                                                           | [`Decimal64`](/ar/sql-reference/data-types/decimal.md)                                         | `int 64`                           |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/ar/sql-reference/data-types/decimal.md)                           | `bin 8 `                           |

<div id="example-usage">
  ## مثال للاستخدام
</div>

الكتابة إلى ملف &quot;.msgpk&quot;:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

| الإعداد                                                                                                                            | الوصف                                                                                             | الافتراضي |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | --------- |
| [`input_format_msgpack_number_of_columns`](/ar/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | عدد الأعمدة في بيانات MsgPack التي تم إدراجها. يُستخدم للاستدلال التلقائي على المخطط من البيانات. | `0`       |
| [`output_format_msgpack_uuid_representation`](/ar/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | كيفية إخراج معرّف UUID بتنسيق MsgPack.                                                            | `EXT`     |