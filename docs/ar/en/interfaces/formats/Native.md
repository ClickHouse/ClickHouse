---
alias: []
description: 'توثيق لتنسيق Native'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'مرجع'
---

| الإدخال | المخرجات | الاسم البديل |
| ------- | -------- | ------------ |
| ✔       | ✔        |              |

<div id="description">
  ## الوصف
</div>

المواصفة الرسمية الكاملة لتنسيق `Native` متاحة [هنا](/ar/interfaces/specs/NativeFormat)، كما أن المواصفة المرافقة لبروتوكول `Native` — أي بروتوكول TCP السلكي الذي ينقله — متاحة [هنا](/ar/interfaces/specs/NativeProtocol).

:::note
تم إنشاء كلتا المواصفتين بواسطة نماذج لغوية كبيرة (LLMs) انطلاقًا من شيفرة ClickHouse المصدرية. وتظل الشيفرة المرجع الأساسي: فإذا اختلفت المواصفة والشيفرة، فالشيفرة هي الصحيحة.
:::

يُعد تنسيق `Native` أكثر تنسيقات ClickHouse كفاءة لأنه &quot;عمودي&quot; حقًا،
إذ لا يحوّل الأعمدة إلى صفوف.

في هذا التنسيق، تُكتب البيانات وتُقرأ على شكل [كتل](/ar/development/architecture#block) بتنسيق ثنائي.
وبالنسبة إلى كل كتلة، يُسجَّل عدد الصفوف، وعدد الأعمدة، وأسماء الأعمدة وأنواعها، وأجزاء الأعمدة في الكتلة، تباعًا.

هذا هو التنسيق المستخدم في الواجهة الأصلية للتخاطب بين الخوادم، ولاستخدام عميل سطر الأوامر، ولعملاء C++.

:::tip
يمكنك استخدام هذا التنسيق لإنشاء ملفات dump بسرعة، ولا يمكن قراءتها إلا بواسطة نظام إدارة قواعد بيانات ClickHouse.
وقد لا يكون من العملي التعامل مع هذا التنسيق بنفسك.
:::

<div id="data-types-wire-format">
  ## التنسيق السلكي لأنواع البيانات
</div>

تُرسَل البيانات عبر تنسيق النقل بتنسيق عمودي، ما يعني أن كل عمود يُرسَل على حدة،
وتُرسَل جميع قيم العمود معًا على شكل مصفوفة واحدة.

يحتوي كل عمود في كتلة على ترويسة مشابهة لـ [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
عند استخدام بروتوكول `native TCP` الثنائي (أو عندما تستقبل نقطة نهاية HTTP القيمة `?client_protocol_version=<n>`)،
تُكتَب بنية `BlockInfo` قبل عدد الأعمدة والصفوف. تستخدم الأمثلة في هذا القسم
واجهة HTTP العادية من دون إصدار بروتوكول، لذا لا تتضمن `BlockInfo`.
:::

<div id="block-structure">
  ### بنية الكتلة
</div>

يعرض الاستعلام التالي عمودين، `number` و`str`، في ثلاثة صفوف:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

تندرج بيانات الإخراج في كتلة ClickHouse واحدة، وستبدو كما يلي:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

<div id="multiple-blocks">
  ### كتل متعددة
</div>

ومع ذلك، في كثير من الحالات، لا تتسع البيانات داخل كتلة واحدة، لذا يرسل ClickHouse البيانات على هيئة كتل متعددة.
انظر إلى الاستعلام التالي الذي يجلب صفّين مع تقليل حجم الكتلة لفرض تقسيم البيانات بحيث يكون كل صف في كتلة منفصلة:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

المخرجات:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

<div id="simple-data-types">
  ### أنواع البيانات البسيطة
</div>

يشبه تنسيق النقل لقيمة فردية من أحد أنواع البيانات الأبسط تنسيق `RowBinary`/`RowBinaryWithNamesAndTypes`.
تتضمن القائمة الكاملة للأنواع التي ينطبق عليها هذا الوصف ما يلي:

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* DateTime
* DateTime64
* IPv4
* IPv6
* UUID

راجِع أوصاف الأنواع أعلاه في [&quot;تنسيق النقل لأنواع البيانات في RowBinary&quot;](/ar/interfaces/formats/RowBinary#data-types-wire-format) لمزيد من التفاصيل.

<div id="complex-data-types">
  ### أنواع البيانات المعقّدة
</div>

يختلف ترميز الأنواع التالية عن الترميز المستخدم في `RowBinary` و`RowBinaryWithNamesAndTypes`.

* Nullable
* LowCardinality
* Array
* Map
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### Nullable
</div>

في تنسيق `Native`، يحتوي العمود من النوع `Nullable` على عدد من البايتات يساوي عدد الصفوف في الكتلة قبل البيانات الفعلية. ويشير كل بايت من هذه البايتات إلى ما إذا كانت القيمة `NULL` أم لا. على سبيل المثال، في هذا الاستعلام ستكون كل قيمة فردية `NULL` بدلًا من ذلك:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

سيكون الناتج كما يلي:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

يعمل ذلك بطريقة مماثلة مع `Nullable(String)`. يأتي مؤشر `NULL` دائمًا من بايت قناع Nullable —
فقيمة القناع `0x01` تعني أن الصف هو `NULL` بغض النظر عن محتوى السلسلة النصية. أما بالنسبة إلى الصفوف `NULL`،
فتُخزَّن السلسلة النصية الأساسية كسلسلة فارغة (بطول LEB128 مقداره `0`). لاحظ أن السلسلة الفارغة غير `NULL`
يكون لها أيضًا طول LEB128 مقداره `0`، لذا فإن بايت القناع وحده هو ما يميّز بين الحالتين. على سبيل المثال، الاستعلام التالي:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

سيكون الناتج كما يلي:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

<div id="lowcardinality">
  #### LowCardinality
</div>

على عكس [RowBinary](RowBinary/RowBinary.md#lowcardinality) حيث يكون `LowCardinality` شفافًا، يستخدم تنسيق Native ترميزًا عموديًا قائمًا على القاموس. يُرمَّز العمود على هيئة بادئة إصدار، ثم قاموس للقيم الفريدة، ثم Array من فهارس صحيحة تشير إلى هذا القاموس.

:::note
يمكن تعريف العمود على أنه `LowCardinality(Nullable(T))`، لكن لا يمكن تعريفه على أنه `Nullable(LowCardinality(T))` — إذ سيؤدي ذلك دائمًا إلى خطأ من الخادم.
:::

بادئة الإصدار هي `UInt64(LE)` بالقيمة `1`، وتُكتب مرة واحدة لكل عمود. ثم، لكل كتلة، يُكتب ما يلي:

* `UInt64(LE)` — حقل بتات `IndexesSerializationType`. تُشفِّر البتات 0–7 عرض الفهرس (0 = UInt8، 1 = UInt16، 2 = UInt32، 3 = UInt64). لا يتم أبدًا ضبط البت 8 (`NeedGlobalDictionaryBit`) في تنسيق Native (يطرح الخادم استثناءً إذا صادفه). يشير البت 9 إلى وجود مفاتيح قاموس إضافية. ويشير البت 10 إلى ضرورة إعادة تعيين القاموس.
* `UInt64(LE)` — عدد مفاتيح القاموس، تليها المفاتيح مُسلسلةً دفعةً واحدة باستخدام ترميز النوع الداخلي.
* `UInt64(LE)` — عدد rows، تليها قيم الفهارس مُسلسلةً دفعةً واحدة باستخدام عرض UInt المناسب.

يحتوي القاموس دائمًا على قيمة افتراضية عند الفهرس 0 (على سبيل المثال، سلسلة فارغة لـ `String`، و0 للأنواع الرقمية). بالنسبة إلى `LowCardinality(Nullable(T))`، يمثّل الفهرس 0 القيمة `NULL`، وتُسلسَل المفاتيح من دون الغلاف `Nullable`.

على سبيل المثال، `LowCardinality(String)` مع 5 rows `['foo', 'bar', 'baz', 'foo', 'bar']`:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

مع `LowCardinality(Nullable(String))`، يكون المؤشر 0 هو `NULL`:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

<div id="array">
  #### Array
</div>

على عكس [RowBinary](RowBinary/RowBinary.md#array)، حيث تُسبق كل مصفوفة بعدّاد لعناصرها بترميز LEB128، يرمّز تنسيق Native المصفوفات على شكل تيارين فرعيين عموديين:

* عدد N من إزاحات `UInt64` التراكمية (little-endian، 8 بايت لكل إزاحة). يحتوي الصف `i` على `offset[i] - offset[i-1]` عنصرًا، مع اعتبار `offset[-1]` مساويًا ضمنيًا لـ 0.
* جميع العناصر المتداخلة في كل الصفوف، مُسلسلة دفعةً واحدة وبشكل متجاور.

على سبيل المثال، `Array(UInt32)` مع 3 صفوف `[[0, 10], [1, 11], [2, 12]]`:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

تكون للمصفوفة الفارغة الإزاحة نفسها كالصف السابق. على سبيل المثال، `Array(String)` مع 4 صفوف `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

<div id="map">
  #### Map
</div>

يُشفَّر `Map(K, V)` بصيغة `Array(Tuple(K, V))` — أي إزاحات المصفوفة، تليها جميع المفاتيح، ثم جميع القيم. ويختلف هذا عن [RowBinary](RowBinary/RowBinary.md#map)، حيث تأتي المفاتيح والقيم متداخلة في كل إدخال.

على سبيل المثال، `Map(String, UInt64)` مع 3 صفوف `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

<div id="variant">
  #### Variant
</div>

على عكس [RowBinary](RowBinary/RowBinary.md#variant)، حيث يحمل كل صف بايت المميِّز الخاص به متبوعًا بالقيمة بشكل مباشر، يفصل تنسيق Native بين المميِّزات والبيانات.

:::warning
كما في RowBinary، تكون الأنواع في التعريف مرتبة دائمًا أبجديًا، ويكون المميِّز هو الفهرس ضمن تلك القائمة المرتبة. وتمثل `0xFF` (255) القيمة `NULL`.
:::

يُرمَّز عمود `Variant` على النحو التالي:

* بادئة وضع المميِّزات `UInt64(LE)` (`0` = BASIC، `1` = COMPACT). تستخدم مخرجات تنسيق Native عادةً BASIC (`0`)؛ وقد يظهر وضع COMPACT عند قراءة البيانات المخزنة مع تفعيل `use_compact_variant_discriminators_serialization`.
* `N` من مميِّزات `UInt8`، واحد لكل صف.
* بيانات كل variant type في عمود مجمّع منفصل لا يحتوي إلا على الصفوف المطابقة، وذلك وفق ترتيب المميِّزات.

على سبيل المثال، `Variant(String, UInt32)` مع 5 صفوف `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (مرتبة: `String` = 0، `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

<div id="dynamic">
  #### Dynamic
</div>

على عكس [RowBinary](RowBinary/RowBinary.md#dynamic)، حيث تكون كل قيمة ذاتية الوصف (بادئة النوع + القيمة)، يُسلسِل تنسيق Native النوع `Dynamic` على هيئة بادئة بنية، يتبعها عمود [Variant](#variant).

تحتوي بادئة البنية على serialization version من النوع `UInt64(LE)`، ثم عدد الأنواع الديناميكية (بصيغة VarUInt)، ثم type names على شكل سلاسل نصية. في الإصدار V1، يُكتَب عدد الأنواع مرتين لأغراض التوافق. أما البيانات التي تلي ذلك فهي عمود `Variant` تكون type list الخاصة به هي الأنواع الديناميكية بالإضافة إلى النوع الداخلي `SharedVariant`، مرتبةً أبجديًا.

على سبيل المثال، `Dynamic` مع 5 صفوف `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

<div id="json">
  #### JSON
</div>

على عكس [RowBinary](RowBinary/RowBinary.md#json)، حيث يكون كل صف موصوفًا ذاتيًا بأسماء المسارات والقيم، فإن تنسيق Native يُسلسِل `JSON` ببنية عمودية. ويكون الترميز معقدًا ويعتمد على الإصدار: إذ يتألف من بادئة تصف البنية وتتضمن إصدار التسلسل، وأسماء المسارات الديناميكية، وتخطيط البيانات المشتركة، ثم تليها المسارات ذات الأنواع المحددة (كل منها على هيئة عمود مجمّع)، والمسارات الديناميكية (كل منها عمود [Dynamic](#dynamic))، والبيانات المشتركة لمسارات الفائض.

ولتسهيل التوافق، يُنصح باستخدام الإعداد `output_format_native_write_json_as_string=1`، الذي يُسلسِل أعمدة JSON كسلاسل نصية عادية بتنسيق JSON (قيمة `String` واحدة لكل صف).