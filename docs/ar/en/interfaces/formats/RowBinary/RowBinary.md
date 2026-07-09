---
alias: []
description: 'توثيق تنسيق RowBinary'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: 'مرجع'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| الإدخال | الإخراج | اسم بديل |
| ------- | ------- | -------- |
| ✔       | ✔       |          |

<div id="description">
  ## الوصف
</div>

يحلّل تنسيق `RowBinary` البيانات صفًا صفًا بتنسيق ثنائي.
تُدرج الصفوف والقيم بشكل متتالٍ، من دون فواصل.
ونظرًا لأن البيانات بالتنسيق الثنائي، فإن المحدِّد بعد `FORMAT RowBinary` يكون محددًا بدقة كما يلي:

* أي عدد من المحارف البيضاء:
  * `' '` (مسافة - الرمز `0x20`)
  * `'\t'` (جدولة - الرمز `0x09`)
  * `'\f'` (form feed - الرمز `0x0C`)
* يتبع ذلك تسلسل سطر جديد واحد فقط:
  * بنمط Windows `"\r\n"`
  * أو بنمط Unix `'\n'`
* ثم تليه مباشرة بيانات ثنائية.

:::note
هذا التنسيق أقل كفاءة من تنسيق [Native](../Native.md) لأنه قائم على الصفوف.
:::

<div id="data-types-wire-format">
  ## تنسيق النقل لأنواع البيانات
</div>

:::tip
يمكن تنفيذ معظم الاستعلامات الواردة في الأمثلة باستخدام curl مع توجيه الإخراج إلى ملف.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

بعد ذلك، يمكن فحص البيانات باستخدام محرر بالنظام السداسي العشري.

<div id="unsigned-leb128">
  ### ‏LEB128 غير الموقَّع (الأساس 128 بترتيب little-endian)
</div>

ترميزٌ بعرض متغيّر للأعداد الصحيحة **غير الموقَّعة بترتيب little-endian**، ويُستخدم لترميز أطوال أنواع البيانات متغيّرة الحجم مثل `String` و`Array` و`Map`. يمكن العثور على تنفيذ نموذجي في [صفحة LEB128 على ويكيبيديا](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

تُرمَّز جميع أنواع الأعداد الصحيحة بعدد مناسب من البايتات وفق ترتيب **little-endian**. وتستخدم الأنواع الموقَّعة (`Int8` حتى `Int256`) تمثيل **متمم الاثنين**. تدعم معظم اللغات استخراج هذه الأعداد الصحيحة من مصفوفات البايتات، سواء باستخدام الأدوات المدمجة أو المكتبات المعروفة. أمّا `Int128`/`Int256` و`UInt128`/`UInt256`، ونظرًا إلى أنها تتجاوز أحجام الأعداد الصحيحة الأصلية في معظم اللغات، فقد يلزم استخدام فك تسلسل مخصّص لها.

<div id="bool">
  ### Bool
</div>

تُرمَّز القيم المنطقية في بايت واحد، ويمكن فك تسلسلها بطريقة مماثلة لـ `UInt8`.

* `0` تعني `false`
* `1` تعني `true`

<div id="float32-float64">
  ### Float32, Float64
</div>

**الأعداد ذات الفاصلة العائمة بترتيب little-endian**، وتُشفَّر في 4 بايت لـ `Float32` و8 بايت لـ `Float64`. وكما هو الحال مع الأعداد الصحيحة، توفّر معظم اللغات أدوات مناسبة لإلغاء تسلسل هذه القيم.

<div id="bfloat16">
  ### BFloat16
</div>

‏[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) ‏(Brain Floating Point) هو تنسيق فاصلة عائمة بطول 16 بت، يتمتع بنطاق Float32 مع دقة أقل، ما يجعله مفيدًا لأحمال عمل التعلّم الآلي. وتنسيق النقل فيه هو عمليًا أعلى 16 بت من قيمة Float32. إذا كانت لغتك لا تدعمه دعمًا أصيلًا، فأسهل طريقة للتعامل معه هي قراءته وكتابته بصيغة UInt16، مع التحويل من Float32 وإليه:

لتحويل BFloat16 إلى Float32 (شيفرة شبه برمجية):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

لتحويل Float32 إلى BFloat16 (شيفرة شبه برمجية):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

أمثلة على القيم الداخلية لـ `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

تُمثَّل الأنواع العشرية كأعداد صحيحة **little-endian** بعرض البت الموافق لكل نوع.

* `Decimal32` - 4 بايت، أو `Int32`.
* `Decimal64` - 8 بايت، أو `Int64`.
* `Decimal128` - 16 بايت، أو `Int128`.
* `Decimal256` - 32 بايت، أو `Int256`.

عند فك تسلسل قيمة `Decimal`، يمكن اشتقاق الجزأين الصحيح والكسري باستخدام شيفرة شبه برمجية التالية:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

حيث يجري `trunc` القطع باتجاه الصفر (وليس القسمة إلى الأسفل، التي تختلف مع القيم السالبة)، و`scale` هو عدد الخانات بعد الفاصلة العشرية. على سبيل المثال، في `Decimal(10, 2)` (وهو مكافئ لـ `Decimal32(2)`)، تكون قيمة `scale` هي `2`، وستُمثَّل القيمة `12345` على الشكل `(123, 45)`.

يتطلب التسلسل إجراء العملية العكسية:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

راجع مزيدًا من التفاصيل في [وثائق ClickHouse حول أنواع Decimal](https://clickhouse.com/docs/sql-reference/data-types/decimal).

<div id="string">
  ### String
</div>

سلاسل ClickHouse هي **تسلسلات عشوائية من البايتات**. ولا يُشترط أن تكون بترميز UTF-8 صالح. وبادئة الطول هي **طول البايتات**، وليس عدد المحارف.

تُرمَّز في جزأين:

1. عدد صحيح بطول متغير (LEB128) يشير إلى طول السلسلة بالبايتات.
2. البايتات الخام للسلسلة.

على سبيل المثال، ستُرمَّز السلسلة `foobar` باستخدام *سبعة* بايتات كما يلي:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

<div id="fixedstring">
  ### FixedString
</div>

على عكس `String`، يكون لـ `FixedString` طول ثابت يُحدَّد في الـ schema. ويُشفَّر كتسلسل من البايتات، مع إضافة بايتات صفرية في النهاية إذا كانت القيمة أقصر من `N`.

:::note
عند قراءة `FixedString`، قد تكون البايتات الصفرية في النهاية إما حشوًا أو محارف `\0` فعلية في البيانات، ولا يمكن التمييز بينها في تنسيق النقل. ويحافظ ClickHouse نفسه على جميع البايتات الـ `N` كما هي.
:::

يحتوي `FixedString(3)` الفارغ على أصفار الحشو فقط:

```text
0x00, 0x00, 0x00
```

قيمة `FixedString(3)` غير الفارغة التي تحتوي على السلسلة `hi`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

سلسلة `FixedString(3)` غير فارغة تحتوي على السلسلة `bar`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

لا حاجة إلى أي بايتات حشو في المثال الأخير، لأن البايتات *الثلاثة* كلها مستخدمة.

<div id="date">
  ### Date
</div>

يُخزَّن على هيئة `UInt16` (بايتان)، ويمثّل عدد الأيام ***منذ*** `1970-01-01`.

نطاق القيم المدعوم: `[1970-01-01, 2149-06-06]`.

أمثلة على القيم الداخلية لـ `Date`:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

يُخزَّن كـ `Int32` (أربعة بايتات) لتمثيل عدد الأيام ***قبل*** `1970-01-01` ***أو بعده***.

النطاق المدعوم للقيم: `[1900-01-01, 2299-12-31]`.

أمثلة على القيم الداخلية لـ `Date32`:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

تاريخ قبل بداية الحقبة:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### DateTime
</div>

يُخزَّن بصيغة `UInt32` (أربعة بايتات)، ويمثّل عدد الثواني ***منذ*** `1970-01-01 00:00:00 UTC`.

البنية:

```text
DateTime([timezone])
```

على سبيل المثال، `DateTime` أو `DateTime('UTC')`.

:::note
تكون القيمة الثنائية دائمًا إزاحة epoch وفق UTC. ولا تغيّر المنطقة الزمنية الترميز. ومع ذلك، تؤثر المنطقة الزمنية فعلًا في كيفية تفسير القيم النصية عند الإدراج: فإدراج `'2024-01-15 10:30:00'` في عمود `DateTime('America/New_York')` يخزّن قيمة epoch مختلفة عن إدراج السلسلة نفسها في عمود `DateTime('UTC')`، لأن السلسلة تُفسَّر على أنها وقت محلي في المنطقة الزمنية للعمود. في تنسيق النقل، كلاهما مجرد `UInt32` يمثّل ثواني epoch.
:::

النطاق المدعوم للقيم: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

أمثلة على القيم الداخلية الأساسية لـ `DateTime`:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

<div id="datetime64">
  ### DateTime64
</div>

يُخزَّن على هيئة `Int64` (ثمانية بايتات)، ويمثّل عدد وحدات **tick** ***قبل*** أو ***بعد*** `1970-01-01 00:00:00 UTC`. ويُحدَّد مستوى دقة الـ `tick` بواسطة المعامل `precision`، راجع الصياغة أدناه:

```text
DateTime64(precision, [timezone])
```

حيث إن `precision` عدد صحيح من `0` إلى `9`. وعادةً لا يُستخدم سوى ما يلي: `3` (ميلي ثانية)، و`6` (ميكروثانية)،
و`9` (نانوثانية).

أمثلة على تعريفات `DateTime64` الصالحة: `DateTime64(0)`، و`DateTime64(3)`، و`DateTime64(6, 'UTC')`، أو `DateTime64(9, 'Europe/Amsterdam')`.

:::note
كما هو الحال مع `DateTime`، تكون القيمة الثنائية دائمًا إزاحةً عن حقبة UTC. وتؤثر المنطقة الزمنية في كيفية تفسير القيم النصية عند الإدراج (راجع ملاحظة [DateTime](#datetime))، لكن الترميز نفسه يكون دائمًا على هيئة `Int64` من وحدات `tick` منذ حقبة UTC.
:::

يمكن تفسير قيمة `Int64` الأساسية للنوع `DateTime64` على أنها عدد الوحدات التالية قبل حقبة Unix أو بعدها:

* `DateTime64(0)` - ثوانٍ.
* `DateTime64(3)` - ميلي ثانية.
* `DateTime64(6)` - ميكروثانية.
* `DateTime64(9)` - نانوثانية.

النطاق المدعوم للقيم: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

نماذج للقيم الأساسية لـ `DateTime64`:

* `DateTime64(3)`: تمثل القيمة `1546300800000` ‏`2019-01-01 00:00:00 UTC`.
* `DateTime64(6)`: تمثل القيمة `1705314600123456` ‏`2024-01-15 10:30:00.123456 UTC`.
* `DateTime64(9)`: تمثل القيمة `1705314600123456789` ‏`2024-01-15 10:30:00.123456789 UTC`.

:::note
دقة القيمة القصوى هي 8. وإذا استُخدمت الدقة القصوى البالغة 9 أرقام (نانوثانية)، فإن القيمة القصوى المدعومة هي 2262-04-11 23:47:16 بتوقيت UTC.
:::

<div id="time">
  ### Time
</div>

يُخزَّن بصيغة `Int32` لتمثيل قيمة زمنية بالثواني. القيم السالبة مسموح بها.

النطاق المدعوم للقيم: `[-999:59:59, 999:59:59]` (أي `[-3599999, 3599999]` ثانية).

:::note
في الوقت الحالي، يجب ضبط الإعداد `enable_time_time64_type` على `1` لاستخدام `Time` أو `Time64`.
:::

قيم التمثيل الداخلية النموذجية لـ `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

<div id="time64">
  ### Time64
</div>

يُخزَّن داخليًا على هيئة `Decimal64` (ويُخزَّن `Decimal64` بدوره على هيئة `Int64`) لتمثيل قيمة زمنية بأجزاء من الثانية، مع دقة قابلة للضبط. القيم السالبة صالحة.

الصيغة:

```text
Time64(precision)
```

حيث تكون `precision` عددًا صحيحًا من `0` إلى `9`. القيم الشائعة: `3` (ميلي ثانية)، `6` (ميكروثانية)، `9` (نانوثانية).

نطاق القيم المدعوم: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
في الوقت الحالي، يجب تعيين الإعداد `enable_time_time64_type` إلى `1` لاستخدام `Time` أو `Time64`.
:::

تمثل قيمة `Int64` الداخلية أجزاء الثانية بعد تحجيمها بمقدار `10^precision`.

أمثلة على القيم الداخلية لـ `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

<div id="interval-types">
  ### أنواع interval
</div>

تُخزَّن جميع أنواع interval بصيغة `Int64` (ثمانية بايتات، little-endian). وتمثل القيمة عدد وحدات الزمن المقابلة. والقيم السالبة صالحة.

أنواع interval هي: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
يحدِّد اسم نوع interval (على سبيل المثال، `IntervalSecond` مقابل `IntervalDay`) وحدةَ القيمة المخزَّنة. أما تنسيق النقل فيبقى دائمًا كما هو.
:::

أمثلة على القيم الداخلية المخزنة:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

<div id="enum8-enum16">
  ### Enum8, Enum16
</div>

تُخزَّن في بايت واحد (`Enum8` == `Int8`) أو بايتين (`Enum16` == `Int16`) لتمثيل فهرس قيمة الـ enum ضمن تعريفه. لاحظ أن نوع التخزين **موقَّع** — لذا يمكن أن تكون قيم الـ enum سالبة (مثلًا: `Enum8('a' = -128, 'b' = 0)`).

يمكن تعريف Enum بطريقة بسيطة، كما يلي:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

سيكون لـ Enum8 المعرّف أعلاه خريطة القيم التالية على جانب العميل:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

أو بشكل أكثر تعقيدًا، على النحو التالي:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

سيكون لـ Enum16 المعرّف أعلاه خريطة القيم التالية في جهة العميل:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

بالنسبة إلى محلّل أنواع البيانات، يتمثل التحدي الرئيسي في تتبّع الرموز المُفلَتة في تعريف `enum`، مثل `\'`، والرموز الخاصة مثل `=` التي قد تظهر داخل السلاسل النصية المحصورة بين علامتَي اقتباس.

<div id="uuid">
  ### معرّف UUID
</div>

يُمثَّل كتسلسل من 16 بايتًا. يُخزَّن معرّف UUID على شكل **قيمتَي `UInt64` بترتيب little-endian**: تُعكس البايتات الثمانية الأولى من التمثيل القياسي لمعرّف UUID، وتُعكس البايتات الثمانية الثانية بشكل مستقل أيضًا.

على سبيل المثال، إذا كان معرّف UUID هو `61f0c404-5cb3-11e7-907b-a6006ad3dba0`:

* التمثيل القياسي للبايتات: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* النصف الأول بعد العكس (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* النصف الثاني بعد العكس (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

القيم الداخلية النموذجية لـ `UUID`:

* يُمثَّل `61f0c404-5cb3-11e7-907b-a6006ad3dba0` على النحو التالي:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* يُمثَّل معرّف UUID الافتراضي `00000000-0000-0000-0000-000000000000` على شكل 16 بايتًا من الأصفار:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

يمكن استخدامه عند إدراج سجل جديد إذا لم تُحدَّد قيمة معرّف UUID.

<div id="ipv4">
  ### IPv4
</div>

يُخزَّن في أربعة بايتات بصيغة `UInt32` وبترتيب البايتات **little-endian**. لاحظ أن هذا يختلف عن ترتيب البايتات التقليدي للشبكة (big-endian) المستخدم شائعًا لعناوين IP. أمثلة على القيم الداخلية الأساسية لـ `IPv4`:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

<div id="ipv6">
  ### IPv6
</div>

يُخزَّن في 16 بايتًا بترتيب **big-endian / network byte order** (البايت الأكثر أهمية MSB أولًا). أمثلة على القيم الداخلية لـ `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

<div id="nullable">
  ### Nullable
</div>

يُشفَّر نوع البيانات Nullable على النحو التالي:

1. بايت واحد يوضّح ما إذا كانت القيمة `NULL` أم لا:
   * `0x00` تعني أن القيمة ليست `NULL`.
   * `0x01` تعني أن القيمة هي `NULL`.
2. إذا لم تكن القيمة `NULL`، فيُشفَّر نوع البيانات الأساسي كالمعتاد. أما إذا كانت القيمة `NULL`، **فلا تُكتب أي بايتات إضافية** للنوع الأساسي.

على سبيل المثال، قيمة `Nullable(UInt32)`:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

<div id="lowcardinality">
  ### LowCardinality
</div>

في تنسيق RowBinary، لا يؤثر وسم low-cardinality في تنسيق النقل. على سبيل المثال، يُرمَّز `LowCardinality(String)` بالطريقة نفسها التي يُرمَّز بها `String` العادي.

:::warning
ينطبق هذا على RowBinary فقط. في تنسيق Native، يستخدم `LowCardinality` ترميزًا مختلفًا قائمًا على القاموس.
:::

:::note
يمكن تعريف عمود على أنه `LowCardinality(Nullable(T))`، لكن لا يمكن تعريفه على أنه `Nullable(LowCardinality(T))` — إذ سيؤدي ذلك دائمًا إلى خطأ من الخادم.
:::

أثناء الاختبار، يمكن ضبط [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) على `1` للسماح بمعظم أنواع البيانات داخل `LowCardinality` بهدف تحقيق تغطية أفضل.

<div id="array">
  ### المصفوفة
</div>

تُشفَّر المصفوفة كما يلي:

1. عدد صحيح [بطول متغير (LEB128)](#unsigned-leb128) يحدد عدد العناصر في المصفوفة.
2. عناصر المصفوفة، مُشفَّرة بالطريقة نفسها المستخدمة مع نوع البيانات الأساسي.

على سبيل المثال، مصفوفة بقيم `UInt32`:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

مثال أكثر تعقيدًا قليلًا:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
يمكن أن تحتوي المصفوفة على قيم تقبل NULL، لكن لا يمكن أن تكون المصفوفة نفسها كذلك.
:::

ما يلي صالح:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

وسيُرمَّز على النحو التالي:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

يمكن الاطلاع على مثال للتعامل مع المصفوفات متعددة الأبعاد في [قسم Geo](#geo-types).

<div id="tuple">
  ### Tuple
</div>

يُشفَّر Tuple بحيث تأتي جميع عناصره متتابعةً، كلٌّ منها وفق تنسيق النقل المقابل، من دون أي معلومات وصفية إضافية أو فواصل.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

يفرض التمثيل النصي لنوع البيانات Tuple تحديات مشابهة لما هو الحال مع [نوع Enum](#enum8-enum16)، مثل تتبّع الرموز المُفلَتة والمحارف الخاصة؛ ومع Tuple يصبح من الضروري أيضًا تتبّع الأقواس المفتوحة والمغلقة. بالإضافة إلى ذلك، لاحظ أن أكثر قيم Tuples تعقيدًا قد تحتوي على Tuples متداخلة أخرى، وArrays، وMaps، وحتى enums.

على سبيل المثال، في الجدول التالي، يحتوي الـ tuple على enum يتضمن علامة اقتباس مفردة وقوسًا في الاسم، مما قد يسبب مشكلات في التحليل إذا لم يُعالَج بشكل صحيح:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

<div id="map">
  ### Map
</div>

يمكن اعتبار الـ map على أنه `Array(Tuple(K, V))`، حيث يشير `K` إلى نوع المفتاح ويشير `V` إلى نوع القيمة. ويُرمَّز الـ map على النحو التالي:

1. عدد صحيح [متغير الطول (LEB128)](#unsigned-leb128) يشير إلى عدد العناصر في الـ map.
2. عناصر الـ map على هيئة أزواج مفتاح-قيمة، وتُرمَّز وفقًا لأنواعها المقابلة.

على سبيل المثال، map بمفاتيح `String` وقيم `UInt32`:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
يمكن أن توجد قيم من النوع map ذات بُنى متداخلة بعمق، مثل `Map(String, Map(Int32, Array(Nullable(String))))`، وستُرمَّز بطريقة مماثلة لما ورد وصفه أعلاه.
:::

<div id="variant">
  ### Variant
</div>

يمثل هذا النوع union لأنواع بيانات أخرى. ويعني النوع `Variant(T1, T2, ..., TN)` أن كل صف من هذا النوع يحتوي على قيمة من النوع `T1` أو `T2` أو … أو `TN`، أو لا ينتمي إلى أيٍّ منها (أي تكون القيمة `NULL`).

:::warning
مع أن `Variant(T1, T2)` يعني للمستخدم النهائي تمامًا الشيء نفسه الذي يعنيه `Variant(T2, T1)`، فإن ترتيب الأنواع في التعريف مهم في تنسيق النقل: إذ تُرتَّب الأنواع في التعريف دائمًا ترتيبًا أبجديًا، وهذا مهم لأن البديل الفعلي يُشفَّر بواسطة &quot;المميِّز&quot; — أي فهرس نوع البيانات في التعريف.
:::

تأمل المثال التالي:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

تُرمَّز قيمة `NULL` ببايت تمييز قيمته `0xFF`:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

يمكن استخدام الإعداد [allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) للسماح بإجراء اختبارات أكثر شمولًا للنوع `Variant`.

<div id="dynamic">
  ### Dynamic
</div>

يمكن لنوع `Dynamic` أن يحتوي على قيم من أي نوع، ويُحدَّد ذلك في وقت التشغيل. في تنسيق RowBinary، تكون كل قيمة ذاتية الوصف: يكون الجزء الأول هو مواصفة النوع بهذا [التنسيق](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding). ثم يأتي المحتوى بعد ذلك، مع ترميز القيمة كما هو موضّح في هذا المستند. لذا، لتحليل قيمة، ما عليك سوى استخدام فهرس النوع لتحديد المُحلِّل المناسب، ثم إعادة استخدام آلية تحليل RowBinary المتوفرة لديك بالفعل في موضع آخر.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

إذ إن `BinaryTypeIndex` هو بايت واحد يحدّد النوع. راجع المرجع [هنا](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding) للاطلاع على فهارس الأنواع والمعلمات.

تُشفَّر قيمة Dynamic من النوع `NULL` باستخدام `BinaryTypeIndex` `0x00` (النوع `Nothing`)، من دون أي بايتات إضافية:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**أمثلة:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

<div id="json">
  ### JSON
</div>

يُشفِّر نوع JSON البيانات ضمن فئتين متمايزتين:

1. **المسارات محددة النوع** - المسارات المُعلنة بأنواع صريحة في المخطط (مثل `JSON(user_id UInt32, name String)`)
2. **المسارات الديناميكية/مسارات الفائض عند تجاوز حد المسارات الديناميكية** - المسارات التي تُكتشف أثناء التشغيل وتُخزَّن بالنوع `Dynamic`. يسبق تعريفُ النوع ترميزَ القيمة.

يختلف wire format والقواعد المطبّقة لكلتا الفئتين.

| فئة المسار                    | مضمّن في التمثيل التسلسلي | ترميز القيمة           | يُسمح بـ Variant/Nullable |
| ----------------------------- | ------------------------- | ---------------------- | ------------------------- |
| **المسارات ذات النوع المحدد** | دائمًا (حتى إن كانت NULL) | تنسيق ثنائي خاص بالنوع | نعم                       |
| **مسارات Dynamic**            | فقط إذا كانت غير NULL     | Dynamic                | لا                        |

تُسلسَل المسارات في ثلاث مجموعات تُكتب بشكل تسلسلي: المسارات ذات الأنواع المحددة، والمسارات الديناميكية، ثم مسارات البيانات المشتركة (الفائضة). تُكتب المسارات ذات الأنواع المحددة والمسارات الديناميكية بترتيب يعتمد على التنفيذ (يُحدَّد عبر تكرار خريطة التجزئة الداخلية)، في حين تُكتب مسارات البيانات المشتركة بترتيب أبجدي. لا ينبغي للمكوّنات القارئة الاعتماد على أي ترتيب محدد للمسارات؛ إذ يُوزَّع كل مسار في مرحلة إلغاء التسلسل استناداً إلى اسمه لا إلى موضعه.

يُسلسَل كل صف JSON بتنسيق RowBinary على النحو التالي:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**أمثلة:**

**1. JSON بسيط يحتوي على مسارات ذات أنواع محددة فقط:**

Schema: `JSON(user_id UInt32, active Bool)`

صف: `{"user_id": 42, "active": true}`

الترميز الثنائي (سداسي عشري مع تعليقات توضيحية):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. JSON بسيط مع مسارات محددة النوع ومسارات Dynamic:**

Schema: `JSON(user_id UInt32, active Bool)`

صف: `{"user_id": 42, "active": true, "name": "Alice"}`

الترميز الثنائي (ست عشري مع تعليقات توضيحية):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. معالجة NULL:**

مع عمود Nullable محدد النوع، تحصل على null:

Schema: `JSON(score Nullable(Int32))`

صف: `{"score": null }`

الترميز الثنائي (ست عشري مع تعليقات توضيحية):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

مع عمود محدد النوع وغير قابل للقيم الفارغة، ستحصل على القيمة الافتراضية:

المخطط: `JSON(name String)`

صف: `{"name": null}`

الترميز الثنائي:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

مع المسار الديناميكي، يُتجاهل:

المخطط: `JSON(id UInt64)`

صف: `{"id": 100, "metadata": null}`

الترميز الثنائي:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

ملاحظة: المسار `metadata` الذي يحمل قيمة NULL **غير مُدرج** لأن المسارات الديناميكية لا تُسلسَل إلا عند كونها غير NULL. وهذا فارق جوهري عن المسارات ذات الأنواع المحددة.

**4. كائنات JSON المتداخلة:**

المخطط: `JSON()`

الصف: `{"user": {"name": "Bob", "age": 30}}`

الترميز الثنائي (سداسي عشري مع تعليقات توضيحية):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

ملاحظة: تُسطَّح الكائنات من النوع Nested إلى مسارات مفصولة بنقاط (مثل `user.name` بدلًا من بنية متداخلة).

**بديل: JSON في وضع String**

باستخدام الإعداد `output_format_binary_write_json_as_string=1`، تُسلسَل أعمدة JSON كسلسلة نصية واحدة بتنسيق JSON بدلًا من التنسيق الثنائي المنظَّم. ويوجد إعداد مقابل للكتابة إلى أعمدة JSON، وهو `input_format_binary_read_json_as_string`. ويعتمد اختيار الإعداد هنا على ما إذا كنت تريد تحليل JSON في العميل أم في الخادم.

<div id="geo-types">
  ### أنواع Geo
</div>

Geo هي فئة من أنواع البيانات التي تمثل البيانات الجغرافية. وتشمل:

* `Point` - على شكل `Tuple(Float64, Float64)`.
* `Ring` - على شكل `Array(Point)` أو `Array(Tuple(Float64, Float64))`.
* `Polygon` - على شكل `Array(Ring)` أو `Array(Array(Tuple(Float64, Float64)))`.
* `MultiPolygon` - على شكل `Array(Polygon)` أو `Array(Array(Array(Tuple(Float64, Float64))))`.
* `LineString` - على شكل `Array(Point)` أو `Array(Tuple(Float64, Float64))`.
* `MultiLineString` - على شكل `Array(LineString)` أو `Array(Array(Tuple(Float64, Float64)))`.

تنسيق النقل لقيم Geo مطابقة تمامًا لتلك الخاصة بـ Tuple وArray. وستحتوي رؤوس تنسيق `RowBinaryWithNamesAndTypes` على الأسماء المستعارة لهذه الأنواع، مثل `Point` و`Ring` و`Polygon` و`MultiPolygon` و`LineString` و`MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

<div id="geometry">
  ### Geometry
</div>

`Geometry` هو نوع `Variant` يمكنه احتواء أيٍّ من أنواع Geo المذكورة أعلاه. في تنسيق النقل، يُرمَّز بالطريقة نفسها تمامًا مثل `Variant`، مع بايت المميِّز يحدّد نوع Geo الذي يليه.

فهارس المميِّز لـ Geometry هي:

| الفهرس | النوع           |
| ------ | --------------- |
| 0      | LineString      |
| 1      | MultiLineString |
| 2      | MultiPolygon    |
| 3      | Point           |
| 4      | Polygon         |
| 5      | Ring            |

بنية تنسيق النقل:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

مثال على ترميز `Point` بوصفه `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

مثال على ترميز `Ring` بوصفه `Geometry`:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

<div id="nested">
  ### Nested
</div>

يعتمد تنسيق النقل الثنائي لـ `Nested` على الإعداد `flatten_nested`.

:::warning
يجب أن تكون جميع مصفوفات المكوّنات في الصف الواحد **بالطول نفسه**. هذا قيد يفرضه الخادم. وسيؤدي عدم تطابق الأطوال إلى أخطاء في الإدراج.
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (الافتراضي)
</div>

باستخدام الإعداد الافتراضي، يُسطَّح `Nested` إلى مصفوفات مستقلة. ويصبح كل عمود فرعي عمود `Array` منفصلًا باسم تفصل بين أجزائه نقاط:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` يعرض الأعمدة بعد تسطيحها:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

تُسلسَل كل مصفوفة على نحو مستقل، كما هو موضح في قسم [Array](#array):

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

<div id="nested-unflattened">
  #### `flatten_nested = 0`
</div>

عند ضبط `flatten_nested = 0`، يُحتفَظ بـ `Nested` كعمود واحد من النوع `Array(Tuple(...))`. ولا يُفصل اسم العمود بنقاط:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

يُظهر `DESCRIBE TABLE foo` عمودًا واحدًا:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

الترميز هو `Array(Tuple(String, Int32))`: بادئة لطول المصفوفة، ثم حقول Tuple لكل عنصر بالترتيب:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

لاحظ كيف تتعاقب الحقول بحسب كل عنصر (a₁, b₁, a₂, b₂) بدلًا من أن تكون مجمّعة حسب العمود (a₁, a₂, b₁, b₂)، كما في التمثيل المسطَّح.

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

يُشفَّر `SimpleAggregateFunction(func, T)` بنفس الطريقة تمامًا مثل نوع البيانات الأساسي `T`. ولا يؤثر اسم الدالة التجميعية في تنسيق النقل.

على سبيل المثال، يُشفَّر `SimpleAggregateFunction(max, UInt32)` بالطريقة نفسها التي يُشفَّر بها `UInt32` عادي:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

تُظهر ترويسة RowBinaryWithNamesAndTypes النوع على هيئة `SimpleAggregateFunction(max, UInt32)`، لكن القيمة في تنسيق النقل هي مجرد `UInt32`:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

يخزّن `AggregateFunction(func, T)` الحالة الوسيطة الكاملة لدالة تجميع. وعلى عكس `SimpleAggregateFunction`، التي تخزّن أيضًا حالة وسيطة لكنها ترمّزها بصورة مطابقة لنوع البيانات الأساسي، فإن `AggregateFunction` يخزّن كائنًا ثنائيًا معتمًا بصيغة خاصة بكل دالة تجميع.

:::warning
لا تحتوي حالات التجميع على **بادئة طول** في RowBinary. يجب أن يفهم المُحلِّل صيغة التسلسل الداخلية لكل دالة تجميع بعينها حتى يعرف عدد البايتات التي ينبغي قراءتها. عمليًا، تتعامل معظم العملاء مع حالات التجميع على أنها كائنات معتمة، وتستخدم المجمّعات `*State` / `*Merge` ليتولى الخادم معالجة التسلسل.
:::

تختلف الصيغة الداخلية باختلاف الدالة. بعض الأمثلة البسيطة:

**`countState`** — يخزّن العدد على هيئة VarUInt ‏(LEB128):

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — يخزّن المجموع التراكمي في عدد صحيح ذي حجم ثابت. ويعتمد عدد البتات على نوع الوسيطة (`UInt64` للوسيطات الصحيحة):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — يخزّن بايت علامة، تليه القيمة من النوع الأساسي. تكون العلامة `0x00` لحالة فارغة (من دون أي قيم مُسجَّلة) أو `0x01` عند وجود قيمة:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

حالة فارغة (لا توجد صفوف مُجمَّعة):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
تستخدم الدوال الأكثر تعقيدًا مثل `uniq` و`quantile` و`groupArray` تنسيقات خاصة بالتنفيذ. إذا كنت بحاجة إلى قراءة هذه الحالات أو كتابتها، فارجع إلى الشيفرة المصدرية في ClickHouse للدالة المعنية.
:::

<div id="qbit">
  ### QBit
</div>

`QBit` هو نوع متجهي للبحث بكفاءة مع مستويات متفاوتة من الدقة. ويُخزَّن داخليًا بتنسيق منقول. في تنسيق النقل، يكون QBit ببساطة `Array` من نوع العنصر الأساسي (`Int8` أو `Float32` أو `Float64` أو `BFloat16`). ويحدث تحسين تبديل البتات للتخزين على الخادم، وليس في بروتوكول RowBinary.

البنية:

```text
QBit(element_type, dimension[, stride])
```

حيث يكون `element_type` هو `Int8` أو `Float32` أو `Float64` أو `BFloat16`، وتكون `dimension` هي البعد الثابت للمتجه. أما `stride` الاختياري، فيقتصر دوره على التحكم في كيفية تجميع مستويات البت ضمن تدفقات التخزين على جهة الخادم؛ ولا يؤثر في تنسيق النقل لـ `RowBinary`، والذي يكون دائمًا المصفوفة الكاملة المكوّنة من `dimension` عنصرًا.

تنسيق النقل: مطابق لـ `Array(element_type)`:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

مثال لترميز `QBit(Float32, 4)` الذي يحتوي على `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<RowBinaryFormatSettings />