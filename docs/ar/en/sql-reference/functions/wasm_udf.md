---
description: 'توثيق الدوال المعرّفة من قبل المستخدم في WebAssembly'
sidebar_label: 'WebAssembly UDFs'
slug: /sql-reference/functions/wasm_udf
title: 'الدوال المعرّفة من قبل المستخدم في WebAssembly'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # دوال WebAssembly المعرّفة من قبل المستخدم
</div>

يدعم ClickHouse إنشاء دوال معرّفة من قبل المستخدم (UDFs) مكتوبة بـ WebAssembly. ويتيح لك ذلك تنفيذ منطق مخصّص مكتوب بلغات مثل Rust أو C أو C++ أو غيرها، بعد تجميعه إلى وحدات WebAssembly.

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## نظرة عامة
</div>

وحدة WebAssembly هي ملف ثنائي مُصرَّف يحتوي على دالة واحدة أو أكثر يمكن لـ ClickHouse استدعاؤها.
يمكنك اعتبار الوحدة بمثابة مكتبة أو كائن مشترك تُحمّله مرة واحدة ثم تعيد استخدامه مرات عديدة.

يمكن كتابة وحدة WebAssembly التي تحتوي على UDFs بأي لغة يمكنها التصرّف إلى WebAssembly، مثل Rust أو C أو C++.

تعمل شيفرة WebAssembly المُصرَّفة (شيفرة &quot;guest&quot;) والتي ينفذها ClickHouse (&quot;host&quot;) داخل بيئة معزولة لا تملك وصولًا إلا إلى مساحة ذاكرة مخصصة.

تُصدّر شيفرة guest دوالًا يمكن لـ ClickHouse استدعاؤها، وتشمل هذه الدوال تلك التي تنفّذ منطقك المخصص (المستخدم لتعريف UDFs)، بالإضافة إلى دوال الدعم المطلوبة لإدارة الذاكرة وتبادل البيانات بين ClickHouse وشفرة WebAssembly.

يجب تصريف شيفرتك إلى WebAssembly &quot;freestanding&quot; (المعروف أيضًا باسم `wasm32-unknown-unknown`) من دون أي تبعيات على نظام تشغيل أو مكتبة قياسية. كما أن هدف WebAssembly الافتراضي ذي 32 بت فقط هو المدعوم (من دون امتداد `wasm64`).
ويجب أن تلتزم الوحدة بأحد بروتوكولات الاتصال (ABIs) المدعومة للتفاعل مع ClickHouse.

بعد التصريف، تُحمَّل الشيفرة الثنائية الخاصة بالوحدة إلى ClickHouse عبر إدراجها في جدول `system.webassembly_modules`.
بعد ذلك، يمكنك إنشاء UDFs تُشير إلى الدوال التي تُصدّرها الوحدة باستخدام العبارة `CREATE FUNCTION ... LANGUAGE WASM`.

<div id="prerequisites">
  ## المتطلبات الأساسية
</div>

فعِّل دعم WebAssembly في تهيئة ClickHouse لديك:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

تنفيذات المحرك المتوفرة:

* `wasmtime` (افتراضي، وموصى به) — يستخدم [WasmTime](https://github.com/bytecodealliance/wasmtime)
* `wasmedge` — يستخدم [WasmEdge](https://github.com/WasmEdge/WasmEdge)

<div id="quick-start">
  ## البدء السريع
</div>

يوضح هذا المثال سير العمل الكامل لإنشاء WebAssembly UDF عبر تنفيذ حاسبة [حدسية كولاتز](https://en.wikipedia.org/wiki/Collatz_conjecture).

سنكتب الشيفرة بصيغة WebAssembly Text ‏(WAT)، وهي تمثيل مقروء بشريًا لـ WebAssembly، لذا لا تحتاج في هذه المرحلة إلى أي لغة برمجة.
ويتطلب ClickHouse أن تكون الوحدة بتنسيق ثنائي، لذلك سنستخدم أداة التحويل لتحويل WAT إلى WASM.
ولإجراء هذا التحويل، يمكنك استخدام `wat2wasm` من [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) أو الأمر `parse` من [wasm-tools](https://github.com/bytecodealliance/wasm-tools).

```bash
cat << 'EOF' | wasm-tools parse | clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
(module
  (func $next (param $n i32) (result i32)
    local.get $n i32.const 1 i32.and
    (if (result i32)
      (then local.get $n i32.const 3 i32.mul i32.const 1 i32.add)
      (else local.get $n i32.const 2 i32.div_u)))
  (func $steps (export "steps") (param $n i32) (result i32)
    (local $count i32)
    local.get $n i32.const 1 i32.lt_u
    (if (then i32.const 0 return))
    (block $done (loop $loop
      local.get $n i32.const 1 i32.eq br_if $done
      local.get $n call $next local.set $n
      local.get $count i32.const 1 i32.add local.set $count
      br $loop))
    local.get $count)
)
EOF
```

في المقتطف أعلاه، نمرّر شيفرة WASM الثنائية مباشرةً إلى ClickHouse client باستخدام `FORMAT RawBlob` لإدراجها في جدول `system.webassembly_modules`.

ثم نعرّف دالة UDF تشير إلى الدالة `steps` التي تُصدّرها الوحدة:

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

لاحظ أننا نحدّد اسم الدالة من الوحدة بعد `::`، لأنه يختلف عن اسم UDF.

يمكننا الآن استخدام الدالة `collatz_steps` في استعلاماتنا:

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

يُحوَّل العمود `number` صراحةً إلى `UInt32`، لأن دوال WebAssembly تتطلب تطابقًا تامًا مع أنواع التوقيع المحددة في عبارة `CREATE FUNCTION`.

في النتيجة، حصلنا على متتالية خطوات Collatz للأعداد من 1 إلى 100، وهي تقابل المتتالية [A006577 from the OEIS](https://oeis.org/A006577).

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## إدارة وحدات WASM عبر جدول النظام
</div>

تُخزَّن وحدات WebAssembly في جدول `system.webassembly_modules` بالبنية التالية:

* **الأعمدة**
  * `name` String — اسم الوحدة. يجب ألا يكون فارغًا، وأن يقتصر على أحرف الكلمة فقط.
  * `code` String — شيفرة WASM الثنائية الخام. للكتابة فقط، وتُرجع عمليات القراءة سلسلة فارغة.
  * `hash` UInt256 — تجزئة SHA256 للملف الثنائي الخاص بالوحدة (تكون صفرًا إذا كانت موجودة على القرص ولكن لم تُحمَّل بعد).

تتم إدارة الوحدات من خلال عمليات SQL القياسية على هذا الجدول:

<div id="insert-a-module">
  ### إدراج وحدة
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

اختياريًا، قدِّم قيمة hash للتحقق من السلامة:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

إذا لم تتطابق قيمة التجزئة المقدَّمة مع قيمة SHA256 المحسوبة لشيفرة الوحدة، فسيفشل الإدراج. وقد يكون ذلك مفيدًا عند تحميل الوحدات من مصادر خارجية مثل S3 أو HTTP.

<div id="distribute-a-module-across-a-cluster">
  ### توزيع وحدة عبر عنقود
</div>

`system.webassembly_modules` هو table على مستوى instance — لا تصل عملية `INSERT` إلا إلى replica التي تتولى connection. لا توجد صيغة `ON CLUSTER` لعبارة `INSERT`، لذا ستفشل عملية `CREATE FUNCTION ... ON CLUSTER` اللاحقة على replicas التي لا تحتوي على الوحدة:

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

لتوزيع عملية insert على جميع العقد، اكتب إلى دالة الجدول `cluster` بدلًا من الجدول المحلي `system.webassembly_modules`:

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
يعتمد هذا النمط على أن يمرّ مسار الكتابة الموزعة الأساسي على كل نسخة متماثلة داخل كل shard، وهذا لا يحدث إلا عندما يكون الـ cluster مضبوطًا على `internal_replication=false`. عند استخدام `internal_replication=true` (وهي القيمة default في clusters التي تستخدم `ReplicatedMergeTree` لتتولى replication بنفسها)، تُرسل عملية insert إلى نسخة متماثلة سليمة واحدة فقط لكل shard، ولا تتم replication لـ `system.webassembly_modules` عبر هذا المسار — لذلك ستظل بعض النسخ المتماثلة تفتقد إلى الوحدة. في هذا الإعداد، تحتاج إلى تنفيذ insert على كل نسخة متماثلة على حدة، على سبيل المثال بالتكرار عبر `system.clusters` والكتابة باستخدام `remote(...)` لكل host، أو بنسخ الـ binary إلى `user_scripts/wasm/` على كل host.

يمكنك التحقق من `internal_replication` في cluster باستخدام `SELECT cluster, shard_num, internal_replication FROM system.clusters`.
:::

بعد تنفيذ insert المتفرّع، تصبح الوحدة موجودة على كل نسخة متماثلة وينجح `CREATE FUNCTION ... ON CLUSTER`:

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

يمكنك التحقق من أن الوحدة مُحمَّلة على جميع النُسخ المتماثلة باستخدام `clusterAllReplicas`:

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

عمليات الإدراج في `system.webassembly_modules` لا تُحدث أثرًا إضافيًا عند تكرارها للزوج نفسه `(name, hash)`، لذا فإن إعادة تشغيل الإدراج الموزَّع آمنة، وتُعد طريقة مناسبة لإصلاح الحالة بعد استبدال إحدى النسخ المتماثلة. لاحظ أن الخوادم المُضافة حديثًا لا تتلقى الوحدات الموجودة بأثر رجعي — يجب عليك إعادة تشغيل الإدراج على العنقود المُحدَّث، أو وضع الملف التنفيذي في الدليل `user_scripts/wasm/` على المضيف الجديد.

<div id="list-modules">
  ### سرد الوحدات
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### حذف وحدة
</div>

يُنفَّذ الحذف باستخدام العبارة `DELETE FROM system.webassembly_modules WHERE name = '...'`.
يجب أن يكون الشرط إما `name = 'literal'` لتحقيق تطابق تام، أو `name LIKE 'pattern'` لحذف كل وحدة يطابق اسمُها النمط؛ ولا يُقبل أي شكل آخر.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

إذا كانت أي من UDFs الحالية تشير إلى إحدى الوحدات المطابقة، فسيفشل الحذف، لذا يجب عليك حذف هذه الـ UDFs أولًا.

<div id="create-a-webassembly-udf">
  ## إنشاء UDF باستخدام WebAssembly
</div>

**البنية**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1 | ABI ASSEMBLYSCRIPT]
[DETERMINISTIC]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**المعاملات**:

* `function_name`: اسم الدالة في ClickHouse. وقد يختلف عن اسم الدالة المُصدَّرة في الوحدة.
* `FROM 'module_name' :: 'source_function_name'`: اسم وحدة WASM المحمّلة واسم الدالة في وحدة WASM المراد استخدامها (والافتراضي هو `function&#95;name`)
* `ARGUMENTS`: قائمة بأسماء الوسائط وأنواعها (الأسماء اختيارية وتُستخدم مع تنسيقات التسلسل التي تدعم الحقول المُسمّاة)
* `ABI`: إصدار واجهة التطبيق الثنائية
  * `ROW_DIRECT`: مطابقة مباشرة للأنواع، مع معالجة صفًا بصف
  * `BUFFERED_V1`: معالجة قائمة على الكتل مع التسلسل
  * `ASSEMBLYSCRIPT`: معالجة صفًا بصف للوحدات التي يُنتجها مصرّف [AssemblyScript](https://www.assemblyscript.org). تُطابِق الأنواع الرقمية الأنواعَ البدائية في AssemblyScript، ويُطابِق `String` في ClickHouse النوع `string` في AssemblyScript.
* `DETERMINISTIC`: يعلن أن الدالة حتمية — أي تُرجع دائمًا المخرجات نفسها للمدخلات نفسها. عند تحديده، قد يطوي ClickHouse الثوابت في الاستدعاءات التي تكون فيها جميع الوسائط ثوابت: تُقيَّم الدالة مرة واحدة أثناء تحليل الاستعلام، ثم يُعاد استخدام النتيجة لكل صف.
* `SHA256_HASH`: قيمة hash المتوقعة للوحدة للتحقق منها (تُملأ تلقائيًا إذا أُغفلت)، ويمكن استخدامها لضمان تحميل وحدة WASM الصحيحة عبر النسخ المتماثلة المختلفة.
* `SETTINGS`: إعدادات خاصة بكل دالة
  * `serialization_format` String — تنسيق التسلسل الذي يتطلبه ABI. القيم المدعومة: `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary`, و `Buffers`. القيمة الافتراضية: `MsgPack`. يجب أن تُرجع التنسيقات القائمة على الكتل مثل `Buffers` عمودًا واحدًا يطابق نوعه توقيع الدالة المُعلَن.
  * `webassembly_udf_enable_fuel` Bool — يفعّل تحديد ميزانية fuel للدالة. القيمة الافتراضية: `true`. عند ضبطه على `false`، يتم تجاهل إعداد مستوى الاستعلام `webassembly_udf_max_fuel` لهذه الدالة. قد يؤدي تعطيل حدود fuel إلى تحسين الأداء عند استخدام المحرك `wasmtime`. ومع ذلك، بالنسبة إلى شيفرة الضيف غير الموثوقة أو التي تحتوي على أخطاء، فقد يزيد ذلك من خطر التنفيذ غير المنضبط.

<div id="abis-versions">
  ## إصدارات ABI
</div>

للتفاعل مع ClickHouse، يجب أن تلتزم وحدات WebAssembly بأحد ABI المدعومة (واجهات التطبيق الثنائية).

* `ROW_DIRECT`: تعيين مباشر للأنواع (الأنواع البدائية `Int32` و`UInt32` و`Int64` و`UInt64` و`Float32` و`Float64` فقط)
* `BUFFERED_V1`: الأنواع المعقدة مع التسلسل
* `ASSEMBLYSCRIPT`: تكامل على أساس كل صف على حدة مع وحدات [AssemblyScript](https://www.assemblyscript.org)؛ يدعم الأنواع الرقمية و`String`.

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

يستدعي دالة WASM مُصدَّرة مباشرةً لكل صف.

* يجب أن تكون الوسيطات وأنواع الإرجاع من الأنواع الرقمية `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`.
* السلاسل النصية غير مدعومة في واجهة ABI هذه.
* يجب أن تتطابق التواقيع مع ما تصدّره WASM (`i32/i64/f32/f64/v128`).
* لا يلزم أن تُصدِّر الوحدة أي دوال دعم.

على سبيل المثال، دالة بالتوقيع التالي:

```
(func (param i32 i64 f32) (result f64) ...)
```

يمكن إنشاؤه كما يلي:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

لا تميّز WebAssembly بين المعاملات الموقَّعة وغير الموقَّعة، بل تستخدم تعليمات مختلفة لتفسير القيم. لذلك، يجب أن يتطابق حجم المعامل تمامًا، بينما يُحدَّد ما إذا كان موقَّعًا أم غير موقَّع من خلال العمليات داخل الدالة.

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
واجهة ABI هذه تجريبية وقابلة للتغيير في الإصدارات المستقبلية.
:::

تعالج blocks كاملة دفعةً واحدة باستخدام (إلغاء)التسلسل عبر ذاكرة WASM. وتدعم أي أنواع للوسائط وأنواع للإرجاع.

تُنسخ البيانات المُسلسلة إلى ذاكرة wasm وتُمرَّر، على شكل مؤشّر إلى المخزن المؤقت (الذي يتكوّن من مؤشّر إلى البيانات وحجم البيانات)، إلى الدالة UDF إلى جانب عدد rows في الإدخال. لذلك، فإن الدالة المعرّفة من قبل المستخدم على جانب wasm تقبل دائمًا وسيطين من نوع `i32` وتُرجع قيمةً واحدة من نوع `i32`.
تعالج شيفرة guest البيانات وتُرجع مؤشّرًا إلى مخزن النتيجة المؤقت الذي يحتوي على بيانات النتيجة المُسلسلة.

يجب أن توفّر شيفرة guest دالتين لإنشاء هذه المخازن المؤقتة وتدميرها.

```
(module
  ;; Allocate a new buffer of specified size
  ;; Returns: handle to Buffer structure (not direct data pointer!) with pointer to data and size
  (func (export "clickhouse_create_buffer")
    (param $size i32)    ;; Size of data to allocate
    (result i32))        ;; Returns buffer handle with enough space

  ;; Free a buffer by its handle
  (func (export "clickhouse_destroy_buffer")
    (param $handle i32)  ;; Buffer handle to free
    (result))            ;; No return value

    ;; User-defined function
    (func (export "user_defined_function1")
      (param $input_buffer_handle i32)  ;; Input buffer handle
      (param $n i32)                    ;; Number of rows in input
      (result i32))                     ;; Returns output buffer handle
)
```

أمثلة على تعريفات C:

```c
typedef struct {
    uint8_t * data;
    uint32_t size;
} ClickhouseBuffer;

ClickhouseBuffer * clickhouse_create_buffer(uint32_t size) { /* ... */ }

void clickhouse_destroy_buffer(ClickhouseBuffer * data) { /* ... */ }

/// Example user-defined functions
ClickhouseBuffer * user_defined_function1(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
ClickhouseBuffer * user_defined_function2(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
```

<div id="abi-assemblyscript">
  ### واجهة ABI لـ ASSEMBLYSCRIPT
</div>

تستهدف الوحدات التي يُنتجها [AssemblyScript](https://www.assemblyscript.org) المصرّف. ويؤدي كل صف إلى استدعاء واحد للدالة المُصدَّرة، مع مواءمة قيم ClickHouse مع الأنواع البدائية والكائنات النصية في AssemblyScript.

**الأنواع المدعومة**:

* الأنواع الرقمية: `Int8`/`UInt8`، `Int16`/`UInt16` (تُوسَّع إلى `i32` عند الواجهة)، `Int32`/`UInt32`، `Int64`/`UInt64`، `Float32`، `Float64`

* `String` — تُربَط بالنوع `string` في AssemblyScript ‏(UTF-16 في ذاكرة WASM). ويتولى ClickHouse تحويل UTF-8 ↔ UTF-16 تلقائيًا.

* أصناف AssemblyScript المخصّصة غير مدعومة كأنواع للوسائط أو قيم الإرجاع — لأن معرّفات الأصناف في بيئة التشغيل ليست مستقرة عبر عمليات التجميع (راجع [AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982)).

**متطلبات الوحدة**:

يجب أن تكون الوحدة مُصرَّفة باستخدام بيئة التشغيل المُدارة الخاصة بـ AssemblyScript بحيث يتم تصدير `__new` و`__pin` و`__unpin`. يعتمد التعامل القياسي مع السلاسل النصية الواردة والصادرة على ذلك. الاستدعاء الموصى به:

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

يستورد AssemblyScript أيضًا `env.abort` لاعتراضات وقت التشغيل (مثل نفاد الذاكرة، وفحوصات الحدود، وما إلى ذلك). ويوفّر ClickHouse هذا الاستيراد تلقائيًا: فعند استدعاء `abort`، يفشل الاستعلام النشط باستثناء `WASM_ERROR` يتضمن رسالة AssemblyScript بعد فك ترميزها وموضعها في الشيفرة المصدرية.

**مثال**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

بعد أن تكون مُصرَّفة باستخدام `asc` وتحميل ملف `.wasm` الناتج إلى `system.webassembly_modules`، عرّف دوال UDFs كما يلي:

```sql
CREATE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;
```

<div id="note-for-developing-udfs-in-rust">
  ### ملاحظة حول تطوير UDFs بلغة Rust
</div>

بالنسبة إلى برامج Rust، نوفّر crate مساعدًا باسم [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) لتسهيل تطوير WebAssembly UDFs لـ ClickHouse. يوفّر هذا الـ crate دوال لإدارة الذاكرة، لذلك لا تحتاج إلى تنفيذ الدالتين `clickhouse_create_buffer` و`clickhouse_destroy_buffer` يدويًا، بل يكفي إضافة الـ crate كتبعية. كما تتوفر ماكرو `#[clickhouse_wasm_udf]` لتغليف دوال Rust العادية لديك بصيغة ABI المطلوبة.

وباستخدام هذا الـ crate، يمكنك كتابة UDFs على النحو التالي:

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

ستولِّد وحدات الماكرو دالة wrapper تقبل بُنى buffer وتُرجعها، وتتولى serialization/deserialization تلقائيًا باستخدام `serde`.

<div id="host-api-available-to-modules">
  ## واجهة برمجة التطبيقات المضيفة المتاحة للوحدات
</div>

يمكن للوحدات استيراد دوال المضيف التالية واستخدامها:

* `clickhouse_server_version() -> i64` — تُرجِع إصدار خادم ClickHouse كعدد صحيح (على سبيل المثال 25011001 للإصدار v25.11.1.1).
* `clickhouse_throw(ptr: i32, size: i32)` — يطلق خطأً بالرسالة المحددة. ويقبل مؤشرًا إلى موضع الذاكرة الذي يحتوي على سلسلة رسالة الخطأ، إضافةً إلى حجم السلسلة.
* `clickhouse_log(ptr: i32, size: i32)` — يسجّل رسالة في السجل النصي لخادم ClickHouse.
* `clickhouse_random(ptr: i32, size: i32)` — يملأ الذاكرة ببايتات عشوائية.
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — متاحة للوحدات المتوافقة مع AssemblyScript. يؤدي استدعاؤها (أو تشغيل AssemblyScript runtime trap الذي يستدعيها) إلى إنهاء UDF مع استثناء `WASM_ERROR` يتضمن الرسالة بعد فك ترميزها وموضع المصدر. ولا تتأثر الوحدات التي لا تستورد `env.abort`.

<div id="settings">
  ## الإعدادات
</div>

تتحكم الإعدادات التالية على مستوى الاستعلام في تنفيذ WebAssembly UDF:

* `webassembly_udf_max_fuel` — حد fuel لكل تنفيذ لمثيل WebAssembly UDF. تستهلك كل تعليمة WebAssembly مقدارًا من fuel. تُضاعَف القيمة بمقدار 1024 قبل تمريرها إلى بيئة التشغيل، لذا فإن `webassembly_udf_max_fuel = 1` يعادل تقريبًا 1024 وحدة fuel. اضبطه على 0 لإلغاء أي حد finite. ينطبق ذلك فقط على الدوال التي تكون قيمة الإعداد الخاص بها `webassembly_udf_enable_fuel` مساوية لـ true، وهي القيمة الافتراضية.

* `webassembly_udf_max_memory` — حد الذاكرة بالبايت لكل مثيل WebAssembly UDF.

* `webassembly_udf_max_input_block_size` — الحد الأقصى لعدد الصفوف التي تُمرَّر إلى WebAssembly UDF في كتلة واحدة. اضبطه على 0 لمعالجة جميع الصفوف دفعةً واحدة.

* `webassembly_udf_max_instances` — الحد الأقصى لعدد مثيلات WebAssembly UDF التي يمكنها العمل بالتوازي لكل دالة.

مثال على الاستخدام:

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## انظر أيضًا
</div>

* [نظرة عامة على دوال UDF في ClickHouse](/ar/sql-reference/functions/udf)