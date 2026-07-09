---
description: 'إعداد مفتاح القاموس وسماته'
sidebar_label: 'السمات'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'سمات القاموس'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

يحدّد بند `structure` مفتاح القاموس والحقول المتاحة للاستعلام.

وصف XML:

```xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

تُوصَف السمات في العناصر التالية:

* `<id>` — عمود المفتاح
* `<attribute>` — عمود البيانات: يمكن أن توجد عدة سمات.

استعلام DDL:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

تُوصَف السمات في جسم الاستعلام:

* `PRIMARY KEY` — عمود المفتاح
* `AttrName AttrType` — عمود بيانات. يمكن أن تكون هناك عدة سمات.

<div id="key">
  ## المفتاح
</div>

يدعم ClickHouse الأنواع التالية من المفاتيح:

* مفتاح رقمي. `UInt64`. يُحدَّد في الوسم `<id>` أو باستخدام الكلمة المفتاحية `PRIMARY KEY`.
* مفتاح مركب. مجموعة من القيم ذات أنواع مختلفة. يُحدَّد في الوسم `<key>` أو باستخدام الكلمة المفتاحية `PRIMARY KEY`.

يمكن أن تحتوي بنية XML على `<id>` أو `<key>` فقط. ويجب أن يتضمن استعلام DDL عبارة `PRIMARY KEY` واحدة.

:::note
يجب عدم وصف المفتاح على أنه سمة.
:::

<div id="numeric-key">
  ### المفتاح الرقمي
</div>

النوع: `UInt64`.

مثال على التهيئة:

```xml
<id>
    <name>Id</name>
</id>
```

حقول الإعداد:

* `name` – اسم العمود الذي يتضمن المفاتيح.

في استعلام DDL:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – اسم العمود الذي يحتوي على المفاتيح.

<div id="composite-key">
  ### مفتاح مركّب
</div>

يمكن أن يكون المفتاح `tuple` مكوّنًا من حقول بأي أنواع. ويجب أن يكون [التخطيط](./layouts/) في هذه الحالة `complex_key_hashed` أو `complex_key_cache`.

:::tip
يمكن أن يتكوّن المفتاح المركّب من عنصر واحد. وهذا يتيح، على سبيل المثال، استخدام سلسلة نصية كمفتاح.
:::

تُحدَّد بنية المفتاح في العنصر `<key>`. وتُحدَّد حقول المفتاح بالتنسيق نفسه المستخدم في [سمات](#attributes) القاموس. مثال:

```xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

أو

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

في استعلام إلى الدالة `dictGet*`، تُمرَّر قيمة من نوع tuple كمفتاح. مثال: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

عندما يتكوّن المفتاح المركب من سمة واحدة، يمكن تمرير قيمة المفتاح مباشرةً من دون تغليفها داخل `tuple`. على سبيل المثال، كلٌّ من `dictGetString('dict_name', 'attr_name', 'key')` و`dictGetString('dict_name', 'attr_name', tuple('key'))` صحيحان.

<div id="attributes">
  ## السمات
</div>

مثال على التهيئة:

```xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

أو

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

حقول الإعداد:

| Tag                                                | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Required |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `name`                                             | اسم العمود.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | نعم      |
| `type`                                             | نوع بيانات ClickHouse: [UInt8](../../../data-types/int-uint.md)، [UInt16](../../../data-types/int-uint.md)، [UInt32](../../../data-types/int-uint.md)، [UInt64](../../../data-types/int-uint.md)، [Int8](../../../data-types/int-uint.md)، [Int16](../../../data-types/int-uint.md)، [Int32](../../../data-types/int-uint.md)، [Int64](../../../data-types/int-uint.md)، [Float32](../../../data-types/float.md)، [Float64](../../../data-types/float.md)، [UUID](../../../data-types/uuid.md)، [Decimal32](../../../data-types/decimal.md)، [Decimal64](../../../data-types/decimal.md)، [Decimal128](../../../data-types/decimal.md)، [Decimal256](../../../data-types/decimal.md)،[Date](../../../data-types/date.md)، [Date32](../../../data-types/date32.md)، [DateTime](../../../data-types/datetime.md)، [DateTime64](../../../data-types/datetime64.md)، [String](../../../data-types/string.md)، [Array](../../../data-types/array.md).<br />يحاول ClickHouse تحويل القيمة من القاموس إلى نوع البيانات المحدد. على سبيل المثال، في MySQL، قد يكون الحقل `TEXT` أو `VARCHAR` أو `BLOB` في جدول المصدر، لكن يمكن تحميله كـ `String` في ClickHouse.<br />نوع [Nullable](../../../data-types/nullable.md) مدعوم حاليًا في قواميس [Flat](./layouts/flat)، و[Hashed](./layouts/hashed)، و[ComplexKeyHashed](./layouts/hashed#complex_key_hashed)، و[Direct](./layouts/direct)، و[ComplexKeyDirect](./layouts/direct#complex_key_direct)، و[RangeHashed](./layouts/range-hashed)، وPolygon، و[Cache](./layouts/cache)، و[ComplexKeyCache](./layouts/cache)، و[SSDCache](./layouts/ssd-cache)، و[SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache). أما في قواميس [IPTrie](./layouts/ip-trie)، فلا تكون أنواع `Nullable` مدعومة. | نعم      |
| `null_value`                                       | القيمة الافتراضية لعنصر غير موجود.<br />في المثال، تكون سلسلة فارغة. ولا يمكن استخدام القيمة [NULL](../../../syntax.md#null) إلا مع الأنواع `Nullable` (راجع السطر السابق الذي يصف الأنواع).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | نعم      |
| `expression`                                       | [تعبير](../../../syntax.md#expressions) ينفّذه ClickHouse على القيمة.<br />يمكن أن يكون التعبير اسم عمود في قاعدة بيانات SQL بعيدة. وبالتالي، يمكنك استخدامه لإنشاء اسم مستعار للعمود البعيد.<br /><br />القيمة الافتراضية: لا يوجد تعبير.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | لا       |
| <a name="hierarchical-dict-attr" /> `hierarchical` | إذا كانت القيمة `true`، فستحتوي السمة على قيمة المفتاح الأب للمفتاح الحالي. راجع [Hierarchical Dictionaries](./layouts/hierarchical).<br /><br />القيمة الافتراضية: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | لا       |
| `injective`                                        | علامة توضّح ما إذا كانت الصورة `id -> attribute` [حقنية](https://en.wikipedia.org/wiki/Injective_function).<br />إذا كانت القيمة `true`، يمكن لـ ClickHouse وضع طلبات القواميس الحقنية تلقائيًا بعد عبارة `GROUP BY`. وعادةً ما يقلّل ذلك بشكل كبير عدد هذه الطلبات.<br /><br />القيمة الافتراضية: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | لا       |
| `is_object_id`                                     | علامة توضّح ما إذا كان الاستعلام يُنفَّذ على مستند MongoDB باستخدام `ObjectID`.<br /><br />القيمة الافتراضية: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |          |