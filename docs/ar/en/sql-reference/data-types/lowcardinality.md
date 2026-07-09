---
description: 'توثيق تحسين LowCardinality للأعمدة النصية'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

يُغيّر التمثيل الداخلي لأنواع بيانات أخرى لتصبح مرمّزة بالقاموس.

<div id="syntax">
  ## الصيغة
</div>

```sql
LowCardinality(data_type)
```

**المعلمات**

* `data_type` — [String](../../sql-reference/data-types/string.md)، و[FixedString](../../sql-reference/data-types/fixedstring.md)، و[Date](../../sql-reference/data-types/date.md)، و[DateTime](../../sql-reference/data-types/datetime.md)، والأعداد باستثناء [Decimal](../../sql-reference/data-types/decimal.md). لا تكون `LowCardinality` فعّالة مع بعض أنواع البيانات؛ راجع وصف الإعداد [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types).

<div id="description">
  ## الوصف
</div>

`LowCardinality` هو بنية تغليف تغيّر أسلوب تخزين البيانات وقواعد معالجتها. يطبّق ClickHouse [ترميز القاموس](https://en.wikipedia.org/wiki/Dictionary_coder) على الأعمدة من النوع `LowCardinality`. ويؤدي التعامل مع البيانات المُرمَّزة بالقاموس إلى تحسين كبير في أداء استعلامات [SELECT](../../sql-reference/statements/select/index.md) في كثير من التطبيقات.

تعتمد كفاءة استخدام نوع البيانات `LowCardinality` على مدى تنوع البيانات. فإذا كان القاموس يحتوي على أقل من 10,000 قيمة مميزة، فإن ClickHouse يحقق غالبًا كفاءة أعلى في قراءة البيانات وتخزينها. أما إذا كان القاموس يحتوي على أكثر من 100,000 قيمة مميزة، فقد يكون أداء ClickHouse أسوأ مقارنةً باستخدام أنواع البيانات العادية.

يُنصح باستخدام `LowCardinality` بدلًا من [Enum](../../sql-reference/data-types/enum.md) عند العمل مع السلاسل النصية. يوفّر `LowCardinality` مرونة أكبر في الاستخدام، وغالبًا ما يحقق الكفاءة نفسها أو كفاءة أعلى.

<div id="example">
  ## مثال
</div>

أنشئ جدولًا يتضمّن عمودًا من النوع `LowCardinality`:

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## الإعدادات والدوال ذات الصلة
</div>

الإعدادات:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/ar/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

الدوال:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## محتوى ذو صلة
</div>

* المدونة: [تحسين ClickHouse باستخدام المخططات وترميزات الضغط](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* المدونة: [العمل مع بيانات السلاسل الزمنية في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [تحسين String (عرض تقديمي مصوّر باللغة الروسية)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [الشرائح باللغة الإنجليزية](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)