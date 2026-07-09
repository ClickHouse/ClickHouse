---
description: 'توثيق دوال العمل مع القواميس المضمّنة'
sidebar_label: 'القاموس المضمّن'
slug: /sql-reference/functions/ym-dict-functions
title: 'دوال العمل مع القواميس المضمّنة'
doc_type: 'مرجع'
---

:::note
لكي تعمل الدوال أدناه، يجب أن تحدد تهيئة الخادم المسارات والعناوين اللازمة للحصول على جميع القواميس المضمّنة. تُحمَّل القواميس عند أول استدعاء لأي من هذه الدوال. وإذا تعذر تحميل القوائم المرجعية، فسيتم طرح استثناء.

وبناءً على ذلك، ستطرح الأمثلة الواردة في هذا القسم استثناءً في [ClickHouse Fiddle](https://fiddle.clickhouse.com/) وفي عمليات النشر السريعة وبيئات الإنتاج افتراضيًا، ما لم تُضبط مسبقًا.
:::

للحصول على معلومات حول إنشاء القوائم المرجعية، راجع قسم [&quot;Dictionaries&quot;](../statements/create/dictionary/embedded).

<div id="multiple-geobases">
  ## قواعد جغرافية متعددة
</div>

يدعم ClickHouse العمل مع عدة قواعد جغرافية بديلة (تسلسلات هرمية للمناطق) في الوقت نفسه، لدعم وجهات نظر مختلفة حول البلدان التي تنتمي إليها بعض المناطق.

يحدد إعداد &#39;clickhouse-server&#39; الملف الذي يحتوي على التسلسل الهرمي للمناطق:

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

وبالإضافة إلى هذا الملف، يبحث أيضًا عن الملفات المجاورة التي تحتوي أسماؤها على الرمز `_` متبوعًا بأي لاحقة قبل امتداد الملف.
فعلى سبيل المثال، سيعثر أيضًا على الملف `/opt/geo/regions_hierarchy_ua.txt` إذا كان موجودًا. وهنا يُسمّى `ua` مفتاح القاموس. أما القاموس الذي لا يحتوي على لاحقة، فيكون مفتاحه سلسلة فارغة.

تُعاد تحميل جميع القواميس أثناء runtime (مرة كل عدد معيّن من الثواني، كما هو محدد في parameter الإعداد [`builtin_dictionaries_reload_interval`](/ar/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval)، أو مرة كل ساعة افتراضيًا). ومع ذلك، تُحدَّد قائمة القواميس المتاحة مرة واحدة فقط عند بدء تشغيل الخادم.

تحتوي جميع الدوال الخاصة بالعمل مع المناطق على argument اختياري في النهاية — وهو مفتاح القاموس. ويُشار إليه باسم geobase.

مثال:

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

يقبل معرّف منطقة و`geobase`، ويُرجع سلسلة نصية تحتوي على اسم المنطقة باللغة المقابلة. إذا لم تكن هناك منطقة بالمعرّف المحدد، فستُرجع سلسلة نصية فارغة.

**الصياغة**

```sql
regionToName(id\[, lang\])
```

**المعلمات**

* `id` — معرّف المنطقة من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase متعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* اسم المنطقة باللغة المقابلة التي يحددها `geobase`. [String](../data-types/string).
* وإلا، سلسلة فارغة.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

يقبل معرّف منطقة من geobase. إذا كانت هذه المنطقة مدينة أو جزءًا من مدينة، فسيُرجع معرّف المنطقة الخاص بالمدينة المناسبة. وإلا، فسيُرجع 0.

**الصياغة**

```sql
regionToCity(id [, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase المتعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* معرّف المنطقة للمدينة المناسبة، إن وُجدت. [UInt32](../data-types/int-uint).
* 0، إذا لم توجد.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```response title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
```

### regionToArea

تحوّل المنطقة إلى مساحة (النوع 5 في geobase). وفيما عدا ذلك، فهذه الدالة مطابقة للدالة [&#39;regionToCity&#39;](#regiontocity).

**البنية**

```sql
regionToArea(id [, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من القاعدة الجغرافية. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد جغرافية متعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* معرّف المنطقة المقابلة، إن وُجدت. [UInt32](../data-types/int-uint).
* 0، إذا لم توجد.

**مثال**

```sql title="Query"
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict

تحوِّل منطقة إلى دائرة اتحادية (النوع 4 في geobase). وفي جميع الجوانب الأخرى، تكون هذه الدالة مماثلة للدالة &#39;regionToCity&#39;.

**الصياغة**

```sql
regionToDistrict(id [, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase متعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* معرّف المنطقة للمدينة المقابلة، إن وُجدت. [UInt32](../data-types/int-uint).
* 0 إذا لم توجد.

**مثال**

```sql title="Query"
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry

يحوّل منطقة إلى بلد (النوع 3 في geobase). ومن جميع الجوانب الأخرى، فهذه الدالة مطابقة للدالة &#39;regionToCity&#39;.

**الصيغة**

```sql
regionToCountry(id [, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من `geobase`. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase المتعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المعادة**

* معرّف المنطقة الخاص بالبلد المقابل، إن وجد. [UInt32](../data-types/int-uint).
* 0، إذا لم يوجد.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
```

### regionToContinent

تحوّل منطقة إلى قارة (النوع 1 في geobase). وفيما عدا ذلك، فهذه الدالة مماثلة للدالة &#39;regionToCity&#39;.

**الصياغة**

```sql
regionToContinent(id [, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase المتعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* معرّف المنطقة للقارة المناسبة، إن وُجدت. [UInt32](../data-types/int-uint).
* 0، إذا لم توجد.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
```

### regionToTopContinent

تُرجِع أعلى قارة في التسلسل الهرمي للمنطقة.

**الصيغة**

```sql
regionToTopContinent(id[, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من `geobase`. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase متعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* معرّف القارة في أعلى مستوى (أي عند الصعود في التسلسل الهرمي للمناطق). [UInt32](../data-types/int-uint).
* 0، إذا لم تكن موجودة.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
```

### regionToPopulation

تُرجع عدد السكان لمنطقة معيّنة. ويمكن أن يكون عدد السكان مسجّلًا في الملفات مع geobase. راجع قسم [&quot;Dictionaries&quot;](../statements/create/dictionary/embedded). وإذا لم يكن عدد السكان مسجّلًا للمنطقة، فستُرجع الدالة 0. في geobase، قد يكون عدد السكان مسجّلًا للمناطق الفرعية، ولكن ليس للمناطق الأصلية.

**الصيغة**

```sql
regionToPopulation(id[, geobase])
```

**المعلمات**

* `id` — معرّف المنطقة من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [Multiple Geobases](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* عدد سكان المنطقة. [UInt32](../data-types/int-uint).
* 0، في حال عدم وجود قيمة.

**مثال**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
```

### regionIn

يتحقق مما إذا كانت المنطقة `lhs` تقع ضمن المنطقة `rhs`. ويُرجع قيمة من نوع UInt8 تساوي 1 إذا كانت كذلك، أو 0 إذا لم تكن كذلك.

**الصيغة**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**المعلمات**

* `lhs` — معرّف المنطقة `lhs` من geobase. [UInt32](../data-types/int-uint).
* `rhs` — معرّف المنطقة `rhs` من geobase. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد geobase متعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* 1، إذا كانت المنطقة تابعة لها. [UInt8](../data-types/int-uint).
* 0، إذا لم تكن كذلك.

**تفاصيل التنفيذ**

العلاقة انعكاسية — أي منطقة تنتمي أيضًا إلى نفسها.

**مثال**

```sql title="Query"
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

```text title="Response"
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder    
```

### regionHierarchy

تقبل عددًا من نوع UInt32 — وهو معرّف المنطقة من geobase. وتُعيد مصفوفة من معرّفات المناطق تضمّ المنطقة المُمرَّرة وجميع المناطق الأصلية لها في التسلسل الهرمي.

**البنية**

```sql
regionHierarchy(id\[, geobase\])
```

**المعلمات**

* `id` — معرّف المنطقة من قاعدة البيانات الجغرافية. [UInt32](../data-types/int-uint).
* `geobase` — مفتاح القاموس. راجع [قواعد البيانات الجغرافية المتعددة](#multiple-geobases). [String](../data-types/string). اختياري.

**القيمة المُعادة**

* مصفوفة من معرّفات المناطق تضمّ المنطقة المُمرَّرة وجميع المناطق الأصل على امتداد السلسلة. [Array](../data-types/array)([UInt32](../data-types/int-uint)).

**مثال**

```sql title="Query"
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

```text title="Response"
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

{/* 
  يُستبدل المحتوى الداخلي للوسوم أدناه أثناء عملية البناء في إطار عمل التوثيق بـ
  التوثيق المُولَّد من system.functions. يُرجى عدم تعديل الوسوم أو إزالتها.
  راجع: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }