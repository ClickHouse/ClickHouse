---
description: 'يتيح الوصول إلى جميع الأجزاء (المُعَدَّة في قسم `remote_servers`)
  من عنقود من دون إنشاء جدول [Distributed](../../engines/table-engines/special/distributed.md).'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'مرجع'
---

يتيح الوصول إلى جميع الأجزاء (المُعَدَّة في قسم `remote_servers`) من عنقود من دون إنشاء جدول [Distributed](../../engines/table-engines/special/distributed.md). ولا يُستعلَم إلا من نسخة متماثلة واحدة لكل جزء.

الدالة `clusterAllReplicas` — مثل `cluster`، ولكن يُستعلَم من جميع النسخ المتماثلة. وتُستخدَم كل نسخة متماثلة في العنقود كجزء/اتصال مستقل.

:::note
تُدرَج جميع العناقيد المتاحة في جدول [system.clusters](../../operations/system-tables/clusters.md).
:::

<div id="syntax">
  ## الصيغة
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## الوسيطات
</div>

| الوسيطات                    | النوع                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`              | اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية؛ وتُستخدم القيمة `default` إذا لم يُحدَّد. |
| `db.table` or `db`, `table` | اسم قاعدة بيانات واسم جدول.                                                                                                      |
| `sharding_key`              | مفتاح التجزئة. اختياري. يجب تحديده إذا كان العنقود يحتوي على أكثر من جزء.                                                      |

<div id="returned_value">
  ## القيمة المُرجَعة
</div>

مجموعة البيانات الخاصة بالعناقيد.

<div id="using_macros">
  ## استخدام الماكرو
</div>

يمكن أن يحتوي `cluster_name` على ماكرو، أي استبدالات داخل `{}`. وتُؤخذ القيمة المستبدلة من قسم [الماكرو](../../operations/server-configuration-parameters/settings.md#macros) في ملف تهيئة الخادم.

مثال:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## الاستخدام والتوصيات
</div>

يُعد استخدام دالتي الجدول `cluster` و`clusterAllReplicas` أقل كفاءة من إنشاء جدول `Distributed`، لأنه في هذه الحالة يُعاد إنشاء الاتصال بالخادم مع كل طلب. عند معالجة عدد كبير من الاستعلامات، يُرجى دائمًا إنشاء جدول `Distributed` مسبقًا، وعدم استخدام دالتي الجدول `cluster` و`clusterAllReplicas`.

قد تكون دالتا الجدول `cluster` و`clusterAllReplicas` مفيدتين في الحالات التالية:

* الوصول إلى عنقود محدد لمقارنة البيانات وتصحيح الأخطاء والاختبار.
* تنفيذ استعلامات على عناقيد ClickHouse والنسخ المتماثلة المختلفة لأغراض البحث.
* الطلبات الموزعة غير المتكررة التي تُنفَّذ يدويًا.

تُؤخذ إعدادات الاتصال مثل `host` و`port` و`user` و`password` و`compression` و`secure` من قسم `<remote_servers>` في `config`. راجع التفاصيل في [Distributed engine](../../engines/table-engines/special/distributed.md).

<div id="related">
  ## انظر أيضًا
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)