---
description: 'توثيق ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'مرجع'
---

تُعدِّل معظم استعلامات `ALTER TABLE` إعدادات الجدول أو بياناته:

| المُعدِّل                                                                   |
| --------------------------------------------------------------------------- |
| [COLUMN](/ar/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/ar/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/ar/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/ar/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/ar/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/ar/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/ar/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/ar/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/ar/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/ar/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/ar/sql-reference/statements/alter/apply-patches.md)           |

:::note
معظم استعلامات `ALTER TABLE` مدعومة فقط مع الجداول [*MergeTree](/ar/engines/table-engines/mergetree-family/index.md) و[Merge](/ar/engines/table-engines/special/merge.md) و[Distributed](/ar/engines/table-engines/special/distributed.md).
:::

تتعامل عبارات `ALTER` التالية مع طرق العرض:

| العبارة                                                                 | الوصف                                                                  |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY QUERY](/ar/sql-reference/statements/alter/view.md) | تُعدِّل بنية [العرض المُجسَّد](/ar/sql-reference/statements/create/view). |

تُعدِّل عبارات `ALTER` التالية الكيانات المرتبطة بالتحكم بالوصول المستند إلى الأدوار:

| العبارة                                                                 |
| ----------------------------------------------------------------------- |
| [USER](/ar/sql-reference/statements/alter/user.md)                         |
| [ROLE](/ar/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/ar/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/ar/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/ar/sql-reference/statements/alter/settings-profile.md) |

| العبارة                                                                       | الوصف                                                                          |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [ALTER TABLE ... MODIFY COMMENT](/ar/sql-reference/statements/alter/comment.md)  | يضيف تعليقات إلى الجدول أو يعدّلها أو يزيلها، سواء كانت مُعيَّنة مسبقًا أم لا. |
| [ALTER NAMED COLLECTION](/ar/sql-reference/statements/alter/named-collection.md) | يُعدِّل [المجموعات المُسمّاة](/ar/operations/named-collections.md).               |

<div id="mutations">
  ## الطفرات
</div>

تُنفَّذ استعلامات `ALTER` المخصَّصة لمعالجة بيانات الجدول بآلية تُسمى &quot;الطفرات&quot;، وأبرزها [ALTER TABLE ... DELETE](/ar/sql-reference/statements/alter/delete.md) و[ALTER TABLE ... UPDATE](/ar/sql-reference/statements/alter/update.md). وهي عمليات غير متزامنة تعمل في الخلفية، وتشبه عمليات الدمج في جداول [MergeTree](/ar/engines/table-engines/mergetree-family/index.md)، إذ تُنتج إصدارات جديدة &quot;مطفَّرة&quot; من الأجزاء.

بالنسبة إلى جداول `*MergeTree`، تُنفَّذ الطفرات عبر **إعادة كتابة أجزاء البيانات بالكامل**.
ولا توجد ذرّية — إذ تُستبدل الأجزاء بالأجزاء المطفَّرة بمجرد أن تصبح جاهزة، وسيعرض استعلام `SELECT` الذي بدأ تنفيذه أثناء طفرة بياناتٍ من أجزاء طُبِّقت عليها الطفرة بالفعل إلى جانب بياناتٍ من أجزاء لم تُطبَّق عليها الطفرة بعد.

تُرتَّب الطفرات ترتيبًا كليًا بحسب ترتيب إنشائها، وتُطبَّق على كل جزء وفق هذا الترتيب. كما أن هناك ترتيبًا جزئيًا بين الطفرات واستعلامات `INSERT INTO`: فالبيانات التي أُدرجت في الجدول قبل إرسال الطفرة ستخضع للطفرة، أما البيانات التي أُدرجت بعد ذلك فلن تخضع لها. لاحظ أن الطفرات لا تمنع عمليات الإدراج بأي شكل.

يرجع استعلام الطفرة فورًا بعد إضافة إدخال الطفرة (في حالة الجداول المكررة إلى ZooKeeper، وفي حالة الجداول غير المكررة إلى نظام الملفات). وتُنفَّذ الطفرة نفسها بشكل غير متزامن باستخدام إعدادات ملف التعريف الخاصة بالنظام. ولمتابعة تقدّم الطفرات، يمكنك استخدام جدول [`system.mutations`](/ar/operations/system-tables/mutations). وستستمر الطفرة التي أُرسلت بنجاح في التنفيذ حتى إذا أُعيد تشغيل خوادم ClickHouse. ولا توجد طريقة للتراجع عن الطفرة بعد إرسالها، ولكن إذا ظلت الطفرة عالقة لسبب ما، فيمكن إلغاؤها باستخدام استعلام [`KILL MUTATION`](/ar/sql-reference/statements/kill.md/#kill-mutation).

لا تُحذف إدخالات الطفرات المكتملة فورًا (ويُحدَّد عدد الإدخالات المحتفَظ بها بواسطة المعامل `finished_mutations_to_keep` لمحرك التخزين). وتُحذف إدخالات الطفرات الأقدم.

<div id="synchronicity-of-alter-queries">
  ## تزامن استعلامات ALTER
</div>

بالنسبة إلى الجداول غير المكررة، تُنفَّذ جميع استعلامات `ALTER` بشكل متزامن. أما بالنسبة إلى الجداول المكررة، فإن الاستعلام لا يضيف سوى تعليمات للإجراءات المناسبة إلى `ZooKeeper`، بينما تُنفَّذ هذه الإجراءات نفسها في أقرب وقت ممكن. ومع ذلك، يمكن أن ينتظر الاستعلام حتى تكتمل هذه الإجراءات على جميع النسخ المتماثلة.

بالنسبة إلى استعلامات `ALTER` التي تُنشئ طفرات (مثل، على سبيل المثال لا الحصر، `UPDATE` و`DELETE` و`MATERIALIZE INDEX` و`MATERIALIZE PROJECTION` و`MATERIALIZE COLUMN` و`APPLY DELETED MASK` و`APPLY PATCHES` و`CLEAR STATISTIC` و`MATERIALIZE STATISTIC`)، فإن التزامن يتحدد بواسطة الإعداد [mutations&#95;sync](/ar/operations/settings/settings.md/#mutations_sync).

أما استعلامات `ALTER` الأخرى التي لا تعدّل سوى البيانات الوصفية، فيمكنك استخدام الإعداد [alter&#95;sync](/ar/operations/settings/settings#alter_sync) لضبط الانتظار.

يمكنك تحديد مدة الانتظار (بالثواني) حتى تنفّذ النسخ المتماثلة غير النشطة جميع استعلامات `ALTER` باستخدام الإعداد [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ar/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
بالنسبة إلى جميع استعلامات `ALTER`، إذا كانت قيمة `alter_sync = 2` وكانت بعض النسخ المتماثلة غير نشطة لمدة تتجاوز الوقت المحدد في الإعداد `replication_wait_for_inactive_replica_timeout`، فسيُطرَح الاستثناء `UNFINISHED`.
:::

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [التعامل مع التحديثات وعمليات الحذف في ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)