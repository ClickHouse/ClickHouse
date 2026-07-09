---
description: 'عند تنفيذ الاستعلامات، يستخدم ClickHouse أنواعًا مختلفة من ذاكرات التخزين المؤقت.'
sidebar_label: 'ذاكرات التخزين المؤقت'
sidebar_position: 65
slug: /operations/caches
title: 'أنواع ذاكرات التخزين المؤقت'
keywords: ['cache']
doc_type: 'reference'
---

عند تنفيذ الاستعلامات، يستخدم ClickHouse أنواعًا مختلفة من ذاكرات التخزين المؤقت لتسريع الاستعلامات
وتقليل الحاجة إلى القراءة من القرص أو الكتابة إليه.

الأنواع الرئيسية لذاكرات التخزين المؤقت هي:

* `mark_cache` — ذاكرة تخزين مؤقت لـ [marks](/ar/development/architecture#merge-tree) تستخدمها محركات الجداول من عائلة [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* `uncompressed_cache` — ذاكرة تخزين مؤقت لـ بيانات غير مضغوطة تستخدمها محركات الجداول من عائلة [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* ذاكرة صفحات نظام التشغيل المؤقتة (تُستخدم بشكل غير مباشر للملفات التي تحتوي على البيانات الفعلية).

توجد أيضًا مجموعة من الأنواع الإضافية لذاكرات التخزين المؤقت:

* ذاكرة DNS المؤقتة.
* ذاكرة [Regexp](/ar/interfaces/formats/Regexp) المؤقتة.
* ذاكرة التخزين المؤقت للتعبيرات المترجمة.
* ذاكرة [فهرس تشابه المتجهات](../engines/table-engines/mergetree-family/annindexes.md) المؤقتة.
* ذاكرة [الفهرس النصي](../engines/table-engines/mergetree-family/textindexes.md#caching) المؤقتة.
* ذاكرة التخزين المؤقت للمخططات الخاصة بـ [Avro format](/ar/interfaces/formats/Avro).
* ذاكرة التخزين المؤقت لبيانات [Dictionaries](../sql-reference/statements/create/dictionary/overview.md).
* ذاكرة التخزين المؤقت لاستنتاج المخطط.
* [ذاكرة التخزين المؤقت لنظام الملفات](storing-data.md) فوق S3 وAzure وLocal وأقراص أخرى.
* [ذاكرة صفحات userspace المؤقتة](/ar/operations/userspace-page-cache)
* [ذاكرة الاستعلامات المؤقتة](query-cache.md).
* [ذاكرة شروط الاستعلام المؤقتة](query-condition-cache.md).
* ذاكرة التخزين المؤقت لمخططات التنسيق.

إذا كنت ترغب في مسح إحدى ذاكرات التخزين المؤقت، لأسباب تتعلق بضبط الأداء أو استكشاف الأخطاء وإصلاحها أو اتساق البيانات،
فيمكنك استخدام عبارة [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md).