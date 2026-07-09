---
description: 'إعدادات تحدّ من تعقيد الاستعلام.'
sidebar_label: 'قيود تعقيد الاستعلام'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'قيود تعقيد الاستعلام'
doc_type: 'مرجع'
---

<div id="overview">
  ## نظرة عامة
</div>

ضمن [الإعدادات](/ar/operations/settings/overview)، يوفّر ClickHouse
إمكانية فرض قيود على تعقيد الاستعلامات. ويساعد ذلك على الحماية من
الاستعلامات التي قد تستهلك الموارد بشكل كبير، بما يضمن تنفيذًا أكثر أمانًا
وقابليةً للتنبؤ، ولا سيما عند استخدام واجهة المستخدم.

تنطبق جميع القيود تقريبًا فقط على استعلامات `SELECT`، وفي
معالجة الاستعلامات الموزعة، تُطبَّق القيود على كل خادم على حدة.

يتحقق ClickHouse عمومًا من القيود فقط بعد اكتمال معالجة أجزاء البيانات،
بدلًا من التحقق منها لكل صف. وقد
يؤدي ذلك إلى حدوث حالة تُنتهك فيها القيود أثناء معالجة
الجزء.

<div id="overflow_mode_setting">
  ## إعدادات `overflow_mode`
</div>

تحتوي معظم القيود أيضًا على إعداد `overflow_mode`، الذي يحدد ما يحدث
عند تجاوز الحد، ويمكن أن يأخذ إحدى القيمتين التاليتين:

* `throw`: أثِر استثناءً (افتراضيًا).
* `break`: أوقِف تنفيذ الاستعلام وأعِد النتيجة الجزئية، كما لو أن
  البيانات المصدرية قد نفدت.

<div id="group_by_overflow_mode_settings">
  ## إعدادات `group_by_overflow_mode`
</div>

يتضمن إعداد `group_by_overflow_mode` أيضًا
القيمة `any`:

* `any` : واصل التجميع للمفاتيح التي أُضيفت إلى المجموعة، ولكن لا تُضِف
  مفاتيح جديدة إلى المجموعة.

<div id="relevant-settings">
  ## قائمة الإعدادات
</div>

تُستخدم الإعدادات التالية لفرض قيود على تعقيد الاستعلام.

:::note
يمكن أن تأخذ القيود المفروضة على «الحد الأقصى لشيءٍ ما» القيمة `0`،
وهذا يعني أنها «غير مقيّدة».
:::

| الإعداد                                                                                                                | وصف مختصر                                                                                                                                           |
| ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`max_memory_usage`](/ar/operations/settings/settings#max_memory_usage)                                                   | الحد الأقصى لمقدار ذاكرة RAM المستخدمة لتشغيل استعلام على خادم واحد.                                                                                |
| [`max_memory_usage_for_user`](/ar/operations/settings/settings#max_memory_usage_for_user)                                 | الحد الأقصى لمقدار ذاكرة RAM المستخدمة لتشغيل استعلامات مستخدم على خادم واحد.                                                                       |
| [`max_rows_to_read`](/ar/operations/settings/settings#max_rows_to_read)                                                   | الحد الأقصى لعدد الصفوف التي يمكن قراءتها من جدول عند تشغيل استعلام.                                                                                |
| [`max_bytes_to_read`](/ar/operations/settings/settings#max_bytes_to_read)                                                 | الحد الأقصى لعدد البايتات (من البيانات غير المضغوطة) التي يمكن قراءتها من جدول عند تشغيل استعلام.                                                   |
| [`read_overflow_mode_leaf`](/ar/operations/settings/settings#read_overflow_mode_leaf)                                     | يحدد ما يحدث عندما يتجاوز حجم البيانات المقروءة أحد حدود العقدة الطرفية                                                                             |
| [`max_rows_to_read_leaf`](/ar/operations/settings/settings#max_rows_to_read_leaf)                                         | الحد الأقصى لعدد الصفوف التي يمكن قراءتها من جدول محلي على عقدة طرفية عند تشغيل استعلام موزع                                                        |
| [`max_bytes_to_read_leaf`](/ar/operations/settings/settings#max_bytes_to_read_leaf)                                       | الحد الأقصى لعدد البايتات (من البيانات غير المضغوطة) التي يمكن قراءتها من جدول محلي على عقدة طرفية عند تشغيل استعلام موزع.                          |
| [`read_overflow_mode_leaf`](/ar/docs/operations/settings/settings#read_overflow_mode_leaf)                                | يحدد ما يحدث عندما يتجاوز حجم البيانات المقروءة أحد حدود العقدة الطرفية.                                                                            |
| [`max_rows_to_group_by`](/ar/operations/settings/settings#max_rows_to_group_by)                                           | الحد الأقصى لعدد المفاتيح الفريدة الناتجة عن التجميع.                                                                                               |
| [`group_by_overflow_mode`](/ar/operations/settings/settings#group_by_overflow_mode)                                       | يحدد ما يحدث عندما يتجاوز عدد المفاتيح الفريدة للتجميع الحد المسموح به                                                                              |
| [`max_bytes_before_external_group_by`](/ar/operations/settings/settings#max_bytes_before_external_group_by)               | يفعّل أو يعطّل تنفيذ عبارة `GROUP BY` في الذاكرة الخارجية.                                                                                          |
| [`max_bytes_ratio_before_external_group_by`](/ar/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | نسبة الذاكرة المتاحة المسموح باستخدامها لـ `GROUP BY`. وعند بلوغها، تُستخدم الذاكرة الخارجية للتجميع.                                               |
| [`max_bytes_before_external_sort`](/ar/operations/settings/settings#max_bytes_before_external_sort)                       | يفعّل أو يعطّل تنفيذ عبارة `ORDER BY` في الذاكرة الخارجية.                                                                                          |
| [`max_bytes_ratio_before_external_sort`](/ar/operations/settings/settings#max_bytes_ratio_before_external_sort)           | نسبة الذاكرة المتاحة المسموح باستخدامها لـ `ORDER BY`. وعند بلوغها، يُستخدم الفرز الخارجي.                                                          |
| [`max_rows_to_sort`](/ar/operations/settings/settings#max_rows_to_sort)                                                   | الحد الأقصى لعدد الصفوف قبل الفرز. يتيح ذلك تقييد استهلاك الذاكرة أثناء الفرز.                                                                      |
| [`max_bytes_to_sort`](/ar/operations/settings/settings#max_rows_to_sort)                                                  | الحد الأقصى لعدد البايتات قبل الفرز.                                                                                                                |
| [`sort_overflow_mode`](/ar/operations/settings/settings#sort_overflow_mode)                                               | يحدد ما يحدث إذا تجاوز عدد الصفوف المستلمة قبل الفرز أحد الحدود.                                                                                    |
| [`max_result_rows`](/ar/operations/settings/settings#max_result_rows)                                                     | يقيّد عدد الصفوف في النتيجة.                                                                                                                        |
| [`max_result_bytes`](/ar/operations/settings/settings#max_result_bytes)                                                   | يقيّد حجم النتيجة بالبايتات (غير مضغوطة).                                                                                                           |
| [`result_overflow_mode`](/ar/operations/settings/settings#result_overflow_mode)                                           | يحدد ما يجب فعله إذا تجاوز حجم النتيجة أحد الحدود.                                                                                                  |
| [`max_execution_time`](/ar/operations/settings/settings#max_execution_time)                                               | الحد الأقصى لوقت تنفيذ الاستعلام بالثواني.                                                                                                          |
| [`timeout_overflow_mode`](/ar/operations/settings/settings#timeout_overflow_mode)                                         | يحدد ما يجب فعله إذا استمر تشغيل الاستعلام مدة أطول من `max_execution_time` أو إذا كان وقت التشغيل التقديري أطول من `max_estimated_execution_time`. |
| [`max_execution_time_leaf`](/ar/operations/settings/settings#max_execution_time_leaf)                                     | مشابه من حيث المعنى لـ `max_execution_time`، لكنه يُطبَّق فقط على العقد الطرفية في الاستعلامات الموزعة أو البعيدة.                                  |
| [`timeout_overflow_mode_leaf`](/ar/operations/settings/settings#timeout_overflow_mode_leaf)                               | يحدد ما يحدث عندما يستمر الاستعلام في العقدة الطرفية مدة أطول من `max_execution_time_leaf`.                                                         |
| [`min_execution_speed`](/ar/operations/settings/settings#min_execution_speed)                                             | الحد الأدنى لسرعة التنفيذ بالصفوف في الثانية.                                                                                                       |
| [`min_execution_speed_bytes`](/ar/operations/settings/settings#min_execution_speed_bytes)                                 | الحد الأدنى لعدد بايتات التنفيذ في الثانية.                                                                                                         |
| [`max_execution_speed`](/ar/operations/settings/settings#max_execution_speed)                                             | الحد الأقصى لعدد صفوف التنفيذ في الثانية.                                                                                                           |
| [`max_execution_speed_bytes`](/ar/operations/settings/settings#max_execution_speed_bytes)                                 | الحد الأقصى لعدد بايتات التنفيذ في الثانية.                                                                                                         |
| [`timeout_before_checking_execution_speed`](/ar/operations/settings/settings#timeout_before_checking_execution_speed)     | يتحقق، بعد انقضاء الوقت المحدد بالثواني، من أن سرعة التنفيذ ليست بطيئة جدًا (أي لا تقل عن `min_execution_speed`).                                   |
| [`max_estimated_execution_time`](/ar/operations/settings/settings#max_estimated_execution_time)                           | الحد الأقصى لوقت التنفيذ التقديري للاستعلام بالثواني.                                                                                               |
| [`max_columns_to_read`](/ar/operations/settings/settings#max_columns_to_read)                                             | الحد الأقصى لعدد الأعمدة التي يمكن قراءتها من جدول ضمن استعلام واحد.                                                                                |
| [`max_temporary_columns`](/ar/operations/settings/settings#max_temporary_columns)                                         | الحد الأقصى لعدد الأعمدة المؤقتة التي يجب الاحتفاظ بها في RAM في الوقت نفسه عند تشغيل استعلام، بما في ذلك الأعمدة الثابتة.                          |
| [`max_temporary_non_const_columns`](/ar/operations/settings/settings#max_temporary_non_const_columns)                     | الحد الأقصى لعدد الأعمدة المؤقتة التي يجب الاحتفاظ بها في RAM في الوقت نفسه عند تشغيل استعلام، من دون احتساب الأعمدة الثابتة.                       |
| [`max_subquery_depth`](/ar/operations/settings/settings#max_subquery_depth)                                               | يحدد ما يحدث إذا احتوى الاستعلام على عدد من الاستعلامات الفرعية المتداخلة يتجاوز العدد المحدد.                                                      |
| [`max_ast_depth`](/ar/operations/settings/settings#max_ast_depth)                                                         | الحد الأقصى لعمق التداخل في شجرة البنية النحوية للاستعلام.                                                                                          |
| [`max_ast_elements`](/ar/operations/settings/settings#max_ast_elements)                                                   | الحد الأقصى لعدد العناصر في شجرة البنية النحوية للاستعلام.                                                                                          |
| [`max_rows_in_set`](/ar/operations/settings/settings#max_rows_in_set)                                                     | الحد الأقصى لعدد الصفوف في مجموعة البيانات ضمن العبارة `IN` المُنشأة من استعلام فرعي.                                                               |
| [`max_bytes_in_set`](/ar/operations/settings/settings#max_bytes_in_set)                                                   | الحد الأقصى لعدد البايتات (من بيانات غير مضغوطة) المستخدمة في مجموعة ضمن العبارة `IN` المُنشأة من استعلام فرعي.                                     |
| [`set_overflow_mode`](/ar/operations/settings/settings#max_bytes_in_set)                                                  | يحدد ما يحدث عند تجاوز كمية البيانات أحد الحدود.                                                                                                    |
| [`max_rows_in_distinct`](/ar/operations/settings/settings#max_rows_in_distinct)                                           | الحد الأقصى لعدد الصفوف المختلفة عند استخدام DISTINCT.                                                                                              |
| [`max_bytes_in_distinct`](/ar/operations/settings/settings#max_bytes_in_distinct)                                         | الحد الأقصى لحجم الحالة في الذاكرة بالبايتات (غير المضغوطة)، التي يستخدمها جدول hash عند استخدام DISTINCT.                                          |
| [`distinct_overflow_mode`](/ar/operations/settings/settings#distinct_overflow_mode)                                       | يحدد ما يحدث عند تجاوز كمية البيانات أحد الحدود.                                                                                                    |
| [`max_rows_to_transfer`](/ar/operations/settings/settings#max_rows_to_transfer)                                           | الحد الأقصى للحجم (بالصفوف) الذي يمكن تمريره إلى خادم بعيد أو حفظه في جدول مؤقت عند تنفيذ مقطع GLOBAL IN/JOIN.                                      |
| [`max_bytes_to_transfer`](/ar/operations/settings/settings#max_bytes_to_transfer)                                         | الحد الأقصى لعدد البايتات (بيانات غير مضغوطة) التي يمكن تمريرها إلى خادم بعيد أو حفظها في جدول مؤقت عند تنفيذ مقطع GLOBAL IN/JOIN.                  |
| [`transfer_overflow_mode`](/ar/operations/settings/settings#transfer_overflow_mode)                                       | يحدد ما يحدث عند تجاوز كمية البيانات أحد الحدود.                                                                                                    |
| [`max_rows_in_join`](/ar/operations/settings/settings#max_rows_in_join)                                                   | يقيّد عدد الصفوف في جدول hash المستخدم عند ربط الجداول.                                                                                             |
| [`max_bytes_in_join`](/ar/operations/settings/settings#max_bytes_in_join)                                                 | الحد الأقصى لحجم جدول hash المستخدم عند ربط الجداول، محسوبًا بالبايتات.                                                                             |
| [`join_overflow_mode`](/ar/operations/settings/settings#join_overflow_mode)                                               | يحدد الإجراء الذي ينفذه ClickHouse عند بلوغ أيٍّ من حدود join التالية.                                                                              |
| [`max_partitions_per_insert_block`](/ar/operations/settings/settings#max_partitions_per_insert_block)                     | يقيّد الحد الأقصى لعدد partition في block مُدرج واحد، ويُطرح استثناء إذا احتوى block على عدد كبير جدًا من partition.                                |
| [`throw_on_max_partitions_per_insert_block`](/ar/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | يتيح لك التحكم في السلوك عند بلوغ `max_partitions_per_insert_block`.                                                                                |
| [`max_temporary_data_on_disk_size_for_user`](/ar/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | الحد الأقصى لكمية البيانات التي تستهلكها الملفات المؤقتة على القرص، بالبايتات، لجميع استعلامات المستخدم المتزامنة قيد التشغيل.                      |
| [`max_temporary_data_on_disk_size_for_query`](/ar/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | الحد الأقصى لكمية البيانات التي تستهلكها الملفات المؤقتة على القرص، بالبايتات، لجميع الاستعلامات المتزامنة قيد التشغيل.                             |
| [`max_sessions_for_user`](/ar/operations/settings/settings#max_sessions_for_user)                                         | الحد الأقصى لعدد الجلسات المتزامنة لكل مستخدم تمت مصادقته على ClickHouse server.                                                                    |
| [`max_partitions_to_read`](/ar/operations/settings/settings#max_partitions_to_read)                                       | يقيّد الحد الأقصى لعدد partition التي يمكن الوصول إليها ضمن استعلام واحد.                                                                           |

<div id="obsolete-settings">
  ## الإعدادات المهملة
</div>

:::note
الإعدادات التالية مهملة
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

الحد الأقصى لعمق مسار المعالجة. وهو يشير إلى عدد التحويلات التي تمرّ بها كل
كتلة بيانات أثناء معالجة الاستعلام. ويُحتسب ذلك ضمن حدود
خادم واحد. وإذا تجاوز عمق مسار المعالجة هذا الحد، فسيتم إطلاق استثناء.