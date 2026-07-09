---
description: 'توثيق KILL'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'تعليمات KILL'
doc_type: 'reference'
---

هناك نوعان من تعليمات KILL: أحدهما لإنهاء استعلام، والآخر لإنهاء عملية التعديل

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

يحاول إنهاء الاستعلامات التي تعمل حاليًا بالقوة.
تُختار الاستعلامات المطلوب إنهاؤها من جدول system.processes باستخدام المعايير المحددة في عبارة `WHERE` من استعلام `KILL`.

أمثلة:

أولًا، ستحتاج إلى الحصول على قائمة بالاستعلامات غير المكتملة. يوفّر استعلام SQL هذا هذه القائمة مرتبةً بحسب الاستعلامات الأطول تشغيلًا:

قائمة من عقدة ClickHouse واحدة:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

قائمة من مجموعة ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

أوقِف الاستعلام بالقوة:

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
إذا كنت تُنهي استعلامًا في ClickHouse Cloud أو في عنقود ذاتي الإدارة، فتأكّد من استخدام الخيار `ON CLUSTER [cluster-name]` لضمان إنهاء الاستعلام على جميع النسخ المتماثلة.
:::

لا يمكن للمستخدمين ذوي صلاحية القراءة فقط إيقاف سوى استعلاماتهم هم.

يُستخدم تلقائيًا الإصدار غير المتزامن من الاستعلامات (`ASYNC`)، وهو لا ينتظر تأكيدًا على توقّف الاستعلامات.

ينتظر الإصدار المتزامن (`SYNC`) حتى تتوقف جميع الاستعلامات، ويعرض معلومات عن كل عملية عند توقفها.
تحتوي الاستجابة على العمود `kill_status`، ويمكن أن يأخذ القيم التالية:

1. `finished` – تم إنهاء الاستعلام بنجاح.
2. `waiting` – جارٍ انتظار انتهاء الاستعلام بعد إرسال إشارة إليه بإنهائه.
3. تشرح القيم الأخرى سبب تعذّر إيقاف الاستعلام.

لا يتحقق استعلام الاختبار (`TEST`) إلا من صلاحيات المستخدم، ويعرض قائمة بالاستعلامات المطلوب إيقافها.

<div id="kill-mutation">
  ## KILL MUTATION
</div>

غالبًا ما يشير وجود `عمليات التعديل` طويلة الأمد أو غير المكتملة إلى أن خدمة ClickHouse لا تعمل بكفاءة. وقد تؤدي الطبيعة غير المتزامنة لـ `عمليات التعديل` إلى استهلاكها جميع الموارد المتاحة على النظام. وقد تحتاج إلى أحد الإجراءين التاليين:

* إيقاف جميع عمليات `INSERT` و`SELECT` الجديدة مؤقتًا، وترك queue الخاصة بـ `عمليات التعديل` حتى تكتمل.
* أو إنهاء بعض عمليات التعديل هذه يدويًا بإرسال أمر `KILL`.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

يحاول إلغاء وحذف [عمليات التعديل](/ar/sql-reference/statements/alter#mutations) الجاري تنفيذها حاليًا. ويجري اختيار عمليات التعديل المطلوب إلغاؤها من جدول [`system.mutations`](/ar/operations/system-tables/mutations) باستخدام عامل التصفية المحدد في عبارة `WHERE` الخاصة باستعلام `KILL`.

لا يقوم الاستعلام الاختباري (`TEST`) إلا بالتحقق من صلاحيات المستخدم وعرض قائمة بعمليات التعديل المطلوب إيقافها.

أمثلة:

احصل على `count()` لعدد عمليات التعديل غير المكتملة:

عدد عمليات التعديل من عقدة ClickHouse واحدة:

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

عدد عمليات التعديل في عنقود ClickHouse مكوَّن من نسخ متماثلة:

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

استعلم عن قائمة عمليات التعديل غير المكتملة:

قائمة عمليات التعديل من عقدة ClickHouse واحدة:

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

قائمة عمليات التعديل في عنقود ClickHouse:

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

أوقِف عمليات التعديل حسب الحاجة:

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

يكون هذا الاستعلام مفيدًا عندما تتعطل عملية التعديل ولا تتمكن من الاكتمال (على سبيل المثال، إذا كانت إحدى الدوال في استعلام عملية التعديل تُطلق استثناءً عند تطبيقها على البيانات الموجودة في الجدول).

لا يتم التراجع عن التغييرات التي أُجريت بالفعل بواسطة عملية التعديل.

:::note
لا يعني العمود `is_killed=1` (في ClickHouse Cloud فقط) في جدول [system.mutations](/ar/operations/system-tables/mutations) بالضرورة أن عملية التعديل قد اكتملت بالكامل بشكل نهائي. فقد تظل عملية التعديل في حالة يكون فيها `is_killed=1` و `is_done=0` لفترة طويلة. ويمكن أن يحدث ذلك إذا كانت عملية التعديل أخرى طويلة التشغيل تحجب عملية التعديل التي تم إيقافها. وهذا وضع طبيعي.
:::