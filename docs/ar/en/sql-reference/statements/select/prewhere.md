---
description: 'توثيق لعبارة PREWHERE'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'عبارة PREWHERE'
doc_type: 'مرجع'
---

يُعدّ PREWHERE تحسينًا لجعل التصفية أكثر كفاءة. وهو مفعّل افتراضيًا حتى إذا لم تُحدَّد عبارة `PREWHERE` صراحةً. ويعمل عبر نقل جزء من شرط [WHERE](../../../sql-reference/statements/select/where.md) تلقائيًا إلى مرحلة PREWHERE. ويقتصر دور عبارة `PREWHERE` على التحكم في هذا التحسين إذا كنت ترى أنك تستطيع ضبطه بصورة أفضل من السلوك الافتراضي.

مع تحسين PREWHERE، تُقرأ أولًا الأعمدة اللازمة فقط لتنفيذ تعبير PREWHERE. ثم تُقرأ الأعمدة الأخرى المطلوبة لتنفيذ بقية الاستعلام، ولكن فقط لتلك الكتل التي تكون فيها قيمة تعبير PREWHERE هي `true` لبعض الصفوف على الأقل. وإذا كان هناك عدد كبير من الكتل التي تكون فيها قيمة تعبير PREWHERE هي `false` لجميع الصفوف، وكانت PREWHERE تحتاج إلى أعمدة أقل من الأجزاء الأخرى من الاستعلام، فإن ذلك يتيح غالبًا قراءة كمية أقل بكثير من البيانات من القرص عند تنفيذ الاستعلام.

<div id="controlling-prewhere-manually">
  ## التحكم في PREWHERE يدويًا
</div>

لهذه العبارة المعنى نفسه لعبارة `WHERE`. ويكمن الفرق في البيانات التي تُقرأ من الجدول. وعند التحكم يدويًا في `PREWHERE` لشروط التصفية التي لا تستخدمها إلا نسبة قليلة من الأعمدة في الاستعلام، لكنها توفّر ترشيحًا قويًا للبيانات، فإن ذلك يقلّل من حجم البيانات المطلوب قراءتها.

يمكن أن يحدّد الاستعلام كلًا من `PREWHERE` و`WHERE` في الوقت نفسه. في هذه الحالة، تُنفَّذ `PREWHERE` قبل عبارة `WHERE`.

إذا ضُبط الإعداد [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) على 0، فسيتم تعطيل الآليات الاستدلالية التي تنقل تلقائيًا أجزاءً من التعبيرات من `WHERE` إلى `PREWHERE`.

إذا كان الاستعلام يحتوي على المعدِّل [FINAL](/ar/sql-reference/statements/select/from#final-modifier)، فلن يكون تحسين `PREWHERE` صحيحًا دائمًا. ولا يُفعَّل إلا إذا كان كل من الإعدادين [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) و[optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) مفعّلين.

:::note
تُنفَّذ عبارة `PREWHERE` قبل `FINAL`، لذا قد تكون نتائج استعلامات `FROM ... FINAL` غير دقيقة عند استخدام `PREWHERE` مع حقول ليست ضمن عبارة `ORDER BY` في الجدول.
:::

<div id="limitations">
  ## القيود
</div>

لا يدعم `PREWHERE` إلا الجداول التابعة لعائلة [*MergeTree](../../../engines/table-engines/mergetree-family/index.md).

<div id="example">
  ## مثال
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```