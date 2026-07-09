---
description: 'توثيق للفهارس الافتراضية (سيناريوهات ماذا لو)'
sidebar_label: 'فهرس افتراضي'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: 'الفهارس الافتراضية'
doc_type: 'مرجع'
---

<div id="hypothetical-indexes">
  # الفهارس الافتراضية
</div>

الفهارس الافتراضية هي فهارس تخطٍّ افتراضية مقتصرة على الجلسة، ويمكنك إرفاقها بجدول من جداول عائلة `MergeTree` من دون إنشائها أو تخزينها فعليًا. وهي لا توجد إلا ضمن الجلسة الحالية، ويستخدمها [`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif) لتقدير مدى تأثير فهرس تخطٍّ حقيقي على الاستعلام — ويشمل ذلك عادةً نسبة التخطي (النسبة من العلامات التي يمكن تخطيها) وتكلفة تقريبية بالعلامات والبايتات.

استخدم الفهارس الافتراضية لتقييم الفهارس المرشحة قبل تحمّل تكلفة إنشائها فعليًا على القرص.

<div id="create-hypothetical-index">
  ## CREATE فهرس افتراضي
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

تحاكي الصياغة `ALTER TABLE ... ADD INDEX`، لكن لا يُنشأ أي فهرس ولا يُكتب — بل يُخزَّن فقط وصف الفهرس ضمن الجلسة الحالية.

* `name` — اسم الفهرس؛ ويجب أن يكون فريدًا ضمن `(database, table)` لهذه الجلسة.
* `expression` — العمود أو التعبير المراد فهرسته.
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. الخياران `text` و`vector_similarity` غير مدعومين ويُرفضان عند تنفيذ `CREATE`، لأن التحقق الفعلي في `ALTER TABLE ... ADD INDEX` يعتمد على إعدادات على مستوى الجدول لا يستطيع المخزن الخاص بالجلسة فقط تكرارها.
* `GRANULARITY value` — عدد حبيبات البيانات لكل حبيبة فهرس. والقيمة الافتراضية هي 1.

يجب أن يكون الجدول الهدف جدولًا من عائلة `MergeTree` ضمن قاعدة بيانات `Atomic` (أي يجب أن يكون له معرّف UUID). وتُرفض الجداول التي لا تملك معرّف UUID — مثل الجداول الموجودة في قاعدة بيانات `Ordinary` قديمة، أو جداول `MergeTree` ذات الصياغة القديمة — لأن مخزن الجلسة يربط الفهارس الافتراضية بمعرّف UUID للجدول.

**مثال**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## تقييم فهرس افتراضي باستخدام EXPLAIN WHATIF
</div>

إن تعريف فهرس افتراضي وحده لا يفعل شيئًا — ولمعرفة كيف سيؤثر في استعلام، شغّل [`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif) على عبارة `SELECT` تمثيلية. يوضّح المُقدِّر قابلية تطبيق كل فهرس مرشّح، وعدد العلامات التي سيقرأها، ونسبة التخطي الناتجة، وكيفية إنتاج هذا التقدير (`empirical` أو `statistical` أو `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

النتيجة:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` هو تقدير مستند إلى متوسط حجم الصف في الجدول، لذا يختلف الرقم الدقيق بحسب التخزين والضغط.

لتخطي الفحص التجريبي داخل الذاكرة والتقدير استنادًا إلى [إحصاءات الأعمدة](/ar/engines/table-engines/mergetree-family/mergetree#column-statistics) بدلًا من ذلك، عرّفها أولًا على الأعمدة المعنية (فهي معطّلة افتراضيًا)، وانتظر حتى تكتمل عملية `materialize` mutation، ثم عطّل المسار التجريبي:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

راجع مرجع [`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif) للاطلاع على المخطط الكامل للمخرجات والإعدادات.

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

يزيل فهرسًا افتراضيًا من الجلسة الحالية.

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

يزيل جميع الفهارس الافتراضية المعرَّفة في الجلسة الحالية، بغض النظر عن الجدول.

<div id="scope-and-lifetime">
  ## النطاق ومدة البقاء
</div>

* لا توجد الفهارس الافتراضية إلا ضمن **الجلسة الحالية** — فهي غير مرئية للجلسات الأخرى، ويُتخلَّص منها عند انتهاء الجلسة.
* لا يؤدي تعريف أيٍّ منها أو إسقاطه إلى إنشاء فهرس فعلي، ولا يؤثر مطلقًا في الاستعلامات العادية على الجدول. ومع ذلك، فإن `EXPLAIN WHATIF` التجريبي يقرأ بالفعل بيانات الجدول لبناء الفهرس المرشَّح في الذاكرة، ويُحتسب هذا الفحص ضمن حدود القراءة والحصص الخاصة بالجلسة.
* افحص الفهارس الافتراضية الخاصة بالجلسة الحالية عبر [`system.hypothetical_indexes`](/ar/operations/system-tables/hypothetical_indexes).

<div id="limitations">
  ## القيود
</div>

يُرفَض المرشحان `text` و`vector_similarity` عند تنفيذ `CREATE HYPOTHETICAL INDEX`، لأن التحقق الفعلي منهما يعتمد على إعدادات على مستوى الجدول لا يستطيع المخزن الخاص بالجلسة فقط (`session-only store`) محاكاتها.

يعرض `EXPLAIN WHATIF` القيمة `status: not_applicable` للاستعلامات التي تتضمن `FINAL` (إذ يتداخل تقليم `فهرس تخطٍّ` مع `PrimaryKeyExpand`)، ويُرجِع الخطأ `NOT_IMPLEMENTED` عندما يُخدَّم الاستعلام من `projection` (لأن فهرس الجدول الأصل لا يُطبَّق ماديًا على `projection parts`).

تمثل القيمة التجريبية `skip_ratio` **حدًا أعلى**: فهي تحسب كل `حبيبة` متبقية بصورة مستقلة، ولا تُنمذج دمج فجوات `seek` (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`)، ولا تراعي أيضًا الجمع بين مرشح و`فهرس تخطٍّ` موجود ضمن `predicate` فصلي (`OR`). لذلك قد يقرأ الفهرس `materialized` الفعلي مقدارًا أكبر قليلًا، أو يُجري `prune` في حالات لا يعكسها هذا التقدير.

<div id="required-privileges">
  ## الصلاحيات المطلوبة
</div>

يتطلب `CREATE HYPOTHETICAL INDEX` امتياز `SELECT` على الأعمدة المشار إليها في تعبير الفهرس — ويكفي امتياز `SELECT` على مستوى العمود (على سبيل المثال `GRANT SELECT(b)`) — لأن `EXPLAIN WHATIF` التجريبي يقرأ هذه الأعمدة.

لا يتطلب `DROP HYPOTHETICAL INDEX` ولا `DROP ALL HYPOTHETICAL INDEXES` أي امتياز إضافي؛ إذ إنهما يزيلان فقط إدخالات من المخزن المحلي للجلسة.

<div id="see-also">
  ## انظر أيضًا
</div>

* [`EXPLAIN WHATIF`](/ar/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/ar/operations/system-tables/hypothetical_indexes)
* [فهارس تجاوز البيانات](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)