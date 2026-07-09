---
description: 'توثيق DETACH'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'تعليمة DETACH'
doc_type: 'reference'
---

يجعل الخادم &quot;ينسى&quot; وجود جدول أو طريقة عرض مادية أو قاموس أو قاعدة بيانات.

**البنية**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

إنّ الفصل لا يحذف البيانات أو البيانات الوصفية الخاصة بجدول أو عرض مادي أو قاموس أو قاعدة بيانات. وإذا لم يتم فصل كيان ما باستخدام `PERMANENTLY`، فسيقرأ الخادم البيانات الوصفية عند التشغيل التالي ويعيد إرفاق الجدول/العرض/القاموس/قاعدة البيانات مرة أخرى. أما إذا تم فصل كيان ما باستخدام `PERMANENTLY`، فلن تتم إعادة إرفاقه تلقائيًا.

وسواء تم فصل جدول أو قاموس أو قاعدة بيانات بشكل دائم أم لا، ففي كلتا الحالتين يمكنك إعادة إرفاقها باستخدام استعلام [ATTACH](../../sql-reference/statements/attach.md).
كما يمكن أيضًا إعادة إرفاق جداول سجلات النظام (مثل `query_log` و`text_log` وغيرها). أما جداول النظام الأخرى فلا يمكن إعادة إرفاقها. وعند التشغيل التالي للخادم، سيعيد الخادم إرفاق هذه الجداول مرة أخرى.

لا تعمل `ATTACH MATERIALIZED VIEW` مع الصيغة المختصرة (من دون `SELECT`)، لكن يمكنك إرفاقها باستخدام استعلام `ATTACH TABLE`.

لاحظ أنه لا يمكنك فصل جدول بشكل دائم إذا كان مفصولًا بالفعل (مؤقتًا). لكن يمكنك إعادة إرفاقه ثم فصله بشكل دائم مرة أخرى.

كذلك، لا يمكنك [DROP](../../sql-reference/statements/drop.md#drop-table) للجدول المنفصل، أو [CREATE TABLE](../../sql-reference/statements/create/table.md) بالاسم نفسه لجدول فُصل بشكل دائم، أو استبداله بجدول آخر باستخدام استعلام [RENAME TABLE](../../sql-reference/statements/rename.md).

ينفّذ المُعدِّل `SYNC` الإجراء دون تأخير.

**مثال**

إنشاء جدول:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

فصل الجدول:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
في ClickHouse Cloud، يجب على المستخدمين استخدام البند `PERMANENTLY`، مثل: `DETACH TABLE <table> PERMANENTLY`. وإذا لم يُستخدم هذا البند، فستُعاد إرفاق الجداول عند إعادة تشغيل العنقود، مثلًا أثناء عمليات الترقية.
:::

**راجع أيضًا**

* [العرض المادي](/ar/sql-reference/statements/create/view#materialized-view)
* [القواميس](./create/dictionary/overview.md)