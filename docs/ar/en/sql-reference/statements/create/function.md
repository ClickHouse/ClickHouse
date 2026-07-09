---
description: 'توثيق FUNCTION'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - دالة معرّفة من قبل المستخدم (UDF)'
doc_type: 'مرجع'
---

ينشئ دالة معرّفة من قبل المستخدم (UDF) من تعبير لامبدا. ويجب أن يتألف هذا التعبير من مُعاملات الدالة أو الثوابت أو العوامل أو استدعاءات دوال أخرى.

**الصيغة**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

يمكن أن يكون للدالة أي عدد من المعلَمات.

توجد بعض القيود:

* يجب أن يكون اسم الدالة فريدًا بين الدوال المعرّفة من قبل المستخدم ودوال النظام.
* لا يُسمح بالدوال العودية.
* يجب تحديد جميع المتغيرات التي تستخدمها الدالة في قائمة معلَماتها.

إذا تمت مخالفة أي قيد، فسيتم رفع استثناء.

**مثال**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

تُستدعى [دالة شرطية](../../../sql-reference/functions/conditional-functions.md) داخل دالة معرّفة من قبل المستخدم في الاستعلام التالي:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

استبدل دالة UDF موجودة:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

<div id="executable-udfs">
  ### [دوال الدالة المعرّفة من قبل المستخدم القابلة للتنفيذ](/ar/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [الدوال المُعرَّفة من قِبل المستخدم في ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
