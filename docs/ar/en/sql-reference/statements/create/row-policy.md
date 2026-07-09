---
description: 'توثيق ROW POLICY'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'reference'
---

ينشئ [سياسة الصفوف](../../../guides/sre/user-management/index.md#row-policy-management)، أي عامل تصفية يُستخدم لتحديد الصفوف التي يمكن للمستخدم قراءتها من جدول.

:::tip
لا تكون سياسة الصفوف مفيدة إلا للمستخدمين الذين لديهم وصول للقراءة فقط. وإذا كان بإمكان المستخدم تعديل جدول أو نسخ الأقسام بين الجداول، فإن ذلك يُبطل قيود سياسة الصفوف.
:::

الصيغة:

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## عبارة USING
</div>

تسمح بتحديد شرط لتصفية الصفوف. يرى المستخدم صفًا إذا كانت نتيجة تقييم الشرط لهذا الصف غير صفرية.

<div id="to-clause">
  ## عبارة TO
</div>

في قسم `TO`، يمكنك تحديد قائمة بالمستخدمين والأدوار التي يجب أن تنطبق عليها هذه السياسة. على سبيل المثال، `CREATE ROW POLICY ... TO accountant, john@localhost`.

تعني الكلمة المفتاحية `ALL` جميع مستخدمي ClickHouse، بما في ذلك current user. وتتيح الكلمة المفتاحية `ALL EXCEPT` استبعاد بعض المستخدمين من قائمة جميع المستخدمين، على سبيل المثال، `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

<div id="as-clause">
  ## عبارة AS
</div>

يُسمح بتفعيل أكثر من سياسة واحدة على الجدول نفسه للمستخدم نفسه في الوقت ذاته. لذلك نحتاج إلى طريقة لدمج الشروط القادمة من سياسات متعددة.

افتراضيًا، تُدمج السياسات باستخدام المعامل المنطقي `OR`. على سبيل المثال، السياسات التالية:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

تسمح للمستخدم `peter` برؤية الصفوف التي يكون فيها إما `b=1` أو `c=2`.

تحدد عبارة `AS` كيفية دمج السياسات مع غيرها من السياسات. ويمكن أن تكون السياسات إما متساهلة أو مقيِّدة. وبشكل افتراضي، تكون السياسات متساهلة، ما يعني أنها تُدمج باستخدام المعامل المنطقي `OR`.

ويمكن بدلًا من ذلك تعريف السياسة على أنها مقيِّدة. وتُدمج السياسات المقيِّدة باستخدام المعامل المنطقي `AND`.

إليك الصيغة العامة:

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

على سبيل المثال، السياسات التالية:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

مكّن المستخدم `peter` من رؤية الصفوف فقط إذا كان كلٌّ من `b=1` AND `c=2`.

تُدمَج سياسات قاعدة البيانات مع سياسات الجدول.

على سبيل المثال، السياسات التالية:

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

يُمكّن المستخدم `peter` من رؤية صفوف `table1` فقط إذا تحقّق الشرطان `b=1` AND `c=2`، رغم أن
أي جدول آخر في `mydb` لن تُطبَّق عليه سوى سياسة `b=1` لهذا المستخدم.

<div id="on-cluster-clause">
  ## عبارة ON CLUSTER
</div>

يتيح إنشاء سياسات الصفوف على مستوى العنقود، راجع [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="examples">
  ## أمثلة
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`