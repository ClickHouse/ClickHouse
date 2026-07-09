---
description: 'يتيح المحرك `ExternalDistributed` تنفيذ استعلامات `SELECT` على البيانات
  المخزنة على خوادم MySQL أو PostgreSQL بعيدة. ويقبل محرك MySQL أو
  PostgreSQL كوسيطة، مما يتيح إمكانية التجزئة.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'محرك الجدول ExternalDistributed'
doc_type: 'مرجع'
---

يتيح المحرك `ExternalDistributed` تنفيذ استعلامات `SELECT` على البيانات المخزنة على خوادم MySQL أو PostgreSQL بعيدة. ويقبل محرك [MySQL](../../../engines/table-engines/integrations/mysql.md) أو [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) كوسيطة، مما يتيح إمكانية التجزئة.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

اطّلع على وصف مفصّل لاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

قد يختلف هيكل الجدول عن هيكل الجدول الأصلي:

* يجب أن تكون أسماء الأعمدة مطابقةً لتلك الموجودة في الجدول الأصلي، لكن يمكنك استخدام بعض هذه الأعمدة فقط وبأي ترتيب.
* قد تختلف أنواع الأعمدة عن تلك الموجودة في الجدول الأصلي. يحاول ClickHouse [تحويل](/ar/sql-reference/functions/type-conversion-functions#CAST) القيم إلى أنواع بيانات ClickHouse.

**معلمات المحرّك**

* `engine` — محرّك الجدول `MySQL` أو `PostgreSQL`.
* `host:port` — عنوان خادم MySQL أو PostgreSQL.
* `database` — اسم قاعدة البيانات البعيدة.
* `table` — اسم الجدول البعيد.
* `user` — اسم المستخدم.
* `password` — كلمة مرور المستخدم.

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

يدعم عدة نسخ متماثلة، ويجب إدراجها باستخدام `|`، كما يجب إدراج المقاطع باستخدام `,`. على سبيل المثال:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

عند تحديد النسخ المتماثلة، تُختار إحدى النسخ المتماثلة المتاحة لكل شريحة عند القراءة. وإذا فشل الاتصال، تُختار النسخة المتماثلة التالية، وهكذا مع جميع النسخ المتماثلة. وإذا فشلت محاولة الاتصال مع جميع النسخ المتماثلة، تُعاد المحاولة بالطريقة نفسها عدة مرات.

يمكنك تحديد أي عدد من الشرائح وأي عدد من النسخ المتماثلة لكل شريحة.

**انظر أيضًا**

* [محرك الجدول MySQL](../../../engines/table-engines/integrations/mysql.md)
* [محرك الجدول PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
* [محرك الجدول Distributed](../../../engines/table-engines/special/distributed.md)