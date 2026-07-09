---
description: 'يتيح محرك PostgreSQL تنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزنة
  على خادم PostgreSQL بعيد.'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'محرك جدول PostgreSQL'
doc_type: 'guide'
---

يتيح محرك PostgreSQL تنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزنة على خادم PostgreSQL بعيد.

:::note
حاليًا، لا يدعم محرك الجدول سوى PostgreSQL بالإصدار 12 فما فوق.
:::

:::tip
اطّلع على خدمة [Managed Postgres](/ar/docs/cloud/managed-postgres) الخاصة بنا. فهي تعتمد على تخزين NVMe موجود فعليًا إلى جانب موارد المعالجة، ما يوفّر أداءً أسرع بما يصل إلى 10 مرات لأحمال العمل التي يحدّها أداء القرص مقارنةً بالبدائل التي تستخدم التخزين المتصل بالشبكة مثل EBS، كما تتيح لك نسخ بيانات Postgres إلى ClickHouse باستخدام موصل Postgres CDC في ClickPipes.
:::

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

اطّلع على وصف تفصيلي للاستعلام [CREATE TABLE](/ar/sql-reference/statements/create/table).

قد يختلف مخطط الجدول عن مخطط جدول PostgreSQL الأصلي:

* يجب أن تكون أسماء الأعمدة مطابقةً لما هي عليه في جدول PostgreSQL الأصلي، ولكن يمكنك استخدام بعض هذه الأعمدة فقط وبأي ترتيب.
* قد تختلف أنواع الأعمدة عن تلك الموجودة في جدول PostgreSQL الأصلي. ويحاول ClickHouse [تحويل](../../../engines/database-engines/postgresql.md#data_types-support) القيم إلى أنواع بيانات ClickHouse.
* يحدد الإعداد [external&#95;table&#95;functions&#95;use&#95;nulls](/ar/operations/settings/settings#external_table_functions_use_nulls) كيفية التعامل مع الأعمدة Nullable. القيمة الافتراضية: 1. وإذا كانت القيمة 0، فلن تُنشئ دالة الجدول أعمدة Nullable، وستُدرج القيم الافتراضية بدلًا من القيم NULL. وينطبق ذلك أيضًا على قيم NULL داخل المصفوفات.

**معلمات المحرك**

* `host:port` — عنوان خادم PostgreSQL.
* `database` — اسم قاعدة البيانات البعيدة.
* `table` — اسم الجدول البعيد، أو استعلام يُمرَّر إلى PostgreSQL كما هو (راجع [تمرير استعلام بدلًا من اسم جدول](#passing-a-query)).
* `user` — مستخدم PostgreSQL.
* `password` — كلمة مرور المستخدم.
* `schema` — مخطط جدول غير افتراضي. اختياري.
* `on_conflict` — استراتيجية حل التعارض. مثال: `ON CONFLICT DO NOTHING`. اختياري. ملاحظة: تؤدي إضافة هذا الخيار إلى تقليل كفاءة الإدراج.

يُوصى باستخدام [المجموعات المسماة](/ar/operations/named-collections.md) (المتاحة منذ الإصدار 21.11) في بيئة الإنتاج. إليك مثالًا:

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

يمكن تجاوز بعض المعلمات باستخدام وسيطات من نوع المفتاح-القيمة:

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

تُنفَّذ استعلامات `SELECT` من جهة PostgreSQL بصيغة `COPY (SELECT ...) TO STDOUT` داخل معاملة PostgreSQL للقراءة فقط، مع تنفيذ commit بعد كل استعلام `SELECT`.

تُنفَّذ عبارة `WHERE` البسيطة مثل `=`, `!=`, `>`, `>=`, `<`, `<=`, و `IN` على خادم PostgreSQL.

تُنفَّذ جميع عمليات الربط والتجميع والفرز، وشروط `IN [ array ]`، وقيد أخذ العينات `LIMIT` في ClickHouse فقط بعد اكتمال الاستعلام المرسل إلى PostgreSQL.

<div id="passing-a-query">
  ## تمرير استعلام بدلًا من اسم جدول
</div>

بدلًا من اسم جدول، يمكن أن تكون الوسيطة `table` استعلام `SELECT` يُمرَّر إلى PostgreSQL كما هو. ويُستنتج هيكل الجدول من نتيجة الاستعلام. ويمكن كتابة الاستعلام إما كاستعلام فرعي أو بتغليفه داخل الدالة `query`:

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

يفيد ذلك في تمرير عمليات `JOIN` والتجميعات وأي معالجة أخرى إلى PostgreSQL. وهذا الجدول للقراءة فقط: لا يُسمح بتنفيذ `INSERT` عليه. كما تدعم دالة الجدول [`postgresql`](/ar/sql-reference/table-functions/postgresql) الصياغة نفسها.

:::note
تُحلَّل صيغة الاستعلام الفرعي `(SELECT ...)` بواسطة ClickHouse ثم تُعاد تسلسلها وفق لهجة PostgreSQL ‏(بما في ذلك اقتباس المعرّفات في PostgreSQL وإفلات القيم الحرفية النصية) قبل إرسالها إلى الخادم. لذلك يجب أن تكون صياغتها صالحة في ClickHouse SQL. ولتمرير صياغة خاصة بـ PostgreSQL لا يحللها ClickHouse، استخدم صيغة `query('...')`، إذ يُرسل نصها إلى PostgreSQL كما هو.

أي `WHERE` أو `LIMIT` خارجي، أو تجميع، وما إلى ذلك، من استعلام ClickHouse المحيط **لا** يُدفَع إلى داخل الاستعلام المُمرَّر — بل يُطبَّق في ClickHouse بعد جلب نتيجة الاستعلام كاملةً. ولتقييد البيانات المقروءة من PostgreSQL، ضع عامل التصفية داخل الاستعلام المُمرَّر. عند استخدام [`external_table_strict_query = 1`](/ar/operations/settings/settings#external_table_strict_query)، يُرفَض عامل التصفية الخارجي الذي لا يمكن دفعه مع ظهور استثناء بدلًا من تطبيقه محليًا.
:::

تُنفَّذ استعلامات `INSERT` على جانب PostgreSQL بالشكل `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` داخل معاملة PostgreSQL، مع الالتزام التلقائي بعد كل تعليمة `INSERT`.

تُحوَّل أنواع `Array` في PostgreSQL إلى مصفوفات في ClickHouse.

:::note
انتبه — في PostgreSQL قد تحتوي بيانات المصفوفة المُنشأة بالشكل `type_name[]` على مصفوفات متعددة الأبعاد ذات عدد أبعاد مختلف في صفوف مختلفة من الجدول ضمن العمود نفسه. لكن في ClickHouse لا يُسمح إلا بمصفوفات متعددة الأبعاد لها العدد نفسه من الأبعاد في جميع صفوف الجدول ضمن العمود نفسه.
:::

يدعم عدة نُسخ متماثلة يجب إدراجها باستخدام `|`. على سبيل المثال:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

تُدعَم أولوية النسخ المتماثلة لمصدر Dictionary في PostgreSQL. وكلما زادت القيمة في `map`، انخفضت الأولوية. وأعلى أولوية هي `0`.

في المثال أدناه، تتمتع النسخة المتماثلة `example01-1` بأعلى أولوية:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## مثال للاستخدام
</div>

<div id="table-in-postgresql">
  ### جدول في PostgreSQL
</div>

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
 (1 row)
```

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### إنشاء جدول في ClickHouse والاتصال بجدول PostgreSQL المُنشأ أعلاه
</div>

يستخدم هذا المثال [محرك الجدول PostgreSQL](/ar/engines/table-engines/integrations/postgresql.md) لربط جدول ClickHouse بجدول PostgreSQL، واستخدام تعليمتَي SELECT وINSERT مع قاعدة بيانات PostgreSQL:

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### إدراج البيانات الأولية من جدول PostgreSQL إلى جدول ClickHouse باستخدام استعلام SELECT
</div>

تنسخ [دالة الجدول postgresql](/ar/sql-reference/table-functions/postgresql.md) البيانات من PostgreSQL إلى ClickHouse، وغالبًا ما يُستخدم ذلك لتحسين أداء الاستعلامات على هذه البيانات عبر الاستعلام عنها أو إجراء التحليلات في ClickHouse بدلًا من PostgreSQL، كما يمكن استخدامه أيضًا لترحيل البيانات من PostgreSQL إلى ClickHouse. وبما أننا سننسخ البيانات من PostgreSQL إلى ClickHouse، فسنستخدم محرك الجدول MergeTree في ClickHouse ونسميه postgresql&#95;copy:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### إدراج البيانات التزايدية من جدول PostgreSQL إلى جدول ClickHouse
</div>

إذا كنت ستُجري بعد الإدراج الأولي مزامنةً مستمرة بين جدول PostgreSQL وجدول ClickHouse، فيمكنك استخدام عبارة WHERE في ClickHouse لإدراج البيانات التي أُضيفت إلى PostgreSQL فقط استنادًا إلى طابع زمني أو معرّف تسلسلي فريد.

ويتطلب ذلك تتبّع أكبر معرّف أو أحدث طابع زمني تمت إضافته سابقًا، كما في المثال التالي:

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

ثم إدراج القيم من جدول PostgreSQL التي تتجاوز الحد الأقصى

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### استعلام البيانات من جدول ClickHouse الناتج
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### استخدام مخطط غير افتراضي
</div>

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**انظر أيضًا**

* [دالة الجدول `postgresql`](../../../sql-reference/table-functions/postgresql.md)
* [استخدام PostgreSQL كمصدر للقاموس](/ar/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [ClickHouse وPostgreSQL - ثنائي مثالي في عالم البيانات - الجزء 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* مدونة: [ClickHouse وPostgreSQL - ثنائي مثالي في عالم البيانات - الجزء 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)