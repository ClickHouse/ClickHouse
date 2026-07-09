---
description: 'يتيح تنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزنة على خادم PostgreSQL
  بعيد.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

يتيح تنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزنة على خادم PostgreSQL بعيد.

<div id="syntax">
  ## الصياغة
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## المعاملات
</div>

| المعامل       | الوصف                                                                                                                 |
| ------------- | --------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | عنوان خادم PostgreSQL.                                                                                                |
| `database`    | اسم قاعدة البيانات البعيدة.                                                                                           |
| `table`       | اسم الجدول البعيد، أو query تُمرَّر إلى PostgreSQL كما هي (انظر [تمرير استعلام بدلاً من اسم جدول](#passing-a-query)). |
| `user`        | اسم مستخدم PostgreSQL.                                                                                                |
| `password`    | كلمة مرور المستخدم.                                                                                                   |
| `schema`      | مخطط جدول غير `default`. اختياري.                                                                                     |
| `on_conflict` | استراتيجية حلّ التعارض. مثال: `ON CONFLICT DO NOTHING`. اختياري.                                                      |

يمكن أيضًا تمرير المعاملات باستخدام [مجموعات مُسمّاة](/ar/operations/named-collections.md). في هذه الحالة، يجب تحديد `host` و`port` كلٌّ على حدة. ويُنصح بهذا النهج في بيئات production.

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن table له الأعمدة نفسها الموجودة في جدول PostgreSQL الأصلي.

:::note
في استعلام `INSERT`، وللتمييز بين table function `postgresql(...)` واسم جدول مرفق بقائمة أسماء الأعمدة، يجب استخدام الكلمتين المحجوزتين `FUNCTION` أو `TABLE FUNCTION`. راجع الأمثلة أدناه.
:::

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

تُنفَّذ استعلامات `SELECT` على جانب PostgreSQL بصيغة `COPY (SELECT ...) TO STDOUT` داخل معاملة PostgreSQL بوضع القراءة فقط، مع إجراء commit بعد كل استعلام `SELECT`.

تُنفَّذ عبارة `WHERE` البسيطة مثل `=`, `!=`, `>`, `>=`, `<`, `<=`, و `IN` على خادم PostgreSQL.

تُنفَّذ جميع عمليات الربط والتجميع والفرز، وشروط `IN [ array ]`، وقيد أخذ العينات `LIMIT` في ClickHouse فقط بعد اكتمال الاستعلام المرسَل إلى PostgreSQL.

<div id="passing-a-query">
  ## تمرير استعلام بدلًا من اسم جدول
</div>

بدلًا من اسم جدول، يمكن أن تكون الوسيطة الثالثة استعلام `SELECT` يُمرَّر إلى PostgreSQL كما هو. وتُستنتج بنية الجدول الناتج من نتيجة الاستعلام. ويمكن كتابة الاستعلام إما على شكل استعلام فرعي أو بتغليفه داخل الدالة `query`:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

يفيد ذلك في push down لعمليات join وaggregations أو أي معالجة أخرى إلى PostgreSQL. ويكون هذا الجدول للقراءة فقط: لا يُسمح بتنفيذ `INSERT` عليه. وتدعم البنية نفسها أيضًا محرك جدول ‏[`PostgreSQL`](/ar/engines/table-engines/integrations/postgresql).

:::note
تُحلَّل صيغة الاستعلام الفرعي `(SELECT ...)` بواسطة ClickHouse ثم يُعاد تسلسلها وفق SQL dialect الخاص بـ PostgreSQL (بما في ذلك وضع علامات الاقتباس حول المعرّفات في PostgreSQL وإفلات القيم الحرفية النصية) قبل إرسالها إلى server. لذلك يجب أن تكون صالحة في ClickHouse SQL. ولتمرير صياغة خاصة بـ PostgreSQL لا يستطيع ClickHouse parse لها، استخدم صيغة `query('...')`، إذ يُرسل نصها إلى PostgreSQL كما هو.

أي `WHERE` أو `LIMIT` خارجي، أو aggregation، وما إلى ذلك، في استعلام ClickHouse المحيط **لا** يُنفَّذ بأسلوب push down داخل الاستعلام الممرَّر، بل يُطبَّق في ClickHouse بعد جلب نتيجة الاستعلام كاملة. ولتقييد البيانات المقروءة من PostgreSQL، ضع filter داخل الاستعلام الممرَّر. ومع [`external_table_strict_query = 1`](/ar/operations/settings/settings#external_table_strict_query)، يُرفض أي filter خارجي لا يمكن تنفيذ push down له ويُعاد exception بدلًا من تطبيقه محليًا.
:::

تُنفَّذ استعلامات `INSERT` على جانب PostgreSQL بصيغة `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` داخل transaction في PostgreSQL، مع auto-commit بعد كل statement من نوع `INSERT`.

تتحول أنواع Array في PostgreSQL إلى arrays في ClickHouse.

:::note
انتبه: في PostgreSQL، قد يحتوي column من نوع بيانات المصفوفة مثل Integer[] على مصفوفات بأبعاد مختلفة في rows مختلفة، لكن في ClickHouse لا يُسمح إلا بمصفوفات متعددة الأبعاد لها البعد نفسه في جميع rows.
:::

يدعم عدة replicas يجب إدراجها باستخدام `|`. على سبيل المثال:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

أو

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

يدعم تحديد أولوية النسخ المتماثلة لمصدر القاموس في PostgreSQL. كلما كان الرقم في map أكبر، انخفضت الأولوية. أعلى أولوية هي `0`.

<div id="examples">
  ## أمثلة
</div>

جدول في PostgreSQL:

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

اختيار البيانات من ClickHouse باستخدام وسائط عادية:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

أو باستخدام [المجموعات المُسمّاة](/ar/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

إدراج:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

استخدام مخطط غير افتراضي:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## مواضيع ذات صلة
</div>

* [محرك جدول PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
* [استخدام PostgreSQL كمصدر للقاموس](/ar/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### نسخ بيانات Postgres أو ترحيلها باستخدام PeerDB
</div>

> بالإضافة إلى دوال الجداول، يمكنك دائمًا استخدام [PeerDB](https://docs.peerdb.io/introduction) من ClickHouse لإعداد مسار بيانات مستمر من Postgres إلى ClickHouse. ‏PeerDB أداة صُممت خصيصًا لنسخ البيانات من Postgres إلى ClickHouse باستخدام التقاط البيانات المتغيرة (CDC).