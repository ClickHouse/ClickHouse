---
description: 'تتيح دالة الجدول `remote` الوصول إلى الخوادم البعيدة مباشرةً،
  أي من دون إنشاء جدول [Distributed](../../engines/table-engines/special/distributed.md). ودالة الجدول `remoteSecure`
  مماثلة لـ `remote`، لكنها تستخدم اتصالًا آمنًا.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

تتيح دالة الجدول `remote` الوصول إلى الخوادم البعيدة مباشرةً، أي من دون إنشاء جدول [Distributed](../../engines/table-engines/special/distributed.md). ودالة الجدول `remoteSecure` مماثلة لـ `remote`، لكنها تستخدم اتصالًا آمنًا.

يمكن استخدام كلتا الدالتين في استعلامات `SELECT` و`INSERT`.

<div id="syntax">
  ## الصيغة
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## المعلمات
</div>

| الوسيطة          | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | عنوان خادم بعيد أو تعبير يُنشئ عدة عناوين لخوادم بعيدة. التنسيق: `host` أو `host:port`.<br /><br />    يمكن تحديد `host` بوصفه اسم خادم، أو عنوان IPv4 أو IPv6. ويجب تحديد عنوان IPv6 داخل `[]`.<br /><br />    يشير `port` إلى منفذ TCP على الخادم البعيد. وإذا لم يتم تحديد المنفذ، فسيُستخدم [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) من ملف إعدادات الخادم لـدالة الجدول `remote` (افتراضيًا 9000)، و[tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) لـدالة الجدول `remoteSecure` (افتراضيًا 9440).<br /><br />    بالنسبة إلى عناوين IPv6، يكون تحديد المنفذ إلزاميًا.<br /><br />    إذا تم تحديد المعلمة `addresses_expr` فقط، فسيستخدم `db` و`table` القيمة الافتراضية `system.one`.<br /><br />    النوع: [String](../../sql-reference/data-types/string.md). |
| `db`             | اسم قاعدة البيانات. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `table`          | اسم الجدول. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `user`           | اسم المستخدم. إذا لم يتم تحديده، فسيُستخدم `default`. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `password`       | كلمة مرور المستخدم. إذا لم يتم تحديدها، فستُستخدم كلمة مرور فارغة. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `sharding_key`   | مفتاح التجزئة لدعم توزيع البيانات عبر العُقد. على سبيل المثال: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. النوع: [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

يمكن أيضًا تمرير الوسيطات باستخدام [المجموعات المسماة](/ar/operations/named-collections.md).

<div id="returned-value">
  ## القيمة المُعادة
</div>

جدول موجود على خادم بعيد.

<div id="usage">
  ## الاستخدام
</div>

نظرًا لأن دالتَي الجدول `remote` و`remoteSecure` تعيدان إنشاء الاتصال مع كل طلب، يُوصى باستخدام جدول `Distributed` بدلًا من ذلك. كذلك، إذا كانت أسماء المضيفين محددة، فستُحلّ الأسماء، ولن تُحتسب الأخطاء عند العمل مع نُسخ متماثلة مختلفة. وعند معالجة عدد كبير من الاستعلامات، أنشئ دائمًا جدول `Distributed` مسبقًا، ولا تستخدم دالة الجدول `remote`.

يمكن أن تكون دالة الجدول `remote` مفيدة في الحالات التالية:

* ترحيل البيانات لمرة واحدة من نظام إلى آخر
* الوصول إلى خادم محدد لمقارنة البيانات، وتصحيح الأخطاء، والاختبار، أي اتصالات حسب الحاجة.
* استعلامات بين عناقيد ClickHouse مختلفة لأغراض البحث.
* طلبات موزعة غير متكررة تُنفَّذ يدويًا.
* طلبات موزعة يُعاد فيها تحديد مجموعة الخوادم في كل مرة.

<div id="addresses">
  ### العناوين
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

يمكن إدراج عدة عناوين مفصولة بفواصل. في هذه الحالة، سيستخدم ClickHouse المعالجة الموزعة ويرسل الاستعلام إلى جميع العناوين المحددة (مثل الشظايا التي تحتوي على بيانات مختلفة). مثال:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## أمثلة
</div>

<div id="selecting-data-from-a-remote-server">
  ### الاستعلام عن البيانات من خادم بعيد:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

أو باستخدام [المجموعات المُسمّاة](/ar/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### إدراج البيانات في جدول على خادم بعيد:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### ترحيل الجداول من نظام إلى آخر:
</div>

يستخدم هذا المثال جدولًا واحدًا من بيانات نموذجية. قاعدة البيانات هي `imdb`، والجدول هو `actors`.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### على نظام ClickHouse المصدر (النظام الذي يستضيف البيانات حاليًا)
</div>

* تحقّق من قاعدة البيانات المصدر واسم الجدول (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* احصل على عبارة CREATE TABLE من المصدر:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

الناتج

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### على نظام ClickHouse الوجهة
</div>

* أنشئ قاعدة البيانات الهدف:

  ```sql
  CREATE DATABASE imdb
  ```

* باستخدام عبارة CREATE TABLE من المصدر، أنشئ الجدول في الوجهة:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### العودة إلى عملية النشر الخاصة بالمصدر
</div>

أدرِج في قاعدة البيانات والجدول الجديدين اللذين أُنشِئا على النظام البعيد. ستحتاج إلى المضيف، والمنفذ، واسم المستخدم، وكلمة المرور، وقاعدة البيانات الهدف، والجدول الهدف.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## المطابقة باستخدام أنماط glob
</div>

تُستخدم الأنماط داخل `{ }` لإنشاء مجموعة من الشظايا وتحديد النسخ المتماثلة. وإذا وُجدت عدة أزواج من `{ }`، فسيُنشأ حاصل الضرب الديكارتي للمجموعات المقابلة.

أنواع الأنماط التالية مدعومة.

* `{a,b,c}` - يمثّل أيًّا من السلاسل البديلة `a` أو `b` أو `c`. يُستبدل هذا النمط بـ `a` في عنوان الشظية الأولى، ويُستبدل بـ `b` في عنوان الشظية الثانية، وهكذا. على سبيل المثال، يولّد `example0{1,2}-1` العنوانين `example01-1` و `example02-1`.
* `{N..M}` - نطاق من الأرقام. يولّد هذا النمط عناوين الشظايا بفهارس متزايدة من `N` إلى `M` (بما في ذلك `M`). على سبيل المثال، يولّد `example0{1..2}-1` العنوانين `example01-1` و `example02-1`.
* `{0n..0m}` - نطاق من الأرقام مع أصفار بادئة. يحافظ هذا النمط على الأصفار البادئة في الفهارس. على سبيل المثال، يولّد `example{01..03}-1` العناوين `example01-1` و `example02-1` و `example03-1`.
* `{a|b}` - أي عدد من البدائل المفصولة بـ `|`. يحدد هذا النمط النسخ المتماثلة. على سبيل المثال، يولّد `example01-{1|2}` النسختين المتماثلتين `example01-1` و `example01-2`.

سيُرسَل الاستعلام إلى أول نسخة متماثلة سليمة. ومع ذلك، بالنسبة إلى `remote`، تُفحَص النسخ المتماثلة وفق الترتيب المضبوط حاليًا في الإعداد [load&#95;balancing](../../operations/settings/settings.md#load_balancing).
يكون عدد العناوين المُولَّدة محدودًا بواسطة الإعداد [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses).