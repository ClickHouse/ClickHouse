---
description: 'يسمح بتنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزّنة على خادم MySQL بعيد.'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

يسمح بتنفيذ استعلامات `SELECT` و`INSERT` على البيانات المخزّنة على خادم MySQL بعيد.

<div id="syntax">
  ## الصيغة
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## الوسائط
</div>

| Argument              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`           | عنوان خادم MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `database`            | اسم قاعدة البيانات البعيدة.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `table`               | اسم الجدول البعيد، أو استعلام يُمرَّر إلى MySQL كما هو (راجع [تمرير استعلام بدلًا من اسم جدول](#passing-a-query)).                                                                                                                                                                                                                                                                                                                                                                             |
| `user`                | مستخدم MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `password`            | كلمة مرور المستخدم.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `replace_query`       | علامة تُحوِّل استعلامات `INSERT INTO` إلى `REPLACE INTO`. القيم الممكنة:<br />    - `0` - يُنفَّذ الاستعلام بصيغة `INSERT INTO`.<br />    - `1` - يُنفَّذ الاستعلام بصيغة `REPLACE INTO`.                                                                                                                                                                                                                                                                                                      |
| `on_duplicate_clause` | التعبير `ON DUPLICATE KEY on_duplicate_clause` الذي يُضاف إلى استعلام `INSERT`. لا يمكن تحديده إلا مع `replace_query = 0` (إذا مرّرت `replace_query = 1` و`on_duplicate_clause` في الوقت نفسه، فسيُصدر ClickHouse استثناءً).<br />    مثال: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    وتكون `on_duplicate_clause` هنا هي `UPDATE c2 = c2 + 1`. راجع توثيق MySQL لمعرفة قيم `on_duplicate_clause` التي يمكنك استخدامها مع عبارة `ON DUPLICATE KEY`. |

يمكن أيضًا تمرير الوسائط باستخدام [المجموعات المسماة](/ar/operations/named-collections.md). في هذه الحالة، يجب تحديد `host` و`port` كلٌّ على حدة. ويُنصح بهذا الأسلوب في بيئات الإنتاج.

تُنفَّذ حاليًا عبارات `WHERE` البسيطة مثل `=, !=, >, >=, <, <=` على خادم MySQL.

أما بقية الشروط وقيد أخذ العينات `LIMIT` فلا تُنفَّذ إلا في ClickHouse بعد اكتمال الاستعلام إلى MySQL.

<div id="passing-a-query">
  ## تمرير استعلام بدلًا من اسم جدول
</div>

بدلًا من اسم جدول، يمكن أن تكون الوسيطة الثالثة استعلام `SELECT` يُمرَّر إلى MySQL كما هو. وتُستنتَج بنية الجدول الناتج من نتيجة الاستعلام. ويمكن كتابة الاستعلام إما كاستعلام فرعي أو بتغليفه داخل الدالة `query`:

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

يفيد ذلك في دفع عمليات JOIN وعمليات التجميع أو أي معالجة أخرى إلى MySQL. ويكون هذا الجدول للقراءة فقط: لا يُسمح بتنفيذ `INSERT` عليه. وتدعم الصياغة نفسها محرك الجدول [`MySQL`](/ar/engines/table-engines/integrations/mysql).

:::note
يحلّل ClickHouse صيغة الاستعلام الفرعي `(SELECT ...)` ثم يعيد تسلسلها بلهجة MySQL (باستخدام علامات الاقتباس الخلفية للمعرّفات) قبل إرسالها إلى الخادم. لذلك يجب أن تكون الصيغة صالحة في ClickHouse SQL. ولتمرير صياغة خاصة بـ MySQL لا يحلّلها ClickHouse، استخدم صيغة `query('...')`، إذ يُرسَل نصها إلى MySQL حرفيًا كما هو.

أي `WHERE` أو `LIMIT` أو تجميع خارجي، وما إلى ذلك، في استعلام ClickHouse المحيط **لا** يُدفَع إلى داخل الاستعلام الممرَّر، بل يُطبَّق في ClickHouse بعد جلب نتيجة الاستعلام كاملةً. ولتقييد البيانات المقروءة من MySQL، ضع عامل التصفية داخل الاستعلام الممرَّر. ومع [`external_table_strict_query = 1`](/ar/operations/settings/settings#external_table_strict_query)، يُرفَض عامل التصفية الخارجي الذي لا يمكن دفعه ويُعاد استثناء بدلًا من تطبيقه محليًا.
:::

يدعم عدة نُسخ متماثلة يجب إدراجها باستخدام `|`. على سبيل المثال:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

OR

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن جدول له الأعمدة نفسها الموجودة في جدول MySQL الأصلي.

:::note
يمكن مطابقة بعض أنواع بيانات MySQL مع أنواع مختلفة في ClickHouse — ويُتحكَّم في ذلك عبر الإعداد على مستوى الاستعلام [mysql&#95;datatypes&#95;support&#95;level](/ar/operations/settings/settings.md#mysql_datatypes_support_level)
:::

:::note
في استعلام `INSERT`، وللتمييز بين دالة الجدول `mysql(...)` واسم جدول مع قائمة بأسماء الأعمدة، يجب استخدام الكلمتين المفتاحيتين `FUNCTION` أو `TABLE FUNCTION`. راجع الأمثلة أدناه.
:::

<div id="examples">
  ## أمثلة
</div>

جدول في MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

استعلام البيانات من ClickHouse:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

أو باستخدام [المجموعات المسماة](/ar/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

يُفعّل الضغط لاتصال بروتوكول MySQL.

القيمة الافتراضية: `false`.

ينطبق هذا الإعداد على:

* دالة الجدول `mysql`;
* محرك الجدول `MySQL`;
* محرك قاعدة البيانات `MySQL`;
* المجموعات المسماة المستخدمة في تكاملات MySQL.

عند تفعيله، يطلب ClickHouse ضغط الاتصال.

مثال:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

الاستبدال والإدراج:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

نسخ البيانات من جدول في MySQL إلى جدول في ClickHouse:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

أو إذا كنت تنسخ فقط دفعةً من البيانات المستجدة من MySQL استنادًا إلى أكبر معرّف حالي:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## مواضيع ذات صلة
</div>

* [محرك الجدول &#39;MySQL&#39;](../../engines/table-engines/integrations/mysql.md)
* [استخدام MySQL كمصدر للقاموس](/ar/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/ar/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ar/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ar/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/ar/operations/settings/settings.md#mysql_max_rows_to_insert)