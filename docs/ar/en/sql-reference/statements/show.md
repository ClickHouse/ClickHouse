---
description: 'توثيق SHOW'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'عبارات SHOW'
doc_type: 'مرجع'
---

:::note

يُخفي `SHOW CREATE (TABLE|DATABASE|USER)` المعلومات السرية ما لم يتم تفعيل الإعدادات التالية:

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (إعداد على مستوى الخادم)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (إعداد التنسيق)

بالإضافة إلى ذلك، يجب أن يمتلك المستخدم امتياز [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect).
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

تعيد هذه العبارات عمودًا واحدًا من نوع String،
يحتوي على استعلام `CREATE` المستخدم لإنشاء الكائن المحدد.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
إذا استخدمت هذه العبارة للحصول على استعلام `CREATE` الخاص بجداول النظام،
فستحصل على استعلام *مزيف* لا يحدّد سوى بنية الجدول،
ولا يمكن استخدامه لإنشاء جدول.
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

تعرض هذه العبارة قائمة بجميع قواعد البيانات.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

وهو مطابق للاستعلام:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### أمثلة
</div>

في هذا المثال، نستخدم `SHOW` للحصول على أسماء قواعد البيانات التي تتضمن التسلسل الرمزي &#39;de&#39; في أسمائها:

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

يمكننا أيضًا القيام بذلك بطريقة تتجاهل حالة الأحرف:

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

أو اعرض أسماء قواعد البيانات التي لا تتضمن &#39;de&#39; في أسمائها:

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

أخيرًا، يمكننا الحصول على أسماء أول قاعدتَي بيانات فقط:

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### انظر أيضًا
</div>

* [`CREATE DATABASE`](/ar/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

تعرض عبارة `SHOW TABLES` قائمة بالجداول.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

إذا لم يُحدَّد عبارة `FROM`، فسيُرجع الاستعلام قائمة بالجداول من قاعدة البيانات الحالية.

هذه التعليمة مطابقة للاستعلام التالي:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### أمثلة
</div>

في هذا المثال، نستخدم تعليمة `SHOW TABLES` للعثور على جميع الجداول التي تحتوي كلمة &#39;user&#39; في أسمائها:

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

يمكننا أيضًا القيام بذلك دون مراعاة حالة الأحرف:

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

أو للعثور على الجداول التي لا تتضمن أسماؤها الحرف &#39;s&#39;:

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

وأخيرًا، يمكننا الحصول على أسماء أول جدولين فحسب:

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also">
  ### انظر أيضًا
</div>

* [`إنشاء الجداول`](/ar/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

تعرض عبارة `SHOW COLUMNS` قائمة بالأعمدة.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

يمكن تحديد اسم قاعدة البيانات واسم الجدول بالصيغة المختصرة `<db>.<table>`,
ما يعني أن `FROM tab FROM db` و `FROM db.tab` متكافئتان.
إذا لم يتم تحديد قاعدة بيانات، فسيُرجع الاستعلام قائمة الأعمدة من قاعدة البيانات الحالية.

توجد أيضًا كلمتان مفتاحيتان اختياريتان: `EXTENDED` و `FULL`. لا يكون للكلمة المفتاحية `EXTENDED` أي تأثير حاليًا،
وهي موجودة للتوافق مع MySQL. وتتسبب الكلمة المفتاحية `FULL` في تضمين أعمدة collation و comment و privilege في الناتج.

تُنتج عبارة `SHOW COLUMNS` جدول نتائج بالبنية التالية:

| العمود      | الوصف                                                                                                                                     | النوع              |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `field`     | اسم العمود                                                                                                                                | `String`           |
| `type`      | نوع بيانات العمود. إذا أُجري الاستعلام عبر MySQL wire protocol، فسيُعرض اسم النوع المكافئ في MySQL.                                       | `String`           |
| `null`      | `YES` إذا كان نوع بيانات العمود Nullable، وإلا `NO`                                                                                       | `String`           |
| `key`       | `PRI` إذا كان العمود جزءًا من المفتاح الأساسي، و`SOR` إذا كان العمود جزءًا من مفتاح الفرز، وإلا فتكون القيمة فارغة                        | `String`           |
| `default`   | التعبير الافتراضي للعمود إذا كان من النوع `ALIAS` أو `DEFAULT` أو `MATERIALIZED`، وإلا فالقيمة `NULL`.                                    | `Nullable(String)` |
| `extra`     | معلومات إضافية، غير مستخدمة حاليًا                                                                                                        | `String`           |
| `collation` | (فقط إذا تم تحديد الكلمة المفتاحية `FULL`) قيمة Collation للعمود، وتكون دائمًا `NULL` لأن ClickHouse لا يدعم collations على مستوى كل عمود | `Nullable(String)` |
| `comment`   | (فقط إذا تم تحديد الكلمة المفتاحية `FULL`) التعليق على العمود                                                                             | `String`           |
| `privilege` | (فقط إذا تم تحديد الكلمة المفتاحية `FULL`) الامتياز المتاح لك على هذا العمود، وهو غير متوفر حاليًا                                        | `String`           |

<div id="examples">
  ### أمثلة
</div>

في هذا المثال، سنستخدم تعليمة `SHOW COLUMNS` للحصول على معلومات عن جميع الأعمدة في الجدول &#39;orders&#39;،
بدءًا من &#39;delivery&#95;&#39;:

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### انظر أيضًا
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

تعرض عبارة `SHOW DICTIONARIES` قائمةً بـ[القواميس](./create/dictionary/overview.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

إذا لم يتم تحديد عبارة `FROM`، فسيُرجع الاستعلام قائمة القواميس من قاعدة البيانات الحالية.

يمكنك الحصول على النتائج نفسها التي يُرجعها الاستعلام `SHOW DICTIONARIES` على النحو التالي:

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### أمثلة
</div>

يجلب الاستعلام التالي أول صفّين من قائمة الجداول في قاعدة البيانات `system` التي تحتوي أسماؤها على `reg`.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

يعرض قائمة بالفهارس الأساسية وفهارس تخطي البيانات الخاصة بجدول.

توجد هذه العبارة أساسًا للتوافق مع MySQL. توفّر جداول النظام [`system.tables`](../../operations/system-tables/tables.md) (للمفاتيح الأساسية) و[`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (لفهارس تخطي البيانات)
معلومات مكافئة، ولكن بأسلوب أكثر أصالةً في ClickHouse.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

يمكن تحديد اسم قاعدة البيانات واسم الجدول بصيغة مختصرة على شكل `<db>.<table>`، أي إن `FROM tab FROM db` و `FROM db.tab` متكافئتان.
إذا لم يتم تحديد قاعدة بيانات، يفترض الاستعلام أن قاعدة البيانات الحالية هي المقصودة.

الكلمة المفتاحية الاختيارية `EXTENDED` ليس لها حاليًا أي تأثير، وهي موجودة للتوافق مع MySQL.

تنتج هذه العبارة جدول نتائج بالبنية التالية:

| العمود          | الوصف                                                                                                                                              | النوع              |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `table`         | اسم الجدول.                                                                                                                                        | `String`           |
| `non_unique`    | دائمًا `1` لأن ClickHouse لا يدعم قيود التفرد.                                                                                                     | `UInt8`            |
| `key_name`      | اسم الفهرس، أو `PRIMARY` إذا كان الفهرس فهرس مفتاح أساسي.                                                                                          | `String`           |
| `seq_in_index`  | بالنسبة إلى فهرس المفتاح الأساسي، فهو موضع العمود بدءًا من `1`. أما بالنسبة إلى فهرس تخطي البيانات، فيكون دائمًا `1`.                              | `UInt8`            |
| `column_name`   | بالنسبة إلى فهرس المفتاح الأساسي، فهو اسم العمود. أما بالنسبة إلى فهرس تخطي البيانات، فتكون `''` (سلسلة فارغة)، راجع الحقل &quot;expression&quot;. | `String`           |
| `collation`     | ترتيب فرز العمود في الفهرس: `A` إذا كان تصاعديًا، و`D` إذا كان تنازليًا، و`NULL` إذا لم يكن مفروزًا.                                               | `Nullable(String)` |
| `cardinality`   | تقدير cardinality الفهرس (عدد القيم الفريدة في الفهرس). وهو حاليًا دائمًا 0.                                                                       | `UInt64`           |
| `sub_part`      | دائمًا `NULL` لأن ClickHouse لا يدعم بادئات الفهرس مثل MySQL.                                                                                      | `Nullable(String)` |
| `packed`        | دائمًا `NULL` لأن ClickHouse لا يدعم فهارس packed (مثل MySQL).                                                                                     | `Nullable(String)` |
| `null`          | غير مستخدم حاليًا                                                                                                                                  |                    |
| `index_type`    | نوع الفهرس، مثل `PRIMARY` و`MINMAX` و`BLOOM_FILTER` وما إلى ذلك.                                                                                   | `String`           |
| `comment`       | معلومات إضافية عن الفهرس، وهي حاليًا دائمًا `''` (سلسلة فارغة).                                                                                    | `String`           |
| `index_comment` | `''` (سلسلة فارغة) لأن الفهارس في ClickHouse لا يمكن أن تحتوي على حقل `COMMENT` (كما في MySQL).                                                    | `String`           |
| `visible`       | إذا كان الفهرس مرئيًا للمُحسِّن، فيكون دائمًا `YES`.                                                                                               | `String`           |
| `expression`    | بالنسبة إلى فهرس تخطي البيانات، فهو تعبير الفهرس. أما بالنسبة إلى فهرس المفتاح الأساسي، فتكون `''` (سلسلة فارغة).                                  | `String`           |

<div id="examples">
  ### أمثلة
</div>

في هذا المثال، نستخدم التعليمة `SHOW INDEX` للحصول على معلومات عن جميع الفهارس في الجدول &#39;tbl&#39;

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also-3">
  ### راجع أيضًا
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

يعرض محتوى الجدول [`system.processes`](/ar/operations/system-tables/processes)، والذي يتضمن قائمة بالاستعلامات الجاري تنفيذها حاليًا، باستثناء استعلامات `SHOW PROCESSLIST`.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

يُرجِع الاستعلام `SELECT * FROM system.processes` بيانات عن جميع الاستعلامات الجارية حاليًا.

:::tip
نفّذ في Console:

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

تعرض عبارة `SHOW GRANTS` الامتيازات الممنوحة لمستخدم.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

إذا لم يتم تحديد المستخدم، فسيُرجع الاستعلام الامتيازات الخاصة بالمستخدم الحالي.

يتيح المُعدِّل `WITH IMPLICIT` عرض الامتيازات الممنوحة ضمنيًا (على سبيل المثال، `GRANT SELECT ON system.one`)

يَدمج المُعدِّل `FINAL` جميع الامتيازات الخاصة بالمستخدم وبالأدوار الممنوحة له (مع التوريث)

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

تعرض عبارة `SHOW CREATE USER` المعلمات التي استُخدمت عند [إنشاء المستخدم](../../sql-reference/statements/create/user.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

تعرض عبارة `SHOW CREATE ROLE` المعاملات المُستخدَمة عند [إنشاء الدور](../../sql-reference/statements/create/role.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

تعرض عبارة `SHOW CREATE ROW POLICY` المعاملات المُستخدمة عند [إنشاء ROW POLICY](../../sql-reference/statements/create/row-policy.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

تُظهر عبارة `SHOW CREATE QUOTA` المعلمات المُستخدمة عند [إنشاء QUOTA](../../sql-reference/statements/create/quota.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

تعرض عبارة `SHOW CREATE SETTINGS PROFILE` المعلمات المُستخدمة عند [إنشاء ملف تعريف الإعدادات](../../sql-reference/statements/create/settings-profile.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

تعيد عبارة `SHOW USERS` قائمة بأسماء [حسابات المستخدمين](../../guides/sre/user-management/index.md#user-account-management).
للاطّلاع على معلمات حسابات المستخدمين، راجع جدول النظام [`system.users`](/ar/operations/system-tables/users).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

تعرض عبارة `SHOW ROLES` قائمةً بـ[الأدوار](../../guides/sre/user-management/index.md#role-management).
ولعرض المعلمات الأخرى،
راجع جداول النظام [`system.roles`](/ar/operations/system-tables/roles) و[`system.role_grants`](/ar/operations/system-tables/role_grants).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

تعرض عبارة `SHOW PROFILES` قائمةً بـ[ملفات تعريف الإعدادات](../../guides/sre/user-management/index.md#settings-profiles-management).
لعرض معلمات حسابات المستخدمين، راجع جدول النظام [`settings_profiles`](/ar/operations/system-tables/settings_profiles).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

تعرض العبارة `SHOW POLICIES` قائمةً بـ [سياسات الصفوف](../../guides/sre/user-management/index.md#row-policy-management) للجدول المحدد.
للاطّلاع على معلمات حسابات المستخدمين، راجع جدول النظام [`system.row_policies`](/ar/operations/system-tables/row_policies).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

تُرجِع عبارة `SHOW QUOTAS` قائمةً من [الحصص](../../guides/sre/user-management/index.md#quotas-management).
لعرض معلمات الحصص، راجع جدول النظام [`system.quotas`](/ar/operations/system-tables/quotas).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

تعرض عبارة `SHOW QUOTA` [استهلاك الحصة](../../operations/quotas.md) لجميع المستخدمين أو للمستخدم الحالي.
لعرض المعلمات الأخرى، راجع جداول النظام [`system.quotas_usage`](/ar/operations/system-tables/quotas_usage) و[`system.quota_usage`](/ar/operations/system-tables/quota_usage).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

تعرض العبارة `SHOW ACCESS` جميع [المستخدمين](../../guides/sre/user-management/index.md#user-account-management)، و[الأدوار](../../guides/sre/user-management/index.md#role-management)، و[ملفات التعريف](../../guides/sre/user-management/index.md#settings-profiles-management)، وما إلى ذلك، وجميع [الامتيازات الممنوحة لها](../../sql-reference/statements/grant.md#privileges).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

تُرجع العبارة `SHOW CLUSTER(S)` قائمة بالمجموعات العنقودية.
تُدرَج جميع المجموعات العنقودية المتاحة في جدول [`system.clusters`](../../operations/system-tables/clusters.md).

:::note
يعرض الاستعلام `SHOW CLUSTER name` الأعمدة `cluster` و`shard_num` و`replica_num` و`host_name` و`host_address` و`port` من جدول `system.clusters` لاسم المجموعة العنقودية المحدد.
:::

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### أمثلة
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

تعرض عبارة `SHOW SETTINGS` قائمة بإعدادات النظام وقيمها.
وتستخرج البيانات من الجدول [`system.settings`](../../operations/system-tables/settings.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### البنود
</div>

يسمح `LIKE|ILIKE` بتحديد نمط مطابقة لاسم الإعداد. ويمكن أن يتضمن أنماط glob مثل `%` أو `_`. وتكون العبارة `LIKE` حساسةً لحالة الأحرف، بينما `ILIKE` غير حساسة لها.

عند استخدام العبارة `CHANGED`، يعرض الاستعلام فقط الإعدادات التي تغيّرت عن قيمها الافتراضية.

<div id="examples">
  ### أمثلة
</div>

استعلام يستخدم العبارة `LIKE`:

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

استعلام يتضمّن عبارة `ILIKE`:

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

استعلام باستخدام البند `CHANGED`:

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

تعرض عبارة `SHOW SETTING` قيمة الإعداد لاسم الإعداد المحدد.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also-3">
  ### راجع أيضًا
</div>

* جدول [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### أمثلة
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also-3">
  ### راجع أيضًا
</div>

* جدول [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-engines">
  ## SHOW ENGINES
</div>

تعرض عبارة `SHOW ENGINES` محتوى جدول [`system.table_engines`](../../operations/system-tables/table_engines.md)،
الذي يتضمن أوصاف محركات الجداول التي يدعمها الخادم ومعلومات عن الميزات التي تدعمها.

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### انظر أيضًا
</div>

* جدول [system.table&#95;engines](../../operations/system-tables/table_engines.md)

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

تعرض عبارة `SHOW FUNCTIONS` محتوى جدول [`system.functions`](../../operations/system-tables/functions.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

إذا تم تحديد العبارة `LIKE` أو `ILIKE`، فسيُرجع الاستعلام قائمةً بدوال النظام التي تتطابق أسماؤها مع `<pattern>` المُعطى.

<div id="see-also">
  ### انظر أيضًا
</div>

* جدول [`system.functions`](../../operations/system-tables/functions.md)

<div id="show-merges">
  ## SHOW MERGES
</div>

تعرض عبارة `SHOW MERGES` قائمة بعمليات الدمج.
تظهر جميع عمليات الدمج في جدول [`system.merges`](../../operations/system-tables/merges.md):

| العمود              | الوصف                                                     |
| ------------------- | --------------------------------------------------------- |
| `table`             | اسم الجدول.                                               |
| `database`          | اسم قاعدة البيانات التي يوجد فيها الجدول.                 |
| `estimate_complete` | الوقت التقديري للاكتمال (بالثواني).                       |
| `elapsed`           | الوقت المنقضي (بالثواني) منذ بدء عملية الدمج.             |
| `progress`          | النسبة المئوية للعمل المكتمل (من 0 إلى 100 بالمئة).       |
| `is_mutation`       | 1 إذا كانت هذه العملية تعديلًا على جزء.                   |
| `size_compressed`   | الحجم الإجمالي للبيانات المضغوطة الخاصة بالأجزاء المدمجة. |
| `memory_usage`      | استهلاك الذاكرة لعملية الدمج.                             |

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### أمثلة
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

تعرض عبارة `SHOW CREATE MASKING POLICY` المعلمات التي استُخدمت عند [إنشاء سياسة الإخفاء](../../sql-reference/statements/create/masking-policy.md).

<div id="syntax">
  ### الصيغة
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```