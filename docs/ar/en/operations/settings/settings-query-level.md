---
description: 'إعدادات على مستوى الاستعلام'
sidebar_label: 'إعدادات الجلسة على مستوى الاستعلام'
slug: /operations/settings/query-level
title: 'إعدادات الجلسة على مستوى الاستعلام'
doc_type: 'reference'
---

<div id="overview">
  ## نظرة عامة
</div>

توجد عدة طرق لتنفيذ التعليمات باستخدام إعدادات محددة.
تُضبط الإعدادات ضمن طبقات، وتُعيد كل طبقة لاحقة تعريف القيم السابقة لكل إعداد.

<div id="order-of-priority">
  ## ترتيب الأولوية
</div>

ترتيب الأولوية لتحديد إعداد هو:

1. تطبيق إعداد على مستخدم مباشرةً، أو ضمن ملف تعريف الإعدادات

   * SQL (موصى به)
   * إضافة ملف واحد أو أكثر بتنسيق XML أو YAML إلى `/etc/clickhouse-server/users.d`

2. إعدادات الجلسة

   * أرسل `SET setting=value` من وحدة تحكم SQL في ClickHouse Cloud أو
     `clickhouse client` في الوضع التفاعلي. وبالمثل، يمكنك استخدام جلسات ClickHouse
     عبر HTTP protocol. وللقيام بذلك، تحتاج إلى تحديد
     معلمة HTTP `session_id`.

3. إعدادات query

   * عند تشغيل `clickhouse client` في الوضع غير التفاعلي، اضبط معلمة
     بدء التشغيل `--setting=value`.
   * عند استخدام واجهة برمجة تطبيقات HTTP، مرّر معلمات CGI (`URL?setting_1=value&setting_2=value...`).
   * حدِّد الإعدادات في clause
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     ضمن استعلام SELECT. لا تُطبَّق قيمة الإعداد إلا على ذلك الاستعلام
     ثم تُعاد إلى القيمة الافتراضية أو السابقة بعد تنفيذ الاستعلام.

<div id="converting-a-setting-to-its-default-value">
  ## إعادة إعداد إلى قيمته الافتراضية
</div>

إذا غيّرت إعدادًا وأردت إرجاعه إلى قيمته الافتراضية، فاضبط القيمة على `DEFAULT`. وتكون الصيغة كما يلي:

```sql
SET setting_name = DEFAULT
```

على سبيل المثال، القيمة الافتراضية لـ `async_insert` هي `0`. لنفترض أنك غيّرت قيمتها إلى `1`:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

الناتج هو:

```response
┌─value──┐
│ 1      │
└────────┘
```

يعيد الأمر التالي ضبط قيمته إلى 0:

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

عاد الإعداد الآن إلى قيمته الافتراضية:

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## الإعدادات المخصصة
</div>

بالإضافة إلى [الإعدادات](/ar/operations/settings/settings.md) الشائعة، يمكن للمستخدمين تعريف إعدادات مخصصة.
تتيح لك الإعدادات المخصصة تمرير **معلمات خاصة بالجلسة** يمكن الرجوع إليها داخل الاستعلامات أو السياسات أو الدوال. ويكون ذلك مفيدًا عندما تحتاج إلى:

* تصفية البيانات استنادًا إلى هوية المستخدم أو المؤسسة
* تطبيق منطق أعمال مختلف بحسب السياق
* الاحتفاظ بمعلومات الحالة عبر الاستعلامات ضمن الجلسة

يجب أن يبدأ اسم الإعداد المخصص بإحدى البادئات المعرّفة مسبقًا من قائمة تحددها أنت.
يمكن تحديد قائمة البادئات باستخدام إعداد الخادم [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes)، والمُعرَّف في ملف إعدادات الخادم.

في المثال أدناه، اختيرت `SQL_` كبادئة مخصصة:

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
في ClickHouse Cloud، لا يمكن تحديد بادئة مخصّصة.
تبدأ جميع إعدادات المستخدم المخصّصة بالبادئة `SQL_`.
:::

لتعريف إعداد مخصّص، استخدم الأمر `SET`:

```sql
SET SQL_a = 123;
```

للحصول على القيمة الحالية لإعداد مخصّص، استخدم الدالة `getSetting()`:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## أمثلة
</div>

تضبط هذه الأمثلة جميعها قيمة الإعداد `async_insert` على `1`،
وتوضح كيفية التحقق من الإعدادات في نظام قيد التشغيل.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### استخدام SQL لتطبيق إعداد على مستخدم مباشرةً
</div>

يُنشئ هذا المستخدم `ingester` بالإعداد `async_inset = 1`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### فحص ملف تعريف الإعدادات وإسناده
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### استخدام SQL لإنشاء ملف تعريف للإعدادات وإسناده إلى مستخدم
</div>

يؤدي هذا إلى إنشاء ملف تعريف الإعدادات `log_ingest` بالإعداد `async_inset = 1`:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

ينشئ هذا المستخدم `ingester` ويُسنِد إليه ملف تعريف الإعدادات `log_ingest`:

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### استخدام XML لإنشاء ملف تعريف إعدادات ومستخدم
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment-1">
  #### افحص ملف تعريف الإعدادات وتعيينه
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### تعيين إعداد لجلسة
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### تعيين إعداد أثناء تنفيذ الاستعلام
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## انظر أيضًا
</div>

* راجع صفحة [الإعدادات](/ar/operations/settings/settings.md) للاطلاع على وصف إعدادات ClickHouse.
* [إعدادات الخادم العامة](/ar/operations/server-configuration-parameters/settings.md)