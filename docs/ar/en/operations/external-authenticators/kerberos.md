---
description: 'يمكن مصادقة مستخدمي ClickHouse الحاليين والمُهيَّئين بشكل صحيح
  باستخدام بروتوكول مصادقة Kerberos.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'مرجع'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

يمكن مصادقة مستخدمي ClickHouse الحاليين والمُعدّين بشكل صحيح عبر بروتوكول مصادقة Kerberos.

حاليًا، لا يمكن استخدام Kerberos إلا كمُصادِق خارجي للمستخدمين الحاليين المعرّفين في `users.xml` أو في مسارات التحكم بالوصول المحلية. ولا يمكن لهؤلاء المستخدمين استخدام سوى طلبات HTTP، ويجب أن يكونوا قادرين على المصادقة باستخدام آلية GSS-SPNEGO.

في هذا النهج، يجب تهيئة Kerberos في النظام، كما يجب تمكينه في config الخاص بـ ClickHouse.

<div id="enabling-kerberos-in-clickhouse">
  ## تمكين Kerberos في ClickHouse
</div>

لتمكين Kerberos، يجب تضمين القسم `kerberos` في `config.xml`. وقد يتضمن هذا القسم معلمات إضافية.

<div id="parameters">
  #### المعلمات
</div>

* `principal` - الاسم القياسي لـ service principal الذي سيجري الحصول عليه واستخدامه عند قبول سياقات الأمان.
  * هذا المعلَم اختياري. وإذا تم إغفاله، فسيُستخدم `principal` الافتراضي.

* `realm` - مجال الذي سيُستخدم لقصر المصادقة على الطلبات التي يتطابق معه مجال الجهة البادئة فقط.
  * هذا المعلَم اختياري. وإذا تم إغفاله، فلن تُطبَّق أي تصفية إضافية بحسب المجال.

* `keytab` - المسار إلى ملف keytab الخاص بالخدمة.
  * هذا المعلَم اختياري. وإذا تم إغفاله، فيجب تعيين مسار ملف keytab الخاص بالخدمة في متغير البيئة `KRB5_KTNAME`.

مثال (يوضع في `config.xml`):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

عند تحديد الـ principal:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

مع التصفية حسب مجال:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
يمكنك تعريف قسم `kerberos` واحد فقط. وسيؤدي وجود عدة أقسام `kerberos` إلى تعطيل ClickHouse لمصادقة Kerberos.
:::

:::note
لا يمكن تحديد القسمين `principal` و`realm` في الوقت نفسه. وسيؤدي وجود كلٍّ من القسمين `principal` و`realm` إلى تعطيل ClickHouse لمصادقة Kerberos.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos كمُصادِق خارجي للمستخدمين الحاليين
</div>

يمكن استخدام Kerberos كآلية للتحقق من هوية المستخدمين المعرّفين محليًا (المستخدمين المعرّفين في `users.xml` أو في مسارات التحكم بالوصول المحلية). حاليًا، **فقط** الطلبات الواردة عبر واجهة HTTP يمكن *إخضاعها لـ Kerberos* (من خلال آلية GSS-SPNEGO).

عادةً ما يتبع تنسيق اسم الـ principal في Kerberos هذا النمط:

* *primary/instance@REALM*

قد يرد الجزء */instance* صفر مرة أو أكثر. **يُتوقَّع أن يتطابق جزء *primary* من اسم الـ principal المعياري الخاص بالمُبادِر مع اسم المستخدم الخاضع لـ Kerberos لكي تنجح المصادقة**.

<div id="enabling-kerberos-in-users-xml">
  ### تمكين Kerberos في `users.xml`
</div>

لتمكين مصادقة Kerberos للمستخدم، حدِّد القسم `kerberos` بدلًا من `password` أو الأقسام المشابهة في تعريف المستخدم.

المعلمات:

* `realm` - مجال يُستخدم لقصر المصادقة على الطلبات التي يتطابق فيها مجال الجهة المُبادِرة معه فقط.
  * هذا المعامل اختياري. وإذا أُهمِل، فلن تُطبَّق أي تصفية إضافية حسب المجال.

مثال (يوضع في `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
لاحظ أنه لا يمكن استخدام مصادقة Kerberos إلى جانب أي آلية مصادقة أخرى. وسيؤدي وجود أي أقسام أخرى مثل `password` إلى جانب `kerberos` إلى إيقاف ClickHouse.
:::

:::info Reminder
لاحظ أنه من الآن فصاعدًا، ما إن يستخدم المستخدم `my_user` ‏`kerberos`، يجب تمكين Kerberos في ملف `config.xml` الرئيسي كما هو موضح سابقًا.
:::

<div id="enabling-kerberos-using-sql">
  ### تمكين Kerberos باستخدام SQL
</div>

عند تمكين [التحكم في الوصول وإدارة الحسابات المعتمدَين على SQL](/ar/operations/access-rights#access-control-usage) في ClickHouse، يمكن أيضًا إنشاء مستخدمين يُعرَّفون عبر Kerberos باستخدام عبارات SQL.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...أو، من دون التصفية حسب مجال:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```