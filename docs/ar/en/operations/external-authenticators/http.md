---
description: 'توثيق HTTP'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

يمكن استخدام خادم HTTP لمصادقة مستخدمي ClickHouse. ولا يمكن استخدام المصادقة عبر HTTP إلا كموثِّق خارجي للمستخدمين الحاليين المعرَّفين في `users.xml` أو في مسارات التحكم في الوصول المحلية. حاليًا، لا يتوفر الدعم إلا لمخطط المصادقة [Basic](https://datatracker.ietf.org/doc/html/rfc7617) باستخدام طريقة GET.

<div id="http-auth-server-definition">
  ## تعريف خادم المصادقة عبر HTTP
</div>

لتعريف خادم المصادقة عبر HTTP، يجب إضافة القسم `http_authentication_servers` إلى `config.xml`.

**مثال**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

لاحظ أنه يمكنك تعريف عدة خوادم HTTP داخل القسم `http_authentication_servers` باستخدام أسماء مختلفة.

**المعلمات**

* `uri` - معرّف URI المستخدم لإجراء طلب المصادقة

المهل الزمنية، بالمللي ثانية، للمقبس المستخدم في الاتصال بالخادم:

* `connection_timeout_ms` - القيمة الافتراضية: 1000 مللي ثانية.
* `receive_timeout_ms` - القيمة الافتراضية: 1000 مللي ثانية.
* `send_timeout_ms` - القيمة الافتراضية: 1000 مللي ثانية.

معلمات إعادة المحاولة:

* `max_tries` - الحد الأقصى لعدد محاولات إجراء طلب المصادقة. القيمة الافتراضية: 3
* `retry_initial_backoff_ms` - الفاصل الزمني الأولي للتراجع عند إعادة المحاولة. القيمة الافتراضية: 50 مللي ثانية
* `retry_max_backoff_ms` - الحد الأقصى لفاصل التراجع. القيمة الافتراضية: 1000 مللي ثانية

رؤوس التمرير:

يحدّد هذا الجزء الرؤوس التي ستُمرَّر من رؤوس طلب العميل إلى موثّق HTTP الخارجي. لاحظ أن الرؤوس ستُطابَق مع الرؤوس المحددة في الإعدادات بطريقة غير حساسة لحالة الأحرف، لكنها ستُمرَّر كما هي، أي من دون تعديل.

<div id="enabling-http-auth-in-users-xml">
  ### تمكين المصادقة عبر HTTP في `users.xml`
</div>

لتمكين المصادقة عبر HTTP لهذا المستخدم، حدِّد قسم `http_authentication` بدلًا من `password` أو الأقسام المشابهة في تعريف المستخدم.

المعلمات:

* `server` - اسم خادم المصادقة عبر HTTP المُعدّ في الملف الرئيسي `config.xml` كما ورد سابقًا.
* `scheme` - مخطط المصادقة عبر HTTP. لا يُدعَم حاليًا سوى `Basic`. القيمة الافتراضية: Basic

مثال (يوضع في `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
لاحظ أنه لا يمكن استخدام المصادقة عبر HTTP بالتزامن مع أي آلية مصادقة أخرى. وسيؤدي وجود أي أقسام أخرى، مثل `password`، إلى جانب `http_authentication` إلى إيقاف ClickHouse.
:::

<div id="enabling-http-auth-using-sql">
  ### تمكين المصادقة عبر HTTP باستخدام SQL
</div>

عند تمكين [التحكم في الوصول وإدارة الحسابات المعتمدان على SQL](/ar/operations/access-rights#access-control-usage) في ClickHouse، يمكن أيضًا إنشاء مستخدمين تُحدَّد هويتهم عبر المصادقة عبر HTTP باستخدام عبارات SQL.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...أو إن `Basic` هو الإعداد الافتراضي عند عدم تحديد مخطط المصادقة صراحةً

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### تمرير إعدادات الجلسة
</div>

إذا كان جسم الاستجابة من خادم المصادقة عبر HTTP بتنسيق JSON ويحتوي على كائن فرعي `settings`، فسيحاول ClickHouse تحليل أزواج المفتاح والقيمة فيه على أنها قيم نصية وتعيينها كإعدادات للجلسة الحالية للمستخدم الذي تمت مصادقته. وإذا فشل التحليل، فسيتم تجاهل جسم الاستجابة الوارد من الخادم.