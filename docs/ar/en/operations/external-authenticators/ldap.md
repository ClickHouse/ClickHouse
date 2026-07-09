---
description: 'دليل لتهيئة مصادقة LDAP في ClickHouse'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

يمكن استخدام خادم LDAP لمصادقة مستخدمي ClickHouse. هناك نهجان مختلفان للقيام بذلك:

* استخدام LDAP كمُصادِق خارجي للمستخدمين الحاليين المعرّفين في `users.xml` أو في مسار التحكم في الوصول المحلي.
* استخدام LDAP كدليل مستخدمين خارجي والسماح بمصادقة المستخدمين غير المعرّفين محليًا إذا كانوا موجودين على خادم LDAP.

في كلٍ من هذين النهجين، يجب تعريف خادم LDAP باسم داخلي في config الخاصة بـ ClickHouse لكي تتمكن الأجزاء الأخرى من config من الإشارة إليه.

<div id="ldap-server-definition">
  ## تعريف خادم LDAP
</div>

لتعريف خادم LDAP، يجب إضافة القسم `ldap_servers` إلى ملف `config.xml`.

**مثال**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

لاحظ أنه يمكنك تعريف عدة خوادم LDAP ضمن قسم `ldap_servers` باستخدام أسماء مختلفة.

**المعلمات**

| المعلمة                        | الافتراضي     | الوصف                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------ | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                         | —             | اسم مضيف خادم LDAP أو عنوان IP الخاص به. هذه المعلمة إلزامية ولا يمكن أن تكون فارغة.                                                                                                                                                                                                                                                                              |
| `port`                         | `636` / `389` | منفذ خادم LDAP. تكون القيمة الافتراضية `636` إذا كانت `enable_tls` مضبوطة على `yes`، وإلا فتكون `389`.                                                                                                                                                                                                                                                            |
| `bind_dn`                      | —             | قالب يُستخدم لإنشاء `DN` الذي سيتم تنفيذ `bind` به. يُنشأ `DN` الناتج باستبدال جميع السلاسل الفرعية `{user_name}` في القالب باسم المستخدم الفعلي أثناء كل محاولة مصادقة.                                                                                                                                                                                          |
| `auth_dn_prefix`               | —             | **مهمل.** بديل لـ `bind_dn`. لا يمكن استخدامه مع `bind_dn` في الوقت نفسه. عند تحديده، يُنشأ `bind DN` بالشكل `auth_dn_prefix + {user_name} + auth_dn_suffix`. على سبيل المثال، فإن ضبط `auth_dn_prefix` على `uid=` و`auth_dn_suffix` على `,ou=users,dc=example,dc=com` يكافئ ضبط `bind_dn` على `uid={user_name},ou=users,dc=example,dc=com`.                      |
| `auth_dn_suffix`               | —             | **مهمل.** راجع `auth_dn_prefix`.                                                                                                                                                                                                                                                                                                                                  |
| `verification_cooldown`        | `0`           | فترة زمنية، بالثواني، بعد محاولة `bind` ناجحة، يُفترض خلالها أن المستخدم قد تمت مصادقته بنجاح لكل الطلبات المتتالية من دون الاتصال بخادم LDAP. حدِّد `0` لتعطيل التخزين المؤقت وفرض الاتصال بخادم LDAP لكل طلب مصادقة.                                                                                                                                            |
| `follow_referrals`             | `false`       | علامة تسمح لمكتبة عميل LDAP بمتابعة إحالات LDAP التي يعيدها الخادم تلقائيًا. يكون هذا مهمًا غالبًا في بيئات Microsoft Active Directory حيث يمكن أن تُرجع عمليات البحث في الشجرة الفرعية عند `base DN` عالي المستوى (مثل `DC=example,DC=com`) إحالات/مراجع بحث (مثل `DC=DomainDnsZones,...`). اضبطها على `true` فقط عندما تحتاج صراحةً إلى عمليات بحث عبر الأقسام. |
| `enable_tls`                   | `yes`         | علامة لتفعيل استخدام الاتصال الآمن بخادم LDAP. حدِّد `no` لاستخدام بروتوكول `ldap://` بالنص الصريح (غير مستحسن)، أو `yes` لاستخدام بروتوكول LDAP عبر SSL/TLS `ldaps://` (مستحسن)، أو `starttls` لاستخدام بروتوكول StartTLS القديم (بروتوكول `ldap://` بالنص الصريح ثم يُرقّى إلى TLS).                                                                            |
| `tls_minimum_protocol_version` | `tls1.2`      | الحد الأدنى لإصدار بروتوكول SSL/TLS. القيم المقبولة: `ssl2`، `ssl3`، `tls1.0`، `tls1.1`، `tls1.2`.                                                                                                                                                                                                                                                                |
| `tls_require_cert`             | `demand`      | سلوك التحقق من شهادة النظير في SSL/TLS. القيم المقبولة: `never`، `allow`، `try`، `demand`.                                                                                                                                                                                                                                                                        |
| `tls_cert_file`                | —             | المسار إلى ملف الشهادة.                                                                                                                                                                                                                                                                                                                                           |
| `tls_key_file`                 | —             | المسار إلى ملف مفتاح الشهادة.                                                                                                                                                                                                                                                                                                                                     |
| `tls_ca_cert_file`             | —             | المسار إلى ملف شهادة CA.                                                                                                                                                                                                                                                                                                                                          |
| `tls_ca_cert_dir`              | —             | المسار إلى الدليل الذي يحتوي على شهادات CA.                                                                                                                                                                                                                                                                                                                       |
| `tls_cipher_suite`             | —             | مجموعة التعمية المسموح بها (بصياغة OpenSSL).                                                                                                                                                                                                                                                                                                                      |
| `search_limit`                 | `256`         | الحد الأقصى لعدد الإدخالات التي يمكن أن تُرجعها استعلامات بحث LDAP التي ينفذها تعريف هذا الخادم (لاكتشاف `user DN` وتعيين الأدوار).                                                                                                                                                                                                                               |

**المعلمات الفرعية لـ `user_dn_detection`**

قسم يحتوي على معلمات بحث LDAP لاكتشاف `user DN` الفعلي للمستخدم الذي تم تنفيذ `bind` له. يُستخدم هذا أساسًا في عوامل تصفية البحث لمزيد من تعيين الأدوار عندما يكون الخادم Active Directory. سيُستخدم `user DN` الناتج عند استبدال السلاسل الفرعية `{user_dn}` حيثما كان ذلك مسموحًا. افتراضيًا، يُضبط `user DN` ليكون مساويًا لـ `bind DN`، ولكن بمجرد تنفيذ البحث، سيُحدَّث إلى قيمة `user DN` الفعلية المكتشفة.

| المعلمة         | الافتراضي | الوصف                                                                                                                                                                                                                                                                   |
| --------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | قالب يُستخدم لإنشاء `base DN` لبحث LDAP. يُنشأ `DN` الناتج باستبدال جميع السلاسل الفرعية `{user_name}` و`{bind_dn}` في القالب باسم المستخدم الفعلي و`bind DN` أثناء بحث LDAP.                                                                                           |
| `scope`         | `subtree` | نطاق بحث LDAP. القيم المقبولة: `base`، `one_level`، `children`، `subtree`.                                                                                                                                                                                              |
| `search_filter` | —         | قالب يُستخدم لإنشاء عامل تصفية البحث لبحث LDAP. يُنشأ عامل التصفية الناتج باستبدال جميع السلاسل الفرعية `{user_name}` و`{bind_dn}` و`{base_dn}` في القالب باسم المستخدم الفعلي و`bind DN` و`base DN` أثناء بحث LDAP. لاحظ أنه يجب إفلات الأحرف الخاصة بشكل صحيح في XML. |

<div id="ldap-external-authenticator">
  ## المُصادِق الخارجي عبر LDAP
</div>

يمكن استخدام خادم LDAP بعيد كوسيلة للتحقق من كلمات مرور المستخدمين المعرّفين محليًا (أي المستخدمين المعرّفين في `users.xml` أو في مسارات التحكم في الوصول المحلية). ولتحقيق ذلك، حدِّد اسم خادم LDAP المعرّف مسبقًا بدلًا من `password` أو الأقسام المشابهة في تعريف المستخدم.

عند كل محاولة تسجيل دخول، يحاول ClickHouse تنفيذ عملية &quot;bind&quot; إلى الاسم المميّز المحدد (DN) والمُعرَّف بواسطة المعلَمة `bind_dn` في [تعريف خادم LDAP](#ldap-server-definition)، باستخدام بيانات الاعتماد المقدَّمة. وإذا نجحت العملية، يُعدّ المستخدم مُصادَقًا عليه. وغالبًا ما يُشار إلى ذلك باسم طريقة &quot;simple bind&quot;.

**مثال**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

لاحظ أن المستخدم `my_user` يرتبط بـ `my_ldap_server`. يجب تكوين خادم LDAP هذا في ملف `config.xml` الرئيسي كما ذُكر سابقًا.

عند تمكين [التحكم في الوصول وإدارة الحسابات](/ar/operations/access-rights#access-control-usage) المعتمدة على SQL، يمكن أيضًا إنشاء المستخدمين الذين تجري مصادقتهم عبر خوادم LDAP باستخدام تعليمة [CREATE USER](/ar/sql-reference/statements/create/user).

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## دليل المستخدمين الخارجي عبر LDAP
</div>

بالإضافة إلى المستخدمين المعرّفين محليًا، يمكن استخدام خادم LDAP بعيد كمصدر لتعريفات المستخدمين. لتحقيق ذلك، حدِّد اسم خادم LDAP المعرّف مسبقًا (راجع [تعريف خادم LDAP](#ldap-server-definition)) في قسم `ldap` داخل قسم `users_directories` في ملف `config.xml`.

عند كل محاولة لتسجيل الدخول، يحاول ClickHouse العثور على تعريف المستخدم محليًا ومصادقته كالمعتاد. وإذا لم يكن المستخدم معرّفًا، فسيفترض ClickHouse أن تعريفه موجود في دليل LDAP الخارجي، وسيحاول إجراء &quot;bind&quot; إلى DN المحدد على خادم LDAP باستخدام بيانات الاعتماد المقدَّمة. وإذا نجحت العملية، فسيُعتبر المستخدم موجودًا ومصادَقًا عليه. وسيُسنَد إلى المستخدم ما يرد في قسم `roles` من أدوار. بالإضافة إلى ذلك، يمكن تنفيذ &quot;search&quot; في LDAP، ثم تحويل النتائج والتعامل معها على أنها أسماء أدوار وإسنادها إلى المستخدم، إذا كان قسم `role_mapping` مهيأً أيضًا. ويعني هذا كله أن [التحكم في الوصول وإدارة الحسابات](/ar/operations/access-rights#access-control-usage) المعتمدة على SQL مُمكَّنة، وأن الأدوار تُنشأ باستخدام عبارة [CREATE ROLE](/ar/sql-reference/statements/create/role).

**مثال**

يوضع في `config.xml`.

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

لاحظ أن `my_ldap_server` المشار إليه في قسم `ldap` داخل قسم `user_directories` يجب أن يكون خادم LDAP مُعرّفًا مسبقًا ومُهيّأً في `config.xml` (راجع [تعريف خادم LDAP](#ldap-server-definition)).

**المعلمات**

| Parameter | Default | Description                                                                                                                                                                                                              |
| --------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `server`  | —       | أحد أسماء خوادم LDAP المعرّفة في قسم `ldap_servers` في الإعدادات أعلاه. هذا المعامل إلزامي ولا يمكن أن يكون فارغًا.                                                                                                      |
| `roles`   | —       | قسم يحتوي على قائمة بالأدوار المعرّفة محليًا التي ستُسنَد إلى كل مستخدم يُسترجَع من خادم LDAP. إذا لم يتم تحديد أي أدوار هنا أو إسنادها أثناء تعيين الأدوار (أدناه)، فلن يتمكن المستخدم من تنفيذ أي عمليات بعد المصادقة. |

**المعلمات الفرعية `role_mapping`**

قسم يحتوي على معلمات بحث LDAP وقواعد التعيين. عندما يصادق المستخدم، وأثناء استمرار ارتباطه بـ LDAP، يُجرى بحث LDAP باستخدام `search_filter` واسم المستخدم الذي سجّل الدخول. ولكل مُدخل يتم العثور عليه أثناء هذا البحث، تُستخرج قيمة السمة المحددة. ولكل قيمة سمة تحمل البادئة المحددة، تُزال هذه البادئة، ويصبح الجزء المتبقي من القيمة اسم دور محلي معرّف في ClickHouse، ويُفترض أن يكون قد أُنشئ مسبقًا باستخدام عبارة [CREATE ROLE](/ar/sql-reference/statements/create/role). يمكن تعريف عدة أقسام `role_mapping` داخل قسم `ldap` نفسه. وتُطبَّق جميعها.

| المعلمة         | الافتراضي | الوصف                                                                                                                                                                                                                                                                                                                  |
| --------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —         | القالب المستخدم لبناء `base DN` لعملية بحث LDAP. سيُنشأ `DN` الناتج باستبدال جميع السلاسل الفرعية `{user_name}` و`{bind_dn}` و`{user_dn}` في القالب باسم المستخدم الفعلي و`bind DN` و`user DN` أثناء كل بحث LDAP.                                                                                                |
| `scope`         | `subtree` | نطاق بحث LDAP. القيم المقبولة: `base` و`one_level` و`children` و`subtree`.                                                                                                                                                                                                                                          |
| `search_filter` | —         | القالب المستخدم لبناء `عامل تصفية البحث` لعملية بحث LDAP. سيُنشأ عامل التصفية الناتج باستبدال جميع السلاسل الفرعية `{user_name}` و`{bind_dn}` و`{user_dn}` و`{base_dn}` في القالب باسم المستخدم الفعلي و`bind DN` و`user DN` و`base DN` أثناء كل بحث LDAP. لاحظ أنه يجب إجراء إفلات للأحرف الخاصة بشكل صحيح في XML. |
| `attribute`     | `cn`      | اسم السمة التي ستُعاد قيمها بواسطة بحث LDAP.                                                                                                                                                                                                                                                                        |
| `prefix`        | فارغ      | البادئة المتوقعة قبل كل سلسلة في القائمة الأصلية للسلاسل التي يعيدها بحث LDAP. ستُزال البادئة من السلاسل الأصلية، وستُعامل السلاسل الناتجة على أنها أسماء أدوار محلية.                                                                                                                                              |