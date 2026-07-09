---
description: 'توثيق SSL X.509'
slug: /operations/external-authenticators/ssl-x509
title: 'المصادقة باستخدام شهادات SSL X.509'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

يُفعّل [خيار SSL &#39;strict&#39;](../server-configuration-parameters/settings.md#openssl) التحقق الإلزامي من الشهادات للاتصالات الواردة. وفي هذه الحالة، لا يمكن إنشاء سوى الاتصالات التي تستخدم شهادات موثوقًا بها. أما الاتصالات التي تستخدم شهادات غير موثوق بها فسيتم رفضها. وبذلك، يتيح التحقق من الشهادة المصادقة بشكل فريد على الاتصال الوارد. ويُستخدم الحقل `Common Name` أو `subjectAltName extension` في الشهادة لتحديد هوية المستخدم المتصل. كما يدعم `subjectAltName extension` استخدام رمز بدل واحد &#39;*&#39; في تهيئة الخادم. ويتيح ذلك ربط عدة شهادات بالمستخدم نفسه. بالإضافة إلى ذلك، فإن إعادة إصدار الشهادات وإبطالها لا تؤثر في إعدادات ClickHouse.

لتمكين المصادقة باستخدام شهادة SSL، يجب تحديد قائمة من قيم `Common Name` أو `Subject Alt Name` لكل مستخدم ClickHouse في ملف الإعدادات `users.xml `:

**مثال**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name_1>
            <ssl_certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_1>
        <user_name_2>
            <ssl_certificates>
                <subject_alt_name>DNS:host.domain.com</subject_alt_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_2>
        <user_name_3>
            <ssl_certificates>
                <!-- Wildcard support -->
                <subject_alt_name>URI:spiffe://foo.com/*/bar</subject_alt_name>
            </ssl_certificates>
        </user_name_3>
    </users>
</clickhouse>
```

لكي تعمل [`سلسلة الثقة`](https://en.wikipedia.org/wiki/Chain_of_trust) الخاصة بـ SSL بشكل صحيح، من المهم أيضًا التأكد من تهيئة المعلمة [`caConfig`](../server-configuration-parameters/settings.md#openssl) بالشكل الصحيح.