---
description: 'تتيح البروتوكولات القابلة للتركيب مرونة أكبر في تهيئة الوصول عبر TCP إلى خادم ClickHouse.'
sidebar_label: 'Composable protocols'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Composable protocols'
doc_type: 'reference'
---

<div id="overview">
  ## نظرة عامة
</div>

تتيح البروتوكولات القابلة للتركيب إعدادًا أكثر مرونة للوصول عبر TCP إلى
خادم ClickHouse. ويمكن أن يتعايش هذا الإعداد مع الإعداد
التقليدي أو أن يحلّ محلّه.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## إعداد البروتوكولات القابلة للتركيب
</div>

يمكن إعداد البروتوكولات القابلة للتركيب في ملف تهيئة بصيغة XML. ويُشار إلى قسم
البروتوكولات بوسوم `protocols` في ملف تهيئة XML:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### إعداد طبقات البروتوكول
</div>

يمكنك تحديد طبقات البروتوكول باستخدام وحدات أساسية. على سبيل المثال، لتحديد
طبقة HTTP، يمكنك إضافة وحدة أساسية جديدة إلى قسم `protocols`:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

يمكن تهيئة الوحدات على النحو التالي:

* `plain_http` - اسم يمكن لطبقة أخرى الإشارة إليه
* `type` - يحدد معالج البروتوكول الذي سيُنشأ لمعالجة البيانات.
  وتتضمن مجموعة معالجات البروتوكول المعرّفة مسبقًا التالية:
  * `tcp` - معالج بروتوكول ClickHouse الأصلي
  * `http` - معالج بروتوكول HTTP لـ ClickHouse
  * `tls` - طبقة تشفير TLS
  * `proxy1` - طبقة PROXYv1
  * `mysql` - معالج بروتوكول التوافق مع MySQL
  * `postgres` - معالج بروتوكول التوافق مع PostgreSQL
  * `prometheus` - معالج بروتوكول Prometheus
  * `interserver` - معالج الاتصال بين خوادم ClickHouse

:::note
لم يُنفَّذ معالج بروتوكول `gRPC` في `البروتوكولات القابلة للتركيب`
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### إعداد نقاط النهاية
</div>

يُشار إلى نقاط النهاية (منافذ الاستماع) باستخدام الوسمين `<port>` و`<host>`، على أن يكون الأخير اختياريًا.
على سبيل المثال، لإعداد نقطة نهاية على طبقة HTTP التي أُضيفت سابقًا، يمكننا
تعديل الإعدادات على النحو التالي:

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

إذا أُغفل الوسم `<host>`، فسيُستخدَم `<listen_host>` من إعدادات الجذر.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### تهيئة تسلسلات الطبقات
</div>

تُعرَّف تسلسلات الطبقات باستخدام الوسم `<impl>`، مع الإشارة إلى
وحدة أخرى. على سبيل المثال، لتهيئة طبقة TLS فوق وحدة plain&#95;http الخاصة بنا،
يمكننا تعديل الإعدادات بشكل إضافي على النحو التالي:

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### ربط نقاط النهاية بالطبقات
</div>

يمكن ربط نقاط النهاية بأي طبقة. على سبيل المثال، يمكننا تعريف نقاط نهاية لكل من
HTTP (المنفذ 8123) وHTTPS (المنفذ 8443):

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### تعريف نقاط نهاية إضافية
</div>

يمكن تعريف نقاط نهاية إضافية بالإشارة إلى أي وحدة وحذف
الوسم `<type>`. على سبيل المثال، يمكننا تعريف نقطة النهاية `another_http` للوحدة
`plain_http` كما يلي:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### معالجات HTTP مخصّصة لكل نقطة نهاية
</div>

افتراضيًا، تشترك جميع إدخالات البروتوكول `type=http` في إعداد
`<http_handlers>` نفسه. يمكنك تغيير ذلك بإضافة وسم `<handlers>` يشير
إلى قسم إعدادات مختلف. يتيح هذا لكل منفذ HTTP تقديم مجموعة مختلفة
من قواعد توجيه HTTP.

على سبيل المثال، لتشغيل واجهة برمجة تطبيقات HTTP بديلة على المنفذ 8124 مع معالجاتها الخاصة:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

في هذا المثال، تستخدم الطلبات الواردة إلى المنفذ 8123 قواعد `<http_handlers>` القياسية،
بينما تستخدم الطلبات الواردة إلى المنفذ 8124 قواعد `<http_handlers_alt>`. وإذا تم حذف `<handlers>`
فستعود نقطة النهاية إلى `<http_handlers>` الافتراضي.

يتبع قسم المعالجات المخصصة التنسيق نفسه كما في
[`<http_handlers>`](/ar/docs/operations/server-configuration-parameters/settings#http_handlers).
ويتم اكتشاف التغييرات التي تطرأ على قسم المعالجات المخصصة أثناء إعادة تحميل config، كما
تُعاد تشغيل نقطة النهاية المقابلة تلقائيًا.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### تحديد معلمات إضافية للطبقة
</div>

قد تتضمن بعض الوحدات معلمات إضافية للطبقة. على سبيل المثال، تتيح طبقة TLS
تحديد مفتاح خاص (`privateKeyFile`) وملفات شهادة (`certificateFile`)
كما يلي:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```