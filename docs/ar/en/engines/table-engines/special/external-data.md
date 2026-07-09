---
description: 'يتيح ClickHouse إرسال البيانات التي يحتاجها الخادم لمعالجة
  استعلام، وذلك مع استعلام `SELECT`. توضع هذه البيانات في جدول مؤقت
  ويمكن استخدامها في الاستعلام (على سبيل المثال، في عوامل التشغيل `IN`).'
sidebar_label: 'البيانات الخارجية لمعالجة الاستعلام'
sidebar_position: 130
slug: /engines/table-engines/special/external-data
title: 'البيانات الخارجية لمعالجة الاستعلام'
doc_type: 'reference'
---

يتيح ClickHouse إرسال البيانات التي يحتاجها الخادم لمعالجة استعلام، وذلك مع استعلام `SELECT`. توضع هذه البيانات في جدول مؤقت (راجع قسم &quot;الجداول المؤقتة&quot;) ويمكن استخدامها في الاستعلام (على سبيل المثال، في عوامل التشغيل `IN`).

على سبيل المثال، إذا كان لديك ملف نصي يحتوي على معرّفات مستخدمين مهمة، فيمكنك رفعه إلى الخادم مع استعلام يستخدم التصفية بناءً على هذه القائمة.

إذا كنت بحاجة إلى تشغيل أكثر من استعلام واحد مع حجم كبير من البيانات الخارجية، فلا تستخدم هذه الميزة. من الأفضل رفع البيانات إلى قاعدة البيانات مسبقًا.

يمكن رفع البيانات الخارجية باستخدام عميل سطر الأوامر (في الوضع غير التفاعلي)، أو عبر واجهة HTTP.

في عميل سطر الأوامر، يمكنك تحديد قسم المعلمات بالتنسيق

```bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

قد يكون لديك عدة أقسام من هذا النوع، بحسب عدد الجداول التي يتم إرسالها.

**–external** – يشير إلى بداية بند.
**–file** – المسار إلى الملف الذي يحتوي على dump الجدول، أو -، الذي يشير إلى stdin.
لا يمكن جلب سوى جدول واحد من stdin.

المعلمات التالية اختيارية: **–name**– اسم الجدول. إذا لم يُحدَّد، فسيُستخدم &#95;data.
**–format** – تنسيق البيانات في الملف. إذا لم يُحدَّد، فسيُستخدم TabSeparated.

أحد المعلمات التالية مطلوب:**–types** – قائمة بأنواع الأعمدة مفصولة بفواصل. على سبيل المثال: `UInt64,String`. ستُسمّى الأعمدة &#95;1 و&#95;2 و...
**–structure**– بنية الجدول بالتنسيق `UserID UInt64`, `URL String`. ويحدّد أسماء الأعمدة وأنواعها.

ستُحلَّل الملفات المحددة في &#39;file&#39; بالتنسيق المحدد في &#39;format&#39;، باستخدام أنواع البيانات المحددة في &#39;types&#39; أو &#39;structure&#39;. وسيُحمَّل الجدول إلى الخادم ويكون متاحًا هناك كجدول مؤقت بالاسم المحدد في &#39;name&#39;.

أمثلة:

```bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

عند استخدام واجهة HTTP، تُرسَل البيانات الخارجية بتنسيق multipart/form-data. ويُرسَل كل جدول كملف منفصل. ويُستمد اسم الجدول من اسم الملف. وتُمرَّر المعاملات `name_format` و`name_types` و`name_structure` إلى `query_string`، حيث يشير `name` إلى اسم الجدول الذي تخصّه هذه المعاملات. ويكون معنى هذه المعاملات هو نفسه عند استخدام عميل سطر الأوامر.

مثال:

```bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

في معالجة الاستعلامات الموزعة، تُرسَل الجداول المؤقتة إلى جميع الخوادم البعيدة.