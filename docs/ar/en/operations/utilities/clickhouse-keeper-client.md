---
description: 'توثيق أداة عميل ClickHouse Keeper'
sidebar_label: 'clickhouse-keeper-client'
slug: /operations/utilities/clickhouse-keeper-client
title: 'أداة clickhouse-keeper-client'
doc_type: 'reference'
---

تطبيق عميل للتفاعل مع clickhouse-keeper عبر بروتوكوله الأصلي.

<div id="clickhouse-keeper-client">
  ## الخيارات
</div>

* `-q QUERY`, `--query=QUERY` — الاستعلام المراد تنفيذه. إذا لم يتم تمرير هذا المعلَم، فسيبدأ `clickhouse-keeper-client` في الوضع التفاعلي.
* `-h HOST`, `--host=HOST` — مضيف الخادم. القيمة الافتراضية: `localhost`.
* `-p N`, `--port=N` — منفذ الخادم. القيمة الافتراضية: 9181
* `-c FILE_PATH`, `--config-file=FILE_PATH` — تعيين مسار ملف الإعدادات للحصول على سلسلة الاتصال. القيمة الافتراضية: `config.xml`.
* `--password=PASSWORD` — كلمة المرور للمصادقة. يمكن أيضًا تعيينها عبر متغير البيئة `CLICKHOUSE_KEEPER_PASSWORD` أو في ملف إعدادات XML تحت `<zookeeper><password>`.
* `--identity=IDENTITY` — الهوية الخاصة بمخطط المصادقة `digest`. يمكن أيضًا تعيينها عبر متغير البيئة `CLICKHOUSE_KEEPER_IDENTITY` أو في ملف إعدادات XML تحت `<zookeeper><identity>`.
* `--connection-timeout=TIMEOUT` — تعيين مهلة الاتصال بالثواني. القيمة الافتراضية: 10s.
* `--session-timeout=TIMEOUT` — تعيين مهلة الجلسة بالثواني. القيمة الافتراضية: 10s.
* `--operation-timeout=TIMEOUT` — تعيين مهلة العملية بالثواني. القيمة الافتراضية: 10s.
* `--history-file=FILE_PATH` — تعيين مسار ملف المحفوظات. القيمة الافتراضية: `~/.keeper-client-history`.
* `--log-level=LEVEL` — تعيين مستوى السجل. القيمة الافتراضية: `information`.
* `--no-confirmation` — إذا تم تعيينه، فلن يتطلب تأكيدًا لبعض الأوامر. القيمة الافتراضية `false` للوضع التفاعلي و`true` للاستعلام
* `--help` — يعرض رسالة المساعدة.

<div id="clickhouse-keeper-client-env">
  ## متغيرات البيئة
</div>

* `CLICKHOUSE_KEEPER_PASSWORD` — تُستخدم كلمة المرور الافتراضية إذا لم يتم تمرير `--password` في سطر الأوامر.
* `CLICKHOUSE_KEEPER_IDENTITY` — تُستخدم الهوية الافتراضية إذا لم يتم تمرير `--identity` في سطر الأوامر.

<div id="clickhouse-keeper-client-auth">
  ## المصادقة
</div>

عند الاتصال بخادم Keeper يتطلب مصادقة، تُحدَّد كلمة المرور وفق ترتيب الأولوية التالي (تُعتمد أول قيمة مطابقة):

1. وسيطة سطر الأوامر `--password`
2. متغير البيئة `CLICKHOUSE_KEEPER_PASSWORD`
3. `<zookeeper><password>` في ملف إعدادات XML المحدد بواسطة `--config-file`

وينطبق ترتيب الأولوية نفسه على `--identity` / `CLICKHOUSE_KEEPER_IDENTITY` / `<zookeeper><identity>`.

مثال على ملف إعدادات XML يتضمن إعدادات المصادقة:

```xml
<clickhouse>
    <zookeeper>
        <password>secret</password>
        <node index="1">
            <host>localhost</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

<div id="clickhouse-keeper-client-example">
  ## مثال
</div>

```bash
./clickhouse-keeper-client -h localhost -p 9181 --connection-timeout 30 --session-timeout 30 --operation-timeout 30
Connected to ZooKeeper at [::1]:9181 with session_id 137
/ :) ls
keeper foo bar
/ :) cd 'keeper'
/keeper :) ls
api_version
/keeper :) cd 'api_version'
/keeper/api_version :) ls

/keeper/api_version :) cd 'xyz'
Path /keeper/api_version/xyz does not exist
/keeper/api_version :) cd ../../
/ :) ls
keeper foo bar
/ :) get 'keeper/api_version'
2
```

<div id="clickhouse-keeper-client-commands">
  ## الأوامر
</div>

* `ls '[path]' [watch_id]` -- يسرد العقد الموجودة ضمن المسار المحدد (الافتراضي: cwd). ويمكنه اختياريًا تعيين watch للأبناء يحدّده `watch_id`
* `cd '[path]'` -- يغيّر مسار العمل (الافتراضي `.`)
* `cp '<src>' '<dest>'`  -- ينسخ العقدة &#39;src&#39; إلى المسار &#39;dest&#39;
* `cpr '<src>' '<dest>'`  -- ينسخ الشجرة الفرعية للعقدة &#39;src&#39; إلى المسار &#39;dest&#39;
* `mv '<src>' '<dest>'`  -- ينقل العقدة &#39;src&#39; إلى المسار &#39;dest&#39;
* `mvr '<src>' '<dest>'`  -- ينقل الشجرة الفرعية للعقدة &#39;src&#39; إلى المسار &#39;dest&#39;
* `exists '<path>' [watch_id]` -- يعيد `1` إذا كانت العقدة موجودة، و`0` خلاف ذلك. ويمكنه اختياريًا تعيين watch يحدّده `watch_id`
* `set '<path>' <value> [version]` -- يحدّث قيمة العقدة. ولا يُجري التحديث إلا إذا تطابق الإصدار (الافتراضي: -1)
* `create '<path>' <value> [mode]` -- ينشئ عقدة جديدة بالقيمة المحددة
* `touch '<path>'` -- ينشئ عقدة جديدة بقيمة سلسلة نصية فارغة. ولا يطرح استثناءً إذا كانت العقدة موجودة بالفعل
* `get '<path>' [watch_id]` -- يعيد قيمة العقدة. ويمكنه اختياريًا تعيين watch للبيانات يحدّده `watch_id`
* `watch <watch_id> [timeout_seconds]` -- ينتظر حدث watch الذي يحدّده `watch_id` ويطبع نوع الحدث والمسار. وإذا تم تحديد `timeout_seconds`، يعيد خطأً بعد انتهاء المهلة المحددة
* `rm '<path>' [version]` -- يزيل العقدة فقط إذا تطابق الإصدار (الافتراضي: -1)
* `rmr '<path>' [limit]` -- يحذف المسار تكراريًا إذا كان حجم الشجرة الفرعية أصغر من الحد. ويتطلب تأكيدًا (الحد الافتراضي = 100)
* `flwc <command>` -- ينفّذ أمر الأحرف الأربعة
* `help` -- يطبع هذه الرسالة
* `get_direct_children_number '[path]'` -- يجلب عدد عقد الأبناء المباشرين ضمن مسار محدد
* `get_all_children_number '[path]'` -- يجلب العدد الإجمالي لعقد الأبناء ضمن مسار محدد
* `get_stat '[path]'` -- يعيد إحصاءات العقدة (الافتراضي `.`)
* `find_super_nodes <threshold> '[path]'` -- يعثر على العقد التي يتجاوز عدد أبنائها حدًا معيّنًا ضمن المسار المحدد (الافتراضي `.`)
* `delete_stale_backups` -- يحذف عقد ClickHouse المستخدمة للنسخ الاحتياطية والتي أصبحت غير نشطة
* `find_big_family [path] [n]` -- يعيد أعلى n عقد ذات أكبر عدد من الأبناء في الشجرة الفرعية (المسار الافتراضي = `.` و n = 10)
* `sync '<path>'` -- يزامن العقدة بين العمليات والعقدة القائدة
* `reconfig <add|remove|set> "<arg>" [version]` -- يعيد تهيئة عنقود Keeper. راجع /docs/en/guides/sre/keeper/clickhouse-keeper#reconfiguration