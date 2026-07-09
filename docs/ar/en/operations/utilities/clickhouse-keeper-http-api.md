---
description: 'توثيق واجهة برمجة تطبيقات HTTP لـ ClickHouse Keeper ولوحة معلومات الويب المضمّنة'
sidebar_label: 'واجهة برمجة تطبيقات HTTP لـ Keeper'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'واجهة برمجة تطبيقات HTTP لـ Keeper ولوحة المعلومات'
doc_type: 'reference'
---

يوفّر ClickHouse Keeper واجهة برمجة تطبيقات HTTP ولوحة معلومات ويب مضمّنة لأغراض المراقبة، والتحقّق من السلامة، وإدارة التخزين.
تتيح هذه الواجهة للمشغّلين فحص حالة العنقود، وتنفيذ الأوامر، وإدارة تخزين Keeper عبر متصفّح ويب أو عملاء HTTP.

<div id="configuration">
  ## الإعداد
</div>

لتمكين واجهة برمجة تطبيقات HTTP، أضِف القسم `http_control` إلى إعداد `keeper_server`:

```xml
<keeper_server>
    <!-- Other keeper_server configuration -->

    <http_control>
        <port>9182</port>
        <!-- <secure_port>9443</secure_port> -->
    </http_control>
</keeper_server>
```

<div id="configuration-options">
  ### خيارات الإعداد
</div>

| الإعداد                                   | الافتراضي | الوصف                                            |
| ----------------------------------------- | --------- | ------------------------------------------------ |
| `http_control.port`                       | -         | منفذ HTTP للوحة المعلومات وواجهة برمجة التطبيقات |
| `http_control.secure_port`                | -         | منفذ HTTPS (يتطلب إعداد SSL)                     |
| `http_control.readiness.endpoint`         | `/ready`  | مسار مخصص لمسبار الجاهزية                        |
| `http_control.storage.session_timeout_ms` | `30000`   | مهلة الجلسة لعمليات واجهة برمجة تطبيقات التخزين  |

<div id="endpoints">
  ## نقاط النهاية
</div>

<div id="dashboard">
  ### لوحة المعلومات
</div>

* **المسار**: `/dashboard`
* **الطريقة**: GET
* **الوصف**: يعرض لوحة معلومات ويب مضمّنة لمراقبة Keeper وإدارته

توفّر لوحة المعلومات ما يلي:

* تصور حالة العنقود في الوقت الفعلي
* مراقبة العقد (الدور، زمن الاستجابة، الاتصالات)
* مستعرض التخزين
* واجهة تنفيذ الأوامر

<div id="readiness-probe">
  ### مسبار الجاهزية
</div>

* **المسار**: `/ready` (قابل للضبط)
* **الطريقة**: GET
* **الوصف**: نقطة نهاية للتحقق من الحالة الصحية

استجابة النجاح (HTTP 200):

```json
{
  "status": "ok",
  "details": {
    "role": "leader",
    "hasLeader": true
  }
}
```

<div id="commands-api">
  ### واجهة برمجة تطبيقات الأوامر
</div>

* **المسار**: `/api/v1/commands/{command}`
* **الطرق**: GET, POST
* **الوصف**: ينفّذ أوامر Four-Letter Word أو أوامر CLI الخاصة بـ ClickHouse Keeper Client

معلمات الاستعلام:

* `command` - الأمر المطلوب تنفيذه
* `cwd` - دليل العمل الحالي للأوامر المستندة إلى المسار (الافتراضي: `/`)

أمثلة:

```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

<div id="storage-api">
  ### واجهة برمجة تطبيقات التخزين
</div>

* **المسار الأساسي**: `/api/v1/storage`
* **الوصف**: واجهة برمجة تطبيقات REST لعمليات التخزين في Keeper

تتبع واجهة برمجة تطبيقات التخزين اصطلاحات REST، حيث تشير طرق HTTP إلى نوع العملية:

| العملية          | المسار                                 | الطريقة | رمز الحالة | الوصف                             |
| ---------------- | -------------------------------------- | ------- | ---------- | --------------------------------- |
| جلب              | `/api/v1/storage/{path}`               | GET     | 200        | جلب بيانات العقدة                 |
| إدراج            | `/api/v1/storage/{path}?children=true` | GET     | 200        | إدراج العقد الفرعية               |
| التحقق من الوجود | `/api/v1/storage/{path}`               | HEAD    | 200        | التحقق مما إذا كانت العقدة موجودة |
| إنشاء            | `/api/v1/storage/{path}`               | POST    | 201        | إنشاء عقدة جديدة                  |
| تحديث            | `/api/v1/storage/{path}?version={v}`   | PUT     | 200        | تحديث بيانات العقدة               |
| حذف              | `/api/v1/storage/{path}?version={v}`   | DELETE  | 204        | حذف العقدة                        |