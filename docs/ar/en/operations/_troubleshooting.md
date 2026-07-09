---
title: استكشاف الأخطاء وإصلاحها
---

[//]: # "هذا الملف مُضمَّن في FAQ > استكشاف الأخطاء وإصلاحها"

* [التثبيت](#troubleshooting-installation-errors)
* [الاتصال بالخادم](#troubleshooting-accepts-no-connections)
* [معالجة الاستعلامات](#troubleshooting-does-not-process-queries)
* [كفاءة معالجة الاستعلامات](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## التثبيت
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### لا يمكنك الحصول على حزم deb من مستودع ClickHouse باستخدام apt-get
</div>

* تحقّق من إعدادات جدار الحماية.
* إذا لم تتمكن من الوصول إلى المستودع لأي سبب، فنزّل الحزم كما هو موضح في مقال [دليل التثبيت](../getting-started/install.md)، ثم ثبّتها يدويًا باستخدام الأمر `sudo dpkg -i <packages>`. وستحتاج أيضًا إلى الحزمة `tzdata`.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### لا يمكنك تحديث حزم deb من مستودع ClickHouse باستخدام apt-get
</div>

* قد تحدث هذه المشكلة عند تغيير مفتاح GPG.

يُرجى استخدام الإرشادات الواردة في صفحة [الإعداد](../getting-started/install.md#setup-the-debian-repository) لتحديث إعدادات المستودع.

<div id="you-get-different-warnings-with-apt-get-update">
  ### تظهر لك رسائل تحذير مختلفة عند تشغيل `apt-get update`
</div>

* تأتي رسائل التحذير الكاملة على أحد الأشكال التالية:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

لحل المشكلة المذكورة أعلاه، يُرجى استخدام البرنامج النصي التالي:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### لا يمكنك الحصول على الحزم باستخدام yum بسبب توقيع غير صحيح
</div>

المشكلة المحتملة: ذاكرة التخزين المؤقت غير صحيحة، وربما تعطّلت بعد تحديث مفتاح GPG في 2022-09.

الحل هو تنظيف ذاكرة التخزين المؤقت ودليل lib الخاصَّين بـ yum:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

بعد ذلك، اتبع [دليل التثبيت](../getting-started/install.md#from-rpm-packages)

<div id="you-cant-run-docker-container">
  ### لا يمكنك تشغيل حاوية Docker
</div>

إذا شغّلت الأمر البسيط `docker run clickhouse/clickhouse-server`، فقد يتعطل ويعرض تتبّع مكدس استدعاء مشابهًا لما يلي:

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

السبب هو استخدام برنامج docker daemon قديم بإصدار أقدم من `20.10.10`. ويمكن إصلاح ذلك إما بترقيته أو بتشغيل `docker run [--privileged | --security-opt seccomp=unconfined]`. لكن الخيار الأخير له تبعات أمنية.

<div id="troubleshooting-accepts-no-connections">
  ## الاتصال بالخادم
</div>

المشكلات المحتملة:

* الخادم غير قيد التشغيل.
* معلمة التكوين غير متوقعة أو غير صحيحة.

<div id="server-is-not-running">
  ### الخادم لا يعمل
</div>

**تحقق من تشغيل الخادم**

الأمر:

```bash
$ sudo service clickhouse-server status
```

إذا لم يكن الخادم يعمل، فابدأه باستخدام الأمر:

```bash
$ sudo service clickhouse-server start
```

**تحقّق من السجلات**

يوجد السجل الرئيسي لـ `clickhouse-server` افتراضيًا في `/var/log/clickhouse-server/clickhouse-server.log`.

إذا بدأ الخادم بنجاح، فمن المفترض أن ترى السلاسل النصية التالية:

* `<Information> Application: starting up.` — تم تشغيل الخادم.
* `<Information> Application: Ready for connections.` — الخادم قيد التشغيل وجاهز للاتصالات.

إذا فشل تشغيل `clickhouse-server` بسبب خطأ في التهيئة، فمن المفترض أن ترى السلسلة `<Error>` متبوعة بوصف للخطأ. على سبيل المثال:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

إذا لم تظهر رسالة خطأ في نهاية الملف، فتفقّد الملف بالكامل بدءًا من السلسلة النصية:

```text
<Information> Application: starting up.
```

إذا حاولت تشغيل مثيل ثانٍ من `clickhouse-server` على الخادم، فسيظهر لك السجل التالي:

```text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**اطّلع على سجلات system.d**

إذا لم تجد أي معلومات مفيدة في سجلات `clickhouse-server`، أو لم تكن هناك سجلات أصلًا، فيمكنك عرض سجلات `system.d` باستخدام الأمر:

```bash
$ sudo journalctl -u clickhouse-server
```

**شغّل clickhouse-server في الوضع التفاعلي**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

يشغّل هذا الأمر الخادم كتطبيق تفاعلي باستخدام المعاملات القياسية لبرنامج نصي التشغيل التلقائي. في هذا الوضع، يطبع `clickhouse-server` جميع رسائل الأحداث في الطرفية.

<div id="configuration-parameters">
  ### معلمات التكوين
</div>

تحقق مما يلي:

* إعدادات Docker.

  إذا كنت تشغّل ClickHouse في Docker ضمن شبكة IPv6، فتأكد من ضبط `network=host`.

* إعدادات نقطة النهاية.

  تحقّق من إعدادَي [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) و[tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port).

  لا يقبل ClickHouse server افتراضيًا سوى اتصالات localhost.

* إعدادات بروتوكول HTTP.

  تحقّق من إعدادات البروتوكول الخاصة بواجهة برمجة تطبيقات HTTP.

* إعدادات الاتصال الآمن.

  تحقّق مما يلي:

  * الإعداد [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure).
  * إعدادات [شهادات SSL](../operations/server-configuration-parameters/settings.md#openssl).

    استخدم المعلمات المناسبة عند الاتصال. على سبيل المثال، استخدم المعلمة `port_secure` مع `clickhouse_client`.

* إعدادات المستخدم.

  قد تكون تستخدم اسم مستخدم أو كلمة مرور غير صحيحَين.

<div id="troubleshooting-does-not-process-queries">
  ## معالجة الاستعلامات
</div>

إذا لم يتمكّن ClickHouse من معالجة الاستعلام، فإنه يرسل وصفًا للخطأ إلى العميل. في `clickhouse-client` ستظهر لك رسالة الخطأ في وحدة التحكم. وإذا كنت تستخدم واجهة HTTP، فسيرسل ClickHouse وصف الخطأ في جسم الاستجابة. على سبيل المثال:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

إذا بدأت `clickhouse-client` باستخدام المَعلمة `stack-trace`، فسيُرجع ClickHouse تتبّع مكدس استدعاءات الخادم مع وصفٍ للخطأ.

قد ترى رسالة تفيد بوجود انقطاع في الاتصال. في هذه الحالة، يمكنك إعادة تنفيذ الاستعلام. إذا انقطع الاتصال في كل مرة تنفّذ فيها الاستعلام، فتحقّق من سجلات الخادم بحثًا عن أخطاء.

<div id="troubleshooting-too-slow">
  ## كفاءة معالجة الاستعلامات
</div>

إذا لاحظت أن ClickHouse يعمل ببطء شديد، فعليك تحليل الحمل على موارد الخادم والشبكة الناتج عن استعلاماتك.

يمكنك استخدام الأداة المساعدة clickhouse-benchmark لتحليل الاستعلامات. فهي تعرض عدد الاستعلامات المُعالجة في الثانية، وعدد الصفوف المُعالجة في الثانية، والقيم المئينية لأزمنة معالجة الاستعلامات.