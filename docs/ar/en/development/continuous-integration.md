---
description: 'نظرة عامة على نظام التكامل المستمر في ClickHouse'
sidebar_label: 'التكامل المستمر (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'التكامل المستمر (CI)'
doc_type: 'مرجع'
---

عند إرسال طلب سحب، يُجري [نظام التكامل المستمر (CI)](tests.md#test-automation) في ClickHouse بعض فحوصات التحقّق المؤتمتة على شيفرتك.
ويحدث ذلك بعد أن يراجع مسؤول صيانة المستودع (أحد أفراد فريق ClickHouse) شيفرتك ويضيف الوسم `can be tested` إلى طلب السحب.
تُعرض نتائج فحوصات التحقّق في صفحة طلب السحب على GitHub كما هو موضّح في [وثائق GitHub الخاصة بحالات التحقّق](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
إذا فشل أحد فحوصات التحقّق، فقد يُطلب منك إصلاحه.
تقدّم هذه الصفحة نظرة عامة على فحوصات التحقّق التي قد تواجهها وما يمكنك فعله لإصلاحها.

إذا بدا أن فشل فحص التحقّق غير مرتبط بتغييراتك، فقد يكون ذلك فشلًا عابرًا أو مشكلة في البنية التحتية.
ادفع `commit` فارغًا إلى طلب السحب لإعادة تشغيل فحوصات `CI`:

```shell
git commit --allow-empty
git push
```

إذا لم تكن متأكدًا مما يجب فعله، فاطلب المساعدة من أحد القائمين على المشروع.

<div id="merge-with-master">
  ## الدمج مع master
</div>

يتحقق من إمكانية دمج طلب السحب في master.
إذا لم يكن ذلك ممكنًا، فسيفشل مع الرسالة `Cannot fetch mergecommit`.
لإصلاح هذا الفحص، عالِج التعارض كما هو موضح في [توثيق GitHub](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github)، أو ادمج الفرع `master` في فرع طلب السحب باستخدام git.

<div id="docs-check">
  ## فحص الوثائق
</div>

يحاول بناء موقع وثائق ClickHouse.
قد يفشل إذا غيّرت شيئًا في الوثائق.
والسبب الأرجح هو وجود رابط داخلي غير صحيح في الوثائق.
انتقل إلى تقرير الفحص وابحث عن رسائل `ERROR` و`WARNING`.

<div id="description-check">
  ## فحص الوصف
</div>

افحص أن وصف طلب السحب الخاص بك يتوافق مع القالب [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
يجب عليك تحديد فئة في سجل التغييرات لتغييرك (على سبيل المثال، Bug Fix)، وكتابة رسالة واضحة للمستخدم تصف التغيير لإضافتها إلى [CHANGELOG.md](../whats-new/changelog/index.md)

<div id="docker-image">
  ## صورة Docker
</div>

يبني صور Docker الخاصة بـ ClickHouse server وKeeper للتحقق من نجاح بنائها.

<div id="official-docker-library-tests">
  ### اختبارات مكتبة Docker الرسمية
</div>

يشغّل هذا اختبارات [مكتبة Docker الرسمية](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files) للتحقق من أن صورة Docker ‏`clickhouse/clickhouse-server` تعمل بشكل صحيح.

لإضافة اختبارات جديدة، أنشئ دليلاً باسم `ci/jobs/scripts/docker_server/tests/$test_name` وأنشئ فيه البرنامج النصي `run.sh`.

يمكن العثور على تفاصيل إضافية حول الاختبارات في [وثائق البرامج النصية لمهام CI](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server).

<div id="marker-check">
  ## فحص العلامة
</div>

يعني هذا الفحص أن نظام CI قد بدأ في معالجة طلب السحب.
عندما تكون حالته &#39;pending&#39;، فهذا يعني أن جميع الفحوصات لم تبدأ بعد.
وبعد بدء جميع الفحوصات، تتغير حالته إلى &#39;success&#39;.

<div id="style-check">
  ## فحص النمط
</div>

يُجري فحوصات متنوعة للنمط على الشيفرة البرمجية. ويتوافق كل فحص فرعي أدناه مع `testname` في [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py)، ويمكن تشغيل كلٍّ منها على حدة باستخدام `--test <name>` (انظر أدناه).

<div id="cpp">
  ##### cpp
</div>

فحوصات أسلوب C++ المعتمدة على Regex عبر [`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh). إذا فشل الفحص، فأصلِح المشكلات وفقًا لـ [دليل أسلوب الشيفرة](style.md).

<div id="whitespace-check">
  ##### whitespace_check
</div>

يكتشف وجود مسافتين بعد الفواصل في C++ إذا لم تكونا جزءًا من محاذاة الأعمدة.

<div id="catch-all">
  ##### catch_all
</div>

يمنع استخدام `catch (...)` خارج الدوال الهدّامة و`main` ونقاط الدخول الخاصة بـ`fuzzer`، إذ إن تجاهل استثناء غير معروف في هذه المواضع غير آمن.

<div id="yamllint">
  ##### yamllint
</div>

يتحقق من ملفات سير العمل بصيغة YAML ضمن `.github/` باستخدام `.yamllint`.

<div id="xmllint">
  ##### xmllint
</div>

يتحقق من سلامة ملفات XML ضمن `tests/` و`programs/`.

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

يفحص الاختبارات غير المعتمدة على الحالة: يجب أن تستخدم الاستعلامات التي تُطبِّق filter على `event_date` ‎`>= yesterday()` بدلًا من `today()` (لتجنّب التذبذب قرب منتصف الليل)، ويجب ألا تتضمن أسماء ملفات الاختبار `fail`.

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

يرصد الفجوات الكبيرة في ترقيم الاختبارات عديمة الحالة (`tests/queries/0_stateless/<NNNNN>_*`).

<div id="symlinks">
  ##### الروابط الرمزية
</div>

يكتشف الروابط الرمزية التالفة في المستودع.

<div id="various">
  ##### متنوعة
</div>

فحوصات متنوعة للمستودع عبر [`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh): يجب أن تُصفّي الاستعلامات على `system.query_log` / `system.parts` / إلخ. بحسب `currentDatabase`، ويجب أن تتضمن مسارات ZooKeeper الخاصة بـ `Replicated*MergeTree` بادئة خاصة بكل اختبار، ويجب أن تحتوي أدلة اختبارات التكامل على `__init__.py`، وألا توجد UTF BOMs، وألا تكون هناك أذونات تنفيذ على ملفات المصدر/البيانات، وألا تُستخدم وسوم `:latest` في صور docker-compose التابعة لجهات خارجية، وغير ذلك.

<div id="running-style-check-locally">
  ### تشغيل مهمة Style Check محليًا
</div>

يمكن تشغيل مهمة *Style Check* بالكامل محليًا داخل حاوية Docker باستخدام:

```sh
python -m ci.praktika run "Style check"
```

لتشغيل فحص محدد (مثل فحص *cpp*):

```sh
python -m ci.praktika run "Style check" --test cpp
```

تسحب هذه الأوامر صورة Docker ‏`clickhouse/style-test` وتشغّل المهمة داخل بيئة معتمدة على الحاويات.
لا توجد حاجة إلى أي تبعيات أخرى سوى بايثون 3 وDocker.

<div id="running-stateless-tests">
  ## تشغيل الاختبارات عديمة الحالة
</div>

قد يعمل ClickHouse المُثبّت محليًا بإعداداته الافتراضية في بعض حالات الاختبار، لكنه لا يستطيع تشغيل جميع استعلامات الاختبار على نحو صحيح. في CI، تُثبِّت كل مهمة تهيئة محددة لـ ClickHouse (مثل S3 storage وparallel replicas)، وقد يكون من المتعب إعادة ذلك يدويًا. لتجنّب هذا، يمكنك إعادة تشغيل أي مهمة من CI محليًا باستخدام آلية التنسيق نفسها المستخدمة في CI، من دون الحاجة إلى أي تهيئة يدوية.

<div id="ci-prerequisites">
  #### المتطلبات الأساسية
</div>

* بايثون 3 (المكتبة القياسية فقط)
* Docker

ثبّت Docker على Ubuntu إذا لزم الأمر، ثم أعِد تسجيل الدخول:

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

<div id="run-ci-job-locally">
  #### تشغيل مهمة CI محليًا
</div>

اختر اسم أي مهمة من تقرير CI وشغّلها محليًا:

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* احرص دائمًا على كتابة اسم المهمة بين علامتَي اقتباس تمامًا كما يظهر في تقرير CI (فقد يحتوي على مسافات وفواصل)، على سبيل المثال: `"Stateless tests (amd_debug, parallel)"`. يضبط هذا الإعدادات نفسها في ClickHouse ويشغّل الاختبارات نفسها المستخدمة في CI.
* المعمارية ونوع البناء في اسم المهمة (مثل `amd_debug`) هما تسميتان خاصتان بـ CI. عند التشغيل محليًا، لا يكون لهما أي تأثير — إذ ستستخدم المهمة الملف التنفيذي الذي توفّره، وعلى أي معمارية تعمل. يحدّد اسم المهمة فقط إعدادات ClickHouse ومجموعة الاختبارات (ما لم يتم تجاوز ذلك باستخدام `--test`).
* في CI، تُقسَّم الاختبارات الوظيفية إلى دفعات لتحسين استخدام الموارد. على سبيل المثال، يغطي كلٌّ من `"Stateless tests (amd_debug, parallel)"` و`"Stateless tests (amd_debug, sequential)"` معًا النطاق الكامل: إذ تُشغَّل الاختبارات الآمنة للتوازي بشكل متزامن، بينما تُشغَّل البقية بشكل تسلسلي. يقلّل هذا التقسيم إجمالي وقت CI إلى أدنى حد عبر زيادة التوازي حيثما أمكن. ولإعادة إنتاج نطاق الاختبار الكامل محليًا، شغّل كلتا الدفعتين.
* توجد أيضًا مهمة CI باسم `"Fast test"` تشغّل نطاقًا محدودًا من الاختبارات الوظيفية للتحقق من الوظائف الأساسية في ClickHouse — وهي تستخدم نسخة بناء لا تتضمن جميع الوحدات الاختيارية، وتُعد أسرع طريقة لاكتشاف حالات التراجع. يمكنك تشغيلها محليًا بالطريقة نفسها. ضع الملف التنفيذي الخاص بـ ClickHouse في أحد مسارات البحث الافتراضية (`./ci/tmp/clickhouse`, `./build/programs/clickhouse`, أو `./clickhouse`) — وإلا فستحاول المهمة أولًا بناء ClickHouse:
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### تشغيل اختبارات محددة داخل مهمة CI
</div>

باستخدام `--test`، تنشئ المهمة إعداد ClickHouse مطابقًا لذلك المستخدم في CI، لكنها لا تشغّل إلا الاختبارات المحددة:

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* يمكنك تمرير أسماء عدة اختبارات:
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* نصيحة: إذا كان أي إعداد لـ ClickHouse مناسبًا وكنت تحتاج فقط إلى تشغيل اختبارات محددة، فاستخدم الاسم المستعار `functional` بدلًا من الاسم الكامل للمهمة:
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### خيارات تخصيص إضافية
</div>

* `--path PATH` — مسار مخصّص إلى الملف التنفيذي لـ ClickHouse. افتراضيًا، يبحث المشغّل بالترتيب في: `./ci/tmp/clickhouse`، و`./build/programs/clickhouse`، و`./clickhouse`.
* `--count N` — كرّر كل اختبار N مرة.
* `--workers N` — تجاوز الحساب التلقائي لعدد الـ workers المتوازيين استنادًا إلى سعة الجهاز.

<div id="build-check">
  ## فحص البناء
</div>

يبني ClickHouse بإعدادات مختلفة لاستخدامه في الخطوات التالية.

<div id="running-builds-locally">
  ### تشغيل عمليات البناء محليًا
</div>

يمكن تشغيل عملية البناء محليًا في بيئة تحاكي CI باستخدام:

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

لا توجد أي تبعيات أخرى مطلوبة سوى بايثون 3 وDocker.

<div id="available-build-jobs">
  #### مهام البناء المتاحة
</div>

أسماء مهام البناء مطابقة تمامًا لما يظهر في تقرير CI:

**بنيات AMD64:**

* `Build (amd_debug)` - بنية Debug مع رموز التصحيح
* `Build (amd_release)` - بنية إصدار محسّنة
* `Build (amd_asan)` - بنية Address Sanitizer
* `Build (amd_tsan)` - بنية Thread Sanitizer
* `Build (amd_msan)` - بنية Memory Sanitizer
* `Build (amd_ubsan)` - بنية Undefined Behavior Sanitizer
* `Build (amd_binary)` - بنية إصدار سريعة بدون Thin LTO
* `Build (amd_compat)` - بنية توافق للأنظمة الأقدم
* `Build (amd_musl)` - بنية باستخدام musl libc
* `Build (amd_darwin)` - بنية MacOS
* `Build (amd_freebsd)` - بنية FreeBSD

**بنيات ARM64:**

* `Build (arm_release)` - بنية إصدار محسّنة لـ ARM64
* `Build (arm_asan)` - بنية Address Sanitizer لـ ARM64
* `Build (arm_coverage)` - بنية ARM64 مع أدوات تغطية
* `Build (arm_binary)` - بنية إصدار سريعة لـ ARM64 بدون Thin LTO
* `Build (arm_darwin)` - بنية MacOS لـ ARM64
* `Build (arm_v80compat)` - بنية توافق ARMv8.0

**معماريات أخرى:**

* `Build (ppc64le)` - PowerPC ‏64-بت بنمط Little Endian
* `Build (riscv64)` - RISC-V ‏64-بت
* `Build (s390x)` - IBM System/390 ‏64-بت
* `Build (loongarch64)` - LoongArch ‏64-بت

إذا نجحت المهمة، فستكون نتائج البناء متاحة في الدليل `<repo_root>/ci/tmp/build`.

**ملاحظة:** بالنسبة إلى البنيات التي لا تندرج ضمن فئة &quot;معماريات أخرى&quot; (والتي تستخدم البناء المتقاطع)، يجب أن تتطابق معمارية جهازك المحلي مع نوع البنية لإنتاجها على النحو الذي يطلبه `BUILD_JOB_NAME`.

<div id="example-run-local">
  #### مثال
</div>

لتشغيل إصدار تصحيح أخطاء محلي:

```bash
python -m ci.praktika run "Build (amd_debug)"
```

إذا لم تنجح معك الطريقة المذكورة أعلاه، فاستخدم خيارات `cmake` من سجلّ البناء واتبع [عملية البناء العامة](../development/build.md).

<div id="functional-stateless-tests">
  ## الاختبارات الوظيفية عديمة الحالة
</div>

يشغّل [الاختبارات الوظيفية عديمة الحالة](tests.md#functional-tests) للملفات التنفيذية لـ ClickHouse المُجمَّعة بإعدادات مختلفة -- release وDebug ومع sanitizers وما إلى ذلك.
اطّلع على التقرير لمعرفة الاختبارات التي تفشل، ثم أعد إنتاج المشكلة محليًا كما هو موضح [هنا](/ar/development/tests#functional-tests).
لاحظ أنه يجب استخدام إعداد البناء الصحيح لإعادة إنتاج المشكلة -- فقد يفشل اختبار مع AddressSanitizer لكنه ينجح في Debug.
نزّل الملف التنفيذي من [صفحة فحوصات بناء CI](/ar/install/advanced)، أو ابنِه محليًا.

<div id="integration-tests">
  ## اختبارات التكامل
</div>

يقوم بتشغيل [اختبارات التكامل](tests.md#integration-tests).

<div id="bugfix-validate-check">
  ## فحص التحقق من إصلاح الأخطاء
</div>

يتحقق من وجود اختبار جديد (وظيفي أو تكاملي)، أو من وجود اختبارات معدّلة تفشل عند تشغيلها باستخدام الملف التنفيذي المبني من فرع master.
يُفعَّل هذا الفحص عندما يكون طلب السحب موسومًا بالوسم &quot;pr-bugfix&quot;.

<div id="stress-test">
  ## اختبار الإجهاد
</div>

يشغّل اختبارات وظيفية عديمة الحالة بالتوازي من عدة عملاء لاكتشاف الأخطاء المتعلقة بالتزامن. إذا فشل:

* أصلح جميع حالات فشل الاختبارات الأخرى أولاً؛
  * راجع التقرير للعثور على سجلات الخادم والتحقق منها لمعرفة الأسباب المحتملة
    للخطأ.

<div id="compatibility-check">
  ## فحص التوافق
</div>

يتحقق من أن الملف التنفيذي `clickhouse` يعمل على التوزيعات التي تستخدم إصدارات قديمة من libc.
إذا فشل ذلك، فاطلب المساعدة من مسؤول الصيانة.

<div id="ast-fuzzer">
  ## AST fuzzer
</div>

يشغّل استعلامات مُولَّدة عشوائيًا لاكتشاف أخطاء البرنامج.
إذا أخفق، فاطلب المساعدة من مسؤول الصيانة.

<div id="performance-tests">
  ## اختبارات الأداء
</div>

قِس التغيّرات في أداء الاستعلامات.
هذا هو أطول فحص، ويستغرق تشغيله ما يقل قليلًا عن 6 ساعات.
يَرِد شرحٌ مفصّل لتقرير اختبار الأداء [هنا](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report).