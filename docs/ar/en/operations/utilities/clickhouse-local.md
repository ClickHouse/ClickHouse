---
description: 'دليل استخدام clickhouse-local لمعالجة البيانات دون الحاجة إلى خادم'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'reference'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## متى تستخدم clickhouse-local مقابل ClickHouse
</div>

يُعد `clickhouse-local` إصدارًا سهل الاستخدام من ClickHouse، وهو مثالي للمطورين الذين يحتاجون إلى معالجة سريعة للملفات المحلية والبعيدة باستخدام SQL من دون الحاجة إلى تثبيت خادم قاعدة بيانات كامل. باستخدام `clickhouse-local`، يمكن للمطورين تنفيذ أوامر SQL (باستخدام [لهجة ClickHouse SQL](../../sql-reference/index.md)) مباشرةً من سطر الأوامر، مما يوفّر طريقة بسيطة وفعّالة للوصول إلى ميزات ClickHouse من دون الحاجة إلى تثبيت ClickHouse كامل. ومن أبرز مزايا `clickhouse-local` أنه يكون مُضمّنًا بالفعل عند تثبيت [clickhouse-client](/ar/operations/utilities/clickhouse-local). وهذا يعني أن المطورين يمكنهم البدء باستخدام `clickhouse-local` بسرعة، من دون الحاجة إلى عملية تثبيت معقدة.

ومع أن `clickhouse-local` أداة ممتازة لأغراض التطوير والاختبار ومعالجة الملفات، فإنه غير مناسب لخدمة المستخدمين النهائيين أو التطبيقات. في هذه الحالات، يُوصى باستخدام [ClickHouse](/ar/install) مفتوح المصدر. ClickHouse هي قاعدة بيانات OLAP قوية صُممت للتعامل مع أعباء العمل التحليلية واسعة النطاق. وهي توفّر معالجة سريعة وفعّالة للاستعلامات المعقدة على مجموعات البيانات الكبيرة، مما يجعلها مثالية للاستخدام في بيئات production التي يكون فيها الأداء العالي بالغ الأهمية. بالإضافة إلى ذلك، يوفّر ClickHouse مجموعة واسعة من الميزات مثل النسخ المتماثل، وتجزئة البيانات، والتوافر العالي، وهي أمور أساسية للتوسع والتعامل مع مجموعات البيانات الكبيرة وخدمة التطبيقات. إذا كنت بحاجة إلى التعامل مع مجموعات بيانات أكبر أو خدمة المستخدمين النهائيين أو التطبيقات، فنحن نوصي باستخدام ClickHouse مفتوح المصدر بدلًا من `clickhouse-local`.

يُرجى قراءة الوثائق أدناه التي تعرض أمثلة على حالات استخدام `clickhouse-local`، مثل [الاستعلام عن ملف محلي](#query_data_in_file) أو [قراءة ملف Parquet في S3](#query-data-in-a-parquet-file-in-aws-s3).

<div id="download-clickhouse-local">
  ## تنزيل clickhouse-local
</div>

يُشغَّل `clickhouse-local` باستخدام الملف التنفيذي `clickhouse` نفسه الذي يُشغِّل خادم ClickHouse و`clickhouse-client`. وأسهل طريقة لتنزيل أحدث إصدار هي استخدام الأمر التالي:

```bash
curl https://clickhouse.com/ | sh
```

:::note
يمكن للملف التنفيذي الذي نزّلته للتو تشغيل مختلف أدوات ClickHouse والبرامج المساعدة التابعة له. إذا أردت تشغيل ClickHouse كخادم قاعدة بيانات، فراجع [البدء السريع](/ar/get-started/quick-start).
:::

<div id="query_data_in_file">
  ## الاستعلام عن البيانات في ملف باستخدام SQL
</div>

من الاستخدامات الشائعة لـ `clickhouse-local` تشغيل استعلامات مخصّصة على الملفات، بحيث لا تحتاج إلى إدراج البيانات في جدول. ويمكن لـ `clickhouse-local` تمرير البيانات من ملف إلى جدول مؤقت وتنفيذ أوامر SQL الخاصة بك.

إذا كان الملف موجودًا على الجهاز نفسه الذي يعمل عليه `clickhouse-local`، فيمكنك ببساطة تحديد الملف المطلوب تحميله. يحتوي ملف `reviews.tsv` التالي على عيّنة من مراجعات منتجات Amazon:

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

يمثل هذا الأمر اختصارًا لـ:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

يعرف ClickHouse من امتداد اسم الملف أن الملف يستخدم تنسيقًا مفصولًا بعلامات الجدولة. وإذا كنت بحاجة إلى تحديد التنسيق صراحةً، فما عليك سوى إضافة أحد [تنسيقات الإدخال العديدة التي يدعمها ClickHouse](../../interfaces/formats.md):

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

تُنشئ دالة الجدول `file` جدولًا، ويمكنك استخدام `DESCRIBE` للاطلاع على المخطط المستنتج:

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
يُسمح لك باستخدام أنماط glob في اسم الملف (راجع [أنماط glob البديلة](/ar/sql-reference/table-functions/file.md/#globs-in-path)).

أمثلة:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

لنبحث عن المنتج الحاصل على أعلى تقييم:

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## الاستعلام عن البيانات في ملف Parquet على AWS S3
</div>

إذا كان لديك ملف في S3، فاستخدم `clickhouse-local` ودالة الجدول `s3` للاستعلام عن الملف في مكانه (من دون إدراج البيانات في جدول ClickHouse). لدينا ملف باسم `house_0.parquet` في bucket عامة يحتوي على أسعار منازل لعقارات بيعت في المملكة المتحدة. لنرَ كم عدد الصفوف التي يحتوي عليها:

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

يحتوي الملف على 2.7M صفًا:

```response
2772030
```

من المفيد دائمًا الاطلاع على المخطط المُستنتَج من الملف بواسطة ClickHouse:

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

دعنا نرَ ما هي الأحياء الأغلى:

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
عندما تكون جاهزًا لإدخال ملفاتك إلى ClickHouse، شغّل خادم ClickHouse وأدرج نتائج دالتي الجدول `file` و`s3` في جدول `MergeTree`. اطّلع على [البدء السريع](/ar/get-started/quick-start) لمزيد من التفاصيل.
:::

<div id="format-conversions">
  ## تحويلات التنسيقات
</div>

يمكنك استخدام `clickhouse-local` للتحويل بين تنسيقات بيانات مختلفة. مثال:

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

يتم التعرّف على التنسيقات تلقائيًا من امتدادات الملفات:

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

وباختصار، يمكنك كتابته باستخدام الوسيطة `--copy`:

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## الاستخدام
</div>

بشكل افتراضي، يمكن لـ `clickhouse-local` الوصول إلى بيانات خادم ClickHouse الموجود على نفس المضيف، ولا يعتمد على تهيئة الخادم. كما يدعم تحميل تهيئة الخادم باستخدام الوسيطة `--config-file`. وبالنسبة إلى البيانات المؤقتة، يُنشأ افتراضيًا دليل فريد للبيانات المؤقتة.

الاستخدام الأساسي (Linux):

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

الاستخدام الأساسي (Mac):

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` مدعوم أيضًا على Windows عبر WSL2.
:::

الوسيطات:

* `-S`, `--structure` — بنية الجدول لبيانات الإدخال.
* `--input-format` — تنسيق الإدخال، و`TSV` هو الافتراضي.
* `-F`, `--file` — مسار البيانات، و`stdin` هو الافتراضي.
* `-q`, `--query` — الاستعلامات المطلوب تنفيذها، مع استخدام `;` كفاصل. يمكن تحديد `--query` عدة مرات، مثل: `--query "SELECT 1" --query "SELECT 2"`. لا يمكن استخدامه بالتزامن مع `--queries-file`.
* `--queries-file` - مسار ملف يحتوي على الاستعلامات المطلوب تنفيذها. يمكن تحديد `--queries-file` عدة مرات، مثل: `--query queries1.sql --query queries2.sql`. لا يمكن استخدامه بالتزامن مع `--query`.
* `--multiquery, -n` – إذا تم تحديده، يمكن إدراج عدة استعلامات مفصولة بفواصل منقوطة بعد الخيار `--query`. وللتسهيل، يمكن أيضًا حذف `--query` وتمرير الاستعلامات مباشرةً بعد `--multiquery`.
* `-N`, `--table` — اسم الجدول الذي ستوضع فيه بيانات الإخراج، و`table` هو الافتراضي.
* `-f`, `--format`, `--output-format` — تنسيق الإخراج، و`TSV` هو الافتراضي.
* `-d`, `--database` — قاعدة البيانات الافتراضية، و`_local` هي الافتراضية.
* `--stacktrace` — ما إذا كان سيتم إخراج مخرجات Debug عند حدوث استثناء.
* `--echo [ <bool> ]` — اطبع كل استعلام قبل التنفيذ. يقبل قيمة Boolean اختيارية. يكون مفعّلًا افتراضيًا في الوضع التفاعلي ومعطّلًا في وضع الدُفعات. ملاحظة: لأن `--echo` يقبل الآن قيمة اختيارية، فإن أي استعلام موضعي يأتي مباشرةً بعد `--echo` وحده سيُؤخذ على أنه قيمته؛ استخدم `--echo --query "..."` أو `--echo -q "..."` أو `--echo=false` أو `stdin` عبر pipe بدلًا من ذلك.
* `--echo-formatted [ <bool> ]` — نسّق الاستعلامات المطبوعة عبر echo. يقبل قيمة Boolean اختيارية. يكون مفعّلًا افتراضيًا في الوضع التفاعلي ومعطّلًا في وضع الدُفعات.
* `--echo-query-id [ <bool> ]` — اطبع `query_id` قبل التنفيذ. يقبل قيمة Boolean اختيارية. يكون مفعّلًا افتراضيًا في الوضع التفاعلي ومعطّلًا في وضع الدُفعات.
* `--echo-query-separator <string>` — اطبع هذا الفاصل قبل الاستعلام المطبوع والمنسّق عبر echo (يتطلب `--echo-formatted`) لتسهيل التمييز بين الاستعلام المكتوب ونسخته المعاد تنسيقها. يكون فارغًا افتراضيًا (أي معطّلًا).
* `--highlight`, `--hilite` `<bool>` — بدّل تمييز بناء الجملة في موجّه الأوامر والاستعلامات المطبوعة عبر echo. يكون مفعّلًا افتراضيًا. لا يُطبَّق التمييز إلا عند الكتابة إلى طرفية.
* `--hints <bool>` — اعرض تلميحات الإكمال التلقائي أثناء الكتابة (نص &quot;شبح&quot; مضمن) لأفضل اقتراح مطابق عندما يكون المؤشر في نهاية الإدخال. تنقّل بين التلميحات باستخدام Up/Down (أو Ctrl-Up/Ctrl-Down)؛ واقبل التلميح المضمن باستخدام Tab أو Right؛ ويقبل `Enter` التلميح فقط بعد تحديده صراحةً، وإلا فإنه ينفّذ الاستعلام؛ كما يفتح `Tab` أيضًا قائمة الإكمال الكلاسيكية. يتطلب `--highlight` (لأن التلميحات تحتاج إلى ألوان) وآلية الاقتراحات (لذا فإن `--disable_suggestion` يعطّلها أيضًا). يكون مفعّلًا افتراضيًا.
* `--verbose` — مزيد من التفاصيل حول تنفيذ الاستعلام.
* `--logger.console` — سجّل إلى Console.
* `--logger.log` — اسم ملف السجل.
* `--logger.level` — مستوى السجل.
* `--ignore-error` — لا توقف المعالجة إذا فشل استعلام.
* `-c`, `--config-file` — مسار ملف الإعدادات بالتنسيق نفسه المستخدم في ClickHouse server، ويكون ملف الإعدادات فارغًا افتراضيًا.
* `--no-system-tables` — لا تقم بإرفاق جداول النظام.
* `--help` — مرجع الوسيطات لـ `clickhouse-local`.
* `-V`, `--version` — اطبع معلومات الإصدار ثم اخرج.

بالإضافة إلى ذلك، توجد وسيطات لكل متغير من متغيرات إعداد ClickHouse، وغالبًا ما تُستخدم بدلًا من `--config-file`.

<div id="commands">
  ## الأوامر
</div>

<div id="ls-command">
  ### أمر LS
</div>

يسرد جميع الملفات في دليل العمل الحالي التي يمكن لـ clickhouse-local الوصول إليها.

يمكنك تشغيله في الوضع التفاعلي على النحو التالي:

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

يمكنك أيضًا تشغيله على شكل استعلام باستخدام الوسيطة `-q`:

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### الأمر CLEAR
</div>

يمسح شاشة الطرفية (على غرار الأمر `clear` في Linux أو Ctrl+L في كثير من الطرفيات). هذا إجراء من جهة العميل، ولا يُرسَل إلى محرك SQL.

في `clickhouse-local`، يُتعرَّف على الأمر الوصفي في الوضع **التفاعلي** ومع إدخال **`-q`** و**`--queries-file`** (مسار العميل نفسه كما في `-q`، والفكرة نفسها كما في `ls`)، لذلك فإن `clear` وحده لا يؤدي إلى ظهور خطأ `UNKNOWN_IDENTIFIER`. أما **`clickhouse-client --queries-file`** البعيد فلا يتغير: إذ تُنفَّذ محتويات الملف على أنها SQL فقط (من دون أوامر وصفية على مستوى النص).

في `clickhouse-client`، لا يُتعرَّف عليه إلا في الوضع **التفاعلي**. ومع **`-q`** أو ملفات الاستعلامات، يظل `clear` يُحلَّل على أنه SQL، بحيث تحتفظ الأتمتة بسلوك الخطأ السابق بدلًا من تحويل الأخطاء المطبعية إلى no-op صامت.

الصيغ المدعومة: `clear`، `CLEAR`، `/clear` (تُتجاهل الفاصلة المنقوطة الاختيارية `;` في النهاية). وإذا لم يكن الإخراج القياسي طرفيةً (على سبيل المثال، عند تمرير الإخراج عبر أنبوب)، فسيُقبَل الأمر الوصفي عند التعرّف عليه، لكنه لن يُصدر تسلسلات تحكم.

مع `clickhouse-local` و`-q`:

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## أمثلة
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

المثال السابق مطابق لما يلي:

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

لستَ مضطرًا لاستخدام `stdin` أو الوسيط `--file`، ويمكنك فتح أي عدد من الملفات باستخدام [دالة الجدول `file`](../../sql-reference/table-functions/file.md):

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

والآن لنعرض مستخدم memory لكل مستخدم Unix:

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

<div id="starting-listeners">
  ## بدء مستمعات TCP وHTTP
</div>

يمكن تحويل `clickhouse-local` إلى خادم خفيف الوزن يقبل اتصالات TCP (البروتوكول الأصلي) واتصالات HTTP. يكون ذلك مفيدًا عندما تريد إتاحة الوصول إلى قواعد البيانات والجداول في مثيل `clickhouse-local` قيد التشغيل لأدوات أو تطبيقات ClickHouse الأخرى. لاحظ أن كل اتصال وارد يحصل على جلسة خاصة به: لا تظهر الجداول المؤقتة وإعدادات مستوى الجلسة الخاصة بجلسة `clickhouse-local` التفاعلية للاتصالات الخارجية.

استخدم `SYSTEM START LISTEN` لفتح مستمع، و`SYSTEM STOP LISTEN` لإغلاقه:

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

تُهيِّئ الخيارات `--listen_host` و`--tcp_port` و`--http_port` عنوان الربط والمنافذ. المنافذ الافتراضية هي `9000` لبروتوكول TCP و`8123` لبروتوكول HTTP.

:::warning الأمان
بشكل افتراضي، يعمل `clickhouse-local` باستخدام تهيئة المستخدمين المؤقتة، لذا فإن أي مستمع يفتحه يكون من دون مصادقة. اربطه بعنوان استرجاع محلي (`127.0.0.1` أو `::1`) ما لم تكن قد هيّأت المستخدمين والتحكم في الوصول صراحةً بتوجيه الإعداد `users_config` إلى ملف `users.xml` مخصص (على سبيل المثال عبر `--config-file`). إن الاستماع على عنوان غير محلي للاسترجاع من دون مصادقة يعرّض بيانات المثيل المحلي لأي شخص يمكنه الوصول إلى المنفذ المحدد.
:::

<div id="related-content-1">
  ## محتوى ذي صلة
</div>

* [استخراج البيانات وتحويلها والاستعلام عنها في الملفات المحلية باستخدام clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [إدخال البيانات إلى ClickHouse - الجزء 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [استكشاف مجموعات بيانات واقعية ضخمة: أكثر من 100 عام من سجلات الطقس في ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* المدونة: [استخراج البيانات وتحويلها والاستعلام عنها في الملفات المحلية باستخدام clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)