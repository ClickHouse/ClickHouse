---
description: 'ينشئ جدولًا من `URL` وفق `format` و`structure` المحدَّدين'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # دالة الجدول url
</div>

تنشئ الدالة `url` جدولًا من `URL` باستخدام `format` و`structure` المحدَّدين.

يمكن استخدام الدالة `url` في استعلامات `SELECT` و`INSERT` على البيانات في جداول [URL](../../engines/table-engines/special/url.md).

<div id="syntax">
  ## الصيغة
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## المعلمات
</div>

| المعلمة     | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URL`       | عنوان URL بين علامتَي اقتباس مفردتَين، ويحدّد مخططه الواجهة الخلفية. يكون عنوان URL من نوع `http`/`https` (أو غير المعروف) عنوانَ خادمٍ يقبل طلبات `GET` أو `POST` (لاستعلامات `SELECT` أو `INSERT` على التوالي)؛ أما مخطط URL المعروف غير الخاص بـ HTTP (`file://`, `s3://`, `az://`, `hdfs://`, …) فيُمرَّر إلى دالة الجدول المطابقة — راجع [التوجيه حسب مخطط URL](#scheme-dispatch). النوع: [String](../../sql-reference/data-types/string.md). |
| `format`    | [تنسيق](/ar/sql-reference/formats) البيانات. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                               |
| `structure` | بنية الجدول بصيغة `'UserID UInt64, Name String'`. تحدد أسماء الأعمدة وأنواعها. النوع: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                          |
| `headers`   | الترويسات بصيغة `'headers('key1'='value1', 'key2'='value2')'`. يمكنك تعيين ترويسات لاستدعاء HTTP.                                                                                                                                                                                                                                                                                                                                                  |

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول بالتنسيق والبنية المحدَّدين، ويحتوي على بيانات من `URL` المحدَّد.

<div id="examples">
  ## أمثلة
</div>

استخراج أول 3 أسطر من جدول يحتوي على أعمدة من النوعين `String` و[UInt32](../../sql-reference/data-types/int-uint.md) من خادم HTTP يستجيب بتنسيق [CSV](/ar/interfaces/formats/CSV).

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

إدراج البيانات من `URL` في جدول:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## التوجيه بحسب مخطّط URL
</div>

تعمل الدالة `url` كطبقة تغليف موحّدة فوق دوال الجداول الأخرى الخاصة بالملفات وتخزين الكائنات: فهي تُوجِّه الطلب إلى الواجهة الخلفية المناسبة بناءً على مخطّط URL. ويتيح لك ذلك القراءة من أي موقع مدعوم باستخدام صياغة موحّدة واحدة.

| المخطّط                                       | يُوجَّه إلى                                      |
| --------------------------------------------- | ------------------------------------------------ |
| `http`, `https` (and any unrecognized scheme) | محرّك `URL` نفسه (‏HTTP `GET`/`POST`)            |
| `file`                                        | الدالة [`file`](file.md)                         |
| `s3`, `gs`, `gcs`, `oss`                      | الدالة [`s3`](s3.md)                             |
| `az`, `azure`, `abfss`, `abfs`                | الدالة [`azureBlobStorage`](azureBlobStorage.md) |
| `hdfs`                                        | الدالة [`hdfs`](hdfs.md)                         |

لا يُوجَّه إلا مخططات S3 التي يحلّها مُعيِّن عنوان URI الخاص بـ S3 إلى نقطة نهاية فعلية من دون إعداد إضافي (`s3`، بالإضافة إلى `gs`/`gcs`/`oss`). أما مخططات المورّدين الأخرى المتوافقة مع S3 (`cos`, `obs`, `eos`, …) فهي خاصة بكل منطقة ولا تملك تعيينًا افتراضيًا لنقطة نهاية، لذا يُعامَل عنوان URL من النوع `cos://…` على أنه مخطّط غير معروف ويُبلَّغ عنه كخطأ؛ استخدم الدالة [`s3`](s3.md) مباشرةً (مع ضبط `url_scheme_mappers`) لتلك الواجهات الخلفية.

بالنسبة إلى `file://`، يُفسَّر المسار النسبي (`file://data.csv`) داخل الدليل [user&#95;files](/ar/operations/server-configuration-parameters/settings#user_files_path)، ويجب أن يشير المسار المطلق (`file:///home/user/data.csv`) إلى موقع داخله كالمعتاد.

تعمل الوسيطات `format` و`structure` و`compression_method` والإعداد [url&#95;base](#resolving-relative-urls) بالطريقة نفسها بغضّ النظر عن هدف التوجيه.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

لا يزال توجيه المخطط عبر [`urlCluster`](urlCluster.md) غير مدعوم: إذ يُرفَض أي مخطط غير `http(s)` يُمرَّر إلى `urlCluster` ويؤدي إلى ظهور خطأ. استخدم بدلًا من ذلك دالة cluster المقابلة (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …) لتلك الأنظمة الخلفية.

<div id="globs-in-url">
  ## أنماط glob في URL
</div>

تُستخدم الأنماط داخل `{ }` لإنشاء مجموعة من الـ shards أو لتحديد عناوين التبديل التلقائي عند التعطل. للاطلاع على أنواع الأنماط المدعومة والأمثلة، راجع وصف الدالة [remote](remote.md#globs-in-addresses).
يُستخدم المحرف `|` داخل الأنماط لتحديد عناوين التبديل التلقائي عند التعطل. ويجري المرور عليها بالترتيب نفسه الذي تظهر به في النمط. ويكون عدد العناوين المُنشأة محدودًا بإعداد [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).
للاطلاع على صياغة glob للمسارات ضمن جزء المسار من URL (مثل `*` و`{a,b}` و`{N..M}` و`**`)، راجع [أنماط glob في المسار](file.md#globs-in-path). لاحظ أن `?` يبدأ سلسلة الاستعلام في URL، ولا يمكن استخدامه كحرف بدل في مكوّن المسار.

<div id="wildcards-with-http-index-pages">
  ## أحرف البدل مع صفحات فهرس HTTP
</div>

بالنسبة إلى `url` ومحرك الجدول `URL`، يمكن لـ ClickHouse توسيع أحرف البدل من خلال جلب صفحات فهرس HTTP ‏(HTML أو plaintext) واستخراج عناوين URL من جسم الاستجابة. يتيح ذلك استخدام أنماط مثل `/**/` عندما يوفّر الخادم سردًا للدلائل.

ملاحظات:

* تُفسَّر عناوين URL النسبية استنادًا إلى عنوان URL لصفحة الفهرس.
* تُوسَّع قوالب `URL` قبل جلب صفحات الفهرس، بما في ذلك توسيع الأجزاء المفصولة بفواصل، وتوسيع النطاقات الرقمية لـ shard، وخيارات `|` الخاصة بـ failover خارج مكوّن المسار.
* أنماط failover التي تتضمن `|` داخل مكوّن المسار غير مدعومة لتوسيع صفحات فهرس HTTP.
* يُطبَّق تطابق أحرف البدل على مكوّن المسار في عنوان URL.
* إذا كان عنوان URL المُدرج يحتوي بالفعل على سلسلة استعلام أو جزء، فلهما أولوية على الموجودين في عنوان URL المصدر. وإلا، فسيُستخدم سلسلة الاستعلام والجزء من عنوان URL المصدر.
* يُسمح بقائمة فارغة؛ أما أخطاء HTTP (مثل 404) في صفحات الفهرس فتؤدي إلى ظهور استثناءات.
* يكون الحد الأقصى لحجم صفحة الفهرس مقيّدًا بواسطة [max&#95;http&#95;index&#95;page&#95;size](/ar/operations/server-configuration-parameters/settings.md#max_http_index_page_size).
* يكون الحد الأقصى لعدد الدلائل المقروءة أثناء التوسيع التكراري مقيّدًا بواسطة [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ar/operations/settings/settings.md#url_wildcard_max_directories_to_read).

مثال:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار `URL`. النوع: `LowCardinality(String)`.
* `_file` — اسم المورد في `URL`. النوع: `LowCardinality(String)`.
* `_size` — حجم المورد بالبايت. النوع: `Nullable(UInt64)`. إذا كان الحجم غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_headers` - ترويسات استجابة HTTP. النوع: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="hive-style-partitioning">
  ## إعداد use_hive_partitioning
</div>

عند تعيين `use_hive_partitioning` إلى القيمة 1، سيكتشف ClickHouse التقسيم بأسلوب Hive في المسار (`/name=value/`)، وسيتيح استخدام أعمدة التقسيم كأعمدة افتراضية في الاستعلام. وستحمل هذه الأعمدة الافتراضية الأسماء نفسها الموجودة في مسار التقسيم.

**مثال**

استخدم عمودًا افتراضيًا أُنشئ باستخدام التقسيم بأسلوب Hive

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## حلّ عناوين URL النسبية
</div>

يتيح الإعداد [url&#95;base](/ar/operations/settings/settings.md#url_base) تمرير عنوان URL نسبي إلى الدالة `url`. عند تعيين `url_base` وكان وسيط الدالة مرجعًا نسبيًا، يُحلّ بالاستناد إلى عنوان URL الأساسي وفقًا لـ [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

قواعد الحلّ هي:

* **نسبي إلى المسار** (مثل `data.csv`): يُدمج مع مسار عنوان URL الأساسي — ويُستبدل كل ما يأتي بعد آخر `/` في المسار الأساسي. وتكون الشرطة المائلة اللاحقة مهمة: ‏`https://example.com/dir/` + `data.csv` تعطي `https://example.com/dir/data.csv`، لكن `https://example.com/dir` + `data.csv` تعطي `https://example.com/data.csv`. وتُطبَّع المقاطع النقطية (`./` و `../`).
* **نسبي إلى المضيف** (مثل `/test/data.csv`): يُحلّ باستخدام المخطط والمضيف من عنوان URL الأساسي.
* **نسبي إلى المخطط** (مثل `//other.com/test/data.csv`): يُحلّ باستخدام مخطط عنوان URL الأساسي.
* **استعلام فقط** (مثل `?x=1`): يُلحَق بالمسار الأساسي الكامل، مع استبدال أي استعلام أو جزء حالي.
* **جزء فقط** (مثل `#frag`): يُلحَق بعنوان URL الأساسي مع الحفاظ على الاستعلام واستبدال أي جزء حالي.
* **فارغ**: يعيد عنوان URL الأساسي بدون جزء.
* **عنوان URL مطلق**: يُمرَّر كما هو دون تغيير؛ ويُتجاهل `url_base`.

**مثال**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## إعدادات التخزين
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#engine_url_skip_empty_files) - يسمح بتخطي الملفات الفارغة أثناء القراءة. وهو معطّل افتراضيًا.
* [enable&#95;url&#95;encoding](/ar/operations/settings/settings.md#enable_url_encoding) - يسمح بتمكين/تعطيل فك ترميز المسار في عنوان URI أو ترميزه. وهو مفعّل افتراضيًا.
* [url&#95;base](/ar/operations/settings/settings.md#url_base) - عنوان URL الأساسي لحل عناوين URL النسبية المُمرَّرة إلى الدالة `url`.

<div id="permissions">
  ## الأذونات
</div>

تتطلب الدالة `url` إذن `CREATE TEMPORARY TABLE`. لذلك، لن تعمل مع المستخدمين الذين لديهم الإعداد [readonly](/ar/operations/settings/permissions-for-queries#readonly) = 1. ويُشترط أن تكون قيمة readonly = 2 على الأقل.

<div id="related">
  ## مواضيع ذات صلة
</div>

* [الأعمدة الافتراضية](/ar/engines/table-engines/index.md#table_engines-virtual_columns)