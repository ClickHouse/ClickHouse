---
description: 'يستعلم عن البيانات من/إلى خادم HTTP/HTTPS بعيد. هذا المحرك مشابه
  لمحرك File.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'محرك جدول URL'
doc_type: 'reference'
---

يستعلم عن البيانات من/إلى خادم HTTP/HTTPS بعيد. هذا المحرك مشابه لمحرك [File](../../../engines/table-engines/special/file.md).

البنية: `URL(URL [,Format] [,CompressionMethod])`

* يجب أن تتوافق المعلمة `URL` مع بنية Uniform Resource Locator. وبالنسبة إلى عنوان URL من نوع `http`/`https` (الـ الواجهة الخلفية الافتراضي)، يجب أن يشير إلى خادم يستخدم HTTP أو HTTPS، وألا يتطلب الحصول على استجابة من الخادم أي headers إضافية. أما إذا كان عنوان URL يستخدم مخطط غير HTTP ومعروفًا (`file://`, `s3://`, `az://`, `hdfs://`, …)، فيُحال بدلًا من ذلك إلى المحرك المطابق — راجع [التوجيه حسب المخطط الخاص بـ URL](#scheme-dispatch) أدناه.

* يجب أن يكون `Format` أحد التنسيقات التي يمكن لـ ClickHouse استخدامها في استعلامات `SELECT`، وعند الحاجة، في عمليات `INSERT`. للاطلاع على القائمة الكاملة بالتنسيقات المدعومة، راجع [Formats](/ar/interfaces/formats#formats-overview).

  إذا لم تُحدَّد هذه الوسيطة، يكتشف ClickHouse التنسيق تلقائيًا من suffix المعلمة `URL`. وإذا لم يطابق suffix المعلمة `URL` أيًا من التنسيقات المدعومة، فسيفشل إنشاء الجدول. على سبيل المثال، في تعبير المحرك `URL('http://localhost/test.json')`، يُطبَّق تنسيق `JSON`.

* يحدد `CompressionMethod` ما إذا كان يجب ضغط HTTP body. وإذا كان الضغط مُمكّنًا، فإن HTTP packets التي يرسلها محرك URL تتضمن header باسم &#39;Content-Encoding&#39; للإشارة إلى Compression method المستخدمة.

لتمكين الضغط، يُرجى أولًا التأكد من أن endpoint ‏HTTP البعيد الذي تشير إليه المعلمة `URL` يدعم خوارزمية الضغط المقابلة.

يجب أن تكون قيم `CompressionMethod` المدعومة إحدى القيم التالية:

* gzip or gz
* deflate
* brotli or br
* lzma or xz
* zstd or zst
* lz4
* bz2
* snappy
* none
* auto

إذا لم يُحدَّد `CompressionMethod`، فستكون قيمته الافتراضية `auto`. وهذا يعني أن ClickHouse يكتشف Compression method تلقائيًا من suffix المعلمة `URL`. وإذا طابق suffix إحدى طرق الضغط المذكورة أعلاه، فسيُطبَّق الضغط المقابل، وإلا فلن يُفعَّل أي ضغط.

على سبيل المثال، في تعبير المحرك `URL('http://localhost/test.gzip')`، تُطبَّق طريقة الضغط `gzip`، أما في `URL('http://localhost/test.fr')` فلا يُفعَّل أي ضغط لأن suffix ‏`fr` لا يطابق أيًا من طرق الضغط المذكورة أعلاه.

<div id="scheme-dispatch">
  ## التوجيه بحسب مخطط URL
</div>

يُعدّ محرك `URL` غلافًا موحّدًا فوق محركات الملفات ومحركات تخزين الكائنات الأخرى: إذ يوجّه الطلب إلى الواجهة الخلفية المناسبة استنادًا إلى مخطط URL. يتولى محرك `URL` نفسه التعامل مع `http`/`https` (وأي مخطط غير معروف)؛ بينما يتولى محرك [File](../../../engines/table-engines/special/file.md) التعامل مع `file://`؛ ويتولى محرك [S3](/ar/engines/table-engines/integrations/s3) التعامل مع `s3://` و`gs://` و`gcs://` و`oss://`؛ ويتولى محرك [AzureBlobStorage](/ar/engines/table-engines/integrations/azureBlobStorage) التعامل مع `az://` و`azure://` و`abfss://` و`abfs://`؛ ويتولى محرك [HDFS](/ar/engines/table-engines/integrations/hdfs) التعامل مع `hdfs://`.

لا يُوجَّه إلا مخططات S3 التي يحلّها مُعيِّن URI الخاص بـ S3 إلى endpoint فعلي من دون إعداد إضافي (`s3`، بالإضافة إلى `gs`/`gcs`/`oss`). أما مخططات المورّدين الأخرى المتوافقة مع S3 (`cos` و`obs` و`eos` و…) فهي خاصة بالمنطقة ولا تملك تعيين endpoint افتراضيًا، لذا فإن تمرير URL من هذا النوع إلى محرك `URL` يُعامَل على أنه مخطط غير معروف ويُبلَّغ عنه كخطأ؛ استخدم محرك [S3](/ar/engines/table-engines/integrations/s3) مباشرةً (مع تهيئة `url_scheme_mappers`) لهذه الواجهات الخلفية.

يُطبَّق الإعداد [url&#95;base](/ar/operations/settings/settings.md#url_base) قبل التوجيه بحسب المخطط، لذا يُحل المرجع النسبي أولًا بالاستناد إلى الأساس، ثم يُوجَّه إلى المحرك المطابق.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## الاستخدام
</div>

تُحوَّل استعلامات `INSERT` و`SELECT` إلى طلبات `POST` و`GET`،
على التوالي. ولمعالجة طلبات `POST`، يجب أن يدعم الخادم البعيد
[ترميز النقل المُجزّأ](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

يمكنك تقييد الحد الأقصى لعدد مرات إعادة توجيه HTTP GET باستخدام الإعداد [max&#95;http&#95;get&#95;redirects](/ar/operations/settings/settings#max_http_get_redirects).

<div id="wildcards-with-http-index-pages">
  ## أحرف البدل مع صفحات فهرس HTTP
</div>

عندما يكون [allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/ar/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) مُمكّنًا، يمكن لمحرك الجدول `URL` توسيع أحرف البدل من خلال جلب صفحات فهرس HTTP واستخراج الروابط منها.
وهذه هي الآلية نفسها المستخدمة في دالة الجدول [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages).

يكون هذا التوسيع مقيّدًا بالقيمة [max&#95;http&#95;index&#95;page&#95;size](/ar/operations/server-configuration-parameters/settings.md#max_http_index_page_size) لكل صفحة فهرس يتم جلبها، وبالقيمة [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ar/operations/settings/settings.md#url_wildcard_max_directories_to_read) عند الاستعراض التكراري للأدلة.

<div id="example">
  ## مثال
</div>

**1.** أنشئ جدول `url_engine_table` على الخادم:

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** أنشئ خادم HTTP بسيطًا باستخدام أدوات بايثون 3 القياسية ثم شغّله:

```python3
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```bash
$ python3 server.py
```

**3.** اطلب البيانات:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## تفاصيل التنفيذ
</div>

* يمكن تنفيذ عمليات القراءة والكتابة بالتوازي
* غير مدعوم:
  * عمليات `ALTER` و`SELECT...SAMPLE`.
  * الفهارس.
  * النسخ المتماثل.

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_path` — مسار `URL`. النوع: `LowCardinality(String)`.
* `_file` — اسم المورد في `URL`. النوع: `LowCardinality(String)`.
* `_size` — حجم المورد بالبايت. النوع: `Nullable(UInt64)`. إذا كان الحجم غير معروف، تكون القيمة `NULL`.
* `_time` — وقت آخر تعديل للملف. النوع: `Nullable(DateTime)`. إذا كان الوقت غير معروف، تكون القيمة `NULL`.
* `_headers` - رؤوس استجابة HTTP. النوع: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="resolving-relative-urls">
  ## حل عناوين URL النسبية
</div>

يتيح الإعداد [url&#95;base](/ar/operations/settings/settings.md#url_base) استخدام عنوان URL نسبي في المحرك `URL`. عند تعيين `url_base`، يُفسَّر عنوان URL المُمرَّر إلى المحرك بالاستناد إليه وفقًا لـ [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). للحصول على وصف كامل لقواعد الحل، راجع [وثائق دالة الجدول URL](../../../sql-reference/table-functions/url.md#resolving-relative-urls).

**مثال**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## إعدادات التخزين
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ar/operations/settings/settings.md#engine_url_skip_empty_files) - يتيح تخطي الملفات الفارغة أثناء القراءة. يكون معطّلًا افتراضيًا.
* [enable&#95;url&#95;encoding](/ar/operations/settings/settings.md#enable_url_encoding) - يتيح تمكين/تعطيل فك ترميز/ترميز المسار في uri. يكون مفعّلًا افتراضيًا.
* [url&#95;base](/ar/operations/settings/settings.md#url_base) - عنوان URL الأساسي لحل عناوين URL النسبية المُمرَّرة إلى المحرك.