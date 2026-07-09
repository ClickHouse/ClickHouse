---
description: 'يسمح بمعالجة الملفات من URL بالتوازي من عدة عُقد في عنقود محدد.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

يسمح بمعالجة الملفات من URL بالتوازي من عدة عُقد في عنقود محدد. على العقدة البادئة، يُنشئ اتصالًا بجميع العُقد في العنقود، ويُوسّع علامة النجمة (*) في مسار ملف URL، ثم يوزّع كل ملف ديناميكيًا. وعلى عقدة العامل، يطلب من العقدة البادئة المهمة التالية لمعالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## الصياغة
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيطة        | الوصف                                                                                                                                     |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعلمات الاتصال الخاصة بالخوادم البعيدة والمحلية.                                             |
| `URL`          | عنوان خادم HTTP أو HTTPS يمكنه قبول طلبات `GET`. النوع: [String](../../sql-reference/data-types/string.md).                               |
| `format`       | [تنسيق](/ar/sql-reference/formats) البيانات. النوع: [String](../../sql-reference/data-types/string.md).                                      |
| `structure`    | بنية الجدول بصيغة `'UserID UInt64, Name String'`. تحدد أسماء الأعمدة وأنواعها. النوع: [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول بالتنسيق والبنية المحدَّدين، ويحتوي على بيانات من `URL` المُحدَّد.

<div id="examples">
  ## أمثلة
</div>

استخراج أول 3 أسطر من جدول يحتوي على أعمدة من النوعين `String` و[UInt32](../../sql-reference/data-types/int-uint.md) من خادم HTTP يستجيب بتنسيق [CSV](/ar/interfaces/formats/CSV).

1. أنشئ خادم HTTP بسيطًا باستخدام أدوات بايثون 3 القياسية، ثم ابدأ تشغيله:

```python
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

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## أنماط glob في URL
</div>

تُستخدم الأنماط ضمن `{ }` لإنشاء مجموعة من الأجزاء الموزعة أو لتحديد عناوين التبديل الاحتياطي. للاطلاع على أنواع الأنماط المدعومة والأمثلة، راجع وصف الدالة [remote](remote.md#globs-in-addresses).
يُستخدم المحرف `|` داخل الأنماط لتحديد عناوين التبديل الاحتياطي. ويجري المرور عليها بالترتيب نفسه الوارد في النمط. ويكون عدد العناوين المُنشأة محدودًا بإعداد [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).

<div id="related">
  ## مواضيع ذات صلة
</div>

* [HDFS engine](/ar/engines/table-engines/integrations/hdfs)
* [دالة الجدول URL](/ar/engines/table-engines/special/url)