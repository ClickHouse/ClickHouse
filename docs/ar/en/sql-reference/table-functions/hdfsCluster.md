---
description: 'يسمح بمعالجة الملفات من HDFS بالتوازي من العديد من العُقد في عنقود
  محدد.'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

يسمح بمعالجة الملفات من HDFS بالتوازي عبر العديد من العُقد في عنقود محدد. على العقدة المُبادِرة، ينشئ اتصالًا بجميع العُقد في العنقود، ويوسّع الأحرف النجمية في مسار ملف HDFS، ثم يوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يطلب من العقدة المُبادِرة المهمة التالية لمعالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.

<div id="syntax">
  ## الصيغة
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيط         | الوصف                                                                                                                                                                                                                                                                                          |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية.                                                                                                                                                                                                         |
| `URI`          | عنوان URI لملف أو عدة ملفات. يدعم أحرف البدل التالية في وضع القراءة فقط: `*`, `**`, `?`, `{'abc','def'}` و `{N..M}`، حيث إن `N` و `M` — أرقام، و`abc` و `def` — سلاسل نصية. لمزيد من المعلومات، راجع [أحرف البدل في المسار](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `format`       | [صيغة](/ar/sql-reference/formats) الملف.                                                                                                                                                                                                                                                          |
| `structure`    | بنية الجدول. بالتنسيق التالي: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                   |

<div id="returned_value">
  ## القيمة المُعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من الملف المحدد.

<div id="examples">
  ## أمثلة
</div>

1. لنفترض أن لدينا عنقود ClickHouse باسم `cluster_simple`، وعدة ملفات بعناوين URI التالية على HDFS:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. استعلم عن عدد الصفوف الموجودة في هذه الملفات:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. نفّذ استعلامًا لمعرفة عدد الصفوف في جميع الملفات ضمن هذين الدليلين:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
إذا كانت قائمة الملفات لديك تحتوي على نطاقات رقمية تبدأ بأصفار، فاستخدم الصيغة ذات الأقواس لكل رقم على حدة، أو استخدم `?`.
:::

<div id="related">
  ## ذات صلة
</div>

* [محرك HDFS](../../engines/table-engines/integrations/hdfs.md)
* [دالة الجدول HDFS](../../sql-reference/table-functions/hdfs.md)