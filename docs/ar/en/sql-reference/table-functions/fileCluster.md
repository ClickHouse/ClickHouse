---
description: 'يُمكّن من معالجة الملفات المطابقة لمسار محدد بالتوازي عبر عدة عقد
  داخل عنقود. تنشئ العقدة البادئة اتصالات بالعقد العاملة، وتوسّع أنماط glob في
  مسار الملف، وتُسنِد مهام قراءة الملفات إلى العقد العاملة. وتطلب كل عقدة عاملة
  من العقدة البادئة الملف التالي المطلوب معالجته، ويتكرر ذلك حتى تكتمل جميع
  المهام (أي تُقرأ جميع الملفات).'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

يُمكّن من معالجة الملفات المطابقة لمسار محدد بالتوازي عبر عدة عقد داخل عنقود. تنشئ العقدة البادئة اتصالات بالعقد العاملة، وتوسّع أنماط glob في مسار الملف، وتُسنِد مهام قراءة الملفات إلى العقد العاملة. وتطلب كل عقدة عاملة من العقدة البادئة الملف التالي المطلوب معالجته، ويتكرر ذلك حتى تكتمل جميع المهام (أي تُقرأ جميع الملفات).

:::note
لن تعمل هذه الدالة *بشكل صحيح* إلا إذا كانت مجموعة الملفات المطابقة للمسار المحدد في البداية متطابقة على جميع العقد، وكان محتواها متسقًا بينها.
إذا اختلفت هذه الملفات بين العقد، فلا يمكن تحديد القيمة المعادة مسبقًا، إذ تعتمد على الترتيب الذي تطلب به العقد العاملة المهام من العقدة البادئة.
:::

<div id="syntax">
  ## الصياغة
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## الوسائط
</div>

| الوسيطة              | الوصف                                                                                                                                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | اسم عنقود يُستخدم لإنشاء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية.                                                                                        |
| `path`               | المسار النسبي للملف من [user&#95;files&#95;path](/ar/operations/server-configuration-parameters/settings.md#user_files_path). كما يدعم مسار الملف [أنماط glob](#globs-in-path). |
| `format`             | [التنسيق](/ar/sql-reference/formats) الخاص بالملفات. النوع: [String](../../sql-reference/data-types/string.md).                                                                 |
| `structure`          | بنية الجدول بصيغة `'UserID UInt64, Name String'`. ويحدد أسماء الأعمدة وأنواعها. النوع: [String](../../sql-reference/data-types/string.md).                                   |
| `compression_method` | طريقة الضغط. أنواع الضغط المدعومة هي `gz` و`br` و`xz` و`zst` و`lz4` و`bz2`.                                                                                                  |

<div id="returned_value">
  ## القيمة المُرجعة
</div>

جدول بالتنسيق والبنية المحددين، ويحتوي على بيانات من الملفات المطابقة للمسار المحدد.

**مثال**

بالنظر إلى عنقود باسم `my_cluster`، وإلى القيمة التالية للإعداد `user_files_path`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

كذلك، وبافتراض وجود الملفين `test1.csv` و`test2.csv` داخل `user_files_path` على كل عقدة في العنقود، وأن محتواهما متطابق على مختلف العُقَد:

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

على سبيل المثال، يمكن إنشاء هذه الملفات بتنفيذ هذين الاستعلامين على كل عقدة في العنقود:

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

الآن، اقرأ محتويات البيانات من `test1.csv` و`test2.csv` باستخدام دالة الجدول `fileCluster`:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## أنماط glob في المسار
</div>

تدعم FileCluster أيضًا جميع الأنماط التي تدعمها دالة الجدول [File](../../sql-reference/table-functions/file.md#globs-in-path).

<div id="related">
  ## ذات صلة
</div>

* [دالة الجدول File](../../sql-reference/table-functions/file.md)