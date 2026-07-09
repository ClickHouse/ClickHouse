---
alias: []
description: 'وثائق تنسيق Regexp'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✗       |                |

<div id="description">
  ## الوصف
</div>

يحلّل تنسيق `Regex` كل سطر من البيانات المستوردة وفقًا للتعبير النمطي المحدد.

**الاستخدام**

يُطبَّق التعبير النمطي من إعداد [format&#95;regexp](/ar/operations/settings/settings-formats.md/#format_regexp) على كل سطر من البيانات المستوردة. ويجب أن يساوي عدد الأنماط الفرعية في التعبير النمطي عدد الأعمدة في مجموعة البيانات المستوردة.

يجب فصل أسطر البيانات المستوردة بمحرف سطر جديد `'\n'` أو بمحرف سطر جديد بنمط DOS `"\r\n"`.

يُحلَّل محتوى كل نمط فرعي مطابق باستخدام أسلوب نوع البيانات المقابل، وفقًا لإعداد [format&#95;regexp&#95;escaping&#95;rule](/ar/operations/settings/settings-formats.md/#format_regexp_escaping_rule).

إذا لم يطابق التعبير النمطي السطر وكان [format&#95;regexp&#95;skip&#95;unmatched](/ar/operations/settings/settings-formats.md/#format_regexp_escaping_rule) مضبوطًا على 1، فسيتم تخطي السطر بصمت. وإلا، يتم طرح استثناء.

<div id="example-usage">
  ## مثال للاستخدام
</div>

لنأخذ الملف `data.tsv`:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

والجدول `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

سنُدرج البيانات من الملف المذكور سابقًا في الجدول أعلاه باستخدام الاستعلام التالي:

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

يمكننا الآن استخدام `SELECT` لاستخراج البيانات من الجدول لمعرفة كيف حلّل تنسيق `Regex` البيانات من الملف:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

عند العمل مع تنسيق `Regexp`، يمكنك استخدام الإعدادات التالية:

* `format_regexp` — [String](/ar/sql-reference/data-types/string.md). يحتوي على تعبير نمطي بتنسيق [re2](https://github.com/google/re2/wiki/Syntax).

* `format_regexp_escaping_rule` — [String](/ar/sql-reference/data-types/string.md). قواعد الإفلات التالية مدعومة:

  * CSV (على غرار [CSV](/ar/interfaces/formats/CSV)
  * JSON (على غرار [JSONEachRow](/ar/interfaces/formats/JSONEachRow)
  * Escaped (على غرار [TSV](/ar/interfaces/formats/TabSeparated)
  * Quoted (على غرار [Values](/ar/interfaces/formats/Values)
  * Raw (يستخرج الأنماط الفرعية بالكامل، من دون قواعد إفلات، على غرار [TSVRaw](/ar/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/ar/sql-reference/data-types/int-uint.md). يحدد ما إذا كان يجب طرح استثناء إذا لم يطابق التعبير `format_regexp` البيانات المستوردة. يمكن ضبطه على `0` أو `1`.