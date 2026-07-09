---
description: 'تنشئ دالة الجدول `executable` جدولًا استنادًا إلى مخرجات دالة يعرّفها
  المستخدم (UDF) تحددها داخل برنامج نصي يُخرج الصفوف إلى **stdout**.'
keywords: ['udf', 'دالة يعرّفها المستخدم', 'clickhouse', 'executable', 'جدول', 'دالة']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'مرجع'
---

تنشئ دالة الجدول `executable` جدولًا استنادًا إلى مخرجات دالة يعرّفها المستخدم (UDF) تحددها داخل برنامج نصي يُخرج الصفوف إلى **stdout**. يُخزَّن البرنامج النصي القابل للتنفيذ في الدليل `users_scripts`، ويمكنه قراءة البيانات من أي مصدر. تأكد من أن خادم ClickHouse لديك يحتوي على جميع الحزم المطلوبة لتشغيل البرنامج النصي القابل للتنفيذ. على سبيل المثال، إذا كان برنامجًا نصيًا بلغة بايثون، فتأكد من أن الخادم يحتوي على حزم بايثون اللازمة مثبّتة.

يمكنك اختياريًا تضمين استعلام إدخال واحد أو أكثر لتمرير نتائجه إلى **stdin** لكي يقرأها البرنامج النصي.

:::note
من المزايا الأساسية التي تميّز دوال UDF العادية عن دالة الجدول `executable` ومحرك الجدول `Executable` أن دوال UDF العادية لا يمكنها تغيير عدد الصفوف. على سبيل المثال، إذا كان الإدخال 100 صف، فيجب أن تُرجع النتيجة 100 صف. عند استخدام دالة الجدول `executable` أو محرك الجدول `Executable`، يمكن لبرنامجك النصي إجراء أي تحويلات للبيانات تريدها، بما في ذلك عمليات التجميع المعقدة.
:::

<div id="syntax">
  ## الصياغة
</div>

تتطلب دالة الجدول `executable` ثلاث معلمات، وتقبل قائمة اختيارية من استعلامات الإدخال:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: اسم ملف البرنامج النصي. ويُحفَظ في مجلد `user_scripts` (المجلد default للإعداد `user_scripts_path`)
* `format`: تنسيق الجدول المُنشأ
* `structure`: مخطط الجدول للجدول المُنشأ
* `input_query`: استعلام اختياري (أو مجموعة أو استعلامات) تُمرَّر نتائجه إلى البرنامج النصي عبر **stdin**

:::note
إذا كنت ستستدعي البرنامج النصي نفسه مرارًا بالاستعلامات نفسها، ففكّر في استخدام [محرك الجدول `Executable`](../../engines/table-engines/special/executable.md).
:::

يحمل برنامج بايثون النصي التالي اسم `generate_random.py` ويُحفَظ في مجلد `user_scripts`. يقرأ العدد `i` ويطبع `i` سلاسل عشوائية، تسبق كلَّ سلسلةٍ منها قيمةٌ رقمية مفصولة عنها بعلامة تبويب:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

لنُشغِّل البرنامج النصي ونجعله يُولِّد 10 سلاسل نصية عشوائية:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

تكون الاستجابة كما يلي:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## الإعدادات
</div>

* `send_chunk_header` - يتحكم في ما إذا كان سيتم إرسال عدد الصفوف قبل إرسال chunk من البيانات للمعالجة. القيمة الافتراضية هي `false`.
* `pool_size` — حجم المجمّع. إذا جرى تحديد 0 للقيمة `pool_size`، فلن تكون هناك أي قيود على حجم المجمّع. القيمة الافتراضية هي `16`.
* `max_command_execution_time` — الحد الأقصى لوقت تنفيذ أمر البرنامج النصي القابل للتنفيذ لمعالجة كتلة من البيانات. يُحدَّد بالثواني. القيمة الافتراضية هي 10.
* `command_termination_timeout` — يجب أن يحتوي البرنامج النصي القابل للتنفيذ على حلقة رئيسية للقراءة والكتابة. بعد إتلاف دالة الجدول، تُغلَق القناة، ويكون أمام الملف التنفيذي `command_termination_timeout` ثانية للتوقف قبل أن يرسل ClickHouse الإشارة SIGTERM إلى العملية الفرعية. يُحدَّد بالثواني. القيمة الافتراضية هي 10.
* `command_read_timeout` - مهلة قراءة البيانات من stdout الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية 10000.
* `command_write_timeout` - مهلة كتابة البيانات إلى stdin الخاص بالأمر، بالمللي ثانية. القيمة الافتراضية 10000.

<div id="passing-query-results-to-a-script">
  ## تمرير نتائج الاستعلام إلى برنامج نصي
</div>

احرص على الاطلاع على المثال في محرك الجدول `Executable` حول [كيفية تمرير نتائج الاستعلام إلى برنامج نصي](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). إليك كيفية تنفيذ البرنامج النصي نفسه الوارد في ذلك المثال باستخدام دالة الجدول `executable`:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```