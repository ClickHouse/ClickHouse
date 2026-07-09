---
description: 'يتيح لك محركا الجدول `Executable` و`ExecutablePool` تعريف
  جدول تُولَّد صفوفه بواسطة برنامج نصي تحدده
  (من خلال كتابة الصفوف إلى **stdout**).'
sidebar_label: 'Executable/ExecutablePool'
sidebar_position: 40
slug: /engines/table-engines/special/executable
title: 'محركا الجدول Executable وExecutablePool'
doc_type: 'reference'
---

يتيح لك محركا الجدول `Executable` و`ExecutablePool` تعريف جدول تُولَّد صفوفه بواسطة برنامج نصي تحدده (من خلال كتابة الصفوف إلى **stdout**). ويُخزَّن البرنامج النصي القابل للتنفيذ في الدليل `user_scripts`، ويمكنه قراءة البيانات من أي مصدر.

* جداول `Executable`: يُشغَّل البرنامج النصي مع كل استعلام
* جداول `ExecutablePool`: تحتفظ بمجمّع من العمليات المستمرة، وتأخذ عمليات من هذا المجمّع عند القراءة

يمكنك اختياريًا تضمين استعلام إدخال واحد أو أكثر لتمرير نتائجه بشكل متدفق إلى **stdin** لكي يقرأها البرنامج النصي.

<div id="creating-an-executable-table">
  ## إنشاء جدول `Executable`
</div>

يتطلب محرك الجدول `Executable` معامِلَين: اسم البرنامج النصي وصيغة البيانات الواردة. ويمكنك اختياريًا تمرير استعلام إدخال واحد أو أكثر:

```sql
Executable(script_name, format, [input_query...])
```

فيما يلي الإعدادات ذات الصلة لجدول `Executable`:

* `send_chunk_header`
  * الوصف: إرسال عدد الصفوف في كل جزء قبل إرسال الجزء للمعالجة. يمكن أن يساعد هذا الإعداد في كتابة برنامجك النصي بكفاءة أكبر من خلال تخصيص بعض الموارد مسبقًا
  * القيمة الافتراضية: false
* `command_termination_timeout`
  * الوصف: مهلة إنهاء الأمر بالثواني
  * القيمة الافتراضية: 10
* `command_read_timeout`
  * الوصف: مهلة قراءة البيانات من `stdout` الخاص بالأمر، بالمللي ثانية
  * القيمة الافتراضية: 10000
* `command_write_timeout`
  * الوصف: مهلة كتابة البيانات إلى `stdin` الخاص بالأمر، بالمللي ثانية
  * القيمة الافتراضية: 10000

لنلقِ نظرة على مثال. يحمل برنامج بايثون النصي التالي اسم `my_script.py` ويُحفَظ في المجلد `user_scripts`. يقرأ العدد `i` ويطبع `i` سلاسل عشوائية، تسبق كلَّ سلسلةٍ منها قيمةٌ رقمية يفصل بينها وبين السلسلة حرفُ جدولة:

```python
#!/usr/bin/python3

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

يُنشأ الجدول `my_executable_table` التالي من مخرجات `my_script.py`، الذي يُولِّد 10 سلاسل نصية عشوائية في كل مرة تُجري فيها استعلام `SELECT` على `my_executable_table`:

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

تكتمل عملية إنشاء الجدول فورًا ولا تستدعي البرنامج النصي. ويؤدي الاستعلام عن `my_executable_table` إلى استدعاء البرنامج النصي:

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

<div id="passing-query-results-to-a-script">
  ## تمرير نتيجة استعلام إلى برنامج نصي
</div>

يترك مستخدمو موقع Hacker News تعليقات. تتضمن لغة بايثون مجموعة أدوات لمعالجة اللغة الطبيعية (`nltk`) تحتوي على `SentimentIntensityAnalyzer` لتحديد ما إذا كانت التعليقات إيجابية أو سلبية أو محايدة، بما في ذلك تخصيص قيمة بين -1 (تعليق سلبي جدًا) و1 (تعليق إيجابي جدًا). لنُنشئ جدول `Executable` يحسب تحليل المشاعر لتعليقات Hacker News باستخدام `nltk`.

يستخدم هذا المثال جدول `hackernews` الموضّح [هنا](/ar/engines/table-engines/mergetree-family/textindexes/#hacker-news-dataset). يتضمن جدول `hackernews` عمود `id` من النوع `UInt64` وعمودًا من النوع `String` باسم `comment`. لنبدأ بتعريف جدول `Executable`:

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

بعض الملاحظات حول جدول `sentiment`:

* يُحفَظ الملف `sentiment.py` في المجلد `user_scripts` (وهو المجلد `default` للإعداد `user_scripts_path`)
* يعني التنسيق `TabSeparated` أن برنامجنا النصي بلغة بايثون يجب أن يُنشئ صفوفًا من البيانات الخام تحتوي على قيَم مفصولة بعلامات الجدولة
* يختار الاستعلام عمودين من `hackernews`. وسيحتاج البرنامج النصي بلغة بايثون إلى استخراج قيم هذين العمودين من الصفوف الواردة

فيما يلي تعريف `sentiment.py`:

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

بعض الملاحظات حول برنامجنا النصي المكتوب ببايثون:

* لكي يعمل ذلك، ستحتاج إلى تشغيل `nltk.downloader.download('vader_lexicon')`. كان يمكن وضع هذا داخل البرنامج النصي، لكن عندئذٍ كان سيُنزَّل في كل مرة يُنفَّذ فيها query على جدول `sentiment` — وهذا غير فعّال
* كل قيمة في `row` ستكون صفًا في result set الخاصة بالاستعلام `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`
* الصف الوارد مفصول بعلامات الجدولة، لذا نحلّل `id` و`comment` باستخدام الدالة `split` في بايثون
* ناتج `polarity_scores` هو JSON object يحتوي على عدد من القيم. وقد قررنا الاكتفاء بأخذ القيمة `compound` من هذا الكائن
* تذكّر أن جدول `sentiment` في ClickHouse يستخدم التنسيق `TabSeparated` ويحتوي على عمودين، لذا تفصل دالة `print` بين هذين العمودين بعلامة جدولة

في كل مرة تكتب فيها query يحدّد صفوفًا من جدول `sentiment`، يُنفَّذ الاستعلام `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` وتُمرَّر النتيجة إلى `sentiment.py`. لنجرب ذلك:

```sql
SELECT *
FROM sentiment
```

يكون الناتج كما يلي:

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

<div id="creating-an-executablepool-table">
  ## إنشاء جدول `ExecutablePool`
</div>

بنية `ExecutablePool` مشابهة لـ `Executable`، ولكن توجد بعض الإعدادات ذات الصلة الخاصة بجدول `ExecutablePool`:

* `pool_size`
  * الوصف: حجم مجمّع العمليات. إذا كانت القيمة 0، فلا تُفرض أي قيود على الحجم
  * القيمة الافتراضية: 16
* `max_command_execution_time`
  * الوصف: الحد الأقصى لوقت تنفيذ الأمر بالثواني
  * القيمة الافتراضية: 10

يمكننا بسهولة تحويل جدول `sentiment` أعلاه لاستخدام `ExecutablePool` بدلًا من `Executable`:

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

سيحتفظ ClickHouse بأربع عمليات عند الطلب عندما يُجري عميلك استعلامًا على جدول `sentiment_pooled`.