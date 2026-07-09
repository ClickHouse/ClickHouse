---
description: 'تهيئة LIFETIME للقاموس للتحديث التلقائي'
sidebar_label: 'LIFETIME'
sidebar_position: 5
slug: /sql-reference/statements/create/dictionary/lifetime
title: 'تحديث بيانات القاموس باستخدام LIFETIME'
doc_type: 'مرجع'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

يُحدِّث ClickHouse القواميس دوريًا استنادًا إلى الوسم `LIFETIME` (المحدَّد بالثواني).
يمثّل `LIFETIME` فترة التحديث للقواميس التي تُنزَّل بالكامل، وفترة إبطال الصلاحية للقواميس المخزَّنة مؤقتًا.

أثناء التحديث، يظل من الممكن الاستعلام عن الإصدار القديم من القاموس.
لا تؤدي تحديثات القاموس إلى حظر الاستعلامات، إلا عند تحميله لأول استخدام.
إذا حدث خطأ أثناء التحديث، فسيُسجَّل الخطأ في سجل الخادم، ويمكن أن تستمر الاستعلامات في استخدام الإصدار القديم من القاموس.
إذا نجح تحديث القاموس، فسيُستبدل الإصدار القديم من القاموس [بشكل ذري](/ar/concepts/glossary#atomicity).

مثال على الإعدادات:

<CloudDetails />

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

أو

```sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

يؤدي ضبط `<lifetime>0</lifetime>` (`LIFETIME(0)`) إلى منع القواميس من التحديث.

يمكنك تعيين فاصل زمني للتحديثات، وسيختار ClickHouse وقتًا عشوائيًا ضمن هذا النطاق وفق توزيع منتظم. وهذا ضروري لتوزيع العبء على مصدر القاموس عند التحديث عبر عدد كبير من الخوادم.

مثال على الإعدادات:

```xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

أو

```sql
LIFETIME(MIN 300 MAX 360)
```

إذا كانت `<min>0</min>` و`<max>0</max>`، فلن يعيد ClickHouse تحميل القاموس عند انتهاء المهلة.
في هذه الحالة، يمكن لـ ClickHouse إعادة تحميل القاموس في وقت أبكر إذا تغيّر ملف إعدادات القاموس أو إذا نُفِّذ الأمر `SYSTEM RELOAD DICTIONARY`.

عند تحديث القواميس، يطبّق ClickHouse server منطقًا مختلفًا بحسب نوع [المصدر](./sources/):

* بالنسبة إلى ملف نصي، يتحقق من وقت التعديل. وإذا كان هذا الوقت مختلفًا عن الوقت المسجّل سابقًا، يُحدَّث القاموس.
* أمّا القواميس من المصادر الأخرى، فتُحدَّث افتراضيًا في كل مرة.

بالنسبة إلى المصادر الأخرى (ODBC وPostgreSQL وClickHouse وما إلى ذلك)، يمكنك إعداد استعلام يحدّث القواميس فقط إذا كانت قد تغيّرت بالفعل، بدلًا من تحديثها في كل مرة. للقيام بذلك، اتبع الخطوات التالية:

* يجب أن يحتوي جدول القاموس على حقل يتغيّر دائمًا عند تحديث بيانات المصدر.
* يجب أن تحدد إعدادات المصدر استعلامًا يسترجع الحقل المتغيّر. يفسّر ClickHouse server نتيجة الاستعلام على أنها صف، وإذا كان هذا الصف قد تغيّر مقارنةً بحالته السابقة، يُحدَّث القاموس. حدِّد الاستعلام في الحقل `<invalidate_query>` ضمن إعدادات [المصدر](./sources/).

مثال على الإعدادات:

```xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

أو

```sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

بالنسبة إلى قواميس `Cache` و`ComplexKeyCache` و`SSDCache` و`SSDComplexKeyCache`، يكون كلٌّ من التحديثات المتزامنة وغير المتزامنة مدعومًا.

ومن الممكن أيضًا أن تقتصر قواميس `Flat` و`Hashed` و`HashedArray` و`ComplexKeyHashed` على طلب البيانات التي تغيّرت بعد التحديث السابق فقط. إذا تم تحديد `update_field` كجزء من تهيئة مصدر القاموس، فستُضاف قيمة وقت التحديث السابق، بالثواني، إلى طلب البيانات. وبحسب نوع المصدر (Executable أو HTTP أو MySQL أو PostgreSQL أو ClickHouse أو ODBC)، سيُطبَّق منطق مختلف على `update_field` قبل طلب البيانات من مصدر خارجي.

* إذا كان المصدر هو HTTP، فستُضاف `update_field` كمعلَمة استعلام، وتكون قيمة المعلَمة هي وقت آخر تحديث.
* إذا كان المصدر هو Executable، فستُضاف `update_field` كوسيطة لبرنامج نصي قابل للتنفيذ، وتكون قيمة الوسيطة هي وقت آخر تحديث.
* إذا كان المصدر هو ClickHouse أو MySQL أو PostgreSQL أو ODBC، فسيُضاف جزء `WHERE` إضافي، حيث تُقارَن `update_field` بوقت آخر تحديث على أنها أكبر منه أو تساويه.
  * افتراضيًا، يُفحَص شرط `WHERE` هذا في أعلى مستوى من استعلام SQL. وبدلًا من ذلك، يمكن فحص الشرط في أي عبارة `WHERE` أخرى داخل الاستعلام باستخدام الكلمة المفتاحية `{condition}`. مثال:
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

إذا كان الخيار `update_field` مضبوطًا، فيمكن أيضًا ضبط الخيار الإضافي `update_lag`. وتُطرَح قيمة الخيار `update_lag` من وقت التحديث السابق قبل طلب البيانات المحدَّثة.

مثال على الإعدادات:

```xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

أو

```sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```