---
description: 'توثيق الدوال المعرّفة من قبل المستخدم (UDFs)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'الدوال المعرّفة من قبل المستخدم (UDFs)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # UDFs الدوال المعرّفة من قبل المستخدم
</div>

يدعم ClickHouse عدة أنواع من الدوال المعرّفة من قبل المستخدم (UDFs):

* تبدأ [الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ](#executable-user-defined-functions) برنامجًا أو برنامجًا نصيًا خارجيًا (بايثون، Bash، إلخ)، وتمرّر إليه كتلًا من البيانات عبر STDIN / STDOUT. استخدمها لدمج الشيفرة أو الأدوات الموجودة دون إعادة تجميع ClickHouse. ويكون لها حمل إضافي أعلى لكل استدعاء مقارنةً بالخيارات التي تعمل داخل العملية نفسها، لذا فهي الأنسب للمنطق الأثقل أو عند الحاجة إلى بيئة تشغيل مختلفة.
* تُعرَّف [الدوال المعرّفة من المستخدم في SQL](#sql-user-defined-functions) باستخدام `CREATE FUNCTION` بالكامل داخل SQL. وتُضمَّن/تُوسَّع داخل خطة الاستعلام (من دون حدود بين العمليات)، مما يجعلها خفيفة ومثالية لإعادة استخدام منطق التعبيرات أو تبسيط الأعمدة المحسوبة المعقدة.
* تُشغِّل [الدوال المعرّفة من المستخدم التجريبية في WebAssembly](#webassembly-user-defined-functions) شيفرةً مُجمَّعة إلى WebAssembly داخل بيئة معزولة ضمن عملية الخادم. وهي توفر حملًا إضافيًا أقل لكل استدعاء من الملفات التنفيذية الخارجية، مع عزل أفضل من الامتدادات الأصلية، مما يجعلها مناسبة للخوارزميات المخصصة المكتوبة بلغات يمكن تجميعها إلى WASM (مثل C/C++/Rust).
* تتيح [الدوال المعرّفة من المستخدم التجريبية القابلة للتنفيذ المعتمدة على برنامج التشغيل](#driver-based-executable-user-defined-functions) لـ &quot;برنامج التشغيل&quot; يوفّره المشغّل تحويل مقتطف شيفرة مُقدَّم في `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'` إلى executable UDF عند إنشاء الدالة (على سبيل المثال، عبر تجميعه). وهي تستند إلى الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ وتتطلب تهيئة برنامج التشغيل على جانب الخادم.

<div id="executable-user-defined-functions">
  ## الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ
</div>

<BetaBadge />

:::note
في ClickHouse Cloud، تكون الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ في الإصدار التجريبي العام، ويتم إنشاؤها عبر واجهة مستخدم Cloud Console. راجع [الدوال المعرّفة من قبل المستخدم في Cloud](/ar/cloud/features/user-defined-functions) للاطلاع على سير العمل الخاص بـ Cloud.
:::

يمكن لـ ClickHouse استدعاء أي برنامج خارجي قابل للتنفيذ أو برنامج نصي لمعالجة البيانات.

يمكن أن يوجد تكوين الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ في ملف XML واحد أو أكثر.
ويُحدَّد مسار التكوين في المعلَمة [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config).

يحتوي تكوين الدالة على الإعدادات التالية:

| المعلمة                       | الوصف                                                                                                                                                                                                                                                                                                                                                                                               | مطلوب   | القيمة الافتراضية         |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ------------------------- |
| `name`                        | اسم الدالة                                                                                                                                                                                                                                                                                                                                                                                          | نعم     | -                         |
| `command`                     | اسم البرنامج النصي المراد تنفيذه، أو الأمر إذا كانت قيمة `execute_direct` هي false                                                                                                                                                                                                                                                                                                                  | نعم     | -                         |
| `argument`                    | وصف الوسيطة مع `type` و`name` اختياري للوسيطة. تُوصَف كل وسيطة في إعداد منفصل. ويكون تحديد الاسم ضروريًا إذا كانت أسماء الوسائط جزءًا من التسلسل لدالة معرّفة من قبل المستخدم بتنسيق مثل [Native](/ar/interfaces/formats/Native) أو [JSONEachRow](/ar/interfaces/formats/JSONEachRow)                                                                                                                     | نعم     | `c` + argument&#95;number |
| `format`                      | [تنسيق](../../interfaces/formats.md) تُمرَّر به الوسائط إلى الأمر. ومن المتوقع أيضًا أن يستخدم خرج الأمر التنسيق نفسه                                                                                                                                                                                                                                                                               | نعم     | -                         |
| `return_type`                 | نوع القيمة المُعادة                                                                                                                                                                                                                                                                                                                                                                                 | نعم     | -                         |
| `return_name`                 | اسم القيمة المُعادة. ويكون تحديد اسم الإرجاع ضروريًا إذا كان اسم الإرجاع جزءًا من التسلسل لدالة معرّفة من قبل المستخدم بتنسيق مثل [Native](/ar/interfaces/formats/Native) أو [JSONEachRow](/ar/interfaces/formats/JSONEachRow)                                                                                                                                                                            | اختياري | `result`                  |
| `type`                        | نوع executable. إذا ضُبطت قيمة `type` على `executable` فسيُشغَّل أمر واحد. وإذا ضُبطت على `executable_pool` فسيُنشأ مجمع من الأوامر                                                                                                                                                                                                                                                                 | نعم     | -                         |
| `max_command_execution_time`  | الحد الأقصى لوقت التنفيذ، بالثواني، لمعالجة block من البيانات. هذا الإعداد صالح فقط لأوامر `executable_pool`                                                                                                                                                                                                                                                                                        | اختياري | `10`                      |
| `command_termination_timeout` | المدة، بالثواني، التي يجب أن ينهي خلالها الأمر عمله بعد إغلاق pipe الخاص به. وبعد انقضاء هذه المدة، تُرسَل الإشارة `SIGTERM` إلى العملية التي تنفذ الأمر                                                                                                                                                                                                                                            | اختياري | `10`                      |
| `command_read_timeout`        | مهلة قراءة البيانات من `stdout` الخاص بالأمر، بالمللي ثانية                                                                                                                                                                                                                                                                                                                                         | اختياري | `10000`                   |
| `command_write_timeout`       | مهلة كتابة البيانات إلى `stdin` الخاص بالأمر، بالمللي ثانية                                                                                                                                                                                                                                                                                                                                         | اختياري | `10000`                   |
| `pool_size`                   | حجم مجمع الأوامر                                                                                                                                                                                                                                                                                                                                                                                    | اختياري | `16`                      |
| `send_chunk_header`           | يحدد ما إذا كان سيتم إرسال عدد الصفوف قبل إرسال chunk من البيانات إلى العملية                                                                                                                                                                                                                                                                                                                       | اختياري | `false`                   |
| `execute_direct`              | إذا كانت قيمة `execute_direct` = `1`، فسيُبحث عن `command` داخل مجلد user&#95;scripts المحدد بواسطة [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). ويمكن تحديد وسيطات إضافية للبرنامج النصي باستخدام فواصل المسافات. مثال: `script_name arg1 arg2`. وإذا كانت قيمة `execute_direct` = `0`، فسيُمرَّر `command` كوسيطة إلى `bin/sh -c` | اختياري | `1`                       |
| `lifetime`                    | فترة إعادة تحميل الدالة بالثواني. إذا ضُبطت على `0` فلن تُعاد تحميل الدالة                                                                                                                                                                                                                                                                                                                          | اختياري | `0`                       |
| `deterministic`               | ما إذا كانت الدالة حتمية (تعيد النتيجة نفسها للمدخل نفسه)                                                                                                                                                                                                                                                                                                                                           | اختياري | `false`                   |
| `stderr_reaction`             | كيفية التعامل مع خرج `stderr` الخاص بالأمر. القيم هي: `none` (تجاهل)، `log` (تسجيل كل خرج `stderr` فورًا)، `log_first` (تسجيل أول 4 KiB بعد الخروج)، `log_last` (تسجيل آخر 4 KiB بعد الخروج)، `throw` (طرح استثناء فورًا عند وجود أي خرج من `stderr`). عند استخدام `log_first` أو `log_last` مع رمز خروج غير صفري، يُضمَّن محتوى `stderr` في رسالة الاستثناء                                        | اختياري | `log_last`                |
| `check_exit_code`             | إذا كانت القيمة `true`، فسيتحقق ClickHouse من رمز خروج الأمر. ويتسبب رمز الخروج غير الصفري في طرح استثناء                                                                                                                                                                                                                                                                                           | اختياري | `true`                    |

يجب أن يقرأ الأمر الوسائط من `STDIN` وأن يكتب النتيجة إلى `STDOUT`. ويجب أن يعالج الأمر الوسائط على نحو تكراري؛ أي بعد معالجة chunk من الوسائط، يجب أن ينتظر وصول الـ chunk التالي.

<div id="executable-user-defined-functions">
  ## الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ
</div>

<div id="examples">
  ## أمثلة
</div>

<div id="udf-inline">
  ### UDF من برنامج نصي مضمن
</div>

أنشئ `test_function_sum` يدويًا مع ضبط `execute_direct` على `0` باستخدام تهيئة XML أو YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    الملف `test_function.xml` (`/etc/clickhouse-server/test_function.xml` باستخدام إعدادات المسار الافتراضية).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum</name>
            <return_type>UInt64</return_type>
            <argument>
                <type>UInt64</type>
                <name>lhs</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>rhs</name>
            </argument>
            <format>TabSeparated</format>
            <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
            <execute_direct>0</execute_direct>
            <deterministic>true</deterministic>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    الملف `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` باستخدام إعدادات المسار الافتراضية).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum
      return_type: UInt64
      argument:
        - type: UInt64
          name: lhs
        - type: UInt64
          name: rhs
      format: TabSeparated
      command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
      execute_direct: 0
      deterministic: true
    ```
  </TabItem>
</Tabs>

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### UDF من برنامج نصي بلغة بايثون
</div>

في هذا المثال، ننشئ UDF يقرأ قيمة من `STDIN` ويُرجعها كسلسلة نصية.

أنشئ `test_function` باستخدام تهيئة XML أو YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    الملف `test_function.xml` (`/etc/clickhouse-server/test_function.xml` باستخدام إعدادات المسار الافتراضية).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_function.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    الملف `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` باستخدام إعدادات المسار الافتراضية).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_python
      return_type: String
      argument:
        - type: UInt64
          name: value
      format: TabSeparated
      command: test_function.py
    ```
  </TabItem>
</Tabs>

<br />

أنشئ ملف البرنامج النصي `test_function.py` داخل مجلد `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function.py` باستخدام إعدادات المسار الافتراضية).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

<div id="udf-stdin">
  ### اقرأ قيمتين من `STDIN` وأعِد مجموعهما ككائن JSON
</div>

أنشئ `test_function_sum_json` باستخدام وسائط مُسمّاة وتنسيق [JSONEachRow](/ar/interfaces/formats/JSONEachRow) عبر إعداد XML أو YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    الملف `test_function.xml` ‏(`/etc/clickhouse-server/test_function.xml` مع إعدادات المسار الافتراضية).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum_json</name>
            <return_type>UInt64</return_type>
            <return_name>result_name</return_name>
            <argument>
                <type>UInt64</type>
                <name>argument_1</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>argument_2</name>
            </argument>
            <format>JSONEachRow</format>
            <command>test_function_sum_json.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    الملف `test_function.yaml` ‏(`/etc/clickhouse-server/test_function.yaml` مع إعدادات المسار الافتراضية).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum_json
      return_type: UInt64
      return_name: result_name
      argument:
        - type: UInt64
          name: argument_1
        - type: UInt64
          name: argument_2
      format: JSONEachRow
      command: test_function_sum_json.py
    ```
  </TabItem>
</Tabs>

<br />

أنشئ ملف البرنامج النصي `test_function_sum_json.py` داخل مجلد `user_scripts` ‏(`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` مع إعدادات المسار الافتراضية).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

<div id="udf-parameters-in-command">
  ### استخدم المعلمات في إعداد `command`
</div>

يمكن للدوال المعرّفة من قبل المستخدم القابلة للتنفيذ أن تستقبل معلمات ثابتة مُعدّة في إعداد `command` (وهذا يعمل فقط مع الدوال المعرّفة من قبل المستخدم من النوع `executable`).
ويتطلب ذلك أيضًا الخيار `execute_direct` لضمان عدم وجود ثغرة ناتجة عن توسيع وسيطات shell.

<Tabs>
  <TabItem value="XML" label="XML" default>
    الملف `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` مع إعدادات المسار الافتراضية).

    ```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
    <functions>
        <function>
            <type>executable</type>
            <execute_direct>true</execute_direct>
            <name>test_function_parameter_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
            </argument>
            <format>TabSeparated</format>
            <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    الملف `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` مع إعدادات المسار الافتراضية).

    ```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
    functions:
      type: executable
      execute_direct: true
      name: test_function_parameter_python
      return_type: String
      argument:
        - type: UInt64
      format: TabSeparated
      command: test_function_parameter_python.py {test_parameter:UInt64}
    ```
  </TabItem>
</Tabs>

<br />

أنشئ ملف البرنامج النصي `test_function_parameter_python.py` داخل المجلد `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` مع إعدادات المسار الافتراضية).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

<div id="udf-shell-script">
  ### UDF من برنامج نصي Shell
</div>

في هذا المثال، ننشئ برنامجًا نصيًا لـ Shell يضاعف كل قيمة بمقدار 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
    الملف `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` باستخدام إعدادات المسار الافتراضية).

    ```xml title="/etc/clickhouse-server/test_function_shell.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_shell</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt8</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_shell.sh</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    الملف `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` باستخدام إعدادات المسار الافتراضية).

    ```yml title="/etc/clickhouse-server/test_function_shell.yaml"
    functions:
      type: executable
      name: test_shell
      return_type: String
      argument:
        - type: UInt8
          name: value
      format: TabSeparated
      command: test_shell.sh
    ```
  </TabItem>
</Tabs>

<br />

أنشئ ملف البرنامج النصي `test_shell.sh` داخل المجلد `user_scripts` (`/var/lib/clickhouse/user_scripts/test_shell.sh` باستخدام إعدادات المسار الافتراضية).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```

<div id="error-handling">
  ## معالجة الأخطاء
</div>

قد تُطلق بعض الدوال استثناءً إذا كانت البيانات غير صالحة.
في هذه الحالة، يُلغى الاستعلام ويُعاد نص الخطأ إلى العميل.
في المعالجة الموزعة، عند حدوث استثناء على أحد الخوادم، تحاول الخوادم الأخرى أيضًا إلغاء الاستعلام.

<div id="evaluation-of-argument-expressions">
  ## تقييم تعبيرات المعاملات
</div>

في معظم لغات البرمجة، قد لا يجري تقييم أحد المعاملات لبعض عوامل التشغيل.
وينطبق ذلك عادةً على عوامل التشغيل `&&` و `||` و `?:`.
في ClickHouse، تُقيَّم معاملات الدوال (عوامل التشغيل) دائمًا.
ويعود ذلك إلى أن أجزاءً كاملة من الأعمدة تُقيَّم دفعةً واحدة، بدلًا من حساب كل صف على حدة.

<div id="performing-functions-for-distributed-query-processing">
  ## تنفيذ الدوال في معالجة الاستعلامات الموزعة
</div>

في معالجة الاستعلامات الموزعة، يُنفَّذ أكبر عدد ممكن من مراحل معالجة الاستعلام على الخوادم البعيدة، بينما تُنفَّذ المراحل المتبقية (دمج النتائج الوسيطة وكل ما يلي ذلك) على الخادم الطالب.

وهذا يعني أن الدوال قد تُنفَّذ على خوادم مختلفة.
على سبيل المثال، في الاستعلام `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

* إذا كان `distributed_table` يحتوي على shardين على الأقل، فإن الدالتين &#39;g&#39; و &#39;h&#39; تُنفَّذان على الخوادم البعيدة، بينما تُنفَّذ الدالة &#39;f&#39; على الخادم الطالب.
* إذا كان `distributed_table` يحتوي على shard واحد فقط، فإن جميع الدوال &#39;f&#39; و &#39;g&#39; و &#39;h&#39; تُنفَّذ على خادم هذا الـ shard.

عادةً لا تعتمد نتيجة الدالة على الخادم الذي تُنفَّذ عليه. لكن في بعض الحالات يكون ذلك مهمًا.
فعلى سبيل المثال، تستخدم الدوال التي تعمل مع القواميس القاموس الموجود على الخادم الذي تعمل عليه.
ومثال آخر هو الدالة `hostName`، التي تُرجع اسم الخادم الذي تعمل عليه، بحيث يمكن تنفيذ `GROUP BY` حسب الخوادم في استعلام `SELECT`.

إذا كانت دالة في استعلام ما تُنفَّذ على الخادم الطالب، لكنك تحتاج إلى تنفيذها على الخوادم البعيدة، فيمكنك تغليفها داخل دالة التجميع &#39;any&#39; أو إضافتها إلى مفتاح في `GROUP BY`.

<div id="sql-user-defined-functions">
  ## دوال SQL المعرّفة من قبل المستخدم
</div>

يمكن إنشاء دوال مخصّصة من تعبيرات لامبدا باستخدام عبارة [CREATE FUNCTION](../statements/create/function.md). ولحذف هذه الدوال، استخدم عبارة [DROP FUNCTION](../statements/drop.md#drop-function).

<div id="webassembly-user-defined-functions">
  ## دوال WebAssembly المعرّفة من قبل المستخدم
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

تتيح لك دوال WebAssembly المعرّفة من قبل المستخدم (WASM UDFs) تشغيل شيفرة مخصّصة مُصرَّفة إلى WebAssembly داخل عملية خادم ClickHouse.

<div id="quick-start">
  ### البدء السريع
</div>

فعِّل دعم WebAssembly التجريبي في إعدادات ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

أدرِج وحدة WASM المُجمَّعة في جدول النظام:

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

أنشئ دالة باستخدام وحدة WASM الخاصة بك:

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

استخدم FUNCTION في استعلاماتك:

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### مزيد من المعلومات
</div>

راجع وثائق [WebAssembly User Defined Functions](wasm_udf.md) لمزيد من التفاصيل.

<div id="driver-based-executable-user-defined-functions">
  ## الدوال التنفيذية المعرّفة من قبل المستخدم المستندة إلى برامج التشغيل
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
هذه ميزة تجريبية قد تتغير في الإصدارات المستقبلية بطرق غير متوافقة مع الإصدارات السابقة. فعِّلها باستخدام إعداد الخادم [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers).
:::

إن *برنامج التشغيل* هو مهايئ يوفّره المشغّل لتحويل مقتطف من شيفرة المستخدم إلى [executable UDF](#executable-user-defined-functions) قابلة للتنفيذ. عند إنشاء دالة باستخدام `ENGINE = DriverName(...)`، يشغّل ClickHouse الأمر `create_command` الخاص بـ برنامج التشغيل، ويمرّر إليه توقيع الدالة ومحتوى الشيفرة؛ ثم يقوم برنامج التشغيل بترجمة المحتوى برمجيًا أو معالجته بطريقة أخرى، ويطبع تهيئة executable UDF، ثم يخزّنها ClickHouse ويحمّلها.

يتيح ذلك للمسؤولين توفير طريقة آمنة ومحدودة للمستخدمين لتعريف الدوال بلغة برمجة اعتباطية (مثل C التي تُصرَّف داخل حاوية معزولة) من دون منحهم حق الوصول إلى ملفات تهيئة الخادم أو نظام الملفات. وتخضع مجموعة برامج التشغيل المتاحة بالكامل لتحكم المشغّل.

<div id="enabling-drivers">
  ### تمكين برامج التشغيل
</div>

تكون UDFs التنفيذية المستندة إلى برامج التشغيل معطّلة افتراضيًا. لتمكينها:

1. اضبط خيار Experimental في تهيئة الخادم:

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. وجّه [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) إلى ملف تهيئة واحد أو أكثر لبرنامج التشغيل (نمط glob مدعوم)، واضبط اختياريًا [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path)، وهو الدليل الذي تُخزَّن فيه تهيئات executable UDF المُولَّدة:

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

يُحمَّل سجل برامج التشغيل عند بدء تشغيل الخادم، ويُحدَّث عند `SYSTEM RELOAD CONFIG`، لذا يمكن إضافة برامج التشغيل أو تعديلها أو إزالتها دون إعادة تشغيل الخادم.

<div id="driver-configuration">
  ### إعداد برنامج التشغيل
</div>

يُعرَّف برنامج التشغيل بملف XML (أو YAML) يحتوي على عنصر `<driver>` في المستوى الأعلى. الحقول التالية مدعومة:

| الحقل              | الوصف                                                                                                                                                 | مطلوب |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `name`             | اسم برنامج التشغيل، كما يُستخدم في `CREATE FUNCTION ... ENGINE = <name>(...)`.                                                                        | نعم   |
| `create_command`   | المسار إلى البرنامج الذي يُستدعى لإنشاء UDF من مقتطف شيفرة. تُفسَّر المسارات النسبية نسبةً إلى ملف إعداد برنامج التشغيل.                              | نعم   |
| `drop_command`     | المسار إلى البرنامج الذي يُستدعى عند حذف دالة تستند إلى برنامج التشغيل هذا.                                                                           | لا    |
| `engine_arguments` | يحدِّد الوسيطات المسموح بها داخل `ENGINE = DriverName(...)`. كل عنصر فرعي هو اسم وسيط؛ ويشير العنصر الفرعي `<required>true</required>` إلى أنه مطلوب. | لا    |
| `env`              | متغيرات البيئة التي تُصدَّر عند استدعاء أوامر برنامج التشغيل.                                                                                         | لا    |

مثال على إعداد برنامج التشغيل:

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### عقد استدعاء برنامج التشغيل
</div>

عند تشغيل `CREATE FUNCTION`، يُستدعى `create_command` بعد ضبط متغيرات `env` المهيأة ومع الوسائط التالية:

* `--name <function_name>`
* `--return <return_type>` (إذا وُجد بند `RETURNS`)
* `--args <signature>` (إذا وُجد بند `ARGUMENTS`)، حيث يكون التوقيع هو قائمة الوسائط المعلنة، على سبيل المثال `x UInt8, y DateTime`
* `--<key> <value>` لكل وسيطة محرك معلنة وممرَّرة في `ENGINE = DriverName(key = value)`

يُرسَل محتوى شيفرة المستخدم (النص الذي يلي `AS`) إلى الإدخال القياسي للأمر. ويجب أن يطبع الأمر تهيئة `executable UDF` إلى الإخراج القياسي. ويُكتشَف التنسيق تلقائيًا: فالمخرجات التي تبدأ بـ `<` تُعامَل على أنها XML، وإلا فتُعامَل على أنها YAML. ويجب أن يطابق اسم الدالة المعرَّف في التهيئة المُولَّدة الاسمَ الجاري إنشاؤه. وإذا أنهى `create_command` التنفيذ برمز خروج غير صفري، تفشل التعليمة مع استثناء يتضمن رمز الخروج والخطأ القياسي لبرنامج التشغيل.

ويُستدعى `drop_command`، عند وجوده، بالطريقة نفسها (من دون محتوى شيفرة على `stdin`) عند إسقاط الدالة.

<div id="creating-a-function-with-a-driver">
  ### إنشاء function
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

يشغّل ClickHouse القيمة `create_command` الخاصة بـ `برنامج التشغيل`، ويكتب التهيئة المُنشأة في [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path)، ثم يلتقطها مُحمِّل executable UDF الحالي. بعد ذلك، يمكن استدعاء الدالة مثل أي دالة أخرى.

<div id="dropping-a-function-with-a-driver">
  ### حذف دالة
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

يستدعي `DROP FUNCTION` الأمر `drop_command` الخاص ببرنامج التشغيل (إن وُجد)، ويزيل الإعداد الديناميكي الذي تم إنشاؤه ودليل العمل الخاص بكل دالة، ثم يعيد تحميل مُحمِّل executable UDF، ويزيل الاستعلام المحفوظ.

<div id="driver-persistence-and-restart">
  ### الاستمرارية وإعادة التشغيل
</div>

يُحفَظ الاستعلام الأصلي على هيئة عبارة `ATTACH FUNCTION ...` في دليل كائنات SQL المعرّفة من قبل المستخدم، بحيث تظل الدالة موجودة بعد إعادة تشغيل الخادم. عند بدء التشغيل، تُحمَّل التكوينات المُنشأة في [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) مباشرةً من دون إعادة تنفيذ برنامج التشغيل. وإذا لم تكن هناك تهيئة مُنشأة مطابقة لعبارة `ATTACH FUNCTION` محفوظة (على سبيل المثال، إذا فُقد الدليل الديناميكي)، فسيُعاد تنفيذ برنامج التشغيل لإعادة إنشائها.

<div id="driver-limitations">
  ### القيود
</div>

* هذه الميزة تجريبية، ولا تتاح إلا عند تفعيل `allow_experimental_executable_udf_drivers`.
* الدوال المعتمدة على برامج التشغيل غير مدعومة مع تخزين الدوال المعرّفة من قبل المستخدم المكرّر (`ON CLUSTER` و `<user_defined_zookeeper_path>`)، لأن الاستعلام الأصلي وحده هو الذي يُنسخ، لا الـartifacts المتولدة.
* يحتفظ `RESTORE` لدالة معتمدة على برنامج تشغيل ومأخوذ لها نسخ احتياطي بالاستعلام، لكنه لا يعيد تشغيل برنامج التشغيل؛ ويُطبَّق التكوين المُولَّد فعليًا لاحقًا أثناء التعافي بعد إعادة التشغيل.

<div id="example-c-drivers">
  ### مثال على برامج التشغيل C
</div>

تتضمن شجرة المصدر برامج تشغيل تجريبية لإثبات الفكرة ضمن `programs/server/user_defined_executable_function_drivers_config.d/`، تقوم بترجمة جسم دالة بلغة C وتشغيله. وهي أمثلة **ولا تُثبَّت عبر الحزم**:

* `DockerC` - يترجم الشيفرة ويشغّلها داخل حاويات Docker معزولة (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`، بالإضافة إلى حدود الذاكرة/CPU/PID)، ويُنتج UDF من النوع `executable_pool`.
* `GVisorC` - نسخة بديلة تشغّل الملف التنفيذي المترجَم ضمن بيئة التشغيل `runsc` الخاصة بـ [gVisor](https://gvisor.dev/).
* `UnsafeC` - يترجم الشيفرة ويشغّلها مباشرةً على المضيف من دون بيئة عزل. وكما يشير الاسم، فهو لا يوفّر أي عزل، وهو مخصّص فقط للبيئات الموثوقة والاختبار.

هذه البرامج التشغيل التجريبية مخصّصة كنقطة بداية؛ راجع آلية العزل وعزّزها بما يناسب بيئتك قبل إتاحتها للمستخدمين غير الموثوقين.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* [الدوال المعرّفة من قبل المستخدم في ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)