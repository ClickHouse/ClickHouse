---
description: 'توثيق واجهة عميل ClickHouse عبر سطر الأوامر'
sidebar_label: 'عميل ClickHouse'
sidebar_position: 18
slug: /interfaces/client
title: 'عميل ClickHouse'
doc_type: 'مرجع'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يوفّر ClickHouse عميل سطر أوامر أصليًا لتنفيذ استعلامات SQL مباشرةً على خادم ClickHouse.
ويدعم كلاً من الوضع التفاعلي (لتنفيذ الاستعلامات مباشرةً) والوضع الدفعي (للبرمجة النصية والأتمتة).
يمكن عرض نتائج الاستعلام في الطرفية أو تصديرها إلى ملف، مع دعم جميع [تنسيقات](formats.md) إخراج ClickHouse، مثل Pretty وCSV وJSON وغيرها.

يوفّر العميل معلومات فورية عن تنفيذ الاستعلام، بما في ذلك شريط التقدّم وعدد الصفوف المقروءة والبايتات المُعالجة ووقت تنفيذ الاستعلام.
كما يدعم كلاً من [خيارات سطر الأوامر](#command-line-options) و[ملفات التهيئة](#configuration_files).

<div id="install">
  ## التثبيت
</div>

لتنزيل ClickHouse، نفّذ:

```bash
curl https://clickhouse.com/ | sh
```

ولتثبيته أيضًا، شغّل:

```bash
sudo ./clickhouse install
```

راجع [تثبيت ClickHouse](../getting-started/install/install.mdx) للاطلاع على مزيد من خيارات التثبيت.

تتوافق الإصدارات المختلفة من العميل والخادم مع بعضها، ولكن قد لا تتوفر بعض الميزات في إصدارات العميل الأقدم. نوصي باستخدام الإصدار نفسه لكل من العميل والخادم.

<div id="run">
  ## التشغيل
</div>

:::note
إذا كنت قد قمت بتنزيل ClickHouse فقط ولم تثبّته، فاستخدم `./clickhouse client` بدلًا من `clickhouse-client`.
:::

للاتصال بخادم ClickHouse، نفّذ:

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

حدِّد تفاصيل اتصال إضافية حسب الحاجة:

| Option                           | Description                                                                                                                                                                |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | المنفذ الذي يستقبل عليه خادم ClickHouse الاتصالات. المنافذ الافتراضية هي 9440 ‏(TLS) و9000 ‏(من دون TLS). لاحظ أن عميل ClickHouse يستخدم البروتوكول الأصلي وليس HTTP(S). |
| `-s [ --secure ]`                | ما إذا كان سيُستخدم TLS (يُكتشف تلقائيًا عادةً).                                                                                                                           |
| `-u [ --user ] <username>`       | مستخدم قاعدة البيانات المراد الاتصال باسمه. يتم الاتصال افتراضيًا باستخدام المستخدم `default`.                                                                             |
| `--password <password>`          | كلمة مرور مستخدم قاعدة البيانات. يمكنك أيضًا تحديد كلمة مرور الاتصال في ملف التهيئة. إذا لم تحدد كلمة المرور، فسيطلبها العميل.                                             |
| `-c [ --config ] <path-to-file>` | موقع ملف التهيئة الخاص بـ عميل ClickHouse إذا لم يكن موجودًا في أحد المواقع الافتراضية. راجع [ملفات التهيئة](#configuration_files).                                      |
| `--connection <name>`            | اسم تفاصيل الاتصال المُعدّة مسبقًا من [ملف التهيئة](#connection-credentials).                                                                                              |

للاطلاع على قائمة كاملة بخيارات سطر الأوامر، راجع [خيارات سطر الأوامر](#command-line-options).

<div id="connecting-cloud">
  ### الاتصال بـ ClickHouse Cloud
</div>

تتوفر تفاصيل خدمة ClickHouse Cloud الخاصة بك في وحدة تحكم ClickHouse Cloud. حدِّد الخدمة التي تريد الاتصال بها، ثم انقر على **Connect**:

<Image img={cloud_connect_button} size="md" alt="زر Connect لخدمة ClickHouse Cloud" />

<br />

<br />

اختر **Native**، وستظهر التفاصيل مع مثال لأمر `clickhouse-client`:

<Image img={connection_details_native} size="md" alt="تفاصيل اتصال Native TCP لخدمة ClickHouse Cloud" />

<div id="connection-credentials">
  ### تخزين بيانات الاتصال في ملف تهيئة
</div>

يمكنك تخزين تفاصيل الاتصال بخادم ClickHouse واحد أو بعدة خوادم ClickHouse في [ملف تهيئة](#configuration_files).

ويكون التنسيق على النحو التالي:

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

راجع [القسم الخاص بملفات التهيئة](#configuration_files) لمزيد من المعلومات.

:::note
للتركيز على صياغة الاستعلام، تتجاهل بقية الأمثلة تفاصيل الاتصال (`--host`, `--port`، وغيرها). تذكّر إضافتها عند استخدام الأوامر.
:::

<div id="interactive-mode">
  ## الوضع التفاعلي
</div>

<div id="using-interactive-mode">
  ### استخدام الوضع التفاعلي
</div>

لتشغيل ClickHouse في الوضع التفاعلي، ما عليك سوى تنفيذ:

```bash
clickhouse-client
```

سيؤدي هذا إلى فتح حلقة Read-Eval-Print Loop ‏(REPL)، حيث يمكنك البدء بكتابة استعلامات SQL بشكل تفاعلي.
وبعد الاتصال، سيظهر لك سطر أوامر يمكنك من خلاله إدخال الاستعلامات:

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

في الوضع التفاعلي، يكون تنسيق الإخراج الافتراضي هو `PrettyCompact`.
يمكنك تغيير التنسيق في بند `FORMAT` في الاستعلام أو بتحديد خيار سطر الأوامر `--format`.
لاستخدام تنسيق Vertical، يمكنك استخدام `--vertical` أو إضافة `\G` في نهاية الاستعلام.
في هذا التنسيق، تُطبع كل قيمة في سطر منفصل، مما يجعله مناسبًا للجداول العريضة.

في الوضع التفاعلي، يُنفَّذ افتراضيًا كل ما تُدخله عند الضغط على `Enter`.
ولا حاجة إلى فاصلة منقوطة في نهاية الاستعلام.

يمكنك تشغيل العميل باستخدام المعامل `-m, --multiline`.
لإدخال استعلام متعدد الأسطر، أدخل شرطة مائلة عكسية `\` قبل محرف سطر جديد.
بعد الضغط على `Enter`، سيُطلب منك إدخال السطر التالي من الاستعلام.
ولتنفيذ الاستعلام، أنهاِه بفاصلة منقوطة ثم اضغط `Enter`.

يعتمد عميل ClickHouse على `replxx` (على غرار `readline`)، لذا فهو يستخدم اختصارات مألوفة من لوحة المفاتيح ويحتفظ بسجل.
ويُكتب هذا السجل افتراضيًا في `~/.clickhouse-client-history`.

للخروج من العميل، اضغط `Ctrl+D`، أو أدخل أحد الخيارات التالية بدلًا من الاستعلام:

* `exit` أو `exit;`
* `quit` أو `quit;`
* `q` أو `Q` أو `:q`
* `logout` أو `logout;`

<div id="getting-help">
  ### الحصول على المساعدة
</div>

يمكنك الاطلاع على توثيق أي دالة أو محرك جدول أو نوع بيانات أو تنسيق أو إعداد أو أي مكوّن آخر في النظام دون مغادرة العميل. أدخِل `help` متبوعًا باسم (وتعمل أيضًا الصيغ المكافئة `/help` و`man` و`/man`):

```text
help domainWithoutWWW
```

البحث غير حساس لحالة الأحرف ويستعلم من جدول [`system.documentation`](../operations/system-tables/documentation.md). ويُعرَض التوثيق المطابق في الطرفية انطلاقًا من Markdown، مع نص عريض/مائل، وجداول، وكتل شيفرة مميّزة الصياغة. وعندما يكون الاسم مشتركًا بين عدة مكوّنات (مثلًا `file`، وهو دالة ومحرك جدول في الوقت نفسه)، تُعرَض جميعها.

عندما لا توجد مطابقة تامة، يسرد العميل أسماءً مشابهة (مع السماح بالأخطاء الإملائية) والمكوّنات التي يَرِد فيها ذلك اللفظ في توثيقها:

```text
help maxx_threads
```

يؤدي إدخال `help` بمفرده إلى طباعة ملخصٍ قصيرٍ للاستخدام.

<div id="processing-info">
  ### معلومات عن معالجة الاستعلام
</div>

عند معالجة استعلام، يعرض العميل ما يلي:

1. Progress، ويُحدَّث افتراضيًا بما لا يزيد على 10 مرات في الثانية.
   في الاستعلامات السريعة، قد لا يتوفر وقت كافٍ لعرض التقدم.
2. الاستعلام المنسّق بعد التحليل، لأغراض تصحيح الأخطاء.
3. النتيجة بالتنسيق المحدد.
4. عدد الأسطر في النتيجة، والوقت المنقضي، ومتوسط سرعة معالجة الاستعلام.
   تشير جميع كميات البيانات إلى بيانات غير مضغوطة.

يمكنك إلغاء استعلام طويل بالضغط على `Ctrl+C`.
ومع ذلك، سيظل عليك الانتظار قليلًا حتى يُلغي الخادم الطلب.
ولا يمكن إلغاء الاستعلام في بعض المراحل.
إذا لم تنتظر وضغطت على `Ctrl+C` مرةً ثانية، فسيُغلق العميل.

يتيح عميل ClickHouse تمرير بيانات خارجية (جداول مؤقتة خارجية) لاستخدامها في الاستعلام.
لمزيد من المعلومات، راجع قسم [البيانات الخارجية لمعالجة الاستعلام](../engines/table-engines/special/external-data.md).

<div id="cli_aliases">
  ### الأسماء المستعارة
</div>

يمكنك استخدام الأسماء المستعارة التالية داخل REPL:

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - أعد تنفيذ آخر استعلام

<div id="keyboard_shortcuts">
  ### اختصارات لوحة المفاتيح
</div>

* `Alt (Option) + Shift + e` - افتح المحرر مع الاستعلام الحالي. يمكن تحديد المحرر المراد استخدامه عبر متغير البيئة `EDITOR`. ويُستخدم `vim` افتراضيًا.
* `Alt (Option) + #` - أضف تعليقًا إلى السطر.
* `Ctrl + r` - بحث تقريبي في السجل.

تتوفر القائمة الكاملة بجميع اختصارات لوحة المفاتيح المتاحة في [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
لضبط عمل مفتاح Meta ‏(Option) بشكل صحيح على MacOS:

iTerm2: انتقل إلى Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key ثم انقر على Esc+
:::

<div id="batch-mode">
  ## الوضع الدفعي
</div>

<div id="using-batch-mode">
  ### استخدام الوضع الدفعي
</div>

بدلًا من استخدام عميل ClickHouse بشكل تفاعلي، يمكنك تشغيله في الوضع الدفعي.
في الوضع الدفعي، ينفّذ ClickHouse استعلامًا واحدًا ثم يخرج فورًا - من دون موجّه تفاعلي أو حلقة تكرار.

يمكنك تحديد استعلام واحد كما يلي:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

يمكنك أيضًا استخدام الخيار `--query` في سطر الأوامر:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

يمكنك تمرير استعلام عبر `stdin`:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

بافتراض وجود جدول `messages`، يمكنك أيضًا إدراج البيانات عبر سطر الأوامر:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

عند تحديد `--query`، يُضاف أي إدخال إلى الطلب بعد محرف تغذية سطر.

<div id="cloud-example">
  ### إدراج ملف CSV في خدمة ClickHouse بعيدة
</div>

يوضح هذا المثال إدراج ملف CSV لمجموعة بيانات نموذجية، `cell_towers.csv`، في جدول موجود باسم `cell_towers` ضمن قاعدة البيانات `default`:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### أمثلة على إدراج البيانات من سطر الأوامر
</div>

هناك عدة طرق لإدراج البيانات من سطر الأوامر.
يوضح المثال أدناه كيفية إدراج صفّين من بيانات CSV في جدول ClickHouse باستخدام الوضع الدفعي:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

في المثال أدناه، يبدأ `cat <<_EOF` كتلة Heredoc تقرأ كل شيء حتى يظهر `_EOF` مرة أخرى، ثم تطبعه:

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

في المثال أدناه، تُرسَل محتويات file.csv إلى stdout باستخدام `cat`، ثم تُمرَّر عبر أنبوب إلى `clickhouse-client` كمدخلات:

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

في الوضع الدفعي، يكون [تنسيق](formats.md) البيانات الافتراضي هو `TabSeparated`.
يمكنك تعيين التنسيق في عبارة `FORMAT` في الاستعلام كما هو موضح في المثال أعلاه.

<div id="cli-queries-with-parameters">
  ## الاستعلامات ذات المعلمات
</div>

يمكنك تحديد معلمات في الاستعلام وتمرير قيم إليها باستخدام خيارات سطر الأوامر.
وهذا يجنّبك تنسيق الاستعلام بقيم ديناميكية محددة من جهة العميل.
على سبيل المثال:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

يمكن أيضًا ضبط المعلمات من داخل [جلسة تفاعلية](#interactive-mode):

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### صياغة الاستعلام
</div>

في الاستعلام، ضع القيم التي تريد تعبئتها باستخدام معلمات سطر الأوامر بين أقواس معقوفة بالتنسيق التالي:

```sql
{<name>:<data type>}
```

| المعلمة     | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`      | معرّف العنصر النائب. خيار سطر الأوامر المقابل هو `--param_<name> = value`.                                                                                                                                                                                                                                                                                                                                                                             |
| `data type` | [نوع البيانات](../sql-reference/data-types/index.md) للمعلمة. <br /><br />على سبيل المثال، يمكن أن يكون لبنية بيانات مثل `(integer, ('string', integer))` نوع البيانات `Tuple(UInt8, Tuple(String, UInt8))` (يمكنك أيضًا استخدام أنواع [integer](../sql-reference/data-types/int-uint.md) أخرى). <br /><br />كما يمكن أيضًا تمرير اسم الجدول واسم قاعدة البيانات وأسماء الأعمدة كمعلمات، وفي هذه الحالة ستحتاج إلى استخدام `Identifier` كنوع البيانات. |

<div id="cli-queries-with-parameters-examples">
  ### أمثلة
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## توليد SQL بالذكاء الاصطناعي
</div>

يتضمن عميل ClickHouse مساعدًا مدمجًا بالذكاء الاصطناعي لإنشاء استعلامات SQL من أوصاف مكتوبة بلغة طبيعية. تساعد هذه الميزة المستخدمين على كتابة استعلامات معقدة دون الحاجة إلى معرفة متعمقة بـ SQL.

تعمل المساعدة بالذكاء الاصطناعي مباشرةً إذا كان لديك متغير البيئة `OPENAI_API_KEY` أو `ANTHROPIC_API_KEY` معيّنًا. ولمزيد من التهيئة المتقدمة، راجع قسم [التهيئة](#ai-sql-generation-configuration).

<div id="ai-sql-generation-usage">
  ### طريقة الاستخدام
</div>

لاستخدام ميزة إنشاء استعلامات SQL بالذكاء الاصطناعي، أضِف `??` في بداية استعلامك المكتوب بلغة طبيعية:

```bash
:) ?? show all users who made purchases in the last 30 days
```

سيقوم الذكاء الاصطناعي بما يلي:

1. استكشاف مخطط قاعدة بياناتك تلقائيًا
2. إنشاء استعلام SQL مناسب استنادًا إلى الجداول والأعمدة المكتشفة
3. تنفيذ الاستعلام الذي تم إنشاؤه فورًا

<div id="ai-sql-generation-example">
  ### مثال
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### التهيئة
</div>

يتطلب توليد استعلامات SQL بالذكاء الاصطناعي تهيئة موفّر ذكاء اصطناعي في ملف الإعدادات لـ ClickHouse Client. يمكنك استخدام OpenAI أو Anthropic أو أي خدمة واجهة برمجة تطبيقات متوافقة مع OpenAI.

<div id="ai-sql-generation-fallback">
  #### الاعتماد على متغيرات البيئة كخيار احتياطي
</div>

إذا لم يتم تحديد أي إعداد للذكاء الاصطناعي في ملف الإعدادات، فسيحاول ClickHouse Client تلقائيًا استخدام متغيرات البيئة:

1. يتحقق أولًا من متغير البيئة `OPENAI_API_KEY`
2. إذا لم يعثر عليه، يتحقق من متغير البيئة `ANTHROPIC_API_KEY`
3. إذا لم يعثر على أيٍّ منهما، فسيتم تعطيل ميزات الذكاء الاصطناعي

يتيح ذلك إعدادًا سريعًا من دون الحاجة إلى ملفات إعدادات:

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### ملف الإعدادات
</div>

لمزيد من التحكم في إعدادات الذكاء الاصطناعي، اضبطها في ملف إعدادات ClickHouse Client الموجود في:

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (أو `~/.config/clickhouse/config.xml` إذا لم يتم تعيين `XDG_CONFIG_HOME`) (بتنسيق XML)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (أو `~/.config/clickhouse/config.yaml` إذا لم يتم تعيين `XDG_CONFIG_HOME`) (بتنسيق YAML)
* `~/.clickhouse-client/config.xml` (بتنسيق XML، الموقع القديم)
* `~/.clickhouse-client/config.yaml` (بتنسيق YAML، الموقع القديم)
* أو حدِّد موقعًا مخصصًا باستخدام `--config-file`

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- مطلوب: مفتاح واجهة برمجة تطبيقات الخاص بك (أو عيّنه عبر متغير البيئة) -->
            <api_key>your-api-key-here</api_key>

            <!-- مطلوب: نوع المزوّد (openai, anthropic) -->
            <provider>openai</provider>

            <!-- النموذج المراد استخدامه (تختلف القيم الافتراضية حسب المزوّد) -->
            <model>gpt-4o</model>

            <!-- اختياري: نقطة نهاية واجهة برمجة تطبيقات مخصصة للخدمات المتوافقة مع OpenAI -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- إعدادات استكشاف المخطط -->
            <enable_schema_access>true</enable_schema_access>

            <!-- معلمات التوليد -->
            <!-- اختياري: لا يتم إرسال temperature إلى النموذج إلا عند تعيينه هنا.
                 ويُهمَل افتراضيًا لأن بعض النماذج ترفض هذه المعلمة. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- اختياري: موجّه نظام مخصص -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # مطلوب: مفتاح واجهة برمجة تطبيقات الخاص بك (أو عيّنه عبر متغير البيئة)
      api_key: your-api-key-here

      # مطلوب: نوع المزوّد (openai, anthropic)
      provider: openai

      # النموذج المراد استخدامه
      model: gpt-4o

      # اختياري: نقطة نهاية واجهة برمجة تطبيقات مخصصة للخدمات المتوافقة مع OpenAI
      # base_url: https://openrouter.ai/api

      # تفعيل الوصول إلى المخطط - يتيح للذكاء الاصطناعي الاستعلام عن معلومات قاعدة البيانات/الجدول
      enable_schema_access: true

      # معلمات التوليد
      # لا يتم إرسال temperature إلى النموذج إلا عند تعيينه هنا؛ ويُهمَل افتراضيًا
      # لأن بعض النماذج ترفض هذه المعلمة.
      # temperature: 0.0    # يتحكم في العشوائية (0.0 = deterministic)
      max_tokens: 1000      # الحد الأقصى لطول الاستجابة
      timeout_seconds: 30   # مهلة الطلب
      max_steps: 10         # الحد الأقصى لخطوات استكشاف المخطط

      # اختياري: موجّه نظام مخصص
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**استخدام واجهات برمجة التطبيقات المتوافقة مع OpenAI (مثل OpenRouter):**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**أمثلة على الحد الأدنى من التهيئة:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### المعلمات
</div>

<details>
  <summary>المعلمات المطلوبة</summary>

  * `api_key` - مفتاح واجهة برمجة تطبيقات الخاص بك لخدمة الذكاء الاصطناعي. يمكن الاستغناء عنه إذا كان مُعيَّنًا عبر متغير بيئة:
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * ملاحظة: تكون الأولوية لمفتاح واجهة برمجة تطبيقات في ملف الإعدادات على متغير البيئة
  * `provider` - موفّر الذكاء الاصطناعي: `openai` أو `anthropic`
    * إذا لم يتم تحديده، فسيُستخدم بديل تلقائي استنادًا إلى متغيرات البيئة المتاحة
</details>

<details>
  <summary>إعدادات النموذج</summary>

  * `model` - النموذج المطلوب استخدامه (الافتراضي: يختلف حسب الموفّر)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo`، إلخ.
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`، إلخ.
    * OpenRouter: استخدم تسمية النماذج الخاصة به مثل `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>إعدادات الاتصال</summary>

  * `base_url` - نقطة نهاية واجهة برمجة تطبيقات مخصّصة للخدمات المتوافقة مع OpenAI (اختياري)
  * `timeout_seconds` - مهلة الطلب بالثواني (الافتراضي: `30`)
</details>

<details>
  <summary>استكشاف المخططات</summary>

  * `enable_schema_access` - السماح للذكاء الاصطناعي باستكشاف مخططات قاعدة البيانات (الافتراضي: `true`)
  * `max_steps` - الحد الأقصى لخطوات استدعاء الأدوات لاستكشاف المخططات (الافتراضي: `10`)
</details>

<details>
  <summary>معلمات التوليد</summary>

  * `temperature` - يتحكم في مستوى العشوائية: 0.0 = حتمي، 1.0 = إبداعي. لا يتم تضمينه افتراضيًا، ولا يُرسل إلى النموذج إلا عند ضبطه صراحةً، لأن بعض النماذج ترفض هذه المعلمة.
  * `max_tokens` - الحد الأقصى لطول الاستجابة بالرموز (الافتراضي: `1000`)
  * `system_prompt` - تعليمات مخصّصة للذكاء الاصطناعي (اختياري)
</details>

<div id="ai-sql-generation-how-it-works">
  ### كيف يعمل
</div>

يستخدم مولّد SQL المدعوم بالذكاء الاصطناعي عملية من عدة خطوات:

<VerticalStepper headerLevel="list">
  1. **اكتشاف المخطط**

  يستخدم الذكاء الاصطناعي أدوات مدمجة لاستكشاف قاعدة بياناتك:

  * يسرد قواعد البيانات المتاحة
  * يكتشف الجداول داخل قواعد البيانات ذات الصلة
  * يفحص بُنى الجداول من خلال عبارات `CREATE TABLE`

  2. **توليد الاستعلامات**

  استنادًا إلى المخطط المكتشف، يولّد الذكاء الاصطناعي SQL بحيث:

  * يطابق طلبك المكتوب بلغة طبيعية
  * يستخدم أسماء الجداول والأعمدة الصحيحة
  * يطبّق عمليات الربط والتجميع المناسبة

  3. **التنفيذ**

  يُنفَّذ SQL المُولَّد تلقائيًا وتُعرَض النتائج
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### القيود
</div>

* يتطلب اتصالًا نشطًا بالإنترنت
* يخضع استخدام واجهة برمجة التطبيقات لقيود على المعدل وتكاليف يفرضها مزوّد الذكاء الاصطناعي
* قد تتطلب الاستعلامات المعقدة عدة تنقيحات
* يملك الذكاء الاصطناعي صلاحية وصول للقراءة فقط إلى معلومات المخطط، وليس إلى البيانات الفعلية

<div id="ai-sql-generation-security">
  ### الأمان
</div>

* لا تُرسَل مفاتيح واجهة برمجة التطبيقات مطلقًا إلى خوادم ClickHouse
* لا يرى الذكاء الاصطناعي إلا معلومات المخطط (أسماء الجداول/الأعمدة والأنواع)، وليس البيانات الفعلية
* تلتزم جميع الاستعلامات المُولَّدة بأذونات قاعدة البيانات الحالية لديك

<div id="connection_string">
  ## سلسلة الاتصال
</div>

<div id="connection-string-usage">
  ### الاستخدام
</div>

يدعم ClickHouse Client أيضًا الاتصال بخادم ClickHouse باستخدام سلسلة اتصال مماثلة لتلك المستخدمة في [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/)، و[PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)، و[MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). وتكون صيغتها كما يلي:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| المكوّن (كلها اختيارية) | الوصف                                                                                                                                       | الافتراضي        |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `user`                  | اسم مستخدم قاعدة البيانات.                                                                                                                  | `default`        |
| `password`              | كلمة مرور مستخدم قاعدة البيانات. إذا تم تحديد `:` وكانت كلمة المرور فارغة، فسيطلب العميل إدخال كلمة مرور المستخدم.                          | -                |
| `hosts_and_ports`       | قائمة بالمضيفين والمنافذ الاختيارية `host[:port] [, host:[port]], ...`.                                                                     | `localhost:9000` |
| `database`              | اسم قاعدة البيانات.                                                                                                                         | `default`        |
| `query_parameters`      | قائمة بأزواج المفتاح والقيمة `param1=value1[,&param2=value2], ...`. لا تتطلب بعض المَعلمات قيمة. أسماء المَعلمات والقيم حساسة لحالة الأحرف. | -                |

<div id="connection-string-notes">
  ### ملاحظات
</div>

إذا جرى تحديد اسم المستخدم أو كلمة المرور أو قاعدة البيانات ضمن سلسلة الاتصال، فلا يمكن تحديدها أيضًا باستخدام `--user` أو `--password` أو `--database` (والعكس صحيح).

يمكن أن يكون جزء المضيف إما اسم مضيف أو عنوان IPv4 أو IPv6.
يجب وضع عناوين IPv6 بين `[]`:

```text
clickhouse://[2001:db8::1234]
```

يمكن أن تحتوي سلاسل الاتصال على عدة مضيفات.
سيحاول ClickHouse Client الاتصال بهذه المضيفات بالترتيب (من اليسار إلى اليمين).
وبعد إنشاء الاتصال، لن تُجرى أي محاولة للاتصال بالمضيفات المتبقية.

يجب تحديد سلسلة الاتصال بوصفها الوسيط الأول لـ `clickHouse-client`.
يمكن استخدام سلسلة الاتصال مع أي عدد من [خيارات سطر الأوامر](#command-line-options) الأخرى، باستثناء `--host` و`--port`.

المفاتيح التالية مسموح بها لـ `query_parameters`:

| Key               | Description                                                                                                                |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `secure` (or `s`) | إذا تم تحديده، فسيتصل العميل بالخادم عبر اتصال آمن (TLS). راجع `--secure` ضمن [خيارات سطر الأوامر](#command-line-options). |

**الترميز بالنسبة المئوية**

يجب أن تكون الأحرف غير التابعة لـ ASCII الأمريكي، والمسافات، والأحرف الخاصة في المعلمات التالية [مرمَّزة بالنسبة المئوية](https://en.wikipedia.org/wiki/URL_encoding):

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### أمثلة
</div>

اتصل بـ`localhost` عبر المنفذ 9000 ونفِّذ الاستعلام `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

اتصل بـ `localhost` باسم المستخدم `john` وكلمة المرور `secret`، والمضيف `127.0.0.1`، والمنفذ `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

اتصل بـ `localhost` باستخدام المستخدم `default`، والمضيف ذي عنوان IPv6 `[::1]`، والمنفذ `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

اتصل بـ`localhost` عبر المنفذ 9000 في وضع متعدد الأسطر.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

اتصل بـ `localhost` عبر المنفذ 9000 باستخدام المستخدم `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

اتصل بـ `localhost` على المنفذ 9000، واستخدم قاعدة البيانات `my_database` كقاعدة بيانات افتراضية.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

اتصل بـ `localhost` على المنفذ 9000، واجعل قاعدة البيانات `my_database` المحددة في سلسلة الاتصال هي القاعدة الافتراضية، مع استخدام اتصال آمن عبر المعلمة المختصرة `s`.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

اتصل بالمضيف الافتراضي باستخدام المنفذ الافتراضي، والمستخدم default، وقاعدة البيانات الافتراضية.

```bash
clickhouse-client clickhouse:
```

اتصل بالمضيف الافتراضي باستخدام المنفذ الافتراضي، باسم المستخدم `my_user` ومن دون كلمة مرور.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

اتصل بـ `localhost` باستخدام البريد الإلكتروني كاسم المستخدم. تُرمَّز علامة `@` بترميز النسبة المئوية إلى `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

اتصل بأحد المضيفين: `192.168.1.15`، `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## تنسيق معرّف الاستعلام
</div>

في الوضع التفاعلي، يعرض ClickHouse Client معرّف الاستعلام لكل استعلام. وبشكل افتراضي، يُنسَّق المعرّف كما يلي:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

يمكن تحديد تنسيق مخصّص في ملف إعدادات ضمن وسم `query_id_formats`. ويُستبدل العنصر النائب `{query_id}` في سلسلة التنسيق بمعرّف الاستعلام. ويُسمح باستخدام عدة سلاسل تنسيق داخل الوسم.
يمكن استخدام هذه الميزة لإنشاء عناوين URL لتسهيل تحليل أداء الاستعلامات.

**مثال**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

باستخدام التهيئة أعلاه، يُعرَض معرّف الاستعلام بالتنسيق التالي:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## ملفات الإعدادات
</div>

يستخدم ClickHouse Client أول ملف موجود من بين الملفات التالية:

* ملف يُحدَّد باستخدام المعامل `-c [ -C, --config, --config-file ]`.
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (أو `~/.config/clickhouse/config.[xml|yaml|yml]` إذا لم يتم تعيين `XDG_CONFIG_HOME`)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

راجع ملف الإعدادات النموذجي في مستودع ClickHouse: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## خيارات متغيرات البيئة
</div>

يمكن ضبط اسم المستخدم وكلمة المرور والمضيف عبر متغيرات البيئة `CLICKHOUSE_USER` و`CLICKHOUSE_PASSWORD` و`CLICKHOUSE_HOST`.
تتقدّم وسيطات سطر الأوامر `--user` و`--password` و`--host`، أو [سلسلة الاتصال](#connection_string) (إن تم تحديدها)، على متغيرات البيئة.

<div id="command-line-options">
  ## خيارات سطر الأوامر
</div>

يمكن تحديد جميع خيارات سطر الأوامر مباشرةً في سطر الأوامر، أو تعيينها كقيم افتراضية في [ملف الإعدادات](#configuration_files).

<div id="command-line-options-general">
  ### الخيارات العامة
</div>

| Option                                              | Description                                                                                                              | Default                      |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | موقع ملف التهيئة الخاص بالعميل إذا لم يكن موجودًا في أحد المواقع الافتراضية. راجع [ملفات التهيئة](#configuration_files). | -                            |
| `--help`                                            | اطبع ملخص الاستخدام ثم اخرج. استخدمه مع `--verbose` لعرض جميع الخيارات المتاحة، بما في ذلك إعدادات الاستعلام.            | -                            |
| `--history_file <path-to-file>`                     | مسار ملف يحتوي على سجل الأوامر.                                                                                          | -                            |
| `--history_max_entries`                             | الحد الأقصى لعدد الإدخالات في ملف السجل.                                                                                 | `1000000` (مليون واحد)       |
| `--prompt <prompt>`                                 | حدِّد موجّهًا مخصصًا.                                                                                                    | `display_name` الخاص بالخادم |
| `--verbose`                                         | زِد من مستوى تفصيل المخرجات.                                                                                             | -                            |
| `-V [ --version ]`                                  | اطبع الإصدار ثم اخرج.                                                                                                    | -                            |

<div id="command-line-options-connection">
  ### خيارات الاتصال
</div>

| Option                               | Description                                                                                                                                                                                                                                                                                                                                                              | Default                                                                                                                     |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `--connection <name>`                | اسم تفاصيل الاتصال المُهيّأة مسبقًا من ملف الإعدادات. راجع [بيانات اعتماد الاتصال](#connection-credentials).                                                                                                                                                                                                                                                             | -                                                                                                                           |
| `-d [ --database ] <database>`       | حدِّد قاعدة البيانات الافتراضية لهذا الاتصال.                                                                                                                                                                                                                                                                                                                            | قاعدة البيانات الحالية من إعدادات الخادم (`default` افتراضيًا)                                                              |
| `-h [ --host ] <host>`               | اسم مضيف خادم ClickHouse المراد الاتصال به. ويمكن أن يكون اسم مضيف أو عنوان IPv4 أو IPv6. كما يمكن تمرير عدة مضيفين باستخدام عدة وسائط.                                                                                                                                                                                                                                  | `localhost`                                                                                                                 |
| `--jwt <value>`                      | استخدم JSON Web Token ‏(JWT) للمصادقة. <br /><br />تفويض JWT على الخادم متاح فقط في ClickHouse Cloud.                                                                                                                                                                                                                                                                    | -                                                                                                                           |
| `login`                              | يستدعي OAuth flow لمنحة الجهاز للمصادقة عبر موفّر هوية. <br /><br />بالنسبة إلى مضيفي ClickHouse Cloud، يتم استنتاج متغيرات OAuth تلقائيًا، وإلا فيجب توفيرها باستخدام `--oauth-url` و`--oauth-client-id` و`--oauth-audience`.                                                                                                                                           | -                                                                                                                           |
| `--no-warnings`                      | عطِّل عرض التحذيرات من `system.warnings` عند اتصال العميل بالخادم.                                                                                                                                                                                                                                                                                                       | -                                                                                                                           |
| `--no-server-client-version-message` | أخفِ رسالة عدم تطابق إصدار الخادم والعميل عند اتصال العميل بالخادم.                                                                                                                                                                                                                                                                                                      | -                                                                                                                           |
| `--password <password>`              | كلمة مرور مستخدم قاعدة البيانات. ويمكنك أيضًا تحديد كلمة مرور الاتصال في ملف الإعدادات. وإذا لم تحدد كلمة المرور، فسيطلبها العميل.                                                                                                                                                                                                                                       | -                                                                                                                           |
| `--port <port>`                      | المنفذ الذي يقبل الخادم الاتصالات عليه. المنافذ الافتراضية هي 9440 ‏(TLS) و9000 (من دون TLS). <br /><br />ملاحظة: يستخدم العميل البروتوكول الأصلي وليس HTTP(S).                                                                                                                                                                                                          | `9440` إذا تم تحديد `--secure`، وإلا `9000`. ويكون الافتراضي دائمًا `9440` إذا كان اسم المضيف ينتهي بـ `.clickhouse.cloud`. |
| `-s [ --secure ]`                    | ما إذا كان سيتم استخدام TLS. <br /><br />يتم تفعيله تلقائيًا عند الاتصال بالمنفذ 9440 (المنفذ الآمن الافتراضي) أو بـ ClickHouse Cloud. <br /><br />قد تحتاج إلى تهيئة شهادات CA في [ملف الإعدادات](#configuration_files). وإعدادات التهيئة المتاحة هي نفسها المستخدمة في [تهيئة TLS على جانب الخادم](../operations/server-configuration-parameters/settings.md#openssl). | يُفعَّل تلقائيًا عند الاتصال بالمنفذ 9440 أو بـ ClickHouse Cloud                                                            |
| `--ssh-key-file <path-to-file>`      | ملف يحتوي على المفتاح الخاص لـ SSH للمصادقة مع الخادم.                                                                                                                                                                                                                                                                                                                   | -                                                                                                                           |
| `--ssh-key-passphrase <value>`       | عبارة المرور للمفتاح الخاص لـ SSH المحدد في `--ssh-key-file`.                                                                                                                                                                                                                                                                                                            | -                                                                                                                           |
| `--tls-sni-override <server name>`   | عند استخدام TLS، اسم الخادم (SNI) الذي سيتم تمريره أثناء المصافحة.                                                                                                                                                                                                                                                                                                       | المضيف الممرَّر عبر `-h` أو `--host`.                                                                                       |
| `-u [ --user ] <username>`           | مستخدم قاعدة البيانات الذي سيتم الاتصال باسمه.                                                                                                                                                                                                                                                                                                                           | `default`                                                                                                                   |

:::note
بدلًا من الخيارات `--host` و`--port` و`--user` و`--password`، يدعم العميل أيضًا [سلاسل الاتصال](#connection_string).
:::

<div id="command-line-options-query">
  ### خيارات الاستعلام
</div>

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | قيمة الاستبدال لمعلمة في [استعلام يحتوي على معلمات](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `-q [ --query ] <query>`        | الاستعلام المراد تشغيله في وضع الدُفعات. يمكن تحديده عدة مرات (`--query "SELECT 1" --query "SELECT 2"`) أو مرة واحدة مع عدة استعلامات مفصولة بفواصل منقوطة (`--query "SELECT 1; SELECT 2;"`). في الحالة الأخيرة، يجب فصل استعلامات `INSERT` ذات التنسيقات الأخرى غير `VALUES` بأسطر فارغة. <br /><br />يمكن أيضًا تحديد استعلام واحد بدون معلمة: `clickhouse-client "SELECT 1"` <br /><br />لا يمكن استخدامه مع `--queries-file` في الوقت نفسه.                                                                                                                                                               |
| `--queries-file <path-to-file>` | مسار ملف يحتوي على استعلامات. يمكن تحديد `--queries-file` عدة مرات، على سبيل المثال: `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />لا يمكن استخدامه مع `--query` في الوقت نفسه.                                                                                                                                                                                                                                                                                                                                                                                                      |
| `-m [ --multiline ]`            | عند تحديده، يسمح بالاستعلامات متعددة الأسطر (أي لا يُرسَل الاستعلام عند الضغط على Enter). لن تُرسَل الاستعلامات إلا إذا انتهت بفاصلة منقوطة.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `--inline-insert-data`          | أرسل `INSERT ... VALUES` (والتنسيقات المضمنة الأخرى) كما هي ضمن نص الاستعلام بدلًا من تحويل البيانات إلى blocks بتنسيق Native format. يقوم الخادم بتحليل البيانات المضمنة بنفسه، مما يجنّب round-trip لإرسال بنية الجدول والقيم الافتراضية للأعمدة مرة أخرى إلى العميل. وقد يؤدي ذلك إلى تحسين الأداء عند وجود عدد كبير من عمليات `INSERT` الصغيرة عبر native protocol. يضبط تلقائيًا [`send_table_structure_on_insert_with_inline_data`](/ar/operations/settings/settings#send_table_structure_on_insert_with_inline_data) إلى `0`. لا يمكن دمجه مع البيانات المضمنة والبيانات الخارجية (من stdin أو `INFILE`). |

<div id="command-line-options-query-settings">
  ### إعدادات الاستعلام
</div>

يمكن تحديد إعدادات الاستعلام بوصفها خيارات في سطر الأوامر ضمن العميل، على سبيل المثال:

```bash
$ clickhouse-client --max_threads 1
```

راجع [الإعدادات](../operations/settings/settings.md) للاطلاع على قائمة بالإعدادات.

<div id="command-line-options-formatting">
  ### خيارات التنسيق
</div>

| Option                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Default                                                           |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `-f [ --format ] <format>`        | استخدم التنسيق المحدد لإخراج النتيجة. <br /><br />راجع [تنسيقات بيانات الإدخال والإخراج](formats.md) للحصول على قائمة بالتنسيقات المدعومة.                                                                                                                                                                                                                                                                                                                                          | `TabSeparated`                                                    |
| `--pager <command>`               | مرّر جميع المخرجات إلى هذا الأمر. وعادةً ما يكون `less` (مثل `less -S` لعرض مجموعات النتائج العريضة) أو ما شابه.                                                                                                                                                                                                                                                                                                                                                                    | -                                                                 |
| `-E [ --vertical ]`               | استخدم [التنسيق العمودي](/ar/interfaces/formats/Vertical) لإخراج النتيجة. وهذا مماثل لـ `–-format Vertical`. في هذا التنسيق، تُطبع كل قيمة في سطر منفصل، مما يفيد عند عرض الجداول العريضة.                                                                                                                                                                                                                                                                                             | -                                                                 |
| `--echo [ <bool> ]`               | اطبع كل استعلام قبل تنفيذه. يقبل قيمة منطقية اختيارية.                                                                                                                                                                                                                                                                                                                                                                                                                              | `true` في الوضع التفاعلي، و`false` في الوضع غير التفاعلي (الدفعي) |
| `--echo-formatted [ <bool> ]`     | نسّق الاستعلامات المطبوعة. يقبل قيمة منطقية اختيارية.                                                                                                                                                                                                                                                                                                                                                                                                                               | `true` في الوضع التفاعلي، و`false` في الوضع غير التفاعلي (الدفعي) |
| `--echo-query-id [ <bool> ]`      | اطبع معرّف الاستعلام قبل تنفيذه. يقبل قيمة منطقية اختيارية.                                                                                                                                                                                                                                                                                                                                                                                                                         | `true` في الوضع التفاعلي، و`false` في الوضع غير التفاعلي (الدفعي) |
| `--echo-query-separator <string>` | اطبع هذا الفاصل قبل الاستعلام المطبوعة نسخته المنسّقة (يتطلب `--echo-formatted`)، مما يسهّل التمييز بين الاستعلام الذي أدخلته ونسخته المعاد تنسيقها.                                                                                                                                                                                                                                                                                                                                | فارغ (معطّل)                                                      |
| `--highlight [ --hilite ] <bool>` | بدّل تمييز بناء الجملة في موجّه الأوامر والاستعلامات المطبوعة.                                                                                                                                                                                                                                                                                                                                                                                                                      | `true`                                                            |
| `--hints <bool>`                  | اعرض تلميحات الإكمال التلقائي أثناء الكتابة (نص &quot;شبح&quot; مضمن) لأفضل اقتراح مطابق عندما يكون المؤشر في نهاية الإدخال. تنقّل بين التلميحات باستخدام Up/Down (أو Ctrl-Up/Ctrl-Down)؛ اقبل التلميح المضمن باستخدام Tab أو Right؛ لا يقبل `Enter` أي تلميح إلا بعد تحديده صراحةً، وإلا فإنه ينفّذ الاستعلام؛ كما يفتح `Tab` أيضًا قائمة الإكمال التقليدية. يتطلب `--highlight` (لأن التلميحات تحتاج إلى ألوان) وآلية الاقتراحات (لذلك فإن `--disable_suggestion` يعطّلها أيضًا). | `true`                                                            |

<div id="command-line-options-execution-details">
  ### تفاصيل التنفيذ
</div>

| Option                           | Description                                                                                                                                                                                                                                                                                                                                                                                           | Default                                                        |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `--chime [N]`                    | اكتب محرف التحكم `BEL` إلى `stderr` عند انتهاء الاستعلام — سواء نجح أو أخفق — بعد تشغيله لمدة لا تقل عن `N` ثانية. لا يُرسَل هذا المحرف إلا إذا كان `stderr` متصلًا بطرفية (TTY)؛ إذ إن إعادة توجيه `stderr` (مثل `2>err.log`) تمنع إرساله، بينما إعادة توجيه `stdout` (مثل `> result.tsv`) لا تمنع ذلك. يؤدي تمرير `--chime` بدون قيمة إلى استخدام قيمة العتبة الافتراضية. اضبط `--chime 0` لتعطيله. | `5` ثوانٍ                                                      |
| `--enable-progress-table-toggle` | فعّل إمكانية تبديل جدول التقدم بالضغط على مفتاح التحكم (Space). ينطبق هذا فقط في الوضع التفاعلي عند تمكين طباعة جدول التقدم.                                                                                                                                                                                                                                                                          | `مُمكّن`                                                       |
| `--hardware-utilization`         | اطبع معلومات استخدام العتاد في شريط التقدم.                                                                                                                                                                                                                                                                                                                                                           | -                                                              |
| `--memory-usage`                 | إذا تم تحديده، فاطبع استخدام الذاكرة إلى `stderr` في الوضع غير التفاعلي. <br /><br />القيم الممكنة: <br />• `none` - لا تطبع استخدام الذاكرة <br />• `default` - اطبع عدد البايتات <br />• `readable` - اطبع استخدام الذاكرة بتنسيق مقروء للبشر                                                                                                                                                       | -                                                              |
| `--print-profile-events`         | اطبع الحزم `ProfileEvents`.                                                                                                                                                                                                                                                                                                                                                                           | -                                                              |
| `--progress`                     | اطبع تقدّم تنفيذ الاستعلام. <br /><br />القيم الممكنة: <br />• `tty\|on\|1\|true\|yes` - يُخرج إلى الطرفية في الوضع التفاعلي <br />• `err` - يُخرج إلى `stderr` في الوضع غير التفاعلي <br />• `off\|0\|false\|no` - يعطّل طباعة التقدم                                                                                                                                                                | `tty` في الوضع التفاعلي، و`off` في الوضع غير التفاعلي (الدفعي) |
| `--progress-table`               | اطبع جدول تقدم يعرض مقاييس متغيرة أثناء تنفيذ الاستعلام. <br /><br />القيم الممكنة: <br />• `tty\|on\|1\|true\|yes` - يُخرج إلى الطرفية في الوضع التفاعلي <br />• `err` - يُخرج إلى `stderr` في الوضع غير التفاعلي <br />• `off\|0\|false\|no` - يعطّل جدول التقدم                                                                                                                                    | `tty` في الوضع التفاعلي، و`off` في الوضع غير التفاعلي (الدفعي) |
| `--stacktrace`                   | اطبع تتبعات مكدس الاستدعاءات الخاصة بالاستثناءات.                                                                                                                                                                                                                                                                                                                                                     | -                                                              |
| `-t [ --time ]`                  | اطبع وقت تنفيذ الاستعلام إلى `stderr` في الوضع غير التفاعلي (لاختبارات الأداء).                                                                                                                                                                                                                                                                                                                       | -                                                              |