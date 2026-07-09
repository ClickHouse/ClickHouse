---
description: 'تشرح هذه الصفحة كيفية تهيئة خادم ClickHouse باستخدام ملفات تكوين
  بصيغة XML أو YAML.'
sidebar_label: 'ملفات التكوين'
sidebar_position: 50
slug: /operations/configuration-files
title: 'ملفات التكوين'
doc_type: 'guide'
---

:::note
ملفات تعريف الإعدادات وملفات التكوين المستندة إلى XML غير مدعومة في ClickHouse Cloud. لذلك، لن تجد في ClickHouse Cloud ملف `config.xml`. وبدلًا من ذلك، يجب استخدام أوامر SQL لإدارة الإعدادات من خلال ملفات تعريف الإعدادات.

لمزيد من التفاصيل، راجع [&quot;تهيئة الإعدادات&quot;](/ar/manage/settings)
:::

يمكن تهيئة خادم ClickHouse باستخدام ملفات تكوين بصيغة XML أو YAML.
في معظم أنواع التثبيت، يعمل خادم ClickHouse باستخدام `/etc/clickhouse-server/config.xml` بوصفه ملف التكوين الافتراضي، ولكن يمكن أيضًا تحديد موقع ملف التكوين يدويًا عند بدء تشغيل الخادم باستخدام خيار سطر الأوامر `--config-file` أو `-C`.
يمكن وضع ملفات تكوين إضافية في الدليل `config.d/` نسبةً إلى ملف التكوين الرئيسي، على سبيل المثال في الدليل `/etc/clickhouse-server/config.d/`.
تُدمَج الملفات الموجودة في هذا الدليل مع التكوين الرئيسي في خطوة معالجة مسبقة قبل تطبيق التكوين في خادم ClickHouse.
تُدمَج ملفات التكوين بترتيب أبجدي.
ولتسهيل التحديثات وتحسين التقسيم إلى وحدات، من أفضل الممارسات ترك ملف `config.xml` الافتراضي دون تعديل ووضع أي تخصيصات إضافية في `config.d/`.
يوجد تكوين ClickHouse Keeper في `/etc/clickhouse-keeper/keeper_config.xml`.
وبالمثل، يجب وضع ملفات التكوين الإضافية الخاصة بـ Keeper في `/etc/clickhouse-keeper/keeper_config.d/`.

يمكن المزج بين ملفات تكوين XML وYAML، فعلى سبيل المثال يمكن أن يكون لديك ملف تكوين رئيسي باسم `config.xml` وملفات تكوين إضافية مثل `config.d/network.xml` و`config.d/timezone.yaml` و`config.d/keeper.yaml`.
لا يدعم المزج بين XML وYAML داخل ملف تكوين واحد.
يجب أن تستخدم ملفات تكوين XML الوسم الأعلى مستوى `<clickhouse>...</clickhouse>`.
في ملفات تكوين YAML، يكون `clickhouse:` اختياريًا، وإذا لم يكن موجودًا فإن المُحلِّل يضيفه تلقائيًا.

<div id="merging">
  ## دمج ملفات التكوين
</div>

يُدمَج ملفّا تكوين (عادةً ملف التكوين الرئيسي وملف تكوين آخر من `config.d/`) على النحو التالي:

* إذا ظهرت عقدة (أي مسار يؤدي إلى عنصر) في كلا الملفين ولم تحتوِ على السمتين `replace` أو `remove`، فستُدرَج في ملف التكوين المدمج، كما تُدرَج العقد الفرعية من كلتا العقدتين ويُدمَج محتواها بشكل تكراري.
* إذا كانت إحدى العقدتين تحتوي على السمة `replace`، فستُدرَج في ملف التكوين المدمج، ولكن لا تُدرَج إلا العقد الفرعية من العقدة التي تحتوي على السمة `replace`.
* إذا كانت إحدى العقدتين تحتوي على السمة `remove`، فلن تُدرَج العقدة في ملف التكوين المدمج (وإذا كانت موجودة بالفعل، فستُحذَف).

على سبيل المثال، بالنظر إلى ملفي تكوين:

```xml title="config.xml"
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
    </config_a>
    <config_b>
        <setting_2>2</setting_2>
    </config_b>
    <config_c>
        <setting_3>3</setting_3>
    </config_c>
</clickhouse>
```

و

```xml title="config.d/other_config.xml"
<clickhouse>
    <config_a>
        <setting_4>4</setting_4>
    </config_a>
    <config_b replace="replace">
        <setting_5>5</setting_5>
    </config_b>
    <config_c remove="remove">
        <setting_6>6</setting_6>
    </config_c>
</clickhouse>
```

سيكون ملف التكوين المدمج الناتج كما يلي:

```xml
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
        <setting_4>4</setting_4>
    </config_a>
    <config_b>
        <setting_5>5</setting_5>
    </config_b>
</clickhouse>
```

<div id="from_env_zk">
  ### الاستبدال عن طريق متغيرات البيئة وعُقد ZooKeeper
</div>

لتحديد أن قيمة عنصر ما يجب استبدالها بقيمة متغير بيئة، يمكنك استخدام السمة `from_env`.

على سبيل المثال، مع متغير البيئة `$MAX_QUERY_SIZE = 150000`:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

سيكون التكوين الناتج كما يلي:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

يمكن أيضًا تحقيق الأمر نفسه باستخدام `from_zk` (عقدة ZooKeeper):

```xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```shell
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

مما ينتج عنه التكوين التالي:

```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

<div id="default-values">
  #### القيم الافتراضية
</div>

يمكن لعنصر يتضمن السمة `from_env` أو `from_zk` أن يتضمن أيضًا السمة `replace="1"` (ويجب أن تظهر الأخيرة قبل `from_env`/`from_zk`).
في هذه الحالة، يمكن للعنصر تحديد قيمة افتراضية.
تُسند إلى العنصر قيمة متغير البيئة أو عقدة ZooKeeper إذا كانت محددة، وإلا فتُسند إليه القيمة الافتراضية.

يتكرر المثال السابق، ولكن بافتراض أن `MAX_QUERY_SIZE` غير محدد:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

لينتج عن ذلك التكوين التالي:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

<div id="substitution-with-file-content">
  ## الاستبدال بمحتوى ملف
</div>

يمكن أيضًا استبدال أجزاء من التكوين بمحتويات ملفات. ويمكن تنفيذ ذلك بطريقتين:

* *استبدال القيم*: إذا كان أحد العناصر يحتوي على السمة `incl`، فستُستبدل قيمته بمحتوى الملف المشار إليه. ويكون المسار الافتراضي إلى ملف الاستبدالات هو `/etc/metrika.xml`. ويمكن تغيير ذلك في العنصر [`include_from`](../operations/server-configuration-parameters/settings.md#include_from) ضمن تكوين الخادم. وتُحدَّد قيم الاستبدال في عناصر `/clickhouse/substitution_name` داخل هذا الملف. وإذا لم يكن الاستبدال المحدد في `incl` موجودًا، فسيُسجَّل ذلك في السجل. ولمنع ClickHouse من تسجيل الاستبدالات المفقودة، حدِّد السمة `optional="true"` (على سبيل المثال، إعدادات [ماكرو](../operations/server-configuration-parameters/settings.md#macros)).
* *استبدال العناصر*: إذا أردت استبدال العنصر بالكامل عبر استبدال، فاستخدم `include` بوصفه اسم العنصر. ويمكن استخدام اسم العنصر `include` مع السمة `from_zk = "/path/to/node"`. وفي هذه الحالة، تُستبدل قيمة العنصر بمحتويات عقدة ZooKeeper الموجودة عند `/path/to/node`. وينجح هذا أيضًا إذا خزّنت شجرة XML فرعية كاملة بوصفها عقدة ZooKeeper، إذ ستُدرج بالكامل في العنصر المصدر.

يظهر مثال على ذلك أدناه:

```xml
<clickhouse>
    <!-- Appends XML subtree found at `/profiles-in-zookeeper` ZK path to `<profiles>` element. -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- Replaces `include` element with the subtree found at `/users-in-zookeeper` ZK path. -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

إذا كنت تريد دمج المحتوى المستبدَل مع التكوين الحالي بدلًا من إلحاقه، فيمكنك استخدام السمة `merge="true"`. على سبيل المثال: `<include from_zk="/some_path" merge="true">`. في هذه الحالة، ستُدمَج التكوين الحالي مع المحتوى القادم من الاستبدال، وستُستبدل إعدادات التكوين الحالية بالقيم القادمة من الاستبدال.

<div id="encryption">
  ## تشفير التكوين وإخفاؤها
</div>

يمكنك استخدام التشفير المتماثل لتشفير عنصر من عناصر التكوين، مثل `password` بنص صريح أو `private key`.
وللقيام بذلك، عليك أولاً تهيئة [codec التشفير](../sql-reference/statements/create/table.md#encryption-codecs)، ثم إضافة السمة `encrypted_by` إلى العنصر المراد تشفيره، على أن تكون قيمتها اسم codec التشفير.

وعلى خلاف السمات `from_zk` و`from_env` و`incl`، أو العنصر `include`، لا يُجرى أي استبدال (أي فك تشفير القيمة المشفَّرة) في الملف المُعالَج مسبقًا.
ويحدث فك التشفير فقط وقت التشغيل داخل عملية الخادم.

على سبيل المثال:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>00112233445566778899aabbccddeeff</key_hex>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

يمكن أيضًا تطبيق الخاصيتين [`from_env`](#from_env_zk) و[`from_zk`](#from_env_zk) على `encryption_codecs`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_env="CLICKHOUSE_KEY_HEX"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

يمكن تعريف مفاتيح التشفير والقيم المشفَّرة في أيٍّ من ملفَّي التكوين.

وفيما يلي مثال لملف `config.xml`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

يَرِد مثال لملف `users.xml` كما يلي:

```xml
<clickhouse>

    <users>
        <test_user>
            <password encrypted_by="AES_128_GCM_SIV">96280000000D000000000030D4632962295D46C6FA4ABF007CCEC9C1D0E19DA5AF719C1D9A46C446</password>
            <profile>default</profile>
        </test_user>
    </users>

</clickhouse>
```

لتشفير قيمة، يمكنك استخدام البرنامج (كمثال) `encrypt_decrypt`:

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

حتى مع تشفير عناصر التكوين، فإن العناصر المشفّرة تظل تظهر في ملف التكوين المُعالَج مسبقًا.
إذا كان هذا يمثّل مشكلة في نشر ClickHouse لديك، فهناك بديلان: إمّا ضبط أذونات الملف المُعالَج مسبقًا على 600، أو استخدام السمة `hide_in_preprocessed`.

على سبيل المثال:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

<div id="user-settings">
  ## إعدادات المستخدم
</div>

يمكن لملف `config.xml` تحديد تكوين منفصل تتضمن إعدادات المستخدم وملفات التعريف والحصص. ويُحدَّد المسار النسبي لهذا التكوين في العنصر `users_config`. وتكون قيمته `users.xml` افتراضيًا. وإذا لم يُحدَّد `users_config`، فستُحدَّد إعدادات المستخدم وملفات التعريف والحصص مباشرةً في `config.xml`.

يمكن تقسيم تكوين المستخدم إلى ملفات منفصلة، على غرار `config.xml` و`config.d/`.
ويُحدَّد اسم الدليل على أنه قيمة الإعداد `users_config` بعد إزالة اللاحقة `.xml` ثم إلحاق `.d`.
ويُستخدم الدليل `users.d` افتراضيًا، لأن القيمة الافتراضية لـ `users_config` هي `users.xml`.

لاحظ أن ملفات التكوين تُ[دمج](#merging) أولًا مع مراعاة الإعدادات، ثم تُعالَج التضمينات بعد ذلك.

<div id="example">
  ## مثال XML
</div>

على سبيل المثال، يمكنك استخدام ملف تكوين منفصل لكل مستخدم، كما يلي:

```bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

```xml
<clickhouse>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</clickhouse>
```

<div id="example-1">
  ## أمثلة YAML
</div>

يمكنك هنا الاطلاع على الإعدادات الافتراضية المكتوبة بتنسيق YAML: [`config.yaml.example`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

توجد بعض الاختلافات بين تنسيقي YAML وXML في ما يتعلق بإعدادات ClickHouse.
فيما يلي بعض الإرشادات لكتابة الإعدادات بتنسيق YAML.

يُمثَّل وسم XML ذي القيمة النصية بزوج مفتاح-قيمة في YAML

```yaml
key: value
```

ملف XML المقابل:

```xml
<key>value</key>
```

تُمثَّل عقدة XML متداخلة على هيئة خريطة YAML:

```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

شيفرة XML المقابلة:

```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

لإنشاء وسم XML نفسه أكثر من مرة، استخدم تسلسلاً في YAML:

```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

تنسيق XML المقابل:

```xml
<seq_key>val1</seq_key>
<seq_key>val2</seq_key>
<seq_key>
    <key1>val3</key1>
</seq_key>
<seq_key>
    <map>
        <key2>val4</key2>
        <key3>val5</key3>
    </map>
</seq_key>
```

لتحديد سمة XML، يمكنك استخدام مفتاح السمة مع بادئة `@`. لاحظ أن `@` محجوزة في معيار YAML، لذا يجب وضعها بين علامتَي اقتباس مزدوجتَين:

```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

XML المقابل:

```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

من الممكن أيضًا استخدام السمات ضمن تسلسل YAML:

```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

تنسيق XML المقابل:

```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

لا تتيح الصياغة المذكورة أعلاه التعبير عن عُقد النص في XML التي تحتوي على سمات XML بصيغة YAML. ويمكن التعامل مع هذه الحالة الخاصة باستخدام
`#text` كمفتاح للسمة:

```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

ملف XML المقابل:

```xml
<map_key attr1="value1">value2</map>
```

<div id="implementation-details">
  ## تفاصيل التنفيذ
</div>

لكل ملف تكوين، يُنشئ الخادم أيضًا ملفات `file-preprocessed.xml` عند بدء التشغيل. تحتوي هذه الملفات على جميع عمليات الاستبدال والتجاوز المكتملة، وهي مخصّصة للاطلاع فقط. وإذا استُخدمت استبدالات ZooKeeper في ملفات التكوين ولكن لم يكن ZooKeeper متاحًا عند بدء تشغيل الخادم، فسيحمّل الخادم التكوين من الملف المُعالَج مسبقًا.

يتتبّع الخادم التغييرات في ملفات التكوين، وكذلك الملفات وعُقد ZooKeeper التي استُخدمت عند تنفيذ عمليات الاستبدال والتجاوز، ويُعيد تحميل إعدادات المستخدمين والعناقيد أثناء التشغيل. وهذا يعني أنه يمكنك تعديل العنقود والمستخدمين وإعداداتهم من دون إعادة تشغيل الخادم.