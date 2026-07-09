---
title: إعدادات الخادم خارج المصدر
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

مفعّل افتراضيًا في عمليات نشر ClickHouse Cloud.

إذا لم يكن هذا الإعداد مفعّلًا افتراضيًا في بيئتك، فبحسب طريقة تثبيت ClickHouse، يمكنك اتباع التعليمات أدناه لتمكينه أو تعطيله.

**التمكين**

لتفعيل جمع محفوظات سجل المقاييس غير المتزامنة يدويًا [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md)، أنشئ `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**التعطيل**

لتعطيل الإعداد `asynchronous_metric_log`، يجب إنشاء الملف التالي `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

استخدم العنوان الأصلي للمصادقة للعملاء المتصلين عبر وكيل.

:::note
يجب استخدام هذا الإعداد بمزيد من الحذر، إذ يمكن انتحال العناوين المُمرَّرة بسهولة. ويجب ألا يكون الوصول إلى الخوادم التي تقبل هذا النوع من المصادقة مباشرًا، بل يقتصر على المرور عبر وكيل موثوق.
:::

<div id="backups">
  ## النسخ الاحتياطية
</div>

إعدادات النسخ الاحتياطية، تُستخدم عند تنفيذ عبارتي [`BACKUP` و`RESTORE`](/ar/operations/backup/overview).

يمكن ضبط الإعدادات التالية عبر الوسوم الفرعية:

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','يحدد ما إذا كان يمكن تشغيل عدة عمليات نسخ احتياطي بالتزامن على المضيف نفسه.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'يحدد ما إذا كان يمكن تشغيل عدة عمليات استعادة بالتزامن على المضيف نفسه.', 'true'),
    ('allowed_disk', 'String', 'القرص الذي سيتم إجراء النسخ الاحتياطي إليه عند استخدام `File()`. يجب تعيين هذا الإعداد لاستخدام `File`.', ''),
    ('allowed_path', 'String', 'المسار الذي سيتم إجراء النسخ الاحتياطي إليه عند استخدام `File()`. يجب تعيين هذا الإعداد لاستخدام `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'عدد محاولات جمع البيانات الوصفية قبل الانتظار في حال وجود عدم اتساق بعد مقارنة البيانات الوصفية المجمعة.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'المهلة بالميلي ثانية لجمع البيانات الوصفية أثناء النسخ الاحتياطي.', '600000'),
    ('compare_collected_metadata', 'Bool', 'إذا كانت القيمة true، فستتم مقارنة البيانات الوصفية المجمعة بالبيانات الوصفية الموجودة للتأكد من أنها لم تتغير أثناء النسخ الاحتياطي.', 'true'),
    ('create_table_timeout', 'UInt64', 'المهلة بالميلي ثانية لإنشاء الجداول أثناء الاستعادة.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'الحد الأقصى لعدد محاولات إعادة المحاولة بعد مواجهة خطأ إصدار غير صالح أثناء النسخ الاحتياطي/الاستعادة المنسقين.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'الحد الأقصى لمدة الانتظار بالميلي ثانية قبل المحاولة التالية لجمع البيانات الوصفية.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'الحد الأدنى لمدة الانتظار بالميلي ثانية قبل المحاولة التالية لجمع البيانات الوصفية.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'إذا فشل الأمر `BACKUP`، فسيحاول ClickHouse إزالة الملفات التي نُسخت بالفعل إلى النسخة الاحتياطية قبل حدوث الفشل، وإلا فسيترك الملفات المنسوخة كما هي.', 'true'),
    ('sync_period_ms', 'UInt64', 'فترة المزامنة بالميلي ثانية للنسخ الاحتياطي/الاستعادة المنسقين.', '5000'),
    ('test_inject_sleep', 'Bool', 'مدة انتظار مرتبطة بالاختبار', 'false'),
    ('test_randomize_order', 'Bool', 'إذا كانت القيمة true، فسيُرتَّب تسلسل بعض العمليات عشوائيًا لأغراض الاختبار.', 'false'),
    ('zookeeper_path', 'String', 'المسار في ZooKeeper حيث تُخزَّن البيانات الوصفية للنسخ الاحتياطي والاستعادة عند استخدام العبارة `ON CLUSTER`.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
  */ }

| الإعداد                                             | النوع  | الوصف                                                                                                                                              | الافتراضي             |
| :-------------------------------------------------- | :----- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | يحدد ما إذا كان يمكن تشغيل عدة عمليات نسخ احتياطي بالتوازي على المضيف نفسه.                                                                        | `true`                |
| `allow_concurrent_restores`                         | Bool   | يحدد ما إذا كان يمكن تشغيل عدة عمليات استعادة بالتوازي على المضيف نفسه.                                                                            | `true`                |
| `allowed_disk`                                      | String | القرص الذي سيُحفَظ عليه النسخ الاحتياطي عند استخدام `File()`. يجب تعيين هذا الإعداد لاستخدام `File`.                                               | &#96;&#96;            |
| `allowed_path`                                      | String | المسار الذي سيُحفَظ فيه النسخ الاحتياطي عند استخدام `File()`. يجب تعيين هذا الإعداد لاستخدام `File`.                                               | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | عدد محاولات جمع البيانات الوصفية قبل الانتظار عند وجود عدم اتساق بعد مقارنة البيانات الوصفية المجمَّعة.                                            | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | المهلة الزمنية، بالمللي ثانية، لجمع البيانات الوصفية أثناء النسخ الاحتياطي.                                                                        | `600000`              |
| `compare_collected_metadata`                        | Bool   | إذا كانت القيمة `true`، تُقارَن البيانات الوصفية المجمَّعة بالبيانات الوصفية الموجودة للتأكد من أنها لم تتغير أثناء النسخ الاحتياطي.               | `true`                |
| `create_table_timeout`                              | UInt64 | المهلة الزمنية، بالمللي ثانية، لإنشاء الجداول أثناء الاستعادة.                                                                                     | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | الحد الأقصى لعدد محاولات إعادة المحاولة بعد حدوث خطأ إصدار غير صالح أثناء النسخ الاحتياطي/الاستعادة المنسَّقين.                                    | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | الحد الأقصى لمدة الانتظار بالمللي ثانية قبل المحاولة التالية لجمع البيانات الوصفية.                                                                | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | الحد الأدنى لمدة الانتظار بالمللي ثانية قبل المحاولة التالية لجمع البيانات الوصفية.                                                                | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | إذا فشل الأمر `BACKUP`، فسيحاول ClickHouse إزالة الملفات التي سبق نسخها إلى النسخة الاحتياطية قبل وقوع الفشل، وإلا فسيترك الملفات المنسوخة كما هي. | `true`                |
| `sync_period_ms`                                    | UInt64 | فترة المزامنة بالمللي ثانية للنسخ الاحتياطي/الاستعادة المنسَّقين.                                                                                  | `5000`                |
| `test_inject_sleep`                                 | Bool   | انتظار لأغراض الاختبار                                                                                                                             | `false`               |
| `test_randomize_order`                              | Bool   | إذا كانت القيمة `true`، فسيُعاد ترتيب بعض العمليات عشوائيًا لأغراض الاختبار.                                                                       | `false`               |
| `zookeeper_path`                                    | String | المسار في ZooKeeper الذي تُخزَّن فيه البيانات الوصفية للنسخ الاحتياطي والاستعادة عند استخدام العبارة `ON CLUSTER`.                                 | `/clickhouse/backups` |

يُضبط هذا الإعداد افتراضيًا على النحو التالي:

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

يحتوي على معلومات عن جميع المهام الخلفية التي تُنفَّذ عبر مختلف مجمعات المعالجة الخلفية.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

عامل العمل لنوع المصادقة `bcrypt_password` الذي يستخدم [خوارزمية Bcrypt](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
ويحدّد عامل العمل مقدار العمليات الحسابية والوقت اللازمين لحساب قيمة التجزئة والتحقق من كلمة المرور.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
بالنسبة إلى التطبيقات التي تُجري عمليات مصادقة بكثافة،
فكِّر في استخدام طُرق مصادقة بديلة بسبب
العبء الحسابي لـ bcrypt عند ارتفاع معاملات العمل.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

إذا ضُبط هذا الإعداد على true، فسيحتاج المستخدمون إلى امتياز لإنشاء جدول باستخدام محرك محدد، على سبيل المثال `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
بشكل افتراضي، ومن أجل backward compatibility، يتجاهل إنشاء جدول باستخدام table engine محدد الامتيازات، ولكن يمكنك تغيير هذا السلوك بضبط هذا الإعداد على true.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

الفاصل الزمني، بالثواني، قبل إعادة تحميل القواميس المضمّنة.

يعيد ClickHouse تحميل القواميس المضمّنة كل x ثانية. ويتيح ذلك تعديل القواميس &quot;أثناء التشغيل&quot; من دون إعادة تشغيل الخادم.

**مثال**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## الضغط
</div>

إعدادات ضغط البيانات للجداول التي تستخدم المحرك [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

:::note
نوصي بعدم تغيير هذا الإعداد إذا كنت قد بدأت للتو في استخدام ClickHouse.
:::

**قالب التهيئة**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**حقول `<case>`**:

* `min_part_size` – الحد الأدنى لحجم جزء البيانات.
* `min_part_size_ratio` – نسبة حجم جزء البيانات إلى حجم الجدول.
* `method` – طريقة الضغط. القيم المقبولة: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – مستوى الضغط. راجع [Codecs](/ar/sql-reference/statements/create/table#general-purpose-codecs).

:::note
يمكنك تهيئة عدة أقسام `<case>`.
:::

**الإجراءات عند استيفاء الشروط**:

* إذا طابق جزء بيانات مجموعةَ شروط، يستخدم ClickHouse طريقة الضغط المحددة.
* إذا طابق جزء بيانات عدةَ مجموعات شروط، يستخدم ClickHouse أول مجموعة شروط تمت مطابقتها.

:::note
إذا لم تنطبق أي شروط على جزء بيانات، يستخدم ClickHouse ضغط `lz4`.
:::

**مثال**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## التشفير
</div>

يُهيّئ أمرًا للحصول على مفتاح لاستخدامه بواسطة [ترميزات التشفير](/ar/sql-reference/statements/create/table#encryption-codecs). يجب كتابة المفتاح (أو المفاتيح) في متغيرات البيئة أو ضبطه في ملف الإعدادات.

يمكن أن تكون المفاتيح بصيغة hex أو سلسلة نصية بطول يساوي 16 بايت.

**مثال**

التحميل من config:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
لا يُنصح بتخزين المفاتيح في ملف الإعدادات، لأن ذلك غير آمن. يمكنك نقل المفاتيح إلى ملف إعدادات منفصل على قرص آمن، ثم وضع رابط رمزي لهذا الملف في المجلد `config.d/`.
:::

التحميل من التهيئة عندما يكون المفتاح بالنظام السداسي عشري:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

جارٍ تحميل المفتاح من متغير البيئة:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

هنا يُعيِّن `current_key_id` المفتاح الحالي للتشفير، ويمكن استخدام جميع المفاتيح المحددة لفك التشفير.

يمكن تطبيق كلٍّ من هذه الطرق على عدة مفاتيح:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

هنا يشير `current_key_id` إلى المفتاح الحالي المستخدم للتشفير.

يمكن للمستخدمين أيضًا إضافة nonce بطول 12 بايتًا (إذ تستخدم عمليتا التشفير وفك التشفير افتراضيًا nonce مكوّنًا من بايتات صفرية):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

أو يمكن ضبطه بالصيغة السداسية العشرية:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
يمكن أيضًا تطبيق كل ما ورد أعلاه على `aes_256_gcm_siv` (لكن يجب أن يكون طول المفتاح 32 بايت).
:::

<div id="error_log">
  ## error_log
</div>

يكون معطّلًا افتراضيًا.

**التفعيل**

لتفعيل جمع سجلّ تاريخ الأخطاء [`system.error_log`](../../operations/system-tables/error_log.md) يدويًا، أنشئ الملف `/etc/clickhouse-server/config.d/error_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**تعطيل**

لتعطيل الإعداد `error_log`، يجب إنشاء الملف التالي `/etc/clickhouse-server/config.d/disable_error_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

قائمة بالبادئات المستخدمة في [الإعدادات المخصصة](/ar/operations/settings/query-level#custom_settings).
يجب فصل البادئات المتعددة بفواصل.

**مثال**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**راجع أيضًا**

* [الإعدادات المخصّصة](/ar/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

يضبط الحدّ المرن لحجم ملف core dump.

:::note
يُضبط الحدّ الصارم باستخدام أدوات النظام
:::

**مثال**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

ملف التعريف الافتراضي للإعدادات. توجد ملفات تعريف الإعدادات في الملف المحدد في الإعداد `user_config`.

**مثال**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

المسار إلى ملف إعدادات القواميس.

المسار:

* حدِّد المسار المطلق أو مسارًا نسبيًا بالنسبة إلى ملف إعدادات الخادم.
* يمكن أن يحتوي المسار على أحرف البدل * و ?.

انظر أيضًا:

* &quot;[القواميس](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**مثال**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

المسار إلى ملف الإعدادات الخاص بالدوال المعرّفة من قبل المستخدم القابلة للتنفيذ.

المسار:

* حدِّد المسار المطلق أو مسارًا نسبيًا بالنسبة إلى ملف إعدادات الخادم.
* يمكن أن يحتوي المسار على محارف البدل * و ?.

انظر أيضًا:

* &quot;[الدوال المعرّفة من قبل المستخدم القابلة للتنفيذ](/ar/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**مثال**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

إرسال البيانات إلى [Graphite](https://github.com/graphite-project).

الإعدادات:

* `host` – خادم Graphite.
* `port` – المنفذ على خادم Graphite.
* `interval` – الفاصل الزمني للإرسال، بالثواني.
* `timeout` – مهلة الإرسال، بالثواني.
* `root_path` – بادئة للمفاتيح.
* `metrics` – إرسال البيانات من جدول [system.metrics](/ar/operations/system-tables/metrics).
* `events` – إرسال بيانات دلتا المتراكمة خلال الفترة الزمنية من جدول [system.events](/ar/operations/system-tables/events).
* `events_cumulative` – إرسال البيانات التراكمية من جدول [system.events](/ar/operations/system-tables/events).
* `asynchronous_metrics` – إرسال البيانات من جدول [system.asynchronous&#95;metrics](/ar/operations/system-tables/asynchronous_metrics).

يمكنك تهيئة عدة مقاطع `<graphite>`. على سبيل المثال، يمكنك استخدام ذلك لإرسال بيانات مختلفة على فواصل زمنية مختلفة.

**مثال**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

إعدادات لتقليل حجم بيانات Graphite.

لمزيد من التفاصيل، راجع [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**مثال**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

يتيح استخدام معالِجات HTTP مخصّصة.
لإضافة معالج http جديد، ما عليك سوى إضافة `<rule>` جديد.
تُفحَص القواعد من الأعلى إلى الأسفل حسب ترتيب تعريفها،
ويُشغَّل المعالج عند أول تطابق.
أما القاعدة التي لا تتضمن شروط تطابق (بل `handler` فقط) فتطابق كل طلب؛ ونظرًا إلى أن القواعد تُفحَص بالترتيب،
فلا تكون هذه القاعدة مفيدة إلا كخيار fallback يوضع في النهاية.

يمكن تهيئة الإعدادات التالية باستخدام الوسوم الفرعية (جميع هذه الوسوم الفرعية اختيارية باستثناء `handler`):

| الوسوم الفرعية             | Definition                                                                                                                                                                                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `url`                | لمطابقة مسار URL للطلب. يتم تجاهل query string عند المطابقة                                                                                                                                                                          |
| `url_prefix`         | لمطابقة مسار URL للطلب مع مسار أساسي: المسار نفسه أو أي مسار فرعي تحته عند حدود مقطع المسار (على سبيل المثال، &#39;/api/v1&#39; يطابق /api/v1 و /api/v1/ و /api/v1/write، ولكن ليس /api/v1beta). يتم تجاهل query string عند المطابقة |
| `url_regexp`         | لمطابقة مسار URL للطلب مع regular expression. يتم تجاهل query string عند المطابقة                                                                                                                                                    |
| `full_url`           | لمطابقة URL الكامل للطلب `scheme://host:port/path`. يتم تجاهل query string عند المطابقة، ويكون host هو عنوان IP الخاص بالاتصال (وليس ترويسة `Host`)                                                                                  |
| `full_url_prefix`    | لمطابقة URL الكامل للطلب `scheme://host:port/path` مع base URL وهو `scheme://host:port/base_path`، عند حدود مقطع المسار (راجع `url_prefix`). يتم تجاهل query string عند المطابقة                                                     |
| `full_url_regexp`    | لمطابقة URL الكامل للطلب `scheme://host:port/path` مع regular expression. يتم تجاهل query string عند المطابقة                                                                                                                        |
| `methods`            | لمطابقة طرق الطلب، ويمكنك استخدام الفواصل للفصل بين عدة طرق                                                                                                                                                                          |
| `headers`            | لمطابقة headers الخاصة بالطلب، طابِق كل عنصر ابن (ويكون اسم العنصر الابن هو اسم header)                                                                                                                                              |
| `headers_regexp`     | مثل `headers`، ولكن تُطابَق قيمة كل عنصر ابن مع regular expression                                                                                                                                                                   |
| `empty_query_string` | تحقّق من عدم وجود query string في URL                                                                                                                                                                                                |
| `handler`            | معالج الطلب (مطلوب)                                                                                                                                                                                                                  |

:::note
بدلًا من `url_regexp` و `full_url_regexp` و `headers_regexp`، يمكنك أيضًا كتابة regular expression داخل `url` أو `full_url` أو `headers` باستخدام البادئة `regex:` (على سبيل المثال `<url>regex:/api/.*</url>`). لا يزال هذا مدعومًا للحفاظ على backward compatibility، لكنه Obsolete: يُفضَّل استخدام الوسوم الفرعية المخصّصة `url_regexp` و `full_url_regexp` و `headers_regexp`.
:::

يحتوي `handler` على الإعدادات التالية، ويمكن تهيئتها باستخدام الوسوم الفرعية:

| الوسوم الفرعية           | Definition                                                                                                                                                                                   |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | موقع لإعادة التوجيه                                                                                                                                                                          |
| `type`             | الأنواع المدعومة: static و dynamic&#95;query&#95;handler و predefined&#95;query&#95;handler و redirect                                                                                       |
| `status`           | يُستخدم مع النوع static، لتحديد status code الخاص بالاستجابة                                                                                                                                 |
| `query_param_name` | يُستخدم مع النوع dynamic&#95;query&#95;handler، ويستخرج القيمة المقابلة لقيمة `<query_param_name>` من params في طلب HTTP ثم ينفّذها                                                          |
| `query`            | يُستخدم مع النوع predefined&#95;query&#95;handler، وينفّذ query عند استدعاء المعالج                                                                                                          |
| `content_type`     | يُستخدم مع النوع static، لتحديد content-type الخاص بالاستجابة                                                                                                                                |
| `response_content` | يُستخدم مع النوع static، وهو محتوى الاستجابة المُرسَل إلى client. وعند استخدام البادئة &#39;file://&#39; أو &#39;config://&#39;، يُجلَب المحتوى من الملف أو configuration ويُرسَل إلى client |

إلى جانب قائمة القواعد، يمكنك تحديد `<defaults/>`، والذي يفعّل جميع المعالجات الافتراضية.

مثال:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

الصفحة التي تظهر افتراضيًا عند الوصول إلى خادم ClickHouse عبر HTTP(s).
القيمة الافتراضية هي &quot;Ok.&quot; (مع محرف سطر جديد في النهاية)

**مثال**

يتم فتح `https://tabix.io/` عند الوصول إلى `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

يُستخدم لإضافة رؤوس إلى الاستجابة لطلب HTTP من النوع `OPTIONS`.
تُستخدم الطريقة `OPTIONS` عند إرسال طلبات CORS التمهيدية (Preflight).

لمزيد من المعلومات، راجع [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

مثال:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

مدة صلاحية HSTS بالثواني.

:::note
تعني القيمة `0` أن ClickHouse يعطّل HSTS. وإذا عيّنت قيمة موجبة، فسيتم تفعيل HSTS وستكون قيمة `max-age` هي القيمة التي عيّنتها.
:::

**مثال**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

قيد على المضيفين المسموح لهم بتبادل البيانات بين خوادم ClickHouse.
إذا كان Keeper مستخدمًا، فسيُطبَّق القيد نفسه على الاتصال بين مثيلات Keeper المختلفة.

:::note
تكون القيمة افتراضيًا مساوية لإعداد [`listen_host`](#listen_host).
:::

**مثال**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

النوع:

default:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

اسم مستخدم وكلمة مرور يُستخدمان للاتصال بالخوادم الأخرى أثناء [النسخ المتماثل](../../engines/table-engines/mergetree-family/replication.md). بالإضافة إلى ذلك، يصادق الخادم النسخ المتماثلة الأخرى باستخدام بيانات الاعتماد هذه.
لذلك، يجب أن تكون `interserver_http_credentials` نفسها في جميع النسخ المتماثلة داخل العنقود.

:::note

* افتراضيًا، إذا لم يتم تضمين قسم `interserver_http_credentials`، فلن تُستخدم المصادقة أثناء النسخ المتماثل.
* لا تتعلق إعدادات `interserver_http_credentials` بـ [تهيئة](../../interfaces/client.md#configuration_files) بيانات اعتماد عميل ClickHouse.
* تُستخدم بيانات الاعتماد هذه نفسها للنسخ المتماثل عبر `HTTP` و `HTTPS`.
  :::

يمكن تهيئة الإعدادات التالية باستخدام الوسوم الفرعية:

* `user` — اسم المستخدم.
* `password` — كلمة المرور.
* `allow_empty` — إذا كانت القيمة `true`، فسيُسمح للنسخ المتماثلة الأخرى بالاتصال من دون مصادقة حتى إذا كانت بيانات الاعتماد مضبوطة. وإذا كانت `false`، فستُرفض الاتصالات غير الموثَّقة. القيمة الافتراضية: `false`.
* `old` — يحتوي على `user` و `password` قديمين يُستخدمان أثناء تدوير بيانات الاعتماد. ويمكن تحديد عدة أقسام `old`.

**تدوير بيانات الاعتماد**

يدعم ClickHouse التدوير الديناميكي لبيانات اعتماد الاتصال بين الخوادم من دون إيقاف جميع النسخ المتماثلة في الوقت نفسه لتحديث إعداداتها. ويمكن تغيير بيانات الاعتماد على عدة مراحل.

لتمكين المصادقة، اضبط `interserver_http_credentials.allow_empty` على `true` وأضف بيانات الاعتماد. يتيح ذلك الاتصالات مع المصادقة أو من دونها.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

بعد تهيئة جميع النُسخ المتماثلة، اضبط `allow_empty` على `false` أو أزل هذا الإعداد. وبذلك تصبح المصادقة باستخدام بيانات اعتماد جديدة إلزامية.

لتغيير بيانات الاعتماد الحالية، انقل اسم المستخدم وكلمة المرور إلى القسم `interserver_http_credentials.old`، ثم حدّث `user` و`password` بقيم جديدة. عندئذٍ يستخدم الخادم بيانات الاعتماد الجديدة للاتصال بالنُسخ المتماثلة الأخرى، ويقبل الاتصالات باستخدام بيانات الاعتماد الجديدة أو القديمة.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

بعد تطبيق بيانات اعتماد جديدة على جميع النسخ المتماثلة، يمكن إزالة بيانات الاعتماد القديمة.

<div id="ldap_servers">
  ## ldap_servers
</div>

أدرِج هنا خوادم LDAP مع معلمات الاتصال الخاصة بها من أجل:

* استخدامها كوسائل مصادقة لمستخدمين محليين مخصّصين، حيث تُحدَّد لهم آلية مصادقة `ldap` بدلًا من `password`
* استخدامها كأدلة مستخدمين بعيدة.

يمكن تهيئة الإعدادات التالية باستخدام الوسوم الفرعية:

| Setting                        | Description                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | قالب يُستخدم لإنشاء DN الذي سيتم الربط به. ويُنشأ DN الناتج باستبدال جميع المقاطع الفرعية `\{user_name\}` في القالب باسم المستخدم الفعلي أثناء كل محاولة مصادقة.                                                                                                                                                                                                                                                 |
| `enable_tls`                   | راية لتفعيل استخدام اتصال آمن بخادم LDAP. حدِّد `no` لاستخدام بروتوكول النص الصريح (`ldap://`) (غير موصى به). وحدِّد `yes` لاستخدام LDAP عبر SSL/TLS (`ldaps://`) (موصى به، وهو الإعداد الافتراضي). وحدِّد `starttls` لاستخدام بروتوكول StartTLS القديم (بروتوكول نص صريح (`ldap://`) تتم ترقيته إلى TLS).                                                                                                       |
| `host`                         | اسم مضيف خادم LDAP أو عنوان IP؛ هذه المعلمة إلزامية ولا يمكن أن تكون فارغة.                                                                                                                                                                                                                                                                                                                                      |
| `port`                         | منفذ خادم LDAP، والقيمة الافتراضية هي 636 إذا كانت `enable_tls` مضبوطة على true، وإلا `389`.                                                                                                                                                                                                                                                                                                                     |
| `tls_ca_cert_dir`              | المسار إلى المجلد الذي يحتوي على شهادات CA.                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_ca_cert_file`             | المسار إلى ملف شهادة CA.                                                                                                                                                                                                                                                                                                                                                                                         |
| `tls_cert_file`                | المسار إلى ملف الشهادة.                                                                                                                                                                                                                                                                                                                                                                                          |
| `tls_cipher_suite`             | مجموعة التشفير المسموح بها (بصياغة OpenSSL).                                                                                                                                                                                                                                                                                                                                                                     |
| `tls_key_file`                 | المسار إلى ملف مفتاح الشهادة.                                                                                                                                                                                                                                                                                                                                                                                    |
| `tls_minimum_protocol_version` | الحد الأدنى لإصدار بروتوكول SSL/TLS. القيم المقبولة هي: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (الافتراضي).                                                                                                                                                                                                                                                                                                |
| `tls_require_cert`             | سلوك التحقق من شهادة النظير في SSL/TLS. القيم المقبولة هي: `never`, `allow`, `try`, `demand` (الافتراضي).                                                                                                                                                                                                                                                                                                        |
| `user_dn_detection`            | قسم يحتوي على معلمات بحث LDAP لاكتشاف DN الفعلي للمستخدم الذي تم الربط به. ويُستخدم هذا أساسًا في مرشحات البحث لمزيد من تعيين الأدوار عندما يكون الخادم Active Directory. وسيُستخدم DN الناتج للمستخدم عند استبدال المقاطع الفرعية `\{user_dn\}` حيثما كان ذلك مسموحًا. افتراضيًا، يُضبط DN الخاص بالمستخدم ليكون مساويًا لـ bind DN، ولكن بمجرد تنفيذ البحث، سيتم تحديثه إلى قيمة DN الفعلية المكتشفة للمستخدم. |
| `verification_cooldown`        | فترة زمنية، بالثواني، بعد محاولة ربط ناجحة، يُفترض خلالها أن المستخدم قد تمت مصادقته بنجاح في جميع الطلبات المتتالية من دون الاتصال بخادم LDAP. حدِّد `0` (الافتراضي) لتعطيل التخزين المؤقت وفرض الاتصال بخادم LDAP لكل طلب مصادقة.                                                                                                                                                                              |

يمكن تهيئة الإعداد `user_dn_detection` باستخدام الوسوم الفرعية:

| Setting         | Description                                                                                                                                                                                                                                                          |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | قالب يُستخدم لإنشاء base DN لبحث LDAP. ويُنشأ DN الناتج باستبدال جميع المقاطع الفرعية `\{user_name\}` و `\{bind_dn\}` في القالب باسم المستخدم الفعلي وbind DN أثناء بحث LDAP.                                                                                        |
| `scope`         | نطاق بحث LDAP. القيم المقبولة هي: `base`, `one_level`, `children`, `subtree` (الافتراضي).                                                                                                                                                                            |
| `search_filter` | قالب يُستخدم لإنشاء مرشح البحث لبحث LDAP. ويُنشأ المرشح الناتج باستبدال جميع المقاطع الفرعية `\{user_name\}` و `\{bind_dn\}` و `\{base_dn\}` في القالب باسم المستخدم الفعلي وbind DN وbase DN أثناء بحث LDAP. لاحظ أنه يجب عمل إفلات للأحرف الخاصة بشكل صحيح في XML. |

مثال:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

مثال (إعداد نموذجي لـ Active Directory مع تهيئة اكتشاف user DN لمزيد من role mapping):

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

<div id="listen_host">
  ## listen_host
</div>

تقييد المضيفات التي يمكن أن تَرِد منها الطلبات. إذا أردت أن يستجيب الخادم لها جميعًا، فحدِّد `::`.

أمثلة:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

موقع رسائل السجل وتنسيقها.

**المفاتيح**:

| Key                          | Description                                                                                                                                                                                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | عندما تكون القيمة `true` (الافتراضي)، يتم التسجيل بشكل غير متزامن (خيط خلفي واحد لكل قناة إخراج). بخلاف ذلك، يتم التسجيل داخل الخيط الذي يستدعي `LOG`                                                                                                      |
| `async_queue_max_size`       | عند استخدام التسجيل غير المتزامن، فهذا هو الحد الأقصى لعدد الرسائل التي سيتم الاحتفاظ بها في قائمة الانتظار بانتظار التفريغ. سيتم إسقاط الرسائل الإضافية                                                                                                   |
| `console`                    | يفعّل التسجيل إلى وحدة التحكم. اضبطه على `1` أو `true` للتفعيل. القيمة الافتراضية هي `1` إذا لم يكن ClickHouse يعمل في وضع daemon، وإلا فهي `0`.                                                                                                           |
| `console_log_level`          | مستوى السجل لإخراج وحدة التحكم. تكون القيمة الافتراضية هي `level`.                                                                                                                                                                                         |
| `console_shutdown_log_level` | يُستخدم مستوى الإيقاف لضبط مستوى سجل وحدة التحكم عند إيقاف الخادم.                                                                                                                                                                                         |
| `console_startup_log_level`  | يُستخدم مستوى بدء التشغيل لضبط مستوى سجل وحدة التحكم عند بدء تشغيل الخادم. بعد بدء التشغيل، يُعاد مستوى السجل إلى الإعداد `console_log_level`                                                                                                              |
| `count`                      | سياسة التدوير: الحد الأقصى لعدد ملفات السجل القديمة التي يحتفظ بها ClickHouse.                                                                                                                                                                             |
| `errorlog`                   | المسار إلى ملف سجل الأخطاء.                                                                                                                                                                                                                                |
| `formatting.type`            | تنسيق السجل لإخراج وحدة التحكم. حاليًا، التنسيق المدعوم الوحيد هو `json`                                                                                                                                                                                   |
| `level`                      | مستوى السجل. القيم المقبولة: `none` (إيقاف التسجيل)، `fatal`، `critical`، `error`، `warning`، `notice`، `information`،`debug`، `trace`، `test`                                                                                                             |
| `log`                        | المسار إلى ملف السجل.                                                                                                                                                                                                                                      |
| `rotation`                   | سياسة التدوير: تتحكم في وقت تدوير ملفات السجل. يمكن أن يستند التدوير إلى الحجم أو الوقت أو مزيج منهما. أمثلة: 100M, daily, 100M,daily. بمجرد أن يتجاوز ملف السجل الحجم المحدد أو عند بلوغ الفاصل الزمني المحدد، يُعاد تسميته وأرشفته، ويُنشأ ملف سجل جديد. |
| `shutdown_level`             | يُستخدم مستوى الإيقاف لضبط مستوى المسجل الجذر عند إيقاف الخادم.                                                                                                                                                                                            |
| `size`                       | سياسة التدوير: الحد الأقصى لحجم ملفات السجل بالبايت. بمجرد أن يتجاوز حجم ملف السجل هذه العتبة، يُعاد تسميته وأرشفته، ويُنشأ ملف سجل جديد.                                                                                                                  |
| `startup_level`              | يُستخدم مستوى بدء التشغيل لضبط مستوى المسجل الجذر عند بدء تشغيل الخادم. بعد بدء التشغيل، يُعاد مستوى السجل إلى الإعداد `level`                                                                                                                             |
| `stream_compress`            | يضغط رسائل السجل باستخدام LZ4. اضبطه على `1` أو `true` للتفعيل.                                                                                                                                                                                            |
| `syslog_level`               | مستوى السجل للتسجيل في syslog.                                                                                                                                                                                                                             |
| `use_syslog`                 | يعيد أيضًا توجيه مخرجات السجل إلى syslog.                                                                                                                                                                                                                  |

**محددات تنسيق السجل**

تدعم أسماء الملفات في المسارين `log` و `errorLog` محددات التنسيق التالية لاسم الملف الناتج (جزء الدليل لا يدعمها).

يعرض العمود &quot;Example&quot; المخرجات عند `2023-07-06 18:32:07`.

| المُحدِّد | الوصف                                                                                                                                                                                               | مثال                       |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%`      | الرمز % حرفيًا                                                                                                                                                                                      | `%`                        |
| `%n`      | محرف سطر جديد                                                                                                                                                                                       |                            |
| `%t`      | محرف جدولة أفقي                                                                                                                                                                                     |                            |
| `%Y`      | السنة كعدد عشري، مثل 2017                                                                                                                                                                           | `2023`                     |
| `%y`      | آخر رقمين من السنة كعدد عشري (النطاق [00,99])                                                                                                                                                       | `23`                       |
| `%C`      | أول رقمين من السنة كعدد عشري (النطاق [00,99])                                                                                                                                                       | `20`                       |
| `%G`      | السنة المبنية على رقم الأسبوع وفق [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) والمكوَّنة من أربعة أرقام، أي السنة التي تحتوي على الأسبوع المحدد. عادةً ما يكون مفيدًا فقط مع `%V` | `2023`                     |
| `%g`      | آخر رقمين من السنة المبنية على رقم الأسبوع وفق [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)، أي السنة التي تحتوي على الأسبوع المحدد.                                               | `23`                       |
| `%b`      | اسم الشهر المختصر، مثل Oct (بحسب الإعدادات المحلية)                                                                                                                                                 | `Jul`                      |
| `%h`      | مرادف لـ %b                                                                                                                                                                                         | `Jul`                      |
| `%B`      | اسم الشهر الكامل، مثل October (بحسب الإعدادات المحلية)                                                                                                                                              | `July`                     |
| `%m`      | الشهر كعدد عشري (النطاق [01,12])                                                                                                                                                                    | `07`                       |
| `%U`      | أسبوع السنة كعدد عشري (الأحد هو أول يوم في الأسبوع) (النطاق [00,53])                                                                                                                                | `27`                       |
| `%W`      | أسبوع السنة كعدد عشري (الاثنين هو أول يوم في الأسبوع) (النطاق [00,53])                                                                                                                              | `27`                       |
| `%V`      | رقم الأسبوع وفق ISO 8601 (النطاق [01,53])                                                                                                                                                           | `27`                       |
| `%j`      | يوم السنة كعدد عشري (النطاق [001,366])                                                                                                                                                              | `187`                      |
| `%d`      | يوم الشهر كعدد عشري مكمَّل بصفر بادئ (النطاق [01,31]). تُسبق القيمة أحادية الرقم بصفر.                                                                                                              | `06`                       |
| `%e`      | يوم الشهر كعدد عشري مكمَّل بمسافة بادئة (النطاق [1,31]). تُسبق القيمة أحادية الرقم بمسافة.                                                                                                          | `&nbsp; 6`                 |
| `%a`      | اسم يوم الأسبوع المختصر، مثل Fri (بحسب الإعدادات المحلية)                                                                                                                                           | `Thu`                      |
| `%A`      | اسم يوم الأسبوع الكامل، مثل Friday (بحسب الإعدادات المحلية)                                                                                                                                         | `Thursday`                 |
| `%w`      | يوم الأسبوع كعدد صحيح بحيث يكون الأحد 0 (النطاق [0-6])                                                                                                                                              | `4`                        |
| `%u`      | يوم الأسبوع كعدد عشري، حيث يكون الاثنين 1 (تنسيق ISO 8601) (النطاق [1-7])                                                                                                                           | `4`                        |
| `%H`      | الساعة كعدد عشري، بنظام 24 ساعة (النطاق [00-23])                                                                                                                                                    | `18`                       |
| `%I`      | الساعة كعدد عشري، بنظام 12 ساعة (النطاق [01,12])                                                                                                                                                    | `06`                       |
| `%M`      | الدقيقة كعدد عشري (النطاق [00,59])                                                                                                                                                                  | `32`                       |
| `%S`      | الثانية كعدد عشري (النطاق [00,60])                                                                                                                                                                  | `07`                       |
| `%c`      | سلسلة التاريخ والوقت القياسية، مثل Sun Oct 17 04:41:13 2010 (بحسب الإعدادات المحلية)                                                                                                                | `Thu Jul  6 18:32:07 2023` |
| `%x`      | تمثيل التاريخ بحسب الإعدادات المحلية                                                                                                                                                                | `07/06/23`                 |
| `%X`      | تمثيل الوقت بحسب الإعدادات المحلية، مثل 18:40:20 أو 6:40:20 PM (بحسب الإعدادات المحلية)                                                                                                             | `18:32:07`                 |
| `%D`      | تاريخ قصير بالتنسيق MM/DD/YY، ويكافئ %m/%d/%y                                                                                                                                                       | `07/06/23`                 |
| `%F`      | تاريخ قصير بصيغة YYYY-MM-DD، ويعادل %Y-%m-%d                                                                                                                                                        | `2023-07-06`               |
| `%r`      | الوقت بنظام 12 ساعة وفقًا للإعدادات المحلية (بحسب الإعدادات المحلية)                                                                                                                                | `06:32:07 PM`              |
| `%R`      | يعادل &quot;%H:%M&quot;                                                                                                                                                                             | `18:32`                    |
| `%T`      | يعادل &quot;%H:%M:%S&quot; (تنسيق الوقت ISO 8601)                                                                                                                                                   | `18:32:07`                 |
| `%p`      | دلالة a.m. أو p.m. وفقًا للإعدادات المحلية (بحسب الإعدادات المحلية)                                                                                                                                 | `PM`                       |
| `%z`      | الإزاحة عن UTC بتنسيق ISO 8601 (مثل -0430)، أو لا تظهر أي محارف إذا لم تكن معلومات المنطقة الزمنية متاحة                                                                                            | `+0800`                    |
| `%Z`      | اسم المنطقة الزمنية أو اختصارها بحسب الإعدادات المحلية، أو لا تظهر أي محارف إذا لم تكن معلومات المنطقة الزمنية متاحة                                                                                | `Z AWST `                  |

**مثال**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

لطباعة رسائل السجل في الطرفية فقط:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**تجاوزات حسب المستوى**

يمكن تجاوز مستوى السجل لكل اسم سجل على حدة. على سبيل المثال، لكتم جميع رسائل المسجّلين &quot;Backup&quot; و&quot;RBAC&quot;.

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

لكتابة رسائل السجل إلى syslog أيضًا:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

المفاتيح الخاصة بـ `<syslog>`:

| المفتاح    | الوصف                                                                                                                                                                                                                                                                              |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | عنوان syslog بالتنسيق `host\[:port\]`. إذا لم يتم تحديده، فسيُستخدم الـ daemon المحلي.                                                                                                                                                                                             |
| `hostname` | اسم المضيف الذي تُرسَل منه السجلات (اختياري).                                                                                                                                                                                                                                      |
| `facility` | [الكلمة المفتاحية لـ facility](https://en.wikipedia.org/wiki/Syslog#Facility) في syslog. يجب كتابتها بأحرف كبيرة مع البادئة &quot;LOG&#95;&quot;، مثل `LOG_USER` و`LOG_DAEMON` و`LOG_LOCAL3` وما إلى ذلك. القيمة الافتراضية: `LOG_USER` إذا تم تحديد `address`، وإلا `LOG_DAEMON`. |
| `format`   | تنسيق رسالة السجل. القيم الممكنة: `bsd` و`syslog.`                                                                                                                                                                                                                                 |

**تنسيقات السجل**

يمكنك تحديد تنسيق السجل الذي سيظهر في سجل وحدة التحكم. حاليًا، لا يتوفر دعم إلا لـ JSON.

**مثال**

فيما يلي مثال على سجل JSON ناتج:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

لتمكين دعم التسجيل بتنسيق JSON، استخدم المقتطف التالي:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**إعادة تسمية المفاتيح في سجلات JSON**

يمكن تعديل أسماء المفاتيح من خلال تغيير قيم الوسوم داخل الوسم `<names>`. على سبيل المثال، لتغيير `DATE_TIME` إلى `MY_DATE_TIME`، يمكنك استخدام `<date_time>MY_DATE_TIME</date_time>`.

**حذف المفاتيح من سجلات JSON**

يمكن حذف خصائص السجل بالتعليق عليها. على سبيل المثال، إذا كنت لا تريد أن يتضمن السجل `query_id`، يمكنك التعليق على الوسم `<query_id>`.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

إعدادات إرسال تقارير الأعطال إلى فريق المطورين الأساسيين في ClickHouse.

نُقدّر كثيرًا تفعيل هذا الخيار، وخاصةً في بيئات ما قبل الإنتاج.

المفاتيح:

| المفتاح               | الوصف                                                                                                                                        |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`             | علامة منطقية لتمكين هذه الميزة، وتكون `true` افتراضيًا. اضبطها على `false` لتجنّب إرسال تقارير الأعطال.                                      |
| `endpoint`            | يمكنك تجاوز عنوان URL لنقطة النهاية المستخدَم لإرسال تقارير الأعطال.                                                                         |
| `send_logical_errors` | يشبه `LOGICAL_ERROR` تعليمة `assert`، وهو خلل برمجي في ClickHouse. تُمكّن هذه العلامة المنطقية من إرسال هذه الاستثناءات (الافتراضي: `true`). |

**الاستخدام الموصى به**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

سيُكتب الجزء العام من مفتاح المضيف في ملف known&#95;hosts
على جهة عميل SSH عند أول اتصال.

تكون إعدادات مفتاح المضيف غير مفعّلة افتراضيًا.
أزل التعليق عن إعدادات مفتاح المضيف، وحدّد المسار إلى مفتاح SSH المقابل لتفعيلها:

مثال:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

المنفذ الخاص بخادم SSH الذي يتيح للمستخدم الاتصال وتنفيذ الاستعلامات بصورة تفاعلية باستخدام العميل المدمج عبر PTY.

مثال:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

يتيح تهيئة التخزين باستخدام عدة أقراص.

تتبع تهيئة التخزين البنية التالية:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### إعداد `disks`
</div>

يتبع إعداد `disks` البنية الموضحة أدناه:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

تُحدِّد العلامات الفرعية أعلاه الإعدادات التالية لـ `disks`:

| الإعداد                 | الوصف                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| `<disk_name_N>`         | اسم القرص، ويجب أن يكون فريدًا.                                                          |
| `path`                  | المسار الذي ستُخزَّن فيه بيانات الخادم (الدليلان `data` و`shadow`). ويجب أن ينتهي بـ `/` |
| `keep_free_space_bytes` | حجم المساحة الحرة المحجوزة على القرص.                                                    |

:::note
لا يهم ترتيب الأقراص.
:::

<div id="configuration-of-policies">
  ### تهيئة السياسات
</div>

تُعرِّف العلامات الفرعية أعلاه الإعدادات التالية لـ `policies`:

| Setting                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | اسم السياسة. يجب أن تكون أسماء السياسات فريدة.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `volume_name_N`              | اسم وحدة التخزين. يجب أن تكون أسماء وحدات التخزين فريدة.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `disk`                       | القرص الموجود داخل وحدة التخزين.                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `max_data_part_size_bytes`   | الحد الأقصى لحجم جزء البيانات الذي يمكن أن يوجد على أي قرص ضمن وحدة التخزين هذه. إذا نتج عن الدمج جزء بيانات يُتوقع أن يتجاوز حجمه `max_data_part_size_bytes`، فسيُكتب هذا الجزء إلى وحدة التخزين التالية. تتيح لك هذه الميزة أساسًا تخزين الأجزاء الجديدة أو الصغيرة على وحدة تخزين سريعة (SSD) ثم نقلها إلى وحدة تخزين أبطأ (HDD) عندما تكبر. لا تستخدم هذا الخيار إذا كانت السياسة تتضمن وحدة تخزين واحدة فقط.                                                            |
| `move_factor`                | نسبة المساحة الحرة المتاحة في وحدة التخزين. إذا انخفضت المساحة عن هذا الحد، فستبدأ البيانات بالانتقال إلى وحدة التخزين التالية، إن وُجدت. ولأغراض النقل، تُرتَّب الأجزاء حسب الحجم من الأكبر إلى الأصغر (ترتيبًا تنازليًا)، وتُختار الأجزاء التي يكفي حجمها الإجمالي لتحقيق شرط `move_factor`. وإذا لم يكن الحجم الإجمالي لجميع الأجزاء كافيًا، فستنقل جميع الأجزاء.                                                                                                         |
| `perform_ttl_move_on_insert` | يعطّل نقل البيانات ذات TTL المنتهي عند الإدراج. افتراضيًا (عند التمكين)، إذا أدرجنا جزءًا من البيانات انتهت صلاحيته بالفعل وفقًا لقاعدة النقل حسب العمر، فسيُنقل فورًا إلى وحدة التخزين / القرص المحدد في قاعدة النقل. قد يؤدي ذلك إلى إبطاء الإدراج بشكل ملحوظ إذا كانت وحدة التخزين / القرص الهدف بطيئة (مثل S3). وإذا كان هذا الخيار معطّلًا، فسيُكتب الجزء من البيانات المنتهي إلى وحدة التخزين الافتراضية ثم يُنقل فورًا إلى وحدة التخزين المحددة في قاعدة TTL المنتهي. |
| `load_balancing`             | سياسة موازنة الأقراص، `round_robin` أو `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `least_used_ttl_ms`          | يضبط المهلة الزمنية (بالملي ثانية) لتحديث المساحة المتاحة على جميع الأقراص (`0` - تحديث دائمًا، `-1` - عدم التحديث مطلقًا، والقيمة الافتراضية هي `60000`). لاحظ أنه إذا كان القرص مستخدمًا فقط بواسطة ClickHouse ولن يخضع لتغيير حجم نظام الملفات أثناء التشغيل، فيمكنك استخدام القيمة `-1`. أما في جميع الحالات الأخرى، فلا يُنصح بذلك لأنه سيؤدي في النهاية إلى تخصيص غير صحيح للمساحة.                                                                                    |
| `prefer_not_to_merge`        | يعطّل دمج أجزاء البيانات على وحدة التخزين هذه. ملاحظة: قد يكون هذا ضارًا وقد يسبب تباطؤًا. عند تمكين هذا الإعداد (لا تفعل ذلك)، يُمنع دمج البيانات على وحدة التخزين هذه (وهذا أمر سيئ). يتيح ذلك التحكم في كيفية تعامل ClickHouse مع الأقراص البطيئة. نوصي بعدم استخدامه مطلقًا.                                                                                                                                                                                             |
| `volume_priority`            | يحدد الأولوية (الترتيب) التي تُملأ بها وحدات التخزين. كلما صغرت القيمة، ارتفعت الأولوية. يجب أن تكون قيم المعامل أعدادًا طبيعية وتغطي النطاق من 1 إلى N (حيث إن N هي أكبر قيمة محددة للمعامل) من دون فجوات.                                                                                                                                                                                                                                                                  |

بالنسبة إلى `volume_priority`:

* إذا كانت جميع وحدات التخزين تحتوي على هذا المعامل، فستُرتَّب أولوياتها وفق الترتيب المحدد.
* إذا كانت *بعض* وحدات التخزين فقط تحتوي عليه، فإن وحدات التخزين التي لا تحتوي عليه تكون ذات أولوية أدنى. أما الوحدات التي تحتوي عليه فتُرتَّب أولوياتها وفقًا لقيمة العلامة، وتُحدَّد أولوية البقية بالنسبة إلى بعضها بعضًا بحسب ترتيب الوصف في ملف التهيئة.
* إذا لم يُعطَ *أيٌّ* من وحدات التخزين هذا المعامل، فيُحدَّد ترتيبها بحسب ترتيب الوصف في ملف التهيئة.
* لا يجوز أن تتطابق أولويات وحدات التخزين.

<div id="macros">
  ## الماكرو
</div>

استبدالات المعلمات الخاصة بالجداول المكررة.

يمكن إغفالها إذا لم تكن تستخدم الجداول المكررة.

لمزيد من المعلومات، راجع قسم [إنشاء الجداول المكررة](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**مثال**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

اسم مجموعة replica لقاعدة البيانات Replicated.

ستتكوّن العنقود التي تنشئها قاعدة البيانات Replicated من النسخ المتماثلة الموجودة في المجموعة نفسها.
ولن تنتظر استعلامات DDL إلا النسخ المتماثلة الموجودة في المجموعة نفسها.

تكون قيمته فارغة افتراضيًا.

**مثال**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

الحد الأقصى للمهلة الزمنية للجلسة، بالثواني.

مثال:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

الضبط الدقيق للجداول في [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

لمزيد من المعلومات، راجع ملف الترويسة MergeTreeSettings.h.

**مثال**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

وهو معطّل بشكل افتراضي.

**التمكين**

لتفعيل جمع سجل المقاييس [`system.metric_log`](../../operations/system-tables/metric_log.md) يدويًا، أنشئ `/etc/clickhouse-server/config.d/metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**تعطيل**

لتعطيل الإعداد `metric_log`، أنشئ الملف التالي `/etc/clickhouse-server/config.d/disable_metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

ضبط دقيق للجداول في [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). تكون أولوية هذا الإعداد أعلى.

لمزيد من المعلومات، راجع ملف ترويسة MergeTreeSettings.h.

**مثال**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

إعدادات جدول النظام [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md).

<SystemLogParameters />

مثال:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

إعدادات SSL للعميل والخادم.

يُوفَّر دعم SSL من خلال مكتبة `libpoco`. وتُشرح خيارات الإعداد المتاحة في [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). ويمكن العثور على القيم الافتراضية في [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

مفاتيح إعدادات الخادم والعميل:

| الخيار                        | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | القيمة الافتراضية                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | يُفعِّل أو يعطِّل التخزين المؤقت للجلسات. يجب استخدامه مع `sessionIdContext`. القيم المقبولة: `true`، `false`.                                                                                                                                                                                                                                                                                                                                                                  | `false`                                                                                    |
| `caConfig`                    | المسار إلى الملف أو الدليل الذي يحتوي على شهادات CA الموثوق بها. إذا كان يشير إلى ملف، فيجب أن يكون بتنسيق PEM ويمكن أن يحتوي على عدة شهادات CA. وإذا كان يشير إلى دليل، فيجب أن يحتوي على ملف ‎.pem‎ واحد لكل شهادة CA. يُبحث عن أسماء الملفات باستخدام قيمة التجزئة لاسم الموضوع الخاص بـ CA. يمكن العثور على التفاصيل في صفحة الدليل الخاصة بـ [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                                                                            |
| `certificateFile`             | مسار ملف شهادة العميل/الخادم بصيغة PEM. ويمكنك عدم تحديده إذا كان `privateKeyFile` يحتوي على الشهادة.                                                                                                                                                                                                                                                                                                                                                                           |                                                                                            |
| `cipherList`                  | خوارزميات التشفير التي يدعمها OpenSSL.                                                                                                                                                                                                                                                                                                                                                                                                                                          | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | البروتوكولات غير المسموح باستخدامها.                                                                                                                                                                                                                                                                                                                                                                                                                                            |                                                                                            |
| `extendedVerification`        | إذا كان مُمكّنًا، فتحقّق من أن CN أو SAN في الشهادة يطابق اسم المضيف للنظير.                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `fips`                        | يُفعّل وضع FIPS في OpenSSL. وهو مدعوم إذا كان إصدار OpenSSL الخاص بالمكتبة يدعم FIPS.                                                                                                                                                                                                                                                                                                                                                                                           | `false`                                                                                    |
| `invalidCertificateHandler`   | فئة (فئة فرعية من CertificateHandler) للتحقق من الشهادات غير الصالحة. على سبيل المثال: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>`.                                                                                                                                                                                                                                                                                        | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | ما إذا كانت شهادات CA المضمّنة في OpenSSL ستُستخدم. يفترض ClickHouse أن شهادات CA المضمّنة موجودة في الملف `/etc/ssl/cert.pem` (أو في الدليل `/etc/ssl/certs`، على الترتيب)، أو في الملف (أو الدليل، على الترتيب) الذي يحدده متغير البيئة `SSL_CERT_FILE` (أو `SSL_CERT_DIR`، على الترتيب).                                                                                                                                                                                     | `true`                                                                                     |
| `preferServerCiphers`         | خوارزميات التعمية الخاصة بالخادم التي يفضّلها العميل.                                                                                                                                                                                                                                                                                                                                                                                                                           | `false`                                                                                    |
| `privateKeyFile`              | المسار إلى الملف الذي يحتوي على المفتاح الخاص لشهادة PEM. وقد يحتوي الملف على المفتاح والشهادة في الوقت نفسه.                                                                                                                                                                                                                                                                                                                                                                   |                                                                                            |
| `privateKeyPassphraseHandler` | فئة (فئة فرعية من PrivateKeyPassphraseHandler) تطلب عبارة المرور اللازمة للوصول إلى المفتاح الخاص. على سبيل المثال: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                           | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | يتطلب اتصالًا عبر TLSv1. القيم المقبولة: `true`، `false`.                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                    |
| `requireTLSv1_1`              | يتطلب اتصالًا عبر TLSv1.1. القيم المقبولة: `true`، `false`.                                                                                                                                                                                                                                                                                                                                                                                                                     | `false`                                                                                    |
| `requireTLSv1_2`              | يتطلب اتصالًا عبر TLSv1.2. القيم المقبولة: `true`، `false`.                                                                                                                                                                                                                                                                                                                                                                                                                     | `false`                                                                                    |
| `sessionCacheSize`            | الحد الأقصى لعدد الجلسات التي يخزّنها الخادم. وتعني القيمة `0` عددًا غير محدود من الجلسات.                                                                                                                                                                                                                                                                                                                                                                                      | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | مجموعة فريدة من الأحرف العشوائية يضيفها الخادم إلى كل معرّف يتم إنشاؤه. يجب ألا يتجاوز طول السلسلة النصية `SSL_MAX_SSL_SESSION_ID_LENGTH`. يُوصى دائمًا بهذه المعلمة لأنها تساعد على تجنّب المشكلات، سواء كان الخادم يخزّن الجلسة مؤقتًا أو كان العميل قد طلب التخزين المؤقت.                                                                                                                                                                                                   | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | مدة التخزين المؤقت للجلسة على الخادم، بالساعات.                                                                                                                                                                                                                                                                                                                                                                                                                                 | `2`                                                                                        |
| `verificationDepth`           | الحد الأقصى لطول سلسلة التحقق. سيفشل التحقق إذا تجاوز طول سلسلة الشهادات القيمة المحددة.                                                                                                                                                                                                                                                                                                                                                                                        | `9`                                                                                        |
| `verificationMode`            | طريقة التحقق من شهادات العقدة. ترد التفاصيل في وصف الفئة [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). القيم الممكنة: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                                                                        | `relaxed`                                                                                  |

**مثال على الإعدادات:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

سجلٌّ للأحداث المرتبطة بـ [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)، مثل إضافة البيانات أو دمجها. يمكنك استخدام هذا السجل لمحاكاة خوارزميات الدمج ومقارنة خصائصها. كما يمكنك تصور عملية الدمج.

تُسجَّل الاستعلامات في جدول [system.part&#95;log](/ar/operations/system-tables/part_log)، وليس في ملف منفصل. ويمكنك تهيئة اسم هذا الجدول في المَعلمة `table` (انظر أدناه).

<SystemLogParameters />

**مثال**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

إعدادات جدول النظام [`processors_profile_log`](../system-tables/processors_profile_log.md).

<SystemLogParameters />

الإعدادات الافتراضية هي:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

إتاحة بيانات المقاييس ليتمكّن [Prometheus](https://prometheus.io) من كشطها.

الإعدادات:

* `endpoint` – نقطة نهاية HTTP لكشط المقاييس بواسطة خادم Prometheus. يجب أن تبدأ بـ &#39;/&#39;.
* `port` – المنفذ الخاص بـ `endpoint`.
* `metrics` – إتاحة المقاييس من جدول [system.metrics](/ar/operations/system-tables/metrics).
* `events` – إتاحة المقاييس من جدول [system.events](/ar/operations/system-tables/events).
* `asynchronous_metrics` – إتاحة قيم المقاييس الحالية من جدول [system.asynchronous&#95;metrics](/ar/operations/system-tables/asynchronous_metrics).
* `errors` - إتاحة عدد الأخطاء حسب رموز الخطأ منذ آخر إعادة تشغيل للخادم. ويمكن أيضًا الحصول على هذه المعلومات من [system.errors](/ar/operations/system-tables/errors).

**مثال**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

تحقّق (استبدل `127.0.0.1` بعنوان IP أو باسم المضيف لخادم ClickHouse لديك):

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

إعداد لتسجيل الاستعلامات الواردة عند استخدام الإعداد [log&#95;queries=1](../../operations/settings/settings.md).

تُسجَّل الاستعلامات في جدول [system.query&#95;log](/ar/operations/system-tables/query_log)، وليس في ملف منفصل. يمكنك تغيير اسم الجدول في المعامل `table` (انظر أدناه).

<SystemLogParameters />

إذا لم يكن الجدول موجودًا، فسينشئه ClickHouse. وإذا تغيّرت بنية سجل الاستعلامات عند تحديث خادم ClickHouse، فستُعاد تسمية الجدول ذي البنية القديمة، ويُنشأ جدول جديد تلقائيًا.

**مثال**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

يكون معطّلًا بشكلٍ افتراضي.

**التفعيل**

لتفعيل جمع محفوظات metrics يدويًا [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md)، أنشئ `/etc/clickhouse-server/config.d/query_metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**تعطيل**

لتعطيل إعداد `query_metric_log`، أنشئ الملف التالي `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` بالمحتوى التالي:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

إعداد [ذاكرة التخزين المؤقت للاستعلامات](../query-cache.md).

الإعدادات التالية متاحة:

| الإعداد                   | الوصف                                                                                                       | القيمة الافتراضية |
| ------------------------- | ----------------------------------------------------------------------------------------------------------- | ----------------- |
| `max_entries`             | الحد الأقصى لعدد نتائج استعلامات `SELECT` المخزنة في ذاكرة التخزين المؤقت.                                  | `1024`            |
| `max_entry_size_in_bytes` | الحد الأقصى للحجم، بالبايت، الذي يمكن أن تبلغه نتائج استعلامات `SELECT` لكي تُحفَظ في ذاكرة التخزين المؤقت. | `1048576`         |
| `max_entry_size_in_rows`  | الحد الأقصى لعدد الصفوف التي يمكن أن تتضمنها نتائج استعلامات `SELECT` لكي تُحفَظ في ذاكرة التخزين المؤقت.   | `30000000`        |
| `max_size_in_bytes`       | الحد الأقصى لحجم ذاكرة التخزين المؤقت بالبايت. تعني القيمة `0` أن ذاكرة التخزين المؤقت للاستعلامات معطَّلة. | `1073741824`      |

:::note

* تسري الإعدادات المعدَّلة فورًا.
* تُخصَّص بيانات ذاكرة التخزين المؤقت للاستعلامات في DRAM. إذا كانت الذاكرة محدودة، فتأكد من تعيين قيمة صغيرة لـ `max_size_in_bytes` أو عطِّل ذاكرة التخزين المؤقت للاستعلامات بالكامل.
  :::

**مثال**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

إعداد لتسجيل خيوط الاستعلامات الواردة عند استخدام الإعداد [log&#95;query&#95;threads=1](/ar/operations/settings/settings#log_query_threads).

تُسجَّل الاستعلامات في جدول [system.query&#95;thread&#95;log](/ar/operations/system-tables/query_thread_log)، وليس في ملف منفصل. يمكنك تغيير اسم الجدول في المعلَمة `table` (انظر أدناه).

<SystemLogParameters />

إذا لم يكن الجدول موجودًا، فسينشئه ClickHouse. وإذا تغيّر هيكل سجل خيوط الاستعلامات عند تحديث خادم ClickHouse، فستُعاد تسمية الجدول ذي الهيكل القديم، ويُنشأ جدول جديد تلقائيًا.

**مثال**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

إعداد لتسجيل العروض (مثل live وmaterialized وغيرها) المرتبطة بالاستعلامات المستلمة عند تفعيل الإعداد [log&#95;query&#95;views=1](/ar/operations/settings/settings#log_query_views).

تُسجَّل الاستعلامات في الجدول [system.query&#95;views&#95;log](/ar/operations/system-tables/query_views_log)، وليس في ملف منفصل. يمكنك تغيير اسم الجدول في المعامل `table` (انظر أدناه).

<SystemLogParameters />

إذا لم يكن الجدول موجودًا، فسينشئه ClickHouse. وإذا تغيّرت بنية سجل عروض الاستعلامات عند تحديث خادم ClickHouse، فستُعاد تسمية الجدول ذي البنية القديمة، ويُنشأ جدول جديد تلقائيًا.

**مثال**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

إعدادات جدول النظام [text&#95;log](/ar/operations/system-tables/text_log) لتسجيل الرسائل النصية.

<SystemLogParameters />

بالإضافة إلى ذلك:

| الإعداد | الوصف                                                                            | القيمة الافتراضية |
| ------- | -------------------------------------------------------------------------------- | ----------------- |
| `level` | الحد الأقصى لمستوى الرسائل الذي سيُخزَّن في الجدول (القيمة الافتراضية: `Trace`). | `Trace`           |

**مثال**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

إعدادات عملية جدول النظام [trace&#95;log](/ar/operations/system-tables/trace_log).

<SystemLogParameters />

يحتوي ملف تهيئة الخادم الافتراضي `config.xml` على قسم الإعدادات التالي:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

إعدادات جدول النظام [asynchronous&#95;insert&#95;log](/ar/operations/system-tables/asynchronous_insert_log) لتسجيل عمليات الإدراج غير المتزامنة.

<SystemLogParameters />

**مثال**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

إعدادات عملية جدول النظام [crash&#95;log](../../operations/system-tables/crash_log.md).

يمكن تهيئة الإعدادات التالية عبر العلامات الفرعية:

| Setting                            | Description                                                                                                                         | Default             | Note                                                                                                                  |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | القيمة الحدية لعدد الأسطر. إذا تم بلوغ هذه القيمة، يبدأ تفريغ السجلات إلى القرص في الخلفية.                                         | `max_size_rows / 2` |                                                                                                                       |
| `database`                         | اسم قاعدة البيانات.                                                                                                                 |                     |                                                                                                                       |
| `engine`                           | [تعريف محرك MergeTree](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) لجدول نظام.       |                     | لا يمكن استخدامه إذا كان `partition_by` أو `order_by` معرّفًا. وإذا لم يتم تحديده، فسيتم اختيار `MergeTree` افتراضيًا |
| `flush_interval_milliseconds`      | الفاصل الزمني لتفريغ البيانات من المخزن المؤقت في الذاكرة إلى الجدول.                                                               | `7500`              |                                                                                                                       |
| `flush_on_crash`                   | يحدد ما إذا كان ينبغي تفريغ السجلات إلى القرص في حال حدوث تعطل.                                                                     | `false`             |                                                                                                                       |
| `max_size_rows`                    | الحد الأقصى لحجم السجلات بالأسطر. عندما يصل عدد السجلات غير المفرغة إلى `max_size_rows`، تُفرَّغ السجلات إلى القرص.                 | `1024`              |                                                                                                                       |
| `order_by`                         | [مفتاح فرز مخصص](/ar/engines/table-engines/mergetree-family/mergetree#order_by) لجدول نظام. لا يمكن استخدامه إذا كان `engine` معرّفًا. |                     | إذا تم تحديد `engine` لجدول نظام، فيجب تحديد المعلمة `order_by` مباشرة داخل &#39;engine&#39;                          |
| `partition_by`                     | [مفتاح تقسيم مخصص](/ar/engines/table-engines/mergetree-family/custom-partitioning-key.md) لجدول نظام.                                  |                     | إذا تم تحديد `engine` لجدول نظام، فيجب تحديد المعلمة `partition_by` مباشرة داخل &#39;engine&#39;                      |
| `reserved_size_rows`               | حجم الذاكرة المخصص مسبقًا للسجلات، بالأسطر.                                                                                         | `1024`              |                                                                                                                       |
| `settings`                         | [معلمات إضافية](/ar/engines/table-engines/mergetree-family/mergetree/#settings) تتحكم في سلوك MergeTree (اختياري).                     |                     | إذا تم تحديد `engine` لجدول نظام، فيجب تحديد المعلمة `settings` مباشرة داخل &#39;engine&#39;                          |
| `storage_policy`                   | اسم سياسة التخزين المطلوب استخدامها للجدول (اختياري).                                                                               |                     | إذا تم تحديد `engine` لجدول نظام، فيجب تحديد المعلمة `storage_policy` مباشرة داخل &#39;engine&#39;                    |
| `table`                            | اسم جدول النظام.                                                                                                                    |                     |                                                                                                                       |
| `ttl`                              | يحدد [TTL](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) للجدول.                                    |                     | إذا تم تحديد `engine` لجدول نظام، فيجب تحديد المعلمة `ttl` مباشرة داخل &#39;engine&#39;                               |

يحتوي ملف تهيئة الخادم الافتراضي `config.xml` على قسم الإعدادات التالي:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

يحدّد هذا الإعداد مسار ذاكرة التخزين المؤقت للأقراص المخصّصة المخزنة مؤقتًا (التي أُنشئت من SQL).
تكون لـ `custom_cached_disks_base_directory` أولوية أعلى للأقراص المخصّصة من `filesystem_caches_path` (الموجود في `filesystem_caches_path.xml`)،
والذي يُستخدم إذا لم يكن الأول موجودًا.
يجب أن يقع مسار إعداد ذاكرة التخزين المؤقت لنظام الملفات داخل ذلك الدليل،
وإلا فسيُطرَح استثناء يمنع إنشاء القرص.

:::note
لن يؤثر هذا في الأقراص التي أُنشئت في إصدار أقدم ثم جرت ترقية الخادم بعدها.
في هذه الحالة، لن يُطرَح استثناء، للسماح للخادم ببدء التشغيل بنجاح.
:::

مثال:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

إعدادات جدول النظام [backup&#95;log](../../operations/system-tables/backup_log.md) المخصّص لتسجيل عمليات `BACKUP` و`RESTORE`.

<SystemLogParameters />

**مثال**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

إعدادات جدول النظام [`blob_storage_log`](../system-tables/blob_storage_log.md).

<SystemLogParameters />

مثال:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

قواعد تستند إلى Regexp، تُطبَّق على الاستعلامات وعلى جميع رسائل السجل قبل تخزينها في سجلات الخادم، وفي جداول [`system.query_log`](/ar/operations/system-tables/query_log) و[`system.text_log`](/ar/operations/system-tables/text_log) و[`system.processes`](/ar/operations/system-tables/processes)، وكذلك في السجلات المُرسلة إلى العميل. يتيح ذلك منع تسرّب البيانات الحساسة من استعلامات SQL، مثل الأسماء وعناوين البريد الإلكتروني والمُعرّفات الشخصية أو أرقام بطاقات الائتمان، إلى السجلات.

**مثال**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**حقول الإعدادات**:

| الإعداد   | الوصف                                                                              |
| --------- | ---------------------------------------------------------------------------------- |
| `name`    | اسم القاعدة (اختياري)                                                              |
| `regexp`  | تعبير نمطي متوافق مع RE2 (إلزامي)                                                  |
| `replace` | سلسلة الاستبدال للبيانات الحساسة (اختيارية، والقيمة الافتراضية هي ست علامات نجمية) |

تُطبَّق قواعد إخفاء الاستعلامات على الاستعلام بالكامل (لمنع تسرّب البيانات الحساسة من الاستعلامات غير السليمة / التي يتعذّر تحليلها).

يحتوي الجدول [`system.events`](/ar/operations/system-tables/events) على العدّاد `QueryMaskingRulesMatch`، الذي يسجّل العدد الإجمالي لمطابقات قواعد إخفاء الاستعلامات.

بالنسبة إلى الاستعلامات الموزعة، يجب تكوين كل خادم على حدة، وإلا فسيتم تخزين الاستعلامات الفرعية المُمرَّرة إلى
العُقد الأخرى بدون إخفاء.

<div id="remote_servers">
  ## remote_servers
</div>

إعدادات المجموعات المستخدمة من قِبل محرك الجدول [Distributed](../../engines/table-engines/special/distributed.md) ودالة الجدول `cluster`.

**مثال**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

للحصول على قيمة السمة `incl`، راجع قسم &quot;[ملفات التهيئة](/ar/operations/configuration-files)&quot;.

**راجع أيضًا**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [اكتشاف العنقود](../../operations/cluster-discovery.md)
* [محرك قاعدة البيانات Replicated](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

قائمة بأسماء المضيفين المسموح باستخدامها في محركات التخزين المرتبطة بـ URL ودوال الجدول.

عند إضافة مضيف باستخدام وسم xml ‏`\<host\>`:

* يجب تحديده تمامًا كما هو في URL، لأن الاسم يُتحقق منه قبل DNS resolution. على سبيل المثال: `<host>clickhouse.com</host>`
* إذا جرى تحديد المنفذ صراحةً في URL، فسيُتحقق من host:port كوحدة واحدة. على سبيل المثال: `<host>clickhouse.com:80</host>`
* إذا جرى تحديد المضيف من دون منفذ، فسيُسمح بأي منفذ لهذا المضيف. على سبيل المثال: إذا جرى تحديد `<host>clickhouse.com</host>`، فسيُسمح بكل من `clickhouse.com:20` ‏(FTP) و`clickhouse.com:80` ‏(HTTP) و`clickhouse.com:443` ‏(HTTPS) وما إلى ذلك.
* إذا جرى تحديد المضيف على شكل عنوان IP، فسيُتحقق منه كما هو محدد في URL. على سبيل المثال: `[2a02:6b8:a::a]`.
* إذا وُجدت عمليات إعادة توجيه وكان دعم إعادة التوجيه مفعّلًا، فسيُتحقق من كل عملية إعادة توجيه (حقل location).

على سبيل المثال:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

المنطقة الزمنية الخاصة بالخادم.

تُحدَّد على شكل معرّف IANA للمنطقة الزمنية UTC أو لموقع جغرافي (على سبيل المثال، Africa/Abidjan).

تكون المنطقة الزمنية ضرورية للتحويل بين تنسيقات String وDateTime عند إخراج حقول DateTime بتنسيق نصي (مطبوعًا على الشاشة أو في ملف)، وكذلك عند الحصول على قيمة DateTime من سلسلة نصية. بالإضافة إلى ذلك، تُستخدَم المنطقة الزمنية في الدوال التي تعمل مع الوقت والتاريخ إذا لم تتلقَّ المنطقة الزمنية ضمن معلمات الإدخال.

**مثال**

```xml
<timezone>Asia/Istanbul</timezone>
```

**راجع أيضًا**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

المنفذ المستخدم للتواصل مع العملاء عبر بروتوكول TCP.

**مثال**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

منفذ TCP للاتصال الآمن مع العميل. استخدمه مع إعدادات [OpenSSL](#openssl).

**القيمة الافتراضية**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

المنفذ المستخدم للاتصال بالعملاء عبر بروتوكول MySQL.

:::note

* تحدد الأعداد الصحيحة الموجبة رقم المنفذ المطلوب الاستماع عليه
* تُستخدم القيم الفارغة لتعطيل الاتصال بالعملاء عبر بروتوكول MySQL.
  :::

**مثال**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

المنفذ المستخدم للاتصال بالعملاء عبر بروتوكول PostgreSQL.

:::note

* تحدد الأعداد الصحيحة الموجبة رقم المنفذ الذي يجب الاستماع عليه
* تُستخدم القيم الفارغة لتعطيل الاتصال بالعملاء عبر بروتوكول PostgreSQL.
  :::

**مثال**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

تهيئة لتحويل بادئات URL المختصرة أو الرمزية إلى عناوين URL كاملة.

مثال:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

الدليل الذي يحتوي على الملفات المعرّفة من قبل المستخدم. يُستخدم لدوال SQL المعرّفة من قبل المستخدم [دوال SQL المعرّفة من قبل المستخدم](/ar/sql-reference/functions/udf).

**مثال**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

المسار إلى الملف الذي يحتوي على:

* إعدادات المستخدمين.
* صلاحيات الوصول.
* ملفات تعريف الإعدادات.
* إعدادات الحصص.

**مثال**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

إعدادات التحسينات الاختيارية في نظام التحكم في الوصول.

| الإعداد                                         | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | الافتراضي |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `on_cluster_queries_require_cluster_grant`      | يحدّد ما إذا كانت استعلامات `ON CLUSTER` تتطلب امتياز `CLUSTER`.                                                                                                                                                                                                                                                                                                                                                                                                                          | `true`    |
| `role_cache_expiration_time_seconds`            | يحدّد عدد الثواني منذ آخر وصول التي يُحتفَظ خلالها بالدور في ذاكرة التخزين المؤقت للأدوار.                                                                                                                                                                                                                                                                                                                                                                                                | `600`     |
| `select_from_information_schema_requires_grant` | يحدّد ما إذا كان `SELECT * FROM information_schema.<table>` يتطلب أي امتيازات، أو يمكن لأي مستخدم تنفيذه. إذا ضُبطت القيمة على true، فإن هذا الاستعلام يتطلب `GRANT SELECT ON information_schema.<table>`، تمامًا كما هو الحال مع الجداول العادية.                                                                                                                                                                                                                                        | `true`    |
| `select_from_system_db_requires_grant`          | يحدّد ما إذا كان `SELECT * FROM system.<table>` يتطلب أي امتيازات، أو يمكن لأي مستخدم تنفيذه. إذا ضُبطت القيمة على true، فإن هذا الاستعلام يتطلب `GRANT SELECT ON system.<table>` تمامًا كما هو الحال مع الجداول غير التابعة للنظام. الاستثناءات: يظل عدد قليل من جداول النظام (`tables` و`columns` و`databases` وبعض الجداول الثابتة مثل `one` و`contributors`) متاحًا للجميع؛ وإذا كان امتياز `SHOW` (مثل `SHOW USERS`) ممنوحًا، فسيكون جدول النظام المقابل (أي `system.users`) متاحًا. | `true`    |
| `settings_constraints_replace_previous`         | يحدّد ما إذا كان القيد في ملف تعريف الإعدادات لإعداد معيّن سيُلغي تأثيرات القيد السابق (المعرّف في ملفات تعريف أخرى) لذلك الإعداد، بما في ذلك الحقول التي لم يضبطها القيد الجديد. كما يفعّل نوع القيد `changeable_in_readonly`.                                                                                                                                                                                                                                                           | `true`    |
| `table_engines_require_grant`                   | يحدّد ما إذا كان إنشاء جدول باستخدام محرك جدول معيّن يتطلب امتيازًا.                                                                                                                                                                                                                                                                                                                                                                                                                      | `false`   |
| `throw_on_unmatched_row_policies`               | يحدّد ما إذا كانت القراءة من جدول يجب أن تطرح استثناءً إذا كان الجدول يحتوي على سياسات صفوف، ولكن لم تكن أيٌّ منها للمستخدم الحالي                                                                                                                                                                                                                                                                                                                                                        | `false`   |
| `users_without_row_policies_can_read_rows`      | يحدّد ما إذا كان بإمكان المستخدمين الذين ليست لديهم سياسات صفوف سماحية قراءة الصفوف باستخدام استعلام `SELECT` مع ذلك. على سبيل المثال، إذا كان هناك مستخدمان A وB وكانت سياسة صفوف معرّفة فقط للمستخدم A، فعندئذ إذا كانت قيمة هذا الإعداد true، فسيرى المستخدم B جميع الصفوف. وإذا كانت قيمة هذا الإعداد false، فلن يرى المستخدم B أي صفوف.                                                                                                                                              | `true`    |

مثال:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

إعدادات جدول النظام `s3queue_log`.

<SystemLogParameters />

الإعدادات التلقائية هي:

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

إعداد خاص بجدول النظام &#39;dead&#95;letter&#95;queue&#39;.

<SystemLogParameters />

الإعدادات الافتراضية هي:

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

يحتوي هذا القسم على إعدادات تسمح لـ ClickHouse بالتفاعل مع عنقود [ZooKeeper](http://zookeeper.apache.org/). يستخدم ClickHouse ‏ZooKeeper لتخزين البيانات الوصفية الخاصة بـ replicas عند استخدام جداول مُنسخة. وإذا لم تكن جداول مُنسخة مستخدمة، فيمكن حذف هذا القسم من المعلمات.

يمكن تهيئة الإعدادات التالية باستخدام وسوم فرعية:

| الإعداد                                         | الوصف                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `node`                                          | نقطة النهاية الخاصة بـ ZooKeeper. يمكنك تعيين عدة نقاط نهاية. مثال: `<node index="1"><host>example_host</host><port>2181</port></node>`. تحدد السمة `index` ترتيب العقدة عند محاولة الاتصال بعنقود ZooKeeper.                                                                                                                                           |
| `operation_timeout_ms`                          | الحد الأقصى لـ timeout لعملية واحدة، بالـ Milliseconds.                                                                                                                                                                                                                                                                                          |
| `session_timeout_ms`                            | الحد الأقصى لـ timeout لجلسة client، بالـ Milliseconds.                                                                                                                                                                                                                                                                                          |
| `root` (optional)                               | الـ znode المستخدم كجذر لـ znodes التي يستخدمها ClickHouse server.                                                                                                                                                                                                                                                                               |
| `fallback_session_lifetime.min` (optional)      | الحد الأدنى لمدة بقاء جلسة zookeeper على عقدة fallback عندما تكون العقدة primary غير متاحة (موازنة التحميل). تُضبط بالثواني. القيمة الافتراضية: 3 ساعات.                                                                                                                                                                                         |
| `fallback_session_lifetime.max` (optional)      | الحد الأقصى لمدة بقاء جلسة zookeeper على عقدة fallback عندما تكون العقدة primary غير متاحة (موازنة التحميل). تُضبط بالثواني. القيمة الافتراضية: 6 ساعات.                                                                                                                                                                                         |
| `identity` (optional)                           | اسم المستخدم وكلمة المرور اللذان يتطلبهما ZooKeeper للوصول إلى znodes المطلوبة.                                                                                                                                                                                                                                                                  |
| `use_compression` (optional)                    | يفعّل Compression في protocol الخاص بـ Keeper إذا ضُبطت قيمته على true.                                                                                                                                                                                                                                                                          |
| `use_xid_64` (optional)                         | يفعّل معرّفات transaction ذات 64 بت. اضبطه على `true` لتمكين تنسيق معرّف transaction الموسّع. القيمة الافتراضية: `false`.                                                                                                                                                                                                                        |
| `pass_opentelemetry_tracing_context` (optional) | يفعّل تمرير سياق tracing الخاص بـ OpenTelemetry إلى requests الخاصة بـ Keeper. عند التمكين، سيتم إنشاء spans للتتبّع لعمليات Keeper، مما يتيح distributed tracing عبر ClickHouse وKeeper. راجع [Tracing ClickHouse Keeper Requests](/ar/operations/opentelemetry#tracing-clickhouse-keeper-requests) لمزيد من التفاصيل. القيمة الافتراضية: `false`. |

يوجد أيضًا الإعداد `zookeeper_load_balancing` (اختياري) الذي يتيح لك اختيار الخوارزمية المستخدمة لتحديد عقدة في ZooKeeper:

| اسم الخوارزمية                   | الوصف                                                                                                                        |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `random`                         | يختار عشوائيًا واحدة من عُقد ZooKeeper.                                                                           |
| `in_order`                       | يختار أول عقدة في ZooKeeper، وإذا لم تكن متاحة فالثانية، وهكذا.                                                              |
| `nearest_hostname`               | يختار عقدة في ZooKeeper يكون hostname الخاص بها الأكثر تشابهًا مع hostname الخاص بالخادم، وتتم مقارنة hostname ببادئة الاسم. |
| `hostname_levenshtein_distance`  | مثل `nearest_hostname`، لكنه يقارن hostname باستخدام مسافة Levenshtein distance.                                             |
| `hostname_longest_common_prefix` | مثل `nearest_hostname`، لكنه يفضّل عقدة التي يشترك hostname الخاص بها مع hostname الخاص بالخادم في أطول prefix مشترك.        |
| `hostname_longest_common_suffix` | مثل `nearest_hostname`، لكنه يفضّل عقدة التي يشترك hostname الخاص بها مع hostname الخاص بالخادم في أطول suffix مشترك.        |
| `first_or_random`                | يختار أول عقدة في ZooKeeper، وإذا لم تكن متاحة يختار عشوائيًا واحدة من عُقد المتبقية في ZooKeeper.                          |
| `round_robin`                    | يختار أول عقدة في ZooKeeper، وإذا حدثت إعادة اتصال يختار العقدة التالية.                                                       |

**Example configuration**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**راجع أيضًا**

* [النسخ المتماثل](../../engines/table-engines/mergetree-family/replication.md)
* [دليل المبرمج لـ ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [اتصال آمن اختياري بين ClickHouse وZooKeeper](/ar/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

طريقة تخزين رؤوس أجزاء البيانات في ZooKeeper. لا ينطبق هذا الإعداد إلا على عائلة [`MergeTree`](/ar/engines/table-engines/mergetree-family). ويمكن تحديده كما يلي:

**على مستوى عام في قسم [merge&#95;tree](#merge_tree) من ملف `config.xml`**

يستخدم ClickHouse هذا الإعداد لجميع الجداول على الخادم. يمكنك تغيير الإعداد في أي وقت. وتغيّر الجداول الحالية سلوكها عند تغيير هذا الإعداد.

**لكل جدول**

عند إنشاء جدول، حدِّد [إعداد المحرك](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) المقابل. ولا يتغيّر سلوك أي جدول موجود يستخدم هذا الإعداد، حتى إذا تغيّر الإعداد العام.

**القيم الممكنة**

* `0` — تكون هذه الوظيفة معطلة.
* `1` — تكون هذه الوظيفة مفعلة.

إذا كانت قيمة [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper)، فإن الجداول [المُنسخة](../../engines/table-engines/mergetree-family/replication.md) تخزّن رؤوس أجزاء البيانات بصورة مضغوطة باستخدام `znode` واحد. وإذا كان الجدول يحتوي على عدد كبير من الأعمدة، فإن طريقة التخزين هذه تقلّل بشكل كبير حجم البيانات المخزّنة في ZooKeeper.

:::note
بعد تطبيق `use_minimalistic_part_header_in_zookeeper = 1`، لا يمكنك الرجوع بإصدار خادم ClickHouse إلى إصدار لا يدعم هذا الإعداد. توخَّ الحذر عند ترقية ClickHouse على الخوادم داخل عنقود. لا تُرقِّ جميع الخوادم دفعة واحدة. ومن الأكثر أمانًا اختبار الإصدارات الجديدة من ClickHouse في بيئة اختبار، أو على عدد قليل فقط من خوادم العنقود.

لا يمكن استعادة رؤوس أجزاء البيانات المخزّنة مسبقًا باستخدام هذا الإعداد إلى تمثيلها السابق (غير المضغوط).
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

إدارة تنفيذ [استعلامات DDL الموزعة](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) على العنقود.
لا يعمل إلا إذا كان [ZooKeeper](/ar/operations/server-configuration-parameters/settings#zookeeper) ممكّنًا.

تتضمن الإعدادات القابلة للتهيئة ضمن `<distributed_ddl>` ما يلي:

| الإعداد                | الوصف                                                                                                                | القيمة الافتراضية                   |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `cleanup_delay_period` | يبدأ التنظيف بعد تلقّي حدث عقدة جديدة إذا لم تكن آخر عملية تنظيف قد أُجريت خلال أقل من `cleanup_delay_period` ثانية. | `60` ثانية                          |
| `max_tasks_in_queue`   | الحد الأقصى لعدد المهام التي يمكن أن تكون في قائمة الانتظار.                                                         | `1,000`                             |
| `path`                 | المسار في Keeper لـ `task_queue` الخاص باستعلامات DDL                                                                |                                     |
| `pool_size`            | عدد استعلامات `ON CLUSTER` التي يمكن تشغيلها بالتزامن                                                                |                                     |
| `profile`              | ملف التعريف المستخدم لتنفيذ استعلامات DDL                                                                            |                                     |
| `task_max_lifetime`    | احذف العقدة إذا كان عمرها أكبر من هذه القيمة.                                                                        | `7 * 24 * 60 * 60` (أسبوع بالثواني) |

**مثال**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

مسار المجلد الذي يخزّن فيه خادم ClickHouse إعدادات المستخدمين والأدوار التي أُنشئت بأوامر SQL.

**انظر أيضًا**

* [التحكم في الوصول وإدارة الحسابات](/ar/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

يحدد ما إذا كان يُسمح بأنواع كلمات المرور ذات النص الصريح (غير الآمنة) أم لا.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

يحدد ما إذا كان يُسمح باستخدام نوع كلمة المرور غير الآمن `no&#95;password` أم لا.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

يمنع إنشاء مستخدم من دون كلمة مرور ما لم يُحدَّد &#39;IDENTIFIED WITH no&#95;password&#39; صراحةً.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

مهلة انتهاء الجلسة الافتراضية، بالثواني.

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

يحدد نوع كلمة المرور الذي يُضبط تلقائيًا في استعلامات مثل `CREATE USER u IDENTIFIED BY 'p'`.

القيم المقبولة هي:

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

قسم في ملف التكوين يحتوي على الإعدادات التالية:

* المسار إلى ملف التكوين الذي يحتوي على مستخدمين معرَّفين مسبقًا.
* المسار إلى المجلد الذي يُخزَّن فيه المستخدمون الذين أُنشِئوا بواسطة أوامر SQL.
* مسار عقدة ZooKeeper الذي يُخزَّن فيه المستخدمون الذين أُنشِئوا بواسطة أوامر SQL وتتم مضاهاة بياناتهم.

إذا تم تحديد هذا القسم، فلن يُستخدم المسار من [users&#95;config](/ar/operations/server-configuration-parameters/settings#users_config) و[access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path).

يمكن أن يحتوي القسم `user_directories` على أي عدد من العناصر، ويعني ترتيب العناصر فيها مستوى الأولوية (كلما كان العنصر أعلى، كانت أولويته أعلى).

**أمثلة**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

يمكن أيضًا تخزين المستخدمين والأدوار وسياسات الصفوف والحصص وملفات التعريف في ZooKeeper:

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

يمكنك أيضًا تعريف القسمين `memory` — أي تخزين المعلومات في الذاكرة فقط من دون كتابتها على القرص — و`ldap` — أي تخزين المعلومات على خادم LDAP.

لإضافة خادم LDAP كدليل مستخدمين بعيد للمستخدمين غير المعرَّفين محليًا، عرّف قسم `ldap` واحدًا بالإعدادات التالية:

| الإعداد  | الوصف                                                                                                                                                                                                                                                                                                          |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`  | قسم يتضمن قائمة بالأدوار المعرَّفة محليًا التي ستُسنَد إلى كل مستخدم يُسترجَع من خادم LDAP. إذا لم تُحدَّد أي أدوار، فلن يتمكن المستخدم من تنفيذ أي إجراءات بعد المصادقة. وإذا كان أي دور من الأدوار المدرجة غير معرَّف محليًا وقت المصادقة، فستفشل محاولة المصادقة كما لو أن كلمة المرور المقدَّمة غير صحيحة. |
| `server` | أحد أسماء خوادم LDAP المعرَّفة في قسم الإعداد `ldap_servers`. هذه المعلمة إلزامية ولا يمكن أن تكون فارغة.                                                                                                                                                                                                      |

**مثال**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

يحدّد قائمةً بنطاقات المستوى الأعلى المخصّصة المراد إضافتها، بحيث يكون كل إدخال بالتنسيق `<name>/path/to/file</name>`.

على سبيل المثال:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

انظر أيضًا:

* الدالة [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) والأشكال المختلفة منها،
  التي تقبل اسم قائمة TLD مخصّصة، وتُرجع جزء النطاق الذي يشمل النطاقات الفرعية العليا حتى أول نطاق فرعي ذي دلالة.

<div id="proxy">
  ## الوكيل
</div>

حدِّد خوادم وكيلة لطلبات HTTP وHTTPS، وهي مدعومة حاليًا في تخزين S3، ودوال table function لـ S3، ودوال URL.

هناك ثلاث طرق لتعريف الخوادم الوكيلة:

* متغيرات البيئة
* قوائم الوكيل
* مُحلِّلات الوكيل البعيدة.

يُدعَم أيضًا تجاوز الخوادم الوكيلة لمضيفين محددين باستخدام `no_proxy`.

**متغيرات البيئة**

تتيح لك متغيرات البيئة `http_proxy` و`https_proxy` تحديد
خادم وكيل لبروتوكول معيّن. إذا كان معيّنًا على نظامك، فيُفترض أن يعمل بسلاسة.

هذا هو النهج الأبسط إذا كان لبروتوكول معيّن
خادم وكيل واحد فقط، وكان هذا الخادم لا يتغيّر.

**قوائم الوكيل**

يتيح لك هذا النهج تحديد خادم وكيل واحد أو أكثر
لبروتوكول معيّن. وإذا جرى تعريف أكثر من خادم وكيل واحد،
يستخدم ClickHouse الخوادم الوكيلة المختلفة بأسلوب round-robin، مع توزيع
الحمل على الخوادم. وهذا هو النهج الأبسط إذا كان هناك أكثر من
خادم وكيل واحد لبروتوكول معيّن، وكانت قائمة الخوادم الوكيلة لا تتغيّر.

**قالب الإعداد**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

اختر حقلًا أبًا من علامات التبويب أدناه لعرض حقوله الفرعية:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | الحقل     | الوصف                               |
    | --------- | ----------------------------------- |
    | `<http>`  | قائمة تضم وكيل HTTP واحدًا أو أكثر  |
    | `<https>` | قائمة تضم وكيل HTTPS واحدًا أو أكثر |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | الحقل   | الوصف                   |
    | ------- | ----------------------- |
    | `<uri>` | معرّف URI الخاص بالوكيل |
  </TabItem>
</Tabs>

**مُحلِّلات الوكيل البعيدة**

قد تتغير خوادم الوكيل ديناميكيًا. في هذه
الحالة، يمكنك تحديد نقطة النهاية الخاصة بمُحلِّل. يرسل ClickHouse
طلب GET فارغًا إلى نقطة النهاية تلك، ويُفترض أن يعيد المُحلِّل البعيد مضيف الوكيل.
وسيستخدمه ClickHouse لتكوين URI الخاص بالوكيل باستخدام القالب التالي: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**قالب التكوين**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

حدِّد حقلاً أبًا في علامات التبويب أدناه لعرض حقوله التابعة:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | الحقل     | الوصف                                   |
    | --------- | --------------------------------------- |
    | `<http>`  | قائمة تضم مُحلِّلًا واحدًا أو أكثر* |
    | `<https>` | قائمة تضم مُحلِّلًا واحدًا أو أكثر* |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | الحقل        | الوصف                                   |
    | ------------ | --------------------------------------- |
    | `<resolver>` | نقطة النهاية وتفاصيل أخرى خاصة بمُحلِّل |

    :::note
    يمكنك استخدام عدة عناصر `<resolver>`، ولكن لا يُستخدم إلا أول
    عنصر `<resolver>` لبروتوكول معيّن. وتُتجاهل أي عناصر `<resolver>`
    أخرى لذلك البروتوكول. وهذا يعني أن موازنة التحميل
    (عند الحاجة) ينبغي أن ينفّذها المُحلِّل البعيد.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | الحقل                | الوصف                                                                                                                                                                         |
    | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `<endpoint>`         | عنوان URI الخاص بمُحلِّل الوكيل                                                                                                                                               |
    | `<proxy_scheme>`     | بروتوكول URI النهائي للوكيل. يمكن أن يكون `http` أو `https` فقط.                                                                                                              |
    | `<proxy_port>`       | رقم منفذ مُحلِّل الوكيل                                                                                                                                                       |
    | `<proxy_cache_time>` | المدة، بالثواني، التي يجب أن يخزّن ClickHouse خلالها القيم الواردة من المُحلِّل مؤقتًا. ويؤدي ضبط هذه القيمة على `0` إلى أن يتصل ClickHouse بالمُحلِّل لكل طلب HTTP أو HTTPS. |
  </TabItem>
</Tabs>

**الأسبقية**

تُحدَّد إعدادات الوكيل بالترتيب التالي:

| الترتيب | الإعداد                  |
| ------- | ------------------------ |
| 1.      | مُحلِّلات الوكيل البعيدة |
| 2.      | قوائم الوكيل             |
| 3.      | متغيرات البيئة           |

سيتحقق ClickHouse من نوع المُحلِّل ذي الأولوية الأعلى لبروتوكول الطلب. وإذا لم يكن معرّفًا،
فسيتحقق من نوع المُحلِّل التالي في الأولوية، حتى يصل إلى مُحلِّل البيئة.
ويتيح هذا أيضًا استخدام مزيج من أنواع المُحلِّلات.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

افتراضيًا، يُستخدم الاتصال النفقي (أي `HTTP CONNECT`) لإرسال طلبات `HTTPS` عبر وكيل `HTTP`. يمكن استخدام هذا الإعداد لتعطيل ذلك.

**no&#95;proxy**

افتراضيًا، تمر جميع الطلبات عبر الوكيل. ولتعطيل ذلك بالنسبة إلى مضيفات معيّنة، يجب ضبط المتغيّر `no_proxy`.
يمكن ضبطه داخل العبارة `<proxy>` لمحلِّلات القوائم والمحلِّلات البعيدة، أو كمتغيّر بيئة لمحلِّل البيئة.
وهو يدعم عناوين IP والنطاقات والنطاقات الفرعية وحرف البدل `'*'` لتجاوز الوكيل بالكامل. كما تُزال النقاط البادئة تمامًا كما يفعل curl.

**مثال**

يضبط الإعداد أدناه تجاوز الوكيل للطلبات الموجَّهة إلى `clickhouse.cloud` وجميع نطاقاته الفرعية (مثل `auth.clickhouse.cloud`).
وينطبق الأمر نفسه على GitLab، رغم احتوائه على نقطة بادئة. لذا سيتجاوز كلٌّ من `gitlab.com` و`about.gitlab.com` الوكيل.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

الدليل المستخدم لتخزين جميع استعلامات `CREATE WORKLOAD` و`CREATE RESOURCE`. وبشكلٍ افتراضي، يُستخدم المجلد `/workload/` ضمن دليل عمل الخادم.

**مثال**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**انظر أيضًا**

* [التسلسل الهرمي لأعباء العمل](/ar/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

المسار إلى عقدة في ZooKeeper، ويُستخدم كموقع تخزين لجميع استعلامات `CREATE WORKLOAD` و`CREATE RESOURCE`. ولضمان الاتساق، تُخزَّن جميع تعريفات SQL كقيمة لهذه العقدة znode الواحدة. بشكل افتراضي، لا يُستخدم ZooKeeper، وتُخزَّن التعريفات على [القرص](#workload_path).

**مثال**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**انظر أيضًا**

* [التسلسل الهرمي لأعباء العمل](/ar/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

إعدادات جدول النظام [`zookeeper_log`](/ar/operations/system-tables/zookeeper_log).

يمكن ضبط الإعدادات التالية عبر العلامات الفرعية:

<SystemLogParameters />

**مثال**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```