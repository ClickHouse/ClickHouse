---
title: Настройки сервера вне Source
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

По умолчанию включен в ClickHouse Cloud.

Если в вашей среде этот параметр не включен по умолчанию, то в зависимости от способа установки ClickHouse вы можете воспользоваться приведенной ниже инструкцией, чтобы включить или отключить его.

**Включение**

Чтобы вручную включить сбор истории асинхронных журналов метрик [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), создайте файл `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` со следующим содержимым:

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

**Отключение**

Чтобы отключить настройку `asynchronous_metric_log`, создайте файл `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` со следующим содержимым:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

Использовать исходный адрес для аутентификации клиентов, подключённых через прокси.

:::note
Этот параметр следует использовать с особой осторожностью, поскольку передаваемые адреса легко подделать: серверы, принимающие такую аутентификацию, не должны быть доступны напрямую и должны обслуживаться только через доверенный прокси.
:::

<div id="backups">
  ## резервные копии
</div>

Настройки резервного копирования, используемые при выполнении команд [`BACKUP` и `RESTORE`](/ru/operations/backup/overview).

Следующие настройки можно задать через под-теги:

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Определяет, могут ли несколько операций резервного копирования выполняться одновременно на одном хосте.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Определяет, могут ли несколько операций восстановления выполняться одновременно на одном хосте.', 'true'),
    ('allowed_disk', 'String', 'Диск, на который выполняется резервное копирование при использовании `File()`. Чтобы использовать `File`, этот параметр должен быть задан.', ''),
    ('allowed_path', 'String', 'Путь, по которому выполняется резервное копирование при использовании `File()`. Чтобы использовать `File`, этот параметр должен быть задан.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Количество попыток собрать метаданные перед паузой в случае несогласованности, обнаруженной после сравнения собранных метаданных.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Тайм-аут в миллисекундах для сбора метаданных во время резервного копирования.', '600000'),
    ('compare_collected_metadata', 'Bool', 'Если true, сравнивает собранные метаданные с существующими, чтобы убедиться, что они не изменились во время резервного копирования.', 'true'),
    ('create_table_timeout', 'UInt64', 'Тайм-аут в миллисекундах на создание таблиц во время восстановления.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Максимальное количество повторных попыток после возникновения ошибки bad version во время координируемого резервного копирования/восстановления.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Максимальная длительность паузы в миллисекундах перед следующей попыткой сбора метаданных.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Минимальная длительность паузы в миллисекундах перед следующей попыткой сбора метаданных.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'Если команда `BACKUP` завершится ошибкой, ClickHouse попытается удалить файлы, уже скопированные в резервную копию до сбоя; в противном случае скопированные файлы будут оставлены как есть.', 'true'),
    ('sync_period_ms', 'UInt64', 'Период синхронизации в миллисекундах для координируемого резервного копирования/восстановления.', '5000'),
    ('test_inject_sleep', 'Bool', 'Пауза для тестирования', 'false'),
    ('test_randomize_order', 'Bool', 'Если true, случайным образом меняет порядок некоторых операций в целях тестирования.', 'false'),
    ('zookeeper_path', 'String', 'Путь в ZooKeeper, где хранятся метаданные резервного копирования и восстановления при использовании предложения `ON CLUSTER`.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Настройка, t.2 AS Тип, t.3 AS Описание, concat('`', t.4, '`') AS По_умолчанию FROM settings FORMAT Markdown
  */ }

| Настройка                                           | Тип    | Описание                                                                                                                                                                                | По умолчанию          |
| :-------------------------------------------------- | :----- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | Определяет, могут ли несколько операций резервного копирования выполняться одновременно на одном хосте.                                                                                 | `true`                |
| `allow_concurrent_restores`                         | Bool   | Определяет, могут ли несколько операций восстановления выполняться одновременно на одном хосте.                                                                                         | `true`                |
| `allowed_disk`                                      | String | Диск для резервного копирования при использовании `File()`. Чтобы использовать `File`, этот параметр должен быть задан.                                                                 | &#96;&#96;            |
| `allowed_path`                                      | String | Путь для резервного копирования при использовании `File()`. Чтобы использовать `File`, этот параметр должен быть задан.                                                                 | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | Количество попыток сбора метаданных перед паузой в случае несоответствия после сравнения собранных метаданных.                                                                          | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | Тайм-аут в миллисекундах на сбор метаданных во время резервного копирования.                                                                                                            | `600000`              |
| `compare_collected_metadata`                        | Bool   | Если `true`, сравнивает собранные метаданные с существующими, чтобы убедиться, что они не изменились во время резервного копирования.                                                   | `true`                |
| `create_table_timeout`                              | UInt64 | Тайм-аут в миллисекундах на создание таблиц во время восстановления.                                                                                                                    | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | Максимальное количество повторных попыток после возникновения ошибки bad version во время координируемого резервного копирования или восстановления.                                    | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Максимальная длительность паузы в миллисекундах перед следующей попыткой сбора метаданных.                                                                                              | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Минимальная длительность паузы в миллисекундах перед следующей попыткой сбора метаданных.                                                                                               | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | Если команда `BACKUP` завершается ошибкой, ClickHouse попытается удалить файлы, уже скопированные в резервную копию до сбоя; в противном случае скопированные файлы останутся как есть. | `true`                |
| `sync_period_ms`                                    | UInt64 | Период синхронизации в миллисекундах для координируемого резервного копирования или восстановления.                                                                                     | `5000`                |
| `test_inject_sleep`                                 | Bool   | Пауза для тестирования                                                                                                                                                                  | `false`               |
| `test_randomize_order`                              | Bool   | Если `true`, случайным образом меняет порядок некоторых операций в целях тестирования.                                                                                                  | `false`               |
| `zookeeper_path`                                    | String | Путь в ZooKeeper, где хранятся метаданные резервного копирования и восстановления при использовании предложения `ON CLUSTER`.                                                           | `/clickhouse/backups` |

По умолчанию этот параметр настроен следующим образом:

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

Содержит информацию обо всех фоновых задачах, выполняемых в различных фоновых пулах.

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

Параметр сложности для типа аутентификации `bcrypt_password`, использующего [алгоритм Bcrypt](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
Этот параметр определяет объём вычислений и время, необходимые для вычисления хэша и проверки пароля.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
Для приложений с частой аутентификацией
рассмотрите альтернативные методы аутентификации из-за
высокой вычислительной нагрузки bcrypt при больших значениях work factor.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

Если установлено значение true, для создания таблицы с определённым движком пользователям требуется grant, например `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
По умолчанию для обратной совместимости при создании таблицы с определённым движком таблицы grant игнорируется, однако это поведение можно изменить, установив этот параметр в true.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

Интервал в секундах до перезагрузки встроенных словарей.

ClickHouse перезагружает встроенные словари каждые x секунд. Это позволяет изменять словари &quot;на лету&quot; без перезапуска сервера.

**Пример**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## сжатие
</div>

Настройки сжатия данных для таблиц на движке [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

:::note
Если вы только начали работать с ClickHouse, рекомендуем не менять этот параметр.
:::

**Шаблон конфигурации**:

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

**Поля `<case>`**:

* `min_part_size` – Минимальный размер части данных.
* `min_part_size_ratio` – Отношение размера части данных к размеру таблицы.
* `method` – Метод сжатия. Допустимые значения: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – Уровень сжатия. См. [Кодеки](/ru/sql-reference/statements/create/table#general-purpose-codecs).

:::note
Можно настроить несколько секций `<case>`.
:::

**Действия при выполнении условий**:

* Если часть данных соответствует набору условий, ClickHouse использует указанный метод сжатия.
* Если часть данных соответствует нескольким наборам условий, ClickHouse использует первый подходящий набор условий.

:::note
Если для части данных не выполнено ни одно условие, ClickHouse использует сжатие `lz4`.
:::

**Пример**

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
  ## encryption
</div>

Настраивает команду для получения ключа, который будет использоваться [кодеками шифрования](/ru/sql-reference/statements/create/table#encryption-codecs). Ключ (или ключи) следует записать в переменные окружения или задать в конфигурационном файле.

Ключи могут быть в шестнадцатеричном формате или в виде строки длиной 16 байт.

**Пример**

Загрузка из конфигурации:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Не рекомендуется хранить ключи в файле конфигурации. Это небезопасно. Ключи можно вынести в отдельный файл конфигурации на защищённом диске и поместить символическую ссылку на него в папку `config.d/`.
:::

Загрузка из конфигурации, если ключ указан в шестнадцатеричном виде:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Загрузка ключа из переменной окружения:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Здесь `current_key_id` задаёт текущий ключ для шифрования, а все указанные ключи могут использоваться для расшифровки.

Каждый из этих методов можно применять к нескольким ключам:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Здесь `current_key_id` обозначает текущий ключ шифрования.

Кроме того, пользователи могут задать `nonce` длиной 12 байт (по умолчанию в процессах шифрования и дешифрования используется `nonce`, состоящий из нулевых байтов):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Или его можно задать в шестнадцатеричном виде:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Всё, что упомянуто выше, можно применить и к `aes_256_gcm_siv` (но длина ключа должна составлять 32 байта).
:::

<div id="error_log">
  ## error_log
</div>

По умолчанию он отключён.

**Включение**

Чтобы вручную включить сбор истории ошибок [`system.error_log`](../../operations/system-tables/error_log.md), создайте файл `/etc/clickhouse-server/config.d/error_log.xml` со следующим содержимым:

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

**Отключение**

Чтобы отключить параметр `error_log`, создайте файл `/etc/clickhouse-server/config.d/disable_error_log.xml` со следующим содержимым:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

Список префиксов, используемых для [пользовательских настроек](/ru/operations/settings/query-level#custom_settings).
Если префиксов несколько, их следует разделять запятыми.

**Пример**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**См. также**

* [Пользовательские настройки](/ru/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

Задаёт мягкое ограничение на размер файла дампа памяти.

:::note
Жёсткое ограничение настраивается с помощью системных инструментов
:::

**Пример**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

Профиль настроек по умолчанию. Профили настроек находятся в файле, указанном в параметре `user_config`.

**Пример**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

Путь к файлу конфигурации словарей.

Путь:

* Укажите абсолютный путь или путь относительно файла конфигурации сервера.
* Путь может содержать подстановочные шаблоны * и ?.

См. также:

* &quot;[Словари](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**Пример**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

Путь к файлу конфигурации исполняемых пользовательских функций.

Путь:

* Укажите абсолютный путь или путь относительно файла конфигурации сервера.
* Путь может содержать подстановочные шаблоны * и ?.

См. также:

* &quot;[Исполняемые пользовательские функции](/ru/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**Пример**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

Отправка данных в [Graphite](https://github.com/graphite-project).

Настройки:

* `host` – Сервер Graphite.
* `port` – Порт сервера Graphite.
* `interval` – Интервал отправки в секундах.
* `timeout` – Тайм-аут отправки данных в секундах.
* `root_path` – Префикс для ключей.
* `metrics` – Отправка данных из таблицы [system.metrics](/ru/operations/system-tables/metrics).
* `events` – Отправка данных о дельтах, накопленных за указанный период времени, из таблицы [system.events](/ru/operations/system-tables/events).
* `events_cumulative` – Отправка накопительных данных из таблицы [system.events](/ru/operations/system-tables/events).
* `asynchronous_metrics` – Отправка данных из таблицы [system.asynchronous&#95;metrics](/ru/operations/system-tables/asynchronous_metrics).

Можно настроить несколько секций `<graphite>`. Например, чтобы отправлять разные данные с разными интервалами.

**Пример**

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

Настройки прореживания данных для Graphite.

Подробнее см. [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Пример**

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

Позволяет использовать пользовательские HTTP-обработчики.
Чтобы добавить новый HTTP-обработчик, просто добавьте новое `<rule>`.
Правила проверяются сверху вниз в заданном порядке,
и при первом совпадении запускается соответствующий обработчик.
Правило без условий совпадения (только `handler`) подходит для любого запроса; поскольку правила проверяются по порядку,
такое правило имеет смысл использовать только как последний резервный вариант.

Следующие настройки можно задать с помощью под-тегов (все эти под-теги необязательны, кроме `handler`):

| Под-теги             | Определение                                                                                                                                                                                                                                                        |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `url`                | Для сопоставления с путём URL запроса. При сопоставлении строка запроса игнорируется                                                                                                                                                                               |
| `url_prefix`         | Для сопоставления пути URL запроса с базовым путём: самим путём или любым путём ниже него по границе сегмента пути (например, &#39;/api/v1&#39; соответствует /api/v1, /api/v1/ и /api/v1/write, но не /api/v1beta). При сопоставлении строка запроса игнорируется |
| `url_regexp`         | Для сопоставления пути URL запроса с регулярным выражением. При сопоставлении строка запроса игнорируется                                                                                                                                                          |
| `full_url`           | Для сопоставления полного URL запроса `scheme://host:port/path`. При сопоставлении строка запроса игнорируется, а в качестве хоста используется IP-адрес соединения (а не заголовок `Host`)                                                                        |
| `full_url_prefix`    | Для сопоставления полного URL запроса `scheme://host:port/path` с базовым URL `scheme://host:port/base_path` по границе сегмента пути (см. `url_prefix`). При сопоставлении строка запроса игнорируется                                                            |
| `full_url_regexp`    | Для сопоставления полного URL запроса `scheme://host:port/path` с регулярным выражением. При сопоставлении строка запроса игнорируется                                                                                                                             |
| `methods`            | Для сопоставления методов запроса; несколько методов можно указать через запятую                                                                                                                                                                                   |
| `headers`            | Для сопоставления заголовков запроса; сопоставляется каждый дочерний элемент (имя дочернего элемента — это имя заголовка)                                                                                                                                          |
| `headers_regexp`     | Как `headers`, но значение каждого дочернего элемента сопоставляется с регулярным выражением                                                                                                                                                                       |
| `empty_query_string` | Проверяет, что в URL отсутствует строка запроса                                                                                                                                                                                                                    |
| `handler`            | Обработчик запроса (обязательно)                                                                                                                                                                                                                                   |

:::note
Вместо `url_regexp`, `full_url_regexp` и `headers_regexp` вы также можете указать регулярное выражение в `url`, `full_url` или `headers`, используя префикс `regex:` (например, `<url>regex:/api/.*</url>`). Это по-прежнему поддерживается для обратной совместимости, но считается устаревшим вариантом: предпочтительнее использовать отдельные под-теги `url_regexp`, `full_url_regexp` и `headers_regexp`.
:::

`handler` содержит следующие настройки, которые можно задать с помощью под-тегов:

| Под-теги           | Определение                                                                                                                                                                                                    |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | URL для перенаправления                                                                                                                                                                                        |
| `type`             | Поддерживаемые типы: static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                                                                                         |
| `status`           | Используется с типом static; код состояния ответа                                                                                                                                                              |
| `query_param_name` | Используется с типом dynamic&#95;query&#95;handler; извлекает и выполняет значение, соответствующее значению `<query_param_name>` в params HTTP-запроса                                                        |
| `query`            | Используется с типом predefined&#95;query&#95;handler; выполняет запрос при вызове обработчика                                                                                                                 |
| `content_type`     | Используется с типом static; content-type ответа                                                                                                                                                               |
| `response_content` | Используется с типом static; содержимое ответа, отправляемое клиенту. При использовании префикса &#39;file://&#39; или &#39;config://&#39; содержимое берётся из файла или конфигурации и отправляется клиенту |

Вместе со списком правил можно указать `<defaults/>`, чтобы включить все обработчики по умолчанию.

Пример:

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

Страница, которая по умолчанию отображается при обращении к HTTP(s)-серверу ClickHouse.
Значение по умолчанию — &quot;Ok.&quot; (с переводом строки в конце)

**Пример**

Открывает `https://tabix.io/` при обращении к `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

Используется для добавления заголовков в ответ на HTTP request `OPTIONS`.
Метод `OPTIONS` используется при выполнении CORS Preflight-запросов.

Дополнительные сведения см. в [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Пример:

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

Время действия HSTS в секундах.

:::note
Значение `0` означает, что ClickHouse отключает HSTS. Если указать положительное число, HSTS будет включен, а `max-age` будет равен указанному значению.
:::

**Пример**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

Ограничение на список хостов, которым разрешён обмен данными между серверами ClickHouse.
Если используется Keeper, это же ограничение применяется к обмену данными между разными экземплярами Keeper.

:::note
По умолчанию значение совпадает с настройкой [`listen_host`](#listen_host).
:::

**Пример**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Тип:

default:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

Имя пользователя и пароль, используемые для подключения к другим серверам во время [репликации](../../engines/table-engines/mergetree-family/replication.md). Кроме того, сервер аутентифицирует другие реплики по этим учетным данным.
Поэтому `interserver_http_credentials` должны быть одинаковыми для всех реплик в кластере.

:::note

* По умолчанию, если раздел `interserver_http_credentials` отсутствует, аутентификация во время репликации не используется.
* Параметры `interserver_http_credentials` не относятся к [конфигурации](../../interfaces/client.md#configuration_files) учетных данных клиента ClickHouse.
* Эти учетные данные используются одновременно для репликации по `HTTP` и `HTTPS`.
  :::

Следующие параметры можно настроить с помощью вложенных тегов:

* `user` — Имя пользователя.
* `password` — Пароль.
* `allow_empty` — Если `true`, другим репликам разрешается подключаться без аутентификации, даже если учетные данные заданы. Если `false`, подключения без аутентификации отклоняются. Значение по умолчанию: `false`.
* `old` — Содержит прежние `user` и `password`, используемые при ротации учетных данных. Можно указать несколько секций `old`.

**Ротация учетных данных**

ClickHouse поддерживает динамическую ротацию межсерверных учетных данных без необходимости одновременно останавливать все реплики для обновления их конфигурации. Учетные данные можно менять в несколько этапов.

Чтобы включить аутентификацию, установите `interserver_http_credentials.allow_empty` в `true` и добавьте учетные данные. Это разрешит подключения как с аутентификацией, так и без нее.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

После настройки всех реплик установите `allow_empty` в `false` или удалите этот параметр. Это сделает обязательной аутентификацию с использованием новых учетных данных.

Чтобы изменить существующие учетные данные, переместите имя пользователя и пароль в раздел `interserver_http_credentials.old` и обновите `user` и `password`, указав новые значения. На этом этапе сервер использует новые учетные данные для подключения к другим репликам и принимает подключения как с новыми, так и со старыми учетными данными.

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

После применения новых учетных данных на всех репликах старые учетные данные можно удалить.

<div id="ldap_servers">
  ## ldap_servers
</div>

Здесь перечисляются LDAP-серверы и параметры подключения к ним, чтобы:

* использовать их как аутентификаторы для выделенных локальных пользователей, у которых вместо `password` указан механизм аутентификации `ldap`
* использовать их как удалённые каталоги пользователей.

Следующие настройки можно задать с помощью вложенных тегов:

| Setting                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | Шаблон, используемый для формирования DN для bind. Итоговый DN формируется заменой всех подстрок `\{user_name\}` в шаблоне на фактическое имя пользователя при каждой попытке аутентификации.                                                                                                                                                                                                                                                      |
| `enable_tls`                   | Флаг, включающий использование защищённого соединения с LDAP-сервером. Укажите `no` для протокола в открытом виде (`ldap://`) (не рекомендуется). Укажите `yes` для протокола LDAP поверх SSL/TLS (`ldaps://`) (рекомендуется, значение по умолчанию). Укажите `starttls` для устаревшего протокола StartTLS (протокол в открытом виде (`ldap://`), повышаемый до TLS).                                                                            |
| `host`                         | Имя хоста LDAP-сервера или IP-адрес; этот параметр обязателен и не может быть пустым.                                                                                                                                                                                                                                                                                                                                                              |
| `port`                         | Порт LDAP-сервера; по умолчанию используется 636, если `enable_tls` имеет значение true, иначе — `389`.                                                                                                                                                                                                                                                                                                                                            |
| `tls_ca_cert_dir`              | путь к каталогу, содержащему CA‑сертификаты.                                                                                                                                                                                                                                                                                                                                                                                                       |
| `tls_ca_cert_file`             | путь к файлу CA‑сертификата.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `tls_cert_file`                | путь к файлу сертификата.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `tls_cipher_suite`             | разрешённый набор шифров (в нотации OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_key_file`                 | путь к файлу ключа сертификата.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `tls_minimum_protocol_version` | Минимальная версия протокола SSL/TLS. Допустимые значения: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (по умолчанию).                                                                                                                                                                                                                                                                                                                            |
| `tls_require_cert`             | Поведение проверки сертификата узла SSL/TLS. Допустимые значения: `never`, `allow`, `try`, `demand` (по умолчанию).                                                                                                                                                                                                                                                                                                                                |
| `user_dn_detection`            | Раздел с параметрами LDAP search для определения фактического user DN привязанного пользователя. В основном используется в search filter для дальнейшего role mapping, если сервером является Active Directory. Полученный user DN будет использоваться при замене подстрок `\{user_dn\}` везде, где это допустимо. По умолчанию user DN равен bind DN, но после выполнения поиска он будет обновлён до фактически определённого значения user DN. |
| `verification_cooldown`        | Период времени в секундах после успешной попытки bind, в течение которого пользователь будет считаться успешно аутентифицированным для всех последующих запросов без обращения к LDAP-серверу. Укажите `0` (значение по умолчанию), чтобы отключить кэширование и принудительно обращаться к LDAP-серверу при каждом запросе аутентификации.                                                                                                       |

Настройку `user_dn_detection` можно задать с помощью вложенных тегов:

| Setting         | Description                                                                                                                                                                                                                                                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | шаблон, используемый для формирования base DN для LDAP search. Итоговый DN формируется заменой всех подстрок `\{user_name\}` и `\{bind_dn\}` в шаблоне на фактическое имя пользователя и bind DN во время LDAP search.                                                                                                                    |
| `scope`         | область LDAP search. Допустимые значения: `base`, `one_level`, `children`, `subtree` (по умолчанию).                                                                                                                                                                                                                                      |
| `search_filter` | шаблон, используемый для формирования search filter для LDAP search. Итоговый filter формируется заменой всех подстрок `\{user_name\}`, `\{bind_dn\}` и `\{base_dn\}` в шаблоне на фактическое имя пользователя, bind DN и base DN во время LDAP search. Обратите внимание: специальные символы должны быть корректно экранированы в XML. |

Пример:

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

Пример (типичная Active Directory с настроенным определением user DN для последующего сопоставления ролей):

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

Ограничение на хосты, с которых могут поступать запросы. Если вы хотите, чтобы сервер отвечал на запросы с любых хостов, укажите `::`.

Примеры:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

Расположение и формат сообщений лога.

**Ключи**:

| Key                          | Description                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | Если задано `true` (по умолчанию), логирование будет выполняться асинхронно (один фоновый поток на каждый канал вывода). В противном случае запись в лог будет выполняться в потоке, вызывающем LOG                                                                                                                                               |
| `async_queue_max_size`       | При использовании асинхронного логирования максимальное количество сообщений, которое будет храниться в очереди в ожидании flush. Лишние сообщения будут отброшены                                                                                                                                                                                |
| `console`                    | Включает логирование в консоль. Установите `1` или `true`, чтобы включить. По умолчанию используется `1`, если ClickHouse не работает в режиме демона, иначе `0`.                                                                                                                                                                                 |
| `console_log_level`          | Уровень логирования для вывода в консоль. По умолчанию используется `level`.                                                                                                                                                                                                                                                                      |
| `console_shutdown_log_level` | Уровень Shutdown используется для задания уровня логирования в консоль при остановке сервера.                                                                                                                                                                                                                                                     |
| `console_startup_log_level`  | Уровень Startup используется для задания уровня логирования в консоль при запуске сервера. После запуска уровень логирования возвращается к значению параметра `console_log_level`                                                                                                                                                                |
| `count`                      | Политика ротации: максимальное количество хранимых исторических файлов журналов ClickHouse.                                                                                                                                                                                                                                                       |
| `errorlog`                   | Путь к файлу журнала ошибок.                                                                                                                                                                                                                                                                                                                      |
| `formatting.type`            | Формат логирования для вывода в консоль. В настоящее время поддерживается только `json`                                                                                                                                                                                                                                                           |
| `level`                      | Уровень логирования. Допустимые значения: `none` (отключает логирование), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                                                                                                                                                                               |
| `log`                        | Путь к файлу журнала.                                                                                                                                                                                                                                                                                                                             |
| `rotation`                   | Политика ротации: определяет, когда выполняется ротация файлов журналов. Ротация может основываться на размере, времени или их сочетании. Примеры: 100M, daily, 100M,daily. Когда файл журнала превышает указанный размер или наступает указанный интервал времени, он переименовывается и архивируется, после чего создаётся новый файл журнала. |
| `shutdown_level`             | Уровень Shutdown используется для задания уровня корневого logger при остановке сервера.                                                                                                                                                                                                                                                          |
| `size`                       | Политика ротации: максимальный размер файлов журналов в байтах. Когда размер файла журнала превышает этот порог, он переименовывается и архивируется, после чего создаётся новый файл журнала.                                                                                                                                                    |
| `startup_level`              | Уровень Startup используется для задания уровня корневого logger при запуске сервера. После запуска уровень логирования возвращается к значению параметра `level`                                                                                                                                                                                 |
| `stream_compress`            | Сжимает сообщения лога с помощью LZ4. Установите `1` или `true`, чтобы включить.                                                                                                                                                                                                                                                                  |
| `syslog_level`               | Уровень логирования для записи в syslog.                                                                                                                                                                                                                                                                                                          |
| `use_syslog`                 | Также перенаправляет вывод логов в syslog.                                                                                                                                                                                                                                                                                                        |

**Спецификаторы формата логов**

Имена файлов в путях `log` и `errorLog` поддерживают указанные ниже спецификаторы формата для результирующего имени файла (часть пути с каталогом их не поддерживает).

Столбец «Пример» показывает вывод для `2023-07-06 18:32:07`.

| Спецификатор | Описание                                                                                                                                                                                        | Пример                     |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%`         | Буквальный символ %                                                                                                                                                                             | `%`                        |
| `%n`         | Символ новой строки                                                                                                                                                                             |                            |
| `%t`         | Символ горизонтальной табуляции                                                                                                                                                                 |                            |
| `%Y`         | Год в виде десятичного числа, например 2017                                                                                                                                                     | `2023`                     |
| `%y`         | Последние 2 цифры года в виде десятичного числа (диапазон [00,99])                                                                                                                              | `23`                       |
| `%C`         | Первые 2 цифры года в виде десятичного числа (диапазон [00,99])                                                                                                                                 | `20`                       |
| `%G`         | Четырёхзначный [год по ISO 8601 с нумерацией по неделям](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), то есть год, содержащий указанную неделю. Обычно используется только вместе с `%V` | `2023`                     |
| `%g`         | Последние 2 цифры [года по ISO 8601 с нумерацией по неделям](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), то есть года, содержащего указанную неделю.                                    | `23`                       |
| `%b`         | Сокращённое название месяца, например Oct (зависит от локали)                                                                                                                                   | `Jul`                      |
| `%h`         | Синоним `%b`                                                                                                                                                                                    | `Jul`                      |
| `%B`         | Полное название месяца, например October (зависит от локали)                                                                                                                                    | `July`                     |
| `%m`         | Месяц в виде десятичного числа (диапазон [01,12])                                                                                                                                               | `07`                       |
| `%U`         | Номер недели в году в виде десятичного числа (воскресенье — первый день недели) (диапазон [00,53])                                                                                              | `27`                       |
| `%W`         | Номер недели в году в виде десятичного числа (понедельник — первый день недели) (диапазон [00,53])                                                                                              | `27`                       |
| `%V`         | Номер недели по ISO 8601 (диапазон [01,53])                                                                                                                                                     | `27`                       |
| `%j`         | День года в виде десятичного числа (диапазон [001,366])                                                                                                                                         | `187`                      |
| `%d`         | День месяца в виде десятичного числа с ведущим нулём (диапазон [01,31]). Перед однозначным числом ставится ноль.                                                                                | `06`                       |
| `%e`         | День месяца в виде десятичного числа с дополнением пробелом (диапазон [1,31]). Перед однозначным числом ставится пробел.                                                                        | `&nbsp; 6`                 |
| `%a`         | Сокращённое название дня недели, например Fri (зависит от локали)                                                                                                                               | `Thu`                      |
| `%A`         | Полное название дня недели, например Friday (зависит от локали)                                                                                                                                 | `Thursday`                 |
| `%w`         | День недели в виде целого числа, где воскресенье — 0 (диапазон [0-6])                                                                                                                           | `4`                        |
| `%u`         | День недели в виде десятичного числа, где понедельник — 1 (формат ISO 8601) (диапазон [1-7])                                                                                                    | `4`                        |
| `%H`         | Час в виде десятичного числа, 24-часовой формат (диапазон [00-23])                                                                                                                              | `18`                       |
| `%I`         | Час в виде десятичного числа, 12-часовой формат (диапазон [01,12])                                                                                                                              | `06`                       |
| `%M`         | Минуты в виде десятичного числа (диапазон [00,59])                                                                                                                                              | `32`                       |
| `%S`         | Секунды в виде десятичного числа (диапазон [00,60])                                                                                                                                             | `07`                       |
| `%c`         | Стандартное строковое представление даты и времени, например Sun Oct 17 04:41:13 2010 (зависит от локали)                                                                                       | `Thu Jul  6 18:32:07 2023` |
| `%x`         | Локализованное представление даты (зависит от локали)                                                                                                                                           | `07/06/23`                 |
| `%X`         | Локализованное представление времени, например 18:40:20 или 6:40:20 PM (зависит от локали)                                                                                                      | `18:32:07`                 |
| `%D`         | Короткий формат даты MM/DD/YY, эквивалентен `%m/%d/%y`                                                                                                                                          | `07/06/23`                 |
| `%F`         | Короткая дата в формате YYYY-MM-DD, эквивалентна %Y-%m-%d                                                                                                                                       | `2023-07-06`               |
| `%r`         | Локализованное время в 12-часовом формате (зависит от локали)                                                                                                                                   | `06:32:07 PM`              |
| `%R`         | Эквивалентно &quot;%H:%M&quot;                                                                                                                                                                  | `18:32`                    |
| `%T`         | Эквивалентно &quot;%H:%M:%S&quot; (формат времени ISO 8601)                                                                                                                                     | `18:32:07`                 |
| `%p`         | Локализованное обозначение a.m. или p.m. (зависит от локали)                                                                                                                                    | `PM`                       |
| `%z`         | Смещение относительно UTC в формате ISO 8601 (например, -0430) или отсутствие символов, если информация о часовом поясе недоступна                                                              | `+0800`                    |
| `%Z`         | Зависящее от локали название или сокращение часового пояса либо отсутствие символов, если информация о часовом поясе недоступна                                                                 | `Z AWST `                  |

**Пример**

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

Чтобы выводить в консоль только сообщения лога:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Переопределение по уровням**

Можно переопределить уровень логирования для отдельных логгеров. Например, чтобы отключить все сообщения от логгеров &quot;Backup&quot; и &quot;RBAC&quot;.

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

Чтобы также записывать сообщения лога в syslog:

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

Ключи для `<syslog>`:

| Key        | Описание                                                                                                                                                                                                                                                                                                              |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | Адрес syslog в формате `host\[:port\]`. Если не указан, используется локальный демон.                                                                                                                                                                                                                                 |
| `hostname` | Имя хоста, с которого отправляются записи журнала (необязательно).                                                                                                                                                                                                                                                    |
| `facility` | [Ключевое слово facility](https://en.wikipedia.org/wiki/Syslog#Facility) для syslog. Должно быть указано в верхнем регистре с префиксом &quot;LOG&#95;&quot;, например `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` и т. д. По умолчанию используется `LOG_USER`, если указан `address`, в противном случае — `LOG_DAEMON`. |
| `format`   | Формат сообщения Log. Возможные значения: `bsd` и `syslog.`                                                                                                                                                                                                                                                           |

**Форматы логирования**

Вы можете указать формат логирования, который будет выводиться в журнале консоли. В настоящее время поддерживается только JSON.

**Пример**

Ниже приведен пример выходного JSON-журнала:

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

Чтобы включить поддержку логирования в формате JSON, используйте следующий фрагмент:

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

**Переименование ключей в JSON-журналах**

Имена ключей можно изменить, поменяв значения тегов внутри тега `<names>`. Например, чтобы заменить `DATE_TIME` на `MY_DATE_TIME`, можно использовать `<date_time>MY_DATE_TIME</date_time>`.

**Исключение ключей из JSON-журналов**

Свойства журнала можно исключить, закомментировав соответствующее свойство. Например, если вы не хотите, чтобы в журнале выводился `query_id`, можно закомментировать тег `<query_id>`.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

Настройки отправки отчетов о сбоях команде разработчиков ядра ClickHouse.

Включение этой возможности, особенно в предпродакшн-средах, крайне приветствуется.

Ключи:

| Key                   | Description                                                                                                                                      |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enabled`             | Булевый флаг для включения этой возможности; по умолчанию — `true`. Установите `false`, чтобы отключить отправку отчетов о сбоях.                |
| `endpoint`            | Можно переопределить URL конечной точки для отправки отчетов о сбоях.                                                                            |
| `send_logical_errors` | `LOGICAL_ERROR` — это аналог `assert`, то есть ошибка в ClickHouse. Этот булевый флаг включает отправку таких исключений (по умолчанию: `true`). |

**Рекомендуемое использование**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

Открытая часть ключа хоста будет записана в файл known&#95;hosts
на стороне SSH-клиента при первом подключении.

Конфигурации ключа хоста по умолчанию отключены.
Раскомментируйте конфигурации ключа хоста и укажите путь к соответствующему SSH-ключу, чтобы активировать их:

Пример:

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

Порт SSH-сервера, позволяющий пользователю подключаться и выполнять запросы в интерактивном режиме с помощью встроенного клиента через PTY.

Пример:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

Позволяет настроить хранилище с использованием нескольких дисков.

Конфигурация хранилища имеет следующую структуру:

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
  ### Конфигурация дисков
</div>

Конфигурация `disks` имеет следующую структуру:

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

Приведённые выше подтеги задают следующие настройки для `disks`:

| Setting                 | Description                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `<disk_name_N>`         | Имя диска; оно должно быть уникальным.                                                                       |
| `path`                  | Путь, по которому будут храниться данные сервера (каталоги `data` и `shadow`). Он должен оканчиваться на `/` |
| `keep_free_space_bytes` | Размер зарезервированного свободного места на диске.                                                         |

:::note
Порядок дисков не имеет значения.
:::

<div id="configuration-of-policies">
  ### Конфигурация политик
</div>

Приведенные выше вложенные теги задают следующие настройки для `policies`:

| Setting                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `policy_name_N`              | Имя политики. Имена политик должны быть уникальными.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `volume_name_N`              | Имя тома. Имена томов должны быть уникальными.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `disk`                       | Диск внутри тома.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `max_data_part_size_bytes`   | Максимальный размер фрагмента данных, который может находиться на любом из дисков этого тома. Если в результате слияния ожидаемый размер фрагмента превысит `max_data_part_size_bytes`, фрагмент будет записан в следующий том. По сути, эта возможность позволяет хранить новые / небольшие фрагменты на горячем томе (SSD), а при достижении большого размера перемещать их на холодный том (HDD). Не используйте этот параметр, если политика содержит только один том.                                               |
| `move_factor`                | Доля доступного свободного места на томе. Если свободного места станет меньше, данные начнут переноситься на следующий том, если он есть. Для переноса фрагменты сортируются по размеру от большего к меньшему (по убыванию), и выбираются фрагменты, суммарный размер которых достаточен для выполнения условия `move_factor`; если суммарного размера всех фрагментов недостаточно, будут перемещены все фрагменты.                                                                                                    |
| `perform_ttl_move_on_insert` | Отключает перемещение данных с истекшим TTL при вставке. По умолчанию (если параметр включен), если вставляется фрагмент данных, который уже истек в соответствии с правилом перемещения по TTL, он немедленно переносится на том/диск, указанный в правиле перемещения. Это может существенно замедлить вставку, если целевой том/диск медленный (например, S3). Если параметр отключен, истекшая часть данных записывается в том по умолчанию, а затем сразу переносится в том, указанный в правиле для истекшего TTL. |
| `load_balancing`             | Политика балансировки дисков: `round_robin` или `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `least_used_ttl_ms`          | Задает тайм-аут (в миллисекундах) для обновления сведений о доступном пространстве на всех дисках (`0` — обновлять всегда, `-1` — никогда не обновлять, значение по умолчанию — `60000`). Обратите внимание: если диск используется только ClickHouse и размер его файловой системы не будет меняться на лету, можно использовать значение `-1`. Во всех остальных случаях это не рекомендуется, так как со временем приведет к некорректному распределению пространства.                                                |
| `prefer_not_to_merge`        | Отключает слияние частей данных на этом томе. Примечание: это потенциально вредно и может привести к замедлению работы. Когда эта настройка включена (не делайте этого), слияние данных на этом томе запрещено (что плохо). Это позволяет управлять тем, как ClickHouse взаимодействует с медленными дисками. Мы рекомендуем вообще не использовать этот параметр.                                                                                                                                                       |
| `volume_priority`            | Определяет приоритет (порядок), в котором заполняются тома. Чем меньше значение, тем выше приоритет. Значения параметра должны быть натуральными числами и покрывать диапазон от 1 до N (где N — наибольшее указанное значение параметра) без пропусков.                                                                                                                                                                                                                                                                 |

Для `volume_priority`:

* Если этот параметр задан для всех томов, они получают приоритет в указанном порядке.
* Если он задан только для *некоторых* томов, тома без него получают самый низкий приоритет. Для томов, у которых он есть, приоритет определяется значением тега, а для остальных — порядком их описания в файле конфигурации относительно друг друга.
* Если этот параметр не задан *ни для одного* тома, их порядок определяется порядком описания в файле конфигурации.
* Приоритеты томов не могут совпадать.

<div id="macros">
  ## макросы
</div>

Подстановки параметров для реплицируемых таблиц.

Можно опустить, если реплицируемые таблицы не используются.

Дополнительные сведения см. в разделе [Создание реплицируемых таблиц](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Пример**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Имя группы реплик для базы данных Replicated.

Кластер, созданный базой данных Replicated, будет состоять из реплик одной и той же группы.
DDL-запросы будут ожидать только реплики из той же группы.

По умолчанию пусто.

**Пример**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

Максимальный тайм-аут сеанса, в секундах.

Пример:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

Тонкая настройка таблиц в [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Дополнительную информацию см. в файле заголовков MergeTreeSettings.h.

**Пример**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

По умолчанию эта функция отключена.

**Включение**

Чтобы вручную включить сбор истории метрик в [`system.metric_log`](../../operations/system-tables/metric_log.md), создайте файл `/etc/clickhouse-server/config.d/metric_log.xml` со следующим содержимым:

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

**Отключение**

Чтобы отключить настройку `metric_log`, создайте файл `/etc/clickhouse-server/config.d/disable_metric_log.xml` со следующим содержимым:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

Тонкая настройка для таблиц на движке [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Этот параметр имеет более высокий приоритет.

Дополнительные сведения см. в заголовочном файле MergeTreeSettings.h.

**Пример**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

Настройки системной таблицы [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md).

<SystemLogParameters />

Пример:

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

Конфигурация SSL для клиента и сервера.

Поддержка SSL обеспечивается библиотекой `libpoco`. Доступные параметры конфигурации описаны в [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Значения по умолчанию приведены в [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Ключи настроек сервера и клиента:

| Параметр                      | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Значение по умолчанию                                                                      |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | Включает или отключает кэширование сеансов. Следует использовать в сочетании с `sessionIdContext`. Допустимые значения: `true`, `false`.                                                                                                                                                                                                                                                                                                                                 | `false`                                                                                    |
| `caConfig`                    | Путь к файлу или каталогу с доверенными CA‑сертификатами. Если указан файл, он должен быть в формате PEM и может содержать несколько CA‑сертификатов. Если указан каталог, он должен содержать по одному файлу .pem для каждого CA‑сертификата. Имена файлов определяются по хеш-значению subject name CA. Подробности см. на man-странице [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                                                                            |
| `certificateFile`             | Путь к файлу сертификата клиента или сервера в формате PEM. Его можно не указывать, если `privateKeyFile` содержит сертификат.                                                                                                                                                                                                                                                                                                                                           |                                                                                            |
| `cipherList`                  | Поддерживаемые алгоритмы шифрования OpenSSL.                                                                                                                                                                                                                                                                                                                                                                                                                             | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | Протоколы, использование которых запрещено.                                                                                                                                                                                                                                                                                                                                                                                                                              |                                                                                            |
| `extendedVerification`        | Если включено, проверяется, что CN или SAN сертификата соответствует имени хоста удалённой стороны.                                                                                                                                                                                                                                                                                                                                                                      | `false`                                                                                    |
| `fips`                        | Активирует режим FIPS в OpenSSL. Поддерживается, если версия OpenSSL, используемая библиотекой, поддерживает FIPS.                                                                                                                                                                                                                                                                                                                                                       | `false`                                                                                    |
| `invalidCertificateHandler`   | Класс (подкласс `CertificateHandler`) для обработки недействительных сертификатов. Например: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                          | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | Будут ли использоваться встроенные CA‑сертификаты OpenSSL. ClickHouse предполагает, что встроенные CA‑сертификаты находятся в файле `/etc/ssl/cert.pem` (соответственно, в каталоге `/etc/ssl/certs`) или в файле (соответственно, каталоге), указанном в переменной окружения `SSL_CERT_FILE` (соответственно, `SSL_CERT_DIR`).                                                                                                                                         | `true`                                                                                     |
| `preferServerCiphers`         | Серверные шифры, предпочитаемые клиентом.                                                                                                                                                                                                                                                                                                                                                                                                                                | `false`                                                                                    |
| `privateKeyFile`              | Путь к файлу с закрытым ключом PEM-сертификата. Файл может одновременно содержать ключ и сертификат.                                                                                                                                                                                                                                                                                                                                                                     |                                                                                            |
| `privateKeyPassphraseHandler` | Класс (подкласс PrivateKeyPassphraseHandler), который запрашивает парольную фразу для доступа к закрытому ключу. Например: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                             | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | Требуется соединение по TLSv1. Допустимые значения: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                     | `false`                                                                                    |
| `requireTLSv1_1`              | Требуется соединение по TLSv1.1. Допустимые значения: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                   | `false`                                                                                    |
| `requireTLSv1_2`              | Требуется соединение по TLSv1.2. Допустимые значения: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                   | `false`                                                                                    |
| `sessionCacheSize`            | Максимальное количество сеансов, кэшируемых сервером. Значение `0` означает неограниченное число сеансов.                                                                                                                                                                                                                                                                                                                                                                | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | Уникальный набор случайных символов, который сервер добавляет к каждому сгенерированному идентификатору. Длина строки не должна превышать `SSL_MAX_SSL_SESSION_ID_LENGTH`. Этот параметр рекомендуется указывать всегда, поскольку он помогает избежать проблем как при кэшировании сеанса на сервере, так и если клиент запросил кэширование.                                                                                                                           | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | Время кэширования сеанса на сервере в часах.                                                                                                                                                                                                                                                                                                                                                                                                                             | `2`                                                                                        |
| `verificationDepth`           | Максимальная длина цепочки проверки. Проверка завершится ошибкой, если длина цепочки сертификатов превысит заданное значение.                                                                                                                                                                                                                                                                                                                                            | `9`                                                                                        |
| `verificationMode`            | Способ проверки сертификатов узла. Подробности см. в описании класса [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Возможные значения: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                                                | `relaxed`                                                                                  |

**Пример настроек:**

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

Журналирование событий, связанных с [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), например добавления или слияния данных. Журнал можно использовать для моделирования алгоритмов слияния и сравнения их характеристик. Также можно визуализировать процесс слияния.

Запросы записываются в таблицу [system.part&#95;log](/ru/operations/system-tables/part_log), а не в отдельный файл. Имя этой таблицы можно настроить с помощью параметра `table` (см. ниже).

<SystemLogParameters />

**Пример**

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

Настройки системной таблицы [`processors_profile_log`](../system-tables/processors_profile_log.md).

<SystemLogParameters />

Настройки по умолчанию:

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

Экспорт метрик для сбора [Prometheus](https://prometheus.io).

Настройки:

* `endpoint` – HTTP-конечная точка для сбора метрик сервером Prometheus. Должна начинаться с &#39;/&#39;.
* `port` – Порт для `endpoint`.
* `metrics` – Экспортирует метрики из таблицы [system.metrics](/ru/operations/system-tables/metrics).
* `events` – Экспортирует метрики из таблицы [system.events](/ru/operations/system-tables/events).
* `asynchronous_metrics` – Экспортирует текущие значения метрик из таблицы [system.asynchronous&#95;metrics](/ru/operations/system-tables/asynchronous_metrics).
* `errors` - Экспортирует количество ошибок по кодам ошибок, произошедших с момента последнего перезапуска сервера. Эту информацию также можно получить из [system.errors](/ru/operations/system-tables/errors).

**Пример**

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

Проверьте (замените `127.0.0.1` на IP-адрес или имя хоста вашего сервера ClickHouse):

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

Настройка журналирования запросов, полученных с настройкой [log&#95;queries=1](../../operations/settings/settings.md).

Запросы записываются в таблицу [system.query&#95;log](/ru/operations/system-tables/query_log), а не в отдельный файл. Имя таблицы можно изменить в параметре `table` (см. ниже).

<SystemLogParameters />

Если таблица не существует, ClickHouse создаст ее. Если при обновлении ClickHouse изменилась структура журнала запросов, таблица со старой структурой переименовывается, а новая таблица создается автоматически.

**Пример**

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

По умолчанию отключено.

**Включение**

Чтобы вручную включить сбор истории метрик [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), создайте файл `/etc/clickhouse-server/config.d/query_metric_log.xml` со следующим содержимым:

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

**Отключение**

Чтобы отключить настройку `query_metric_log`, создайте файл `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` со следующим содержимым:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

Конфигурация [кэша запросов](../query-cache.md).

Доступны следующие настройки:

| Настройка                 | Описание                                                                           | Значение по умолчанию |
| ------------------------- | ---------------------------------------------------------------------------------- | --------------------- |
| `max_entries`             | Максимальное количество результатов `SELECT`-запросов, хранимых в кэше.            | `1024`                |
| `max_entry_size_in_bytes` | Максимальный размер в байтах для сохранения результатов `SELECT`-запросов в кэше.  | `1048576`             |
| `max_entry_size_in_rows`  | Максимальное количество строк для сохранения результатов `SELECT`-запросов в кэше. | `30000000`            |
| `max_size_in_bytes`       | Максимальный размер кэша в байтах. `0` означает, что кэш запросов отключён.        | `1073741824`          |

:::note

* Изменённые настройки вступают в силу немедленно.
* Для кэша запросов данные размещаются в DRAM. Если оперативной памяти недостаточно, задайте небольшое значение `max_size_in_bytes` или полностью отключите кэш запросов.
  :::

**Пример**

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

Настройка для логирования потоков запросов при включённой настройке [log&#95;query&#95;threads=1](/ru/operations/settings/settings#log_query_threads).

Запросы записываются в таблицу [system.query&#95;thread&#95;log](/ru/operations/system-tables/query_thread_log), а не в отдельный файл. Имя таблицы можно изменить в параметре `table` (см. ниже).

<SystemLogParameters />

Если таблица не существует, ClickHouse создаст её. Если при обновлении сервера ClickHouse изменилась структура журнала потоков запросов, таблица со старой структурой будет переименована, а новая таблица создастся автоматически.

**Пример**

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

Настройка для логирования представлений (live, materialized и т. д.), зависящих от запросов, полученных при включённой настройке [log&#95;query&#95;views=1](/ru/operations/settings/settings#log_query_views).

Запросы записываются в таблицу [system.query&#95;views&#95;log](/ru/operations/system-tables/query_views_log), а не в отдельный файл. Имя таблицы можно изменить в параметре `table` (см. ниже).

<SystemLogParameters />

Если таблица не существует, ClickHouse создаст её. Если при обновлении сервера ClickHouse изменилась структура журнала представлений запросов, таблица со старой структурой будет переименована, а новая таблица создастся автоматически.

**Пример**

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

Настройки системной таблицы [text&#95;log](/ru/operations/system-tables/text_log) для записи текстовых сообщений в журнал.

<SystemLogParameters />

Дополнительно:

| Настройка | Описание                                                                                    | Значение по умолчанию |
| --------- | ------------------------------------------------------------------------------------------- | --------------------- |
| `level`   | Максимальный уровень сообщения (по умолчанию `Trace`), который будет сохраняться в таблице. | `Trace`               |

**Пример**

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

Настройки операции системной таблицы [trace&#95;log](/ru/operations/system-tables/trace_log).

<SystemLogParameters />

Файл конфигурации сервера `config.xml` по умолчанию содержит следующий раздел настроек:

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

Настройки системной таблицы [asynchronous&#95;insert&#95;log](/ru/operations/system-tables/asynchronous_insert_log) для ведения журнала асинхронных вставок.

<SystemLogParameters />

**Пример**

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

Настройки работы системной таблицы [crash&#95;log](../../operations/system-tables/crash_log.md).

Следующие настройки можно задать с помощью вложенных тегов:

| Setting                            | Description                                                                                                                                                     | Default             | Note                                                                                                                            |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | Пороговое количество строк. При достижении порога журналы в фоновом режиме сбрасываются на диск.                                                                | `max_size_rows / 2` |                                                                                                                                 |
| `database`                         | Имя базы данных.                                                                                                                                                |                     |                                                                                                                                 |
| `engine`                           | [Определение движка MergeTree](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) для системной таблицы.                |                     | Нельзя использовать, если заданы `partition_by` или `order_by`. Если не указано, по умолчанию используется `MergeTree`          |
| `flush_interval_milliseconds`      | Интервал сброса данных из буфера в памяти в таблицу.                                                                                                            | `7500`              |                                                                                                                                 |
| `flush_on_crash`                   | Определяет, следует ли выгружать журналы на диск в случае сбоя.                                                                                                 | `false`             |                                                                                                                                 |
| `max_size_rows`                    | Максимальный размер журналов в строках. Когда количество несброшенных журналов достигает max&#95;size, журналы выгружаются на диск.                             | `1024`              |                                                                                                                                 |
| `order_by`                         | [Пользовательский ключ сортировки](/ru/engines/table-engines/mergetree-family/mergetree#order_by) для системной таблицы. Нельзя использовать, если задан `engine`. |                     | Если для системной таблицы указан `engine`, параметр `order_by` следует указывать непосредственно внутри &#39;engine&#39;       |
| `partition_by`                     | [Пользовательский ключ партиционирования](/ru/engines/table-engines/mergetree-family/custom-partitioning-key.md) для системной таблицы.                            |                     | Если для системной таблицы указан `engine`, параметр `partition_by` следует указывать непосредственно внутри &#39;engine&#39;   |
| `reserved_size_rows`               | Предварительно выделенный размер памяти для журналов в строках.                                                                                                 | `1024`              |                                                                                                                                 |
| `settings`                         | [Дополнительные параметры](/ru/engines/table-engines/mergetree-family/mergetree/#settings), управляющие поведением MergeTree (необязательно).                      |                     | Если для системной таблицы указан `engine`, параметр `settings` следует указывать непосредственно внутри &#39;engine&#39;       |
| `storage_policy`                   | Имя политики хранения, используемой для таблицы (необязательно).                                                                                                |                     | Если для системной таблицы указан `engine`, параметр `storage_policy` следует указывать непосредственно внутри &#39;engine&#39; |
| `table`                            | Имя системной таблицы.                                                                                                                                          |                     |                                                                                                                                 |
| `ttl`                              | Задаёт [TTL](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) таблицы.                                                             |                     | Если для системной таблицы указан `engine`, параметр `ttl` следует указывать непосредственно внутри &#39;engine&#39;            |

Файл конфигурации сервера `config.xml` по умолчанию содержит следующий раздел с настройками:

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

Этот параметр задаёт путь к кэшу для пользовательских кэшированных дисков (созданных из SQL).
Для пользовательских дисков `custom_cached_disks_base_directory` имеет более высокий приоритет, чем `filesystem_caches_path` (указанный в `filesystem_caches_path.xml`),
который используется, если первый параметр отсутствует.
Путь настройки файлового кэша должен находиться внутри этого каталога,
иначе будет сгенерировано исключение, и диск не будет создан.

:::note
Это не влияет на диски, созданные в более старой версии, для которых затем был обновлён сервер.
В этом случае исключение не будет сгенерировано, чтобы сервер мог успешно запуститься.
:::

Пример:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

Настройки системной таблицы [backup&#95;log](../../operations/system-tables/backup_log.md) для журналирования операций `BACKUP` и `RESTORE`.

<SystemLogParameters />

**Пример**

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

Настройки системной таблицы [`blob_storage_log`](../system-tables/blob_storage_log.md).

<SystemLogParameters />

Пример:

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

Правила на основе регулярных выражений, которые применяются к запросам, а также ко всем сообщениям лога перед сохранением в серверных журналах,
таблицах [`system.query_log`](/ru/operations/system-tables/query_log), [`system.text_log`](/ru/operations/system-tables/text_log), [`system.processes`](/ru/operations/system-tables/processes), а также в журналах, отправляемых клиенту. Это позволяет предотвращать
утечку конфиденциальных данных из SQL-запросов, таких как имена, адреса электронной почты, персональные идентификаторы или номера кредитных карт, в журналы.

**Пример**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Поля конфигурации**:

| Setting   | Description                                                                                    |
| --------- | ---------------------------------------------------------------------------------------------- |
| `name`    | имя правила (необязательно)                                                                    |
| `regexp`  | регулярное выражение, совместимое с RE2 (обязательно)                                          |
| `replace` | строка подстановки для конфиденциальных данных (необязательно, по умолчанию — шесть звёздочек) |

Правила маскирования применяются ко всему запросу (чтобы предотвратить утечку конфиденциальных данных из некорректных / не поддающихся разбору запросов).

В таблице [`system.events`](/ru/operations/system-tables/events) есть Counter `QueryMaskingRulesMatch`, который показывает общее число срабатываний правил маскирования запросов.

Для распределённых запросов каждый server нужно настраивать отдельно, иначе подзапросы, передаваемые на другие
узлы, будут сохраняться без маскирования.

<div id="remote_servers">
  ## remote_servers
</div>

Конфигурация кластеров, используемых движком таблицы [Distributed](../../engines/table-engines/special/distributed.md) и табличной функцией `cluster`.

**Пример**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

О значении атрибута `incl` см. в разделе &quot;[Файлы конфигурации](/ru/operations/configuration-files)&quot;.

**См. также**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [Cluster Discovery](../../operations/cluster-discovery.md)
* [Движок базы данных Replicated](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

Список хостов, которые разрешено использовать в связанных с `URL` движках хранения и табличных функциях.

При добавлении хоста с помощью XML-тега `\<host\>`:

* он должен быть указан в точности так же, как в `URL`, поскольку имя проверяется до DNS-разрешения. Например: `<host>clickhouse.com</host>`
* если порт явно указан в `URL`, то `host:port` проверяется как единое целое. Например: `<host>clickhouse.com:80</host>`
* если хост указан без порта, то разрешён любой порт этого хоста. Например, если указан `<host>clickhouse.com</host>`, то разрешены `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS) и т. д.
* если хост указан в виде IP-адреса, то он проверяется в том виде, в котором указан в `URL`. Например: `[2a02:6b8:a::a]`.
* если есть перенаправления и их поддержка включена, то проверяется каждое перенаправление (поле location).

Например:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

Часовой пояс сервера.

Указывается как идентификатор IANA для часового пояса UTC или географического местоположения (например, Africa/Abidjan).

Часовой пояс необходим для преобразований между форматами String и дата и время, когда поля дата и время выводятся в текстовом формате (на экран или в файл), а также при получении значения дата и время из строки. Кроме того, часовой пояс используется в функциях, работающих со временем и датой, если он не был передан во входных параметрах.

**Пример**

```xml
<timezone>Asia/Istanbul</timezone>
```

**См. также**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

Порт для обмена данными с клиентами по протоколу TCP.

**Пример**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

TCP-порт для защищённого подключения клиентов. Используйте его вместе с настройками [OpenSSL](#openssl).

**Значение по умолчанию**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

Порт для обмена данными с клиентами по протоколу MySQL.

:::note

* Положительные целые числа задают номер порта, который будет прослушиваться
* Пустые значения используются для отключения обмена данными с клиентами по протоколу MySQL.
  :::

**Пример**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

Порт для связи с клиентами по протоколу PostgreSQL.

:::note

* Положительные целые числа указывают номер порта для прослушивания
* Пустые значения используются для отключения связи с клиентами по протоколу PostgreSQL.
  :::

**Пример**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

Конфигурация для преобразования сокращённых или символьных префиксов URL в полные URL-адреса.

Пример:

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

Каталог для пользовательских файлов. Используется для пользовательских функций SQL [пользовательские функции SQL](/ru/sql-reference/functions/udf).

**Пример**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

Путь к файлу, содержащему:

* Конфигурации пользователей.
* Права доступа.
* Профили настроек.
* Настройки квот.

**Пример**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

Настройки необязательных улучшений системы управления доступом.

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | Устанавливает, требуют ли запросы `ON CLUSTER` привилегию `CLUSTER`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `true`  |
| `role_cache_expiration_time_seconds`            | Устанавливает, сколько секунд после последнего обращения роль хранится в кэше ролей.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `600`   |
| `select_from_information_schema_requires_grant` | Устанавливает, требуют ли запросы `SELECT * FROM information_schema.<table>` каких-либо привилегий и могут ли они выполняться любым пользователем. Если значение равно true, этот запрос требует `GRANT SELECT ON information_schema.<table>` так же, как и для обычных таблиц.                                                                                                                                                                                                                                                                                                          | `true`  |
| `select_from_system_db_requires_grant`          | Устанавливает, требуют ли запросы `SELECT * FROM system.<table>` каких-либо привилегий и могут ли они выполняться любым пользователем. Если значение равно true, этот запрос требует `GRANT SELECT ON system.<table>` так же, как и для обычных таблиц. Исключения: некоторые системные таблицы (`tables`, `columns`, `databases`, а также некоторые константные таблицы, такие как `one` и `contributors`) по-прежнему доступны всем; кроме того, если выдана привилегия `SHOW` (например, `SHOW USERS`), то будет доступна соответствующая системная таблица (то есть `system.users`). | `true`  |
| `settings_constraints_replace_previous`         | Устанавливает, будет ли ограничение в профиле настроек для некоторой настройки отменять действие предыдущего ограничения (заданного в других профилях) для этой настройки, включая поля, которые не заданы новым ограничением. Также включает тип ограничения `changeable_in_readonly`.                                                                                                                                                                                                                                                                                                  | `true`  |
| `table_engines_require_grant`                   | Устанавливает, требует ли создание таблицы с определённым движком таблицы наличия привилегии.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `false` |
| `throw_on_unmatched_row_policies`               | Устанавливает, должно ли чтение из таблицы сгенерировать исключение, если для таблицы существуют политики на уровне строк, но ни одна из них не относится к текущему пользователю                                                                                                                                                                                                                                                                                                                                                                                                      | `false` |
| `users_without_row_policies_can_read_rows`      | Устанавливает, могут ли пользователи без разрешающих политик на уровне строк по-прежнему читать строки с помощью запроса `SELECT`. Например, если есть два пользователя, A и B, и политика на уровне строк определена только для A, то при значении true пользователь B увидит все строки. Если значение false, пользователь B не увидит ни одной строки.                                                                                                                                                                                                                            | `true`  |

Пример:

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

Настройки системной таблицы `s3queue_log`.

<SystemLogParameters />

По умолчанию используются следующие настройки:

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

Настройка для системной таблицы &#39;dead&#95;letter&#95;queue&#39;.

<SystemLogParameters />

По умолчанию используются следующие настройки:

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

Содержит настройки, позволяющие ClickHouse взаимодействовать с кластером [ZooKeeper](http://zookeeper.apache.org/). ClickHouse использует ZooKeeper для хранения метаданных реплик при использовании реплицируемых таблиц. Если реплицируемые таблицы не используются, этот раздел параметров можно опустить.

Следующие настройки можно задать с помощью подтегов:

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | Конечная точка ZooKeeper. Можно указать несколько конечных точек. Например: `<node index="1"><host>example_host</host><port>2181</port></node>`. Атрибут `index` задаёт порядок узлов при попытке подключения к кластеру ZooKeeper.                                                                                                                                   |
| `operation_timeout_ms`                          | Максимальный тайм-аут одной операции в миллисекундах.                                                                                                                                                                                                                                                                                                                 |
| `session_timeout_ms`                            | Максимальный тайм-аут клиентского сеанса в миллисекундах.                                                                                                                                                                                                                                                                                                             |
| `root` (optional)                               | znode, используемый как корневой для znode, которые использует сервер ClickHouse.                                                                                                                                                                                                                                                                                     |
| `fallback_session_lifetime.min` (optional)      | Минимальное время жизни сеанса ZooKeeper для резервного узла, когда основной недоступен (балансировка нагрузки). Задаётся в секундах. По умолчанию: 3 часа.                                                                                                                                                                                                           |
| `fallback_session_lifetime.max` (optional)      | Максимальное время жизни сеанса ZooKeeper для резервного узла, когда основной недоступен (балансировка нагрузки). Задаётся в секундах. По умолчанию: 6 часов.                                                                                                                                                                                                         |
| `identity` (optional)                           | Имя пользователя и пароль, необходимые ZooKeeper для доступа к запрошенным znode.                                                                                                                                                                                                                                                                                     |
| `use_compression` (optional)                    | Включает сжатие в протоколе Keeper, если установлено значение `true`.                                                                                                                                                                                                                                                                                                 |
| `use_xid_64` (optional)                         | Включает 64-битные идентификаторы транзакций. Установите `true`, чтобы включить расширенный формат идентификаторов транзакций. По умолчанию: `false`.                                                                                                                                                                                                                 |
| `pass_opentelemetry_tracing_context` (optional) | Включает передачу контекста трассировки OpenTelemetry в запросы Keeper. Когда параметр включён, для операций Keeper создаются спаны, что позволяет выполнять распределённую трассировку между ClickHouse и Keeper. Подробнее см. в разделе [Tracing ClickHouse Keeper Requests](/ru/operations/opentelemetry#tracing-clickhouse-keeper-requests). По умолчанию: `false`. |

Также доступна настройка `zookeeper_load_balancing` (необязательно), которая позволяет выбрать алгоритм выбора узла ZooKeeper:

| Algorithm Name                   | Description                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `random`                         | случайным образом выбирает один из узлов ZooKeeper.                                                                                  |
| `in_order`                       | выбирает первый узел ZooKeeper; если он недоступен, то второй, и так далее.                                                          |
| `nearest_hostname`               | выбирает узел ZooKeeper, имя хоста которого наиболее похоже на имя хоста сервера; сравнение выполняется по префиксу.                 |
| `hostname_levenshtein_distance`  | так же, как nearest&#95;hostname, но сравнивает имена хостов по расстоянию Левенштейна.                                              |
| `hostname_longest_common_prefix` | так же, как nearest&#95;hostname, но предпочитает узел, имя хоста которого имеет самый длинный общий префикс с именем хоста сервера. |
| `hostname_longest_common_suffix` | так же, как nearest&#95;hostname, но предпочитает узел, имя хоста которого имеет самый длинный общий суффикс с именем хоста сервера. |
| `first_or_random`                | выбирает первый узел ZooKeeper; если он недоступен, случайным образом выбирает один из оставшихся узлов ZooKeeper.                   |
| `round_robin`                    | выбирает первый узел ZooKeeper; при переподключении выбирает следующий.                                                              |

**Пример конфигурации**

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

**См. также**

* [Репликация](../../engines/table-engines/mergetree-family/replication.md)
* [Руководство программиста по ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [Необязательное защищённое соединение между ClickHouse и ZooKeeper](/ru/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

Способ хранения заголовков частей данных в ZooKeeper. Этот параметр применяется только к семейству [`MergeTree`](/ru/engines/table-engines/mergetree-family). Его можно указать:

**Глобально в разделе [merge&#95;tree](#merge_tree) файла `config.xml`**

ClickHouse использует этот параметр для всех таблиц на сервере. Вы можете изменить параметр в любое время. Поведение существующих таблиц изменяется при изменении параметра.

**Для каждой таблицы**

При создании таблицы укажите соответствующий [параметр движка](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). Поведение существующей таблицы с этим параметром не меняется, даже если глобальный параметр изменится.

**Возможные значения**

* `0` — Функциональность отключена.
* `1` — Функциональность включена.

Если [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), то [реплицируемые](../../engines/table-engines/mergetree-family/replication.md) таблицы хранят заголовки частей данных в компактном виде, используя один `znode`. Если таблица содержит много столбцов, этот способ хранения значительно уменьшает объем данных, хранящихся в ZooKeeper.

:::note
После применения `use_minimalistic_part_header_in_zookeeper = 1` вы не сможете понизить версию сервера ClickHouse до версии, которая не поддерживает этот параметр. Будьте осторожны при обновлении ClickHouse на серверах в кластере. Не обновляйте все серверы сразу. Безопаснее тестировать новые версии ClickHouse в тестовой среде или только на нескольких серверах кластера.

Заголовки частей данных, уже сохраненные с этим параметром, нельзя восстановить до их прежнего (некомпактного) представления.
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

Управляет выполнением [распределённых DDL-запросов](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) в кластере.
Работает только если [ZooKeeper](/ru/operations/server-configuration-parameters/settings#zookeeper) включен.

Настраиваемые параметры в `<distributed_ddl>` включают:

| Настройка              | Описание                                                                                                                                 | Значение по умолчанию                  |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `cleanup_delay_period` | очистка начинается после получения события о новом узле, если с момента последней очистки прошло не менее `cleanup_delay_period` секунд. | `60` секунд                            |
| `max_tasks_in_queue`   | максимальное количество задач, которые могут находиться в очереди.                                                                       | `1,000`                                |
| `path`                 | путь в Keeper к `task_queue` для DDL-запросов                                                                                            |                                        |
| `pool_size`            | сколько запросов с предложением `ON CLUSTER` может выполняться одновременно                                                              |                                        |
| `profile`              | профиль, используемый для выполнения DDL-запросов                                                                                        |                                        |
| `task_max_lifetime`    | удалить узел, если его возраст превышает это значение.                                                                                   | `7 * 24 * 60 * 60` (неделя в секундах) |

**Пример**

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

Путь к папке, в которой сервер ClickHouse хранит конфигурации пользователей и ролей, созданные SQL-командами.

**См. также**

* [Система управления доступом и учётными записями](/ru/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

Определяет, разрешены ли небезопасные типы паролей в открытом виде.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

Определяет, разрешён ли небезопасный тип пароля `no_password`.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

Запрещает создавать пользователя без пароля, если только явно не указано &#39;IDENTIFIED WITH no&#95;password&#39;.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

Тайм-аут сеанса по умолчанию, в секундах.

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

Устанавливает тип пароля, который будет автоматически устанавливаться в запросах вида `CREATE USER u IDENTIFIED BY 'p'`.

Допустимые значения:

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

Раздел файла конфигурации, содержащий следующие настройки:

* Путь к файлу конфигурации с предопределенными пользователями.
* Путь к папке, где хранятся пользователи, созданные SQL-командами.
* Путь к узлу ZooKeeper, где хранятся и реплицируются пользователи, созданные SQL-командами.

Если этот раздел указан, пути из [users&#95;config](/ru/operations/server-configuration-parameters/settings#users_config) и [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path) использоваться не будут.

Раздел `user_directories` может содержать любое количество элементов; порядок элементов определяет их старшинство (чем выше элемент, тем выше старшинство).

**Примеры**

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

Пользователи, роли, политики на уровне строк, квоты и профили могут также храниться в ZooKeeper:

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

Вы также можете определить разделы `memory` — это означает хранение информации только в памяти, без записи на диск, и `ldap` — это означает хранение информации на LDAP-сервере.

Чтобы добавить LDAP-сервер в качестве удалённого каталога пользователей для пользователей, не определённых локально, задайте один раздел `ldap` со следующими настройками:

| Setting  | Description                                                                                                                                                                                                                                                                                                                                                                                          |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`  | раздел со списком локально определённых ролей, которые будут назначаться каждому пользователю, полученному с LDAP-сервера. Если роли не указаны, пользователь не сможет выполнять никакие действия после аутентификации. Если на момент аутентификации какая-либо из перечисленных ролей не определена локально, попытка аутентификации завершится неудачей, как если бы был указан неверный пароль. |
| `server` | одно из имён LDAP-серверов, заданных в разделе `ldap_servers` config. Этот параметр обязателен и не может быть пустым.                                                                                                                                                                                                                                                                               |

**Пример**

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

Задаёт список пользовательских доменов верхнего уровня для добавления, где каждый элемент имеет формат `<name>/path/to/file</name>`.

Например:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

См. также:

* функцию [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) и её варианты,
  которая принимает имя пользовательского списка TLD и возвращает часть домена, включающую поддомены верхнего уровня вплоть до первого значимого поддомена.

<div id="proxy">
  ## прокси
</div>

Определите прокси-серверы для HTTP- и HTTPS-запросов. Сейчас это поддерживается хранилищем S3, табличными функциями S3 и URL-функциями.

Есть три способа задать прокси-серверы:

* переменные окружения
* списки прокси
* удалённые резолверы прокси.

Также поддерживается обход прокси-серверов для определённых хостов с помощью `no_proxy`.

**Переменные окружения**

Переменные окружения `http_proxy` и `https_proxy` позволяют указать
прокси-сервер для заданного протокола. Если они настроены в вашей системе,
всё должно работать без дополнительной настройки.

Это самый простой способ, если для заданного протокола используется
только один прокси-сервер и он не меняется.

**Списки прокси**

Этот подход позволяет указать один или несколько
прокси-серверов для протокола. Если задано более одного прокси-сервера,
ClickHouse использует их по принципу round-robin, распределяя
нагрузку между серверами. Это самый простой подход, если для протокола есть
несколько прокси-серверов и их список не меняется.

**Шаблон конфигурации**

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

Выберите родительское поле на вкладках ниже, чтобы просмотреть его дочерние поля:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Поле      | Описание                                     |
    | --------- | -------------------------------------------- |
    | `<http>`  | Список из одного или нескольких HTTP-прокси  |
    | `<https>` | Список из одного или нескольких HTTPS-прокси |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | Поле    | Описание   |
    | ------- | ---------- |
    | `<uri>` | URI прокси |
  </TabItem>
</Tabs>

**Удалённые резолверы прокси**

Прокси-серверы могут динамически меняться. В таком
случае можно указать конечную точку резолвера. ClickHouse отправляет
на эту конечную точку пустой GET-запрос, а удалённый резолвер должен вернуть хост прокси.
ClickHouse использует его для формирования URI прокси по следующему шаблону: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Шаблон конфигурации**

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

Выберите родительское поле на вкладках ниже, чтобы просмотреть его дочерние поля:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Поле      | Описание                                        |
    | --------- | ----------------------------------------------- |
    | `<http>`  | Список из одного или нескольких резолверов* |
    | `<https>` | Список из одного или нескольких резолверов* |
  </TabItem>

  <TabItem value="http_https" label="<http> и <https>">
    | Поле         | Описание                                     |
    | ------------ | -------------------------------------------- |
    | `<resolver>` | Конечная точка и другие сведения о резолвере |

    :::note
    Можно указать несколько элементов `<resolver>`, но для каждого
    протокола используется только первый `<resolver>`. Все остальные
    элементы `<resolver>` для этого протокола игнорируются. Это значит, что балансировка нагрузки
    (если она нужна) должна быть реализована удалённым резолвером.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | Поле                 | Описание                                                                                                                                                                                                  |
    | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `<endpoint>`         | URI резолвера прокси                                                                                                                                                                                      |
    | `<proxy_scheme>`     | Протокол итогового URI прокси. Это может быть `http` или `https`.                                                                                                                                         |
    | `<proxy_port>`       | Номер порта резолвера прокси                                                                                                                                                                              |
    | `<proxy_cache_time>` | Время в секундах, в течение которого ClickHouse должен кэшировать значения от резолвера. Если установить это значение в `0`, ClickHouse будет обращаться к резолверу для каждого HTTP- или HTTPS-запроса. |
  </TabItem>
</Tabs>

**Старшинство**

Настройки прокси определяются в следующем порядке:

| Порядок | Настройка                  |
| ------- | -------------------------- |
| 1.      | Удалённые резолверы прокси |
| 2.      | Списки прокси              |
| 3.      | Переменные окружения       |

ClickHouse проверит тип резолвера с наивысшим приоритетом для протокола запроса. Если он не определён,
будет проверен следующий по приоритету тип резолвера, пока не дойдёт до резолвера окружения.
Это также позволяет использовать комбинацию разных типов резолверов.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

По умолчанию для выполнения запросов `HTTPS` через прокси `HTTP` используется туннелирование (то есть `HTTP CONNECT`). Этот параметр позволяет его отключить.

**no&#95;proxy**

По умолчанию все запросы проходят через прокси. Чтобы отключить прокси для определённых хостов, необходимо задать переменную `no_proxy`.
Её можно задать в секции `<proxy>` для list- и remote-resolver&#39;ов, а для environment resolver — как переменную окружения.
Поддерживаются IP-адреса, домены, поддомены и подстановочный символ `'*'` для полного обхода. Начальные точки удаляются так же, как в curl.

**Пример**

Приведённая ниже конфигурация отключает прокси для запросов к `clickhouse.cloud` и всем его поддоменам (например, `auth.clickhouse.cloud`).
То же самое относится и к GitLab, несмотря на начальную точку. И `gitlab.com`, и `about.gitlab.com` будут обходить прокси.

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

Каталог, используемый для хранения всех запросов `CREATE WORKLOAD` и `CREATE RESOURCE`. По умолчанию используется папка `/workload/` в рабочем каталоге сервера.

**Пример**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**См. также**

* [Иерархия рабочих нагрузок](/ru/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

Путь к узлу ZooKeeper, который используется как хранилище для всех запросов `CREATE WORKLOAD` и `CREATE RESOURCE`. Для согласованности все SQL-определения хранятся как значение этого единственного znode. По умолчанию ZooKeeper не используется, и определения хранятся на [disk](#workload_path).

**Пример**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**См. также**

* [Иерархия рабочих нагрузок](/ru/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

Настройки системной таблицы [`zookeeper_log`](/ru/operations/system-tables/zookeeper_log).

Следующие настройки задаются с помощью вложенных тегов:

<SystemLogParameters />

**Пример**

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