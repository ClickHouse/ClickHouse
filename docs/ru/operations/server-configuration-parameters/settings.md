---
sidebar_position: 57
sidebar_label: "Конфигурационные параметры сервера"
---

# Конфигурационные параметры сервера {#server-configuration-parameters-reference}

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

Интервал (в секундах) перезагрузки встроенных словарей.

ClickHouse перезагружает встроенные словари с заданным интервалом. Это позволяет править словари «на лету» без перезапуска сервера.

Значение по умолчанию - 3600.

**Пример**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#compression}

Настройки компрессии данных.

:::danger "Внимание"
    Лучше не использовать, если вы только начали работать с ClickHouse.

Общий вид конфигурации:

``` xml
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

Поля блока `<case>`:

-   `min_part_size` - Минимальный размер части таблицы.
-   `min_part_size_ratio` - Отношение размера минимальной части таблицы к полному размеру таблицы.
-   `method` - Метод сжатия. Возможные значения: `lz4`, `lz4hc`, `zstd`.
-   `level` – Уровень сжатия. См. [Кодеки](../../sql-reference/statements/create/table/#create-query-common-purpose-codecs).

Можно сконфигурировать несколько разделов `<case>`.

ClickHouse проверяет условия для `min_part_size` и `min_part_size_ratio` и выполнит те блоки `case`, для которых условия совпали.

-   Если кусок данных совпадает с условиями, ClickHouse использует указанные метод сжатия.
-   Если кусок данных совпадает с несколькими блоками `case`, ClickHouse использует первый совпавший блок условий.

Если ни один `<case>` не подходит, то ClickHouse применит алгоритм сжатия `lz4`.

**Пример**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

## encryption {#server-settings-encryption}

Настраивает команду для получения ключа, используемого [кодеками шифрования](../../sql-reference/statements/create/table.md#create-query-encryption-codecs). Ключ (или несколько ключей) должен быть записан в переменные окружения или установлен в конфигурационном файле.

Ключи могут быть представлены в шестнадцатеричной или строковой форме. Их длина должна быть равна 16 байтам.

**Пример**

Загрузка из файла конфигурации:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>12345567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

    :::note "Примечание"
    Хранение ключей в конфигурационном файле не рекомендовано. Это не безопасно. Вы можете переместить ключи в отдельный файл на секретном диске и сделать symlink к этому конфигурационному файлу в папке `config.d/`.
    :::
Загрузка из файла конфигурации, когда ключ представлен в шестнадцатеричной форме:

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

Параметр `current_key_id` устанавливает текущий ключ для шифрования, и все указанные ключи можно использовать для расшифровки.

Все эти методы могут быть применены для нескольких ключей:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Параметр `current_key_id` указывает текущий ключ для шифрования.

Также пользователь может добавить одноразовое случайное число длинной 12 байт (по умолчанию шифрование и дешифровка будут использовать одноразовое число длинной 12 байт, заполненное нулями):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Одноразовое число также может быть представлено в шестнадцатеричной форме:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Всё вышеперечисленное также применимо для алгоритма `aes_256_gcm_siv` (но ключ должен быть длиной 32 байта).

## custom_settings_prefixes {#custom_settings_prefixes}

Список префиксов для [пользовательских настроек](../../operations/settings/#custom_settings). Префиксы должны перечисляться через запятую.

**Пример**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**См. также**

-   [Пользовательские настройки](../../operations/settings#custom_settings)

## core_dump {#server_configuration_parameters-core_dump}

Задает мягкое ограничение для размера файла дампа памяти.

Возможные значения:

-   положительное целое число.

Значение по умолчанию: `1073741824` (1 ГБ).

:::info "Примечание"
    Жесткое ограничение настраивается с помощью системных инструментов.

**Пример**

```xml
<core_dump>
    <size_limit>1073741824</size_limit>
</core_dump>
```

## database_atomic_delay_before_drop_table_sec {#database_atomic_delay_before_drop_table_sec}

Устанавливает задержку перед удалением табличных данных, в секундах. Если запрос имеет идентификатор `SYNC`, эта настройка игнорируется.

Значение по умолчанию: `480` (8 минут).

## default\_database {#default-database}

База данных по умолчанию.

Перечень баз данных можно получить запросом [SHOW DATABASES](../../operations/server-configuration-parameters/settings.md#show-databases).

**Пример**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

Профиль настроек по умолчанию.

Профили настроек находятся в файле, указанном в параметре `user_config`.

**Пример**

``` xml
<default_profile>default</default_profile>
```

## default_replica_path {#default_replica_path}

Путь к таблице в ZooKeeper.

**Пример**

``` xml
<default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
```
## default_replica_name {#default_replica_name}

Имя реплики в ZooKeeper.

**Пример**

``` xml
<default_replica_name>{replica}</default_replica_name>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

Путь к конфигурации внешних словарей.

Путь:

-   Указывается абсолютным или относительно конфигурационного файла сервера.
-   Может содержать wildcard-ы \* и ?.

Смотрите также «[Внешние словари](../../operations/server-configuration-parameters/settings.md)».

**Пример**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## user_defined_executable_functions_config {#server_configuration_parameters-user_defined_executable_functions_config}

Путь к файлу конфигурации для исполняемых пользовательских функций.

Путь:

-   Указывается абсолютным или относительно конфигурационного файла сервера.
-   Может содержать wildcard-ы \* и ?.

Смотрите также “[Исполняемые пользовательские функции](../../sql-reference/functions/index.md#executable-user-defined-functions).”.

**Пример**

``` xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

Отложенная загрузка словарей.

Если `true`, то каждый словарь создаётся при первом использовании. Если словарь не удалось создать, то вызов функции, использующей словарь, сгенерирует исключение.

Если `false`, то все словари создаются при старте сервера, если словарь или словари создаются слишком долго или создаются с ошибкой, то сервер загружается без
этих словарей и продолжает попытки создать эти словари.

По умолчанию - `true`.

**Пример**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

Путь к каталогу со схемами для входных данных. Например со схемами для формата [CapnProto](../../interfaces/formats.md#capnproto).

**Пример**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## graphite {#server_configuration_parameters-graphite}

Отправка данных в [Graphite](https://github.com/graphite-project).

Настройки:

-   host – Сервер Graphite.
-   port – Порт сервера Graphite.
-   interval – Период отправки в секундах.
-   timeout – Таймаут отправки данных в секундах.
-   root_path – Префикс для ключей.
-   metrics – Отправка данных из таблицы [system.metrics](../../operations/server-configuration-parameters/settings.md#system_tables-metrics).
-   events – Отправка дельты данных, накопленной за промежуток времени из таблицы [system.events](../../operations/server-configuration-parameters/settings.md#system_tables-events).
-   events_cumulative – Отправка суммарных данных из таблицы [system.events](../../operations/server-configuration-parameters/settings.md#system_tables-events).
-   asynchronous_metrics – Отправка данных из таблицы [system.asynchronous_metrics](../../operations/server-configuration-parameters/settings.md#system_tables-asynchronous_metrics).

Можно определить несколько секций `<graphite>`, например, для передачи различных данных с различной частотой.

**Пример**

``` xml
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

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

Настройка прореживания данных для Graphite.

Подробнее читайте в разделе [GraphiteMergeTree](../../operations/server-configuration-parameters/settings.md).

**Пример**

``` xml
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

## http_port/https_port {#http-porthttps-port}

Порт для обращений к серверу по протоколу HTTP(s).

Если указан `https_port`, то требуется конфигурирование [openSSL](#server_configuration_parameters-openssl).

Если указан `http_port`, то настройка openSSL игнорируется, даже если она задана.

**Пример**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

Страница, показываемая по умолчанию, при обращении к HTTP(s) серверу ClickHouse.
Значение по умолчанию «Ok.» (с переводом строки на конце).

**Пример**

Показывает `https://tabix.io/` при обращении к `http://localhost:http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## hsts_max_age  {#hsts-max-age}

Срок действия HSTS в секундах. Значение по умолчанию `0` (HSTS выключен). Для включения HSTS задайте положительное число. Срок действия HSTS будет равен введенному числу.

**Пример**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## include_from {#server_configuration_parameters-include_from}

Путь к файлу с подстановками.

Подробности смотрите в разделе «[Конфигурационные файлы](../configuration-files.md#configuration_files)».

**Пример**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_http_port {#interserver-http-port}

Порт для обмена между серверами ClickHouse.

**Пример**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

Имя хоста, которое могут использовать другие серверы для обращения к этому хосту.

Если не указано, то определяется аналогично команде `hostname -f`.

Удобно использовать, чтобы отвязаться от конкретного сетевого интерфейса.

**Пример**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver_https_port {#interserver-https-port}

Порт для обмена данными между репликами ClickHouse по протоколу `HTTPS`.

**Пример**

``` xml
<interserver_https_port>9010</interserver_https_port>
```

## interserver_https_host {#interserver-https-host}

Имя хоста, которое могут использовать другие реплики для обращения к нему по протоколу `HTTPS`.

**Пример**

``` xml
<interserver_https_host>example.yandex.ru</interserver_https_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

Имя пользователя и пароль, использующиеся для подключения к другим серверам при [репликации](../../engines/table-engines/mergetree-family/replication.md) движками Replicated\*. Сервер использует эти же учетные данные при аутентификации других реплик. Поэтому настройки `interserver_http_credentials` должны быть заданы одинаковыми для всех реплик кластера.

По умолчанию, если секция `interserver_http_credentials` не задана в конфигурации, аутентификация при репликации не используется.

    :::note "Примечание"
    Настройки `interserver_http_credentials` не относятся к [конфигурации](../../interfaces/cli.md#configuration_files) учетных данных клиента ClickHouse.
    :::
    :::note "Примечание"
    Учетные данные в `interserver_http_credentials` являются общими для репликации по `HTTP` и `HTTPS`.
    :::
Раздел содержит следующие параметры:

-   `user` — имя пользователя.
-   `password` — пароль.
-   `allow_empty` — если `true`, то другие реплики могут подключаться без аутентификации, даже если учетные данные заданы. Если `false`, то подключение без аутентификации не допускается. Значение по умолчанию: `false`.
-   `old` — секция содержит старые значения `user` и `password`, которые используются в процессе изменения учетных данных. Можно указывать несколько секций `old`.

**Изменение учетных данных**

ClickHouse поддерживает динамическое изменение учетных данных. При этом не требуется одновременно останавливать все реплики, чтобы обновить конфигурацию. Изменение учетных данных выполняется за несколько шагов.

Чтобы включить аутентификацию, установите `interserver_http_credentials.allow_empty` в значение `true` и задайте учетные данные. С такой конфигурацией разрешены подключения как с аутентификацией, так и без нее.

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

После конфигурации всех реплик установите `allow_empty` в значение `false` или удалите эту настройку. Это сделает аутентификацию с новыми учетными данными обязательной.

Чтобы изменить учетные данные, перенесите имя пользователя и пароль в секцию `interserver_http_credentials.old` и укажите новые значения для `user` и `password`. Сервер будет использовать новые учетные данные при подключении к другим репликам и при этом будет разрешать подключения как с новыми, так и со старыми учетными данными.

``` xml
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

Когда новые учетные данные обновятся на всех репликах, старые учетные данные можно удалить из конфигурации.

## keep_alive_timeout {#keep-alive-timeout}

Время в секундах, в течение которого ClickHouse ожидает входящих запросов прежде чем закрыть соединение. Значение по умолчанию: 10 секунд.

**Пример**

``` xml
<keep_alive_timeout>10</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

Ограничение по хостам, с которых может прийти запрос. Если необходимо, чтобы сервер отвечал всем, то надо указать `::`.

Примеры:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## listen_backlog {#server_configuration_parameters-listen_backlog}

Бэклог (размер очереди соединений, ожидающих принятия) прослушивающего сокета.

Значение по умолчанию: `4096` (как в linux [5.4+](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=19f92a030ca6d772ab44b22ee6a01378a8cb32d4)).

Обычно это значение незачем менять по следующим причинам:
-  значение по умолчанию достаточно велико,
-  для принятия соединения клиента у сервера есть отдельный поток.

Так что даже если у вас `TcpExtListenOverflows` (из `nstat`) ненулевой и растет для сервера ClickHouse, это не повод увеличивать значение по умолчанию, поскольку:
-  обычно если 4096 недостаточно, это говорит о внутренних проблемах ClickHouse с масштабированием, так что лучше сообщить о проблеме,
-  и это не значит, что сервер сможет принять еще больше подключений в дальнейшем (а если и сможет, клиенты, вероятно, уже отсоединятся).

Примеры:

``` xml
<listen_backlog>4096</listen_backlog>
```

## logger {#server_configuration_parameters-logger}

Настройки логирования.

Ключи:

-   `level` - Уровень логирования. Допустимые значения: `trace`, `debug`, `information`, `warning`, `error`.
-   `log` - Файл лога. Содержит все записи согласно `level`.
-   `errorlog` - Файл лога ошибок.
-   `size` - Размер файла. Действует для `log` и `errorlog`. Как только файл достиг размера `size`, ClickHouse архивирует и переименовывает его, а на его месте создает новый файл лога.
-   `count` - Количество заархивированных файлов логов, которые сохраняет ClickHouse.

**Пример**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Также, существует поддержка записи в syslog. Пример настроек:

``` xml
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

Ключи для syslog:

-   use_syslog - обязательная настройка, если требуется запись в syslog
-   address - хост\[:порт\] демона syslogd. Если не указан, используется локальный
-   hostname - опционально, имя хоста, с которого отсылаются логи
-   facility - [категория syslog](https://en.wikipedia.org/wiki/Syslog#Facility), записанная в верхнем регистре, с префиксом «LOG_»: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` и прочие).
    Значения по умолчанию: при указанном `address` - `LOG_USER`, иначе - `LOG_DAEMON`
-   format - формат сообщений. Возможные значения - `bsd` и `syslog`

## send_crash_reports {#server_configuration_parameters-send_crash_reports}

Настройки для отправки сообщений о сбоях в команду разработчиков ядра ClickHouse через [Sentry](https://sentry.io).
Включение этих настроек, особенно в pre-production среде, может дать очень ценную информацию и поможет развитию ClickHouse.

Сервер на котором включены данные настройки должен иметь доступ в Интернет по протоколу IPv4 (на момент написания документации IPv6 не поддерживается публичным облаком Sentry) для правильной работы данной функциональности.

Ключи:

-   `enabled` – Булевый флаг чтобы включить функциональность, по умолчанию `false`. Установите `true` чтобы разрешить отправку отчетов о сбоях.
-   `endpoint` – Вы можете переопределить URL на который будут отсылаться отчеты об ошибках и использовать собственную инсталляцию Sentry. Используйте URL синтаксис [Sentry DSN](https://docs.sentry.io/error-reporting/quickstart/?platform=native#configure-the-sdk).
-   `anonymize` - Запретить отсылку имени хоста сервера в отчете о сбое.
-   `http_proxy` - Настройка HTTP proxy для отсылки отчетов о сбоях.
-   `debug` - Настроить клиентскую библиотеку Sentry в debug режим.
-   `tmp_path` - Путь в файловой системе для временного хранения состояния отчетов о сбоях перед отправкой на сервер Sentry.

**Рекомендованные настройки**

``` xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## macros {#macros}

Подстановки параметров реплицируемых таблиц.

Можно не указывать, если реплицируемые таблицы не используются.

Подробнее смотрите в разделе [Создание реплицируемых таблиц](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Пример**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

Приблизительный размер (в байтах) кэша засечек, используемых движками таблиц семейства [MergeTree](../../operations/server-configuration-parameters/settings.md).

Кэш общий для сервера, память выделяется по мере необходимости.

**Пример**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_server_memory_usage {#max_server_memory_usage}

Ограничивает объём оперативной памяти, используемой сервером ClickHouse. Настройка может быть задана только для профиля `default`.

Возможные значения:

-   Положительное целое число.
-   0 — автоматически.

Значение по умолчанию: `0`.

**Дополнительная информация**

Значение по умолчанию для `max_server_memory_usage` рассчитывается как `memory_amount * max_server_memory_usage_to_ram_ratio`.

**См. также**

-   [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage)
-   [max_server_memory_usage_to_ram_ratio](#max_server_memory_usage_to_ram_ratio)

## max_server_memory_usage_to_ram_ratio {#max_server_memory_usage_to_ram_ratio}

Определяет долю оперативной памяти, доступную для использования сервером Clickhouse. Если сервер попытается использовать больше, предоставляемый ему объём памяти будет ограничен до расчётного значения.

Возможные значения:

-   Положительное число с плавающей запятой.
-   0 — сервер Clickhouse может использовать всю оперативную память.

Значение по умолчанию: `0.9`.

**Использование**

На серверах с небольшим объёмом оперативной памяти и файла подкачки может потребоваться установить настройку `max_server_memory_usage_to_ram_ratio` в значение, большее 1.

**Пример**

``` xml
<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
```

**См. также**

-   [max_server_memory_usage](#max_server_memory_usage)

## max_concurrent_queries {#max-concurrent-queries}

Определяет максимальное количество одновременно обрабатываемых запросов, связанных с таблицей семейства `MergeTree`. Запросы также могут быть ограничены настройками: [max_concurrent_insert_queries](#max-concurrent-insert-queries), [max_concurrent_select_queries](#max-concurrent-select-queries), [max_concurrent_queries_for_user](#max-concurrent-queries-for-user), [max_concurrent_queries_for_all_users](#max-concurrent-queries-for-all-users), [min_marks_to_honor_max_concurrent_queries](#min-marks-to-honor-max-concurrent-queries).

:::info "Примечание"
	Параметры этих настроек могут быть изменены во время выполнения запросов и вступят в силу немедленно. Запросы, которые уже запущены, выполнятся без изменений.

Возможные значения:

-   Положительное целое число.
-   0 — нет лимита.

Значение по умолчанию: `100`.

**Пример**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max_concurrent_insert_queries {#max-concurrent-insert-queries}

Определяет максимальное количество одновременных `INSERT` запросов.

:::info "Примечание"
	Параметры этих настроек могут быть изменены во время выполнения запросов и вступят в силу немедленно. Запросы, которые уже запущены, выполнятся без изменений.

Возможные значения:

-   Положительное целое число.
-   0 — нет лимита.

Значение по умолчанию: `0`.

**Example**

``` xml
<max_concurrent_insert_queries>100</max_concurrent_insert_queries>
```

## max_concurrent_select_queries {#max-concurrent-select-queries}

Определяет максимальное количество одновременных `SELECT` запросов.

:::info "Примечание"
	Параметры этих настроек могут быть изменены во время выполнения запросов и вступят в силу немедленно. Запросы, которые уже запущены, выполнятся без изменений.

Возможные значения:

-   Положительное целое число.
-   0 — нет лимита.

Значение по умолчанию: `0`.

**Example**

``` xml
<max_concurrent_select_queries>100</max_concurrent_select_queries>
```

## max_concurrent_queries_for_user {#max-concurrent-queries-for-user}

Определяет максимальное количество одновременно обрабатываемых запросов, связанных с таблицей семейства `MergeTree`, для пользователя.

Возможные значения:

-   Положительное целое число.
-   0 — нет лимита.

Значение по умолчанию: `0`.

**Пример**

``` xml
<max_concurrent_queries_for_user>5</max_concurrent_queries_for_user>
```

## max_concurrent_queries_for_all_users {#max-concurrent-queries-for-all-users}

Если значение этой настройки меньше или равно текущему количеству одновременно обрабатываемых запросов, то будет сгенерировано исключение.

Пример: `max_concurrent_queries_for_all_users` установлен на 99 для всех пользователей. Чтобы выполнять запросы даже когда сервер перегружен, администратор баз данных устанавливает для себя значение настройки на 100.

Изменение настройки для одного запроса или пользователя не влияет на другие запросы.

Возможные значения:

-   Положительное целое число.
-   0 — нет лимита.

Значение по умолчанию: `0`.

**Пример**

``` xml
<max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users>
```

**Смотрите также**

-   [max_concurrent_queries](#max-concurrent-queries)

## min_marks_to_honor_max_concurrent_queries {#min-marks-to-honor-max-concurrent-queries}

Определяет минимальное количество засечек, считываемых запросом для применения настройки [max_concurrent_queries](#max-concurrent-queries).

Возможные значения:

-   Положительное целое число.
-   0 — выключена.

**Пример**

``` xml
<min_marks_to_honor_max_concurrent_queries>10</min_marks_to_honor_max_concurrent_queries>
```

## max_connections {#max-connections}

Максимальное количество входящих соединений.

**Пример**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

Максимальное количество открытых файлов.

По умолчанию - `maximum`.

Рекомендуется использовать в Mac OS X, поскольку функция `getrlimit()` возвращает некорректное значение.

**Пример**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

Ограничение на удаление таблиц.

Если размер таблицы семейства [MergeTree](../../operations/server-configuration-parameters/settings.md) превышает `max_table_size_to_drop` (в байтах), то ее нельзя удалить запросом DROP.

Если таблицу все же необходимо удалить, не перезапуская при этом сервер ClickHouse, то необходимо создать файл `<clickhouse-path>/flags/force_drop_table` и выполнить запрос DROP.

Значение по умолчанию - 50GB.

Значение 0 означает, что можно удалять все таблицы без ограничений.

**Пример**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max_thread_pool_size {#max-thread-pool-size}

ClickHouse использует потоки из глобального пула потоков для обработки запросов. Если в пуле нет свободных потоков, то в нем создается еще один. Параметр `max_thread_pool_size` ограничивает максимальное количество потоков в пуле.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `10000`.

**Пример**

``` xml
<max_thread_pool_size>12000</max_thread_pool_size>
```

## max_thread_pool_free_size {#max-thread-pool-free-size}

Если в глобальном пуле потоков количество **свободных** потоков больше, чем задано параметром `max_thread_pool_free_size`, то ClickHouse освобождает ресурсы, занятые некоторыми потоками. В таком случае размер пула уменьшается. При необходимости потоки будут созданы заново.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `1000`.

**Пример**

``` xml
<max_thread_pool_free_size>1200</max_thread_pool_free_size>
```

## thread_pool_queue_size {#thread-pool-queue-size}

Максимальное количество задач, которые запланированы для выполнения в глобальном пуле потоков. При увеличении этого параметра возрастает использование памяти. Рекомендуется, чтобы значение этого параметра совпадало со значением параметра [max_thread_pool_size](#max-thread-pool-size).

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `10000`.

**Пример**

``` xml
<thread_pool_queue_size>12000</thread_pool_queue_size>
```

## merge_tree {#server_configuration_parameters-merge_tree}

Тонкая настройка таблиц семейства [MergeTree](../../operations/server-configuration-parameters/settings.md).

Подробнее смотрите в заголовочном файле MergeTreeSettings.h.

**Пример**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## metric_log {#metric_log}

Эта настройка включена по умолчанию. Если это не так, вы можете включить ее сами.

**Включение**

Чтобы вручную включить сбор истории метрик в таблице [`system.metric_log`](../../operations/system-tables/metric_log.md), создайте `/etc/clickhouse-server/config.d/metric_log.xml` следующего содержания:

``` xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</clickhouse>
```

**Выключение**

Чтобы отключить настройку `metric_log` , создайте файл `/etc/clickhouse-server/config.d/disable_metric_log.xml` следующего содержания:

``` xml
<clickhouse>
<metric_log remove="1" />
</clickhouse>
```

## replicated\_merge\_tree {#server_configuration_parameters-replicated_merge_tree}

Тонкая настройка таблиц в [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Эта настройка имеет более высокий приоритет.

Подробнее смотрите в заголовочном файле MergeTreeSettings.h.

**Пример**

``` xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

Настройки клиента/сервера SSL.

Поддержку SSL обеспечивает библиотека `libpoco`. Описание интерфейса находится в файле [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Ключи настроек сервера/клиента:

-   privateKeyFile - Путь к файлу с секретным ключом сертификата в формате PEM. Файл может содержать ключ и сертификат одновременно.
-   certificateFile - Путь к файлу сертификата клиента/сервера в формате PEM. Можно не указывать, если `privateKeyFile` содержит сертификат.
-   caConfig - Путь к файлу или каталогу, которые содержат доверенные корневые сертификаты.
-   verificationMode - Способ проверки сертификатов узла. Подробности находятся в описании класса [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Допустимые значения: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth - Максимальная длина верификационной цепи. Верификация завершится ошибкой, если длина цепи сертификатов превысит установленное значение.
-   loadDefaultCAFile - Признак того, что будут использоваться встроенные CA-сертификаты для OpenSSL. Допустимые значения: `true`, `false`. \|
-   cipherList - Поддерживаемые OpenSSL-шифры. Например, `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions - Включение/выключение кеширования сессии. Использовать обязательно вместе с `sessionIdContext`. Допустимые значения: `true`, `false`.
-   sessionIdContext - Уникальный набор произвольных символов, которые сервер добавляет к каждому сгенерированному идентификатору. Длина строки не должна превышать `SSL_MAX_SSL_SESSION_ID_LENGTH`. Рекомендуется к использованию всегда, поскольку позволяет избежать проблем как в случае, если сервер кеширует сессию, так и если клиент затребовал кеширование. По умолчанию `${application.name}`.
-   sessionCacheSize - Максимальное количество сессий, которые кэширует сервер. По умолчанию - 1024\*20. 0 - неограниченное количество сессий.
-   sessionTimeout - Время кеширования сессии на сервере.
-   extendedVerification - Автоматическая расширенная проверка сертификатов после завершении сессии. Допустимые значения: `true`, `false`.
-   requireTLSv1 - Требование соединения TLSv1. Допустимые значения: `true`, `false`.
-   requireTLSv1_1 - Требование соединения TLSv1.1. Допустимые значения: `true`, `false`.
-   requireTLSv1_2 - Требование соединения TLSv1.2. Допустимые значения: `true`, `false`.
-   fips - Активация режима OpenSSL FIPS. Поддерживается, если версия OpenSSL, с которой собрана библиотека поддерживает fips.
-   privateKeyPassphraseHandler - Класс (подкласс PrivateKeyPassphraseHandler)запрашивающий кодовую фразу доступа к секретному ключу. Например, `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler - Класс (подкласс CertificateHandler) для подтверждения не валидных сертификатов. Например, `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>`.
-   disableProtocols - Запрещенные к использованию протоколы.
-   preferServerCiphers - Предпочтение серверных шифров на клиенте.

**Пример настройки:**

``` xml
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

## part_log {#server_configuration_parameters-part-log}

Логирование событий, связанных с данными типа [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Например, события добавления или мержа данных. Лог можно использовать для симуляции алгоритмов слияния, чтобы сравнивать их характеристики. Также, можно визуализировать процесс слияния.

Запросы логируются не в отдельный файл, а в таблицу [system.part_log](../../operations/system-tables/part_log.md#system_tables-part-log). Вы можете изменить название этой таблицы в параметре `table` (см. ниже).

При настройке логирования используются следующие параметры:

-   `database` — имя базы данных;
-   `table` — имя таблицы;
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../operations/server-configuration-parameters/settings.md). Нельзя использовать если используется `engine`
-   `engine` - устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать если используется `partition_by`.
-   `flush_interval_milliseconds` — период сброса данных из буфера в памяти в таблицу.

**Пример**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## path {#server_configuration_parameters-path}

Путь к каталогу с данными.

:::danger "Обратите внимание"
    Завершающий слеш обязателен.

**Пример**

``` xml
<path>/var/lib/clickhouse/</path>
```

## prometheus {#server_configuration_parameters-prometheus}

Опубликовать данные о метриках, для сбора с помощью системы мониторинга [Prometheus](https://prometheus.io).

Настройки:

-   `endpoint` – путь по которому будет осуществляться экспорт данных метрик по HTTP протоколу для сбора с помощью prometheus. Должен начинаться с ‘/’.
-   `port` – порт по которому будет доступен endpoint для сбора метрик.
-   `metrics` – флаг для экспорта текущих значений метрик из таблицы [system.metrics](../system-tables/metrics.md#system_tables-metrics).
-   `events` – флаг для экспорта текущих значений метрик из таблицы [system.events](../system-tables/events.md#system_tables-events).
-   `asynchronous_metrics` – флаг для экспорта текущих значений значения метрик из таблицы [system.asynchronous_metrics](../system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics).

**Пример**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query_log {#server_configuration_parameters-query-log}

Настройка логирования запросов, принятых с настройкой [log_queries=1](../../operations/settings/settings.md).

Запросы логируются не в отдельный файл, а в системную таблицу [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log). Вы можете изменить название этой таблицы в параметре `table` (см. ниже).

При настройке логирования используются следующие параметры:

-   `database` — имя базы данных;
-   `table` — имя таблицы, куда будет записываться лог;
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../operations/server-configuration-parameters/settings.md). Нельзя использовать если используется `engine`
-   `engine` - устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать если используется `partition_by`.
-   `flush_interval_milliseconds` — период сброса данных из буфера в памяти в таблицу.

Если таблица не существует, то ClickHouse создаст её. Если структура журнала запросов изменилась при обновлении сервера ClickHouse, то таблица со старой структурой переименовывается, а новая таблица создается автоматически.

**Пример**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query_thread_log}

Настройка логирования потоков выполнения запросов, принятых с настройкой [log_query_threads=1](../settings/settings.md#settings-log-query-threads).

Запросы логируются не в отдельный файл, а в системную таблицу [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log). Вы можете изменить название этой таблицы в параметре `table` (см. ниже).

При настройке логирования используются следующие параметры:

-   `database` — имя базы данных;
-   `table` — имя таблицы, куда будет записываться лог;
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../operations/server-configuration-parameters/settings.md). Нельзя использовать если используется `engine`
-   `engine` - устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать если используется `partition_by`.
-   `flush_interval_milliseconds` — период сброса данных из буфера в памяти в таблицу.

Если таблица не существует, то ClickHouse создаст её. Если структура журнала запросов изменилась при обновлении сервера ClickHouse, то таблица со старой структурой переименовывается, а новая таблица создается автоматически.

**Пример**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## query_views_log {#server_configuration_parameters-query_views_log}

Настройки логирования информации о зависимых представлениях (materialized, live и т.п.) в запросах принятых с настройкой [log_query_views=1](../../operations/settings/settings.md#settings-log-query-views).

Запросы логируются в таблице [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log). Вы можете изменить название этой таблицы в параметре `table` (см. ниже).

При настройке логирования используются следующие параметры:

-   `database` – имя базы данных.
-   `table` – имя системной таблицы, где будут логироваться запросы.
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../engines/table-engines/mergetree-family/custom-partitioning-key.md). Нельзя использовать, если задан параметр `engine`.
-   `engine` — устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать, если задан параметр `partition_by`.
-   `flush_interval_milliseconds` — период сброса данных из буфера в памяти в таблицу.

Если таблица не существует, то ClickHouse создаст её. Если структура журнала запросов изменилась при обновлении сервера ClickHouse, то таблица со старой структурой переименовывается, а новая таблица создается автоматически.

**Пример**

``` xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_views_log>
```

## text_log {#server_configuration_parameters-text_log}

Настройка логирования текстовых сообщений в системную таблицу [text_log](../../operations/system-tables/text_log.md#system_tables-text_log).

Параметры:

-   `level` — Максимальный уровень сообщения (по умолчанию `Trace`) которое будет сохранено в таблице.
-   `database` — имя базы данных для хранения таблицы.
-   `table` — имя таблицы, куда будут записываться текстовые сообщения.
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../engines/table-engines/mergetree-family/custom-partitioning-key.md). Нельзя использовать если используется `engine`
-   `engine` - устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать если используется `partition_by`.
-   `flush_interval_milliseconds` — период сброса данных из буфера в памяти в таблицу.

**Пример**
```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```


## trace_log {#server_configuration_parameters-trace_log}

Настройки для [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table operation.

Parameters:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — устанавливает [произвольный ключ партиционирования](../../operations/server-configuration-parameters/settings.md). Нельзя использовать если используется `engine`
-   `engine` - устанавливает [настройки MergeTree Engine](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) для системной таблицы. Нельзя использовать если используется `partition_by`.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

По умолчанию файл настроек сервера `config.xml` содержит следующие настройки:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query_masking_rules {#query-masking-rules}

Правила, основанные на регулярных выражениях, которые будут применены для всех запросов, а также для всех сообщений перед сохранением их в лог на сервере,
`system.query_log`, `system.text_log`, `system.processes` таблицы, а также в логах, отсылаемых клиенту. Это позволяет предотвратить утечку конфиденциальных данных из SQL запросов (такие как имена, электронные письма, личные идентификаторы или номера кредитных карт) в логи.

**Пример**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Параметры конфигурации:
- `name` - имя правила (необязательно)
- `regexp` - совместимое с RE2 регулярное выражение (обязательное)
- `replace` - строка замены для конфиденциальных данных (опционально, по умолчанию - шесть звездочек)

Правила маскировки применяются ко всему запросу (для предотвращения утечки конфиденциальных данных из неправильно оформленных / не интерпретируемых запросов).

`system.events` таблица содержит счетчик `QueryMaskingRulesMatch` который считает общее кол-во совпадений правил маскировки.

Для распределенных запросов каждый сервер должен быть сконфигурирован отдельно, иначе, подзапросы,
переданные на другие узлы, будут сохраняться без маскировки.

## remote_servers {#server-settings-remote-servers}

Конфигурация кластеров, которые использует движок таблиц [Distributed](../../operations/server-configuration-parameters/settings.md) и табличная функция `cluster`.

**Пример**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

Значение атрибута `incl` смотрите в разделе «[Конфигурационные файлы](../configuration-files.md#configuration_files)».

**Смотрите также**

-   [skip_unavailable_shards](../settings/settings.md#settings-skip_unavailable_shards)

## timezone {#server_configuration_parameters-timezone}

Временная зона сервера.

Указывается идентификатором IANA в виде часового пояса UTC или географического положения (например, Africa/Abidjan).

Временная зона необходима при преобразованиях между форматами String и DateTime, которые возникают при выводе полей DateTime в текстовый формат (на экран или в файл) и при получении DateTime из строки. Также, временная зона используется в функциях, которые работают со временем и датой, если они не получили временную зону в параметрах вызова.

**Пример**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

Порт для взаимодействия с клиентами по протоколу TCP.

**Пример**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

TCP порт для защищённого обмена данными с клиентами. Используйте с настройкой [OpenSSL](#server_configuration_parameters-openssl).

**Возможные значения**

Положительное целое число.

**Значение по умолчанию**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

Порт для взаимодействия с клиентами по протоколу MySQL.

**Возможные значения**

Положительное целое.

Пример

``` xml
<mysql_port>9004</mysql_port>
```

## tmp_path {#tmp-path}

Путь ко временным данным для обработки больших запросов.

:::danger "Обратите внимание"
    Завершающий слеш обязателен.

**Пример**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#tmp-policy}

Политика из [storage_configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) для хранения временных файлов.

Если политика не задана, используется [tmp_path](#tmp-path). В противном случае `tmp_path` игнорируется.

    :::note "Примечание"
    - `move_factor` игнорируется.
    - `keep_free_space_bytes` игнорируется.
    - `max_data_part_size_bytes` игнорируется.
    - В данной политике у вас должен быть ровно один том.
    :::
## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

Размер кеша (в байтах) для несжатых данных, используемых движками таблиц семейства [MergeTree](../../operations/server-configuration-parameters/settings.md).

Кеш единый для сервера. Память выделяется по требованию. Кеш используется в том случае, если включена опция [use_uncompressed_cache](../settings/settings.md).

Несжатый кеш выгодно использовать для очень коротких запросов в отдельных случаях.

**Пример**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

Каталог с пользовательскими файлами. Используется в табличной функции [file()](../../operations/server-configuration-parameters/settings.md).

**Пример**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## user_scripts_path {#server_configuration_parameters-user_scripts_path}

Каталог с файлами пользовательских скриптов. Используется для исполняемых пользовательских функций [Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions).

**Пример**

``` xml
<user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
```

## user_defined_path {#server_configuration_parameters-user_defined_path}

Каталог с определенными пользователем файлами. Используется для пользовательских SQL функций [SQL User Defined Functions](../../sql-reference/functions/index.md#user-defined-functions).

**Example**

``` xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## users_config {#users-config}

Путь к файлу, который содержит:

-   Конфигурации пользователей.
-   Права доступа.
-   Профили настроек.
-   Настройки квот.

**Пример**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

Содержит параметры, позволяющие ClickHouse взаимодействовать с кластером [ZooKeeper](http://zookeeper.apache.org/).

ClickHouse использует ZooKeeper для хранения метаданных о репликах при использовании реплицированных таблиц. Если реплицированные таблицы не используются, этот раздел параметров может отсутствовать.

Раздел содержит следующие параметры:

-   `node` — адрес ноды (сервера) ZooKeeper. Можно сконфигурировать несколько нод.

    Например:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      Атрибут `index` задает порядок опроса нод при попытках подключиться к кластеру ZooKeeper.

- `session_timeout_ms` — максимальный таймаут клиентской сессии в миллисекундах.
- `operation_timeout_ms` — максимальный таймаут для одной операции в миллисекундах.
- `root` — [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes), который используется как корневой для всех znode, которые использует сервер ClickHouse. Необязательный.
- `identity` — пользователь и пароль, которые может потребовать ZooKeeper для доступа к запрошенным znode. Необязательный.

**Пример конфигурации**

``` xml
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
</zookeeper>
```

**Смотрите также**

-   [Репликация](../../engines/table-engines/mergetree-family/replication.md)
-   [ZooKeeper Programmer’s Guide](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Способ хранения заголовков кусков данных в ZooKeeper.

Параметр применяется только к семейству таблиц `MergeTree`. Его можно установить:

-   Глобально в разделе [merge_tree](#server_configuration_parameters-merge_tree) файла `config.xml`.

    ClickHouse использует этот параметр для всех таблиц на сервере. Вы можете изменить настройку в любое время. Существующие таблицы изменяют свое поведение при изменении параметра.

-   Для каждой отдельной таблицы.

    При создании таблицы укажите соответствующую [настройку движка](../../operations/server-configuration-parameters/settings.md#table_engine-mergetree-creating-a-table). Поведение существующей таблицы с установленным параметром не изменяется даже при изменении глобального параметра.

**Возможные значения**

-   0 — функциональность выключена.
-   1 — функциональность включена.

Если `use_minimalistic_part_header_in_zookeeper = 1`, то [реплицированные](../../operations/server-configuration-parameters/settings.md) таблицы хранят заголовки кусков данных в компактном виде, используя только одну `znode`. Если таблица содержит много столбцов, этот метод хранения значительно уменьшает объём данных, хранящихся в Zookeeper.

    :::note "Внимание"
    После того как вы установили `use_minimalistic_part_header_in_zookeeper = 1`, невозможно откатить ClickHouse до версии, которая не поддерживает этот параметр. Будьте осторожны при обновлении ClickHouse на серверах в кластере. Не обновляйте все серверы сразу. Безопаснее проверять новые версии ClickHouse в тестовой среде или только на некоторых серверах кластера.

      Заголовки частей данных, ранее сохранённые с этим параметром, не могут быть восстановлены в их предыдущем (некомпактном) представлении.
    :::
**Значение по умолчанию**: 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

Отключает внутренний кеш DNS записей. Используется при эксплуатации ClickHouse в системах
с часто меняющейся инфраструктурой, таких как Kubernetes.

**Значение по умолчанию**: 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

Период обновления IP адресов у записей во внутреннем DNS кеше ClickHouse (в секундах).
Обновление выполняется асинхронно, отдельным системным потоком.

**Значение по умолчанию**: 15.

**Смотрите также**

-   [background_schedule_pool_size](../settings/settings.md#background_schedule_pool_size)

## distributed_ddl {#server-settings-distributed_ddl}

Управление запуском [распределенных ddl запросов](../../sql-reference/distributed-ddl.md) (CREATE, DROP, ALTER, RENAME) в кластере.
Работает только если разрешена [работа с ZooKeeper](#server-settings_zookeeper).

**Пример**

```xml
<distributed_ddl>
    <!-- Путь в ZooKeeper для очереди содержащей DDL запросы -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Настройки из этого профиля будут использованы для запуска DDL запросов -->
    <profile>default</profile>

    <!-- Контроль того как много ON CLUSTER запросов могут исполняться одновременно. -->
    <pool_size>1</pool_size>

    <!--
         Настройки очистки (активные задачи в очереди не будут удаляться)
    -->

    <!-- Время TTL для задач в секундах (по умолчанию 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Как часто будет запускаться очистка данных  (в секундах) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Как много задач может быть в очереди -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

## access_control_path {#access_control_path}

Путь к каталогу, где сервер ClickHouse хранит конфигурации пользователей и ролей, созданные командами SQL.

Значение по умолчанию: `/var/lib/clickhouse/access/`.

**Смотрите также**

- [Управление доступом](../../operations/access-rights.md#access-control)

## user_directories {#user_directories}

Секция конфигурационного файла,которая содержит настройки:
-   Путь к конфигурационному файлу с предустановленными пользователями.
-   Путь к файлу, в котором содержатся пользователи, созданные при помощи SQL команд.
-   Путь к узлу ZooKeeper, где хранятся и реплицируются пользователи, созданные с помощью команд SQL (экспериментальная функциональность).

Если эта секция определена, путь из [users_config](../../operations/server-configuration-parameters/settings.md#users-config) и [access_control_path](../../operations/server-configuration-parameters/settings.md#access_control_path) не используется.

Секция `user_directories` может содержать любое количество элементов, порядок расположения элементов обозначает их приоритет (чем выше элемент, тем выше приоритет).

**Примеры**

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Пользователи, роли, политики доступа к строкам, квоты и профили могут храниться в ZooKeeper:

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

Также вы можете добавить секции `memory` — означает хранение информации только в памяти, без записи на диск, и `ldap` — означает хранения информации на [LDAP-сервере](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol).

Чтобы добавить LDAP-сервер в качестве удаленного каталога пользователей, которые не определены локально, определите один раздел `ldap` со следующими параметрами:
-   `server` — имя одного из LDAP-серверов, определенных в секции `ldap_servers` конфигурационного файла. Этот параметр является необязательным и может быть пустым.
-   `roles` — раздел со списком локально определенных ролей, которые будут назначены каждому пользователю, полученному с LDAP-сервера. Если роли не заданы, пользователь не сможет выполнять никаких действий после аутентификации. Если какая-либо из перечисленных ролей не определена локально во время проверки подлинности, попытка проверки подлинности завершится неудачей, как если бы предоставленный пароль был неверным.

**Пример**

``` xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

## total_memory_profiler_step {#total-memory-profiler-step}

Задает размер памяти (в байтах) для трассировки стека на каждом шаге выделения максимума памяти. Данные хранятся в системной таблице [system.trace_log](../../operations/system-tables/trace_log.md) с `query_id`, равным пустой строке.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `4194304`.

## total_memory_tracker_sample_probability {#total-memory-tracker-sample-probability}

Позволяет собирать случайные выделения и освобождения памяти и записывать их в системную таблицу [system.trace_log](../../operations/system-tables/trace_log.md) с `trace_type`, равным `MemorySample`, с указанной вероятностью. Вероятность касается каждого выделения или освобождения памяти, независимо от размера выделения. Обратите внимание, что выборка происходит только тогда, когда объем неотслеживаемой памяти превышает лимит неотслеживаемой памяти (значение по умолчанию: `4` MiB). Значение настройки может быть уменьшено, если значение настройки [total_memory_profiler_step](#total-memory-profiler-step) уменьшено. Вы можете установить значение настройки `total_memory_profiler_step`, равным `1`, для особой детализованной выборки.

Возможные значения:

-   Положительное целое число.
-   0 — запись случайных выделений и освобождений памяти в системную таблицу `system.trace_log` отключена.

Значение по умолчанию: `0`.

## mmap_cache_size {#mmap-cache-size}

Задает размер кеша (в байтах) для сопоставленных файлов. Эта настройка позволяет избежать частых открытых/[mmap/munmap](https://en.wikipedia.org/wiki/Mmap)/закрытых вызовов (очень дорогостоящие из-за последующих ошибок страниц) и повторного использования сопоставления из нескольких потоков и запросов. Значение настройки — это количество сопоставленных областей (обычно равно количеству сопоставленных файлов). Объем данных в сопоставленных файлах можно отслеживать в системных таблицах [system.metrics](../../operations/system-tables/metrics.md), [system.metric_log](../../operations/system-tables/metric_log.md) по метрикам `MMappedFiles` и `MMappedFileBytes`, в таблицах [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md), [system.asynchronous_metrics_log](../../operations/system-tables/asynchronous_metric_log.md) по метрике `MMapCacheCells`, а также в [system.events](../../operations/system-tables/events.md), [system.processes](../../operations/system-tables/processes.md), [system.query_log](../../operations/system-tables/query_log.md), [system.query_thread_log](../../operations/system-tables/query_thread_log.md), [system.query_views_log](../../operations/system-tables/query_views_log.md) по событиям `CreatedReadBufferMMap`, `CreatedReadBufferMMapFailed`, `MMappedFileCacheHits`, `MMappedFileCacheMisses`. Обратите внимание, что объем данных в сопоставленных файлах не потребляет память напрямую и не учитывается в запросе или использовании памяти сервера, поскольку эта память может быть удалена аналогично кешу страниц ОС. Кеш удаляется (т.е. файлы закрываются) автоматически при удалении старых кусков в таблицах семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), также его можно удалить вручную с помощью запроса `SYSTEM DROP MMAP CACHE`.

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `1000`.

## compiled_expression_cache_size {#compiled-expression-cache-size}

Задает размер кеша (в байтах) для [скомпилированных выражений](../../operations/caches.md).

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `134217728`.

## compiled_expression_cache_elements_size {#compiled_expression_cache_elements_size}

Задает размер кеша (в элементах) для [скомпилированных выражений](../../operations/caches.md).

Возможные значения:

-   Положительное целое число.

Значение по умолчанию: `10000`.
