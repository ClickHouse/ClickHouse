# Серверные настройки

<a name="server_settings-builtin_dictionaries_reload_interval"></a>

## builtin_dictionaries_reload_interval

Интервал (в секундах) перезагрузки встроенных словарей.

ClickHouse перезагружает встроенные словари с заданным интервалом. Это позволяет править словари "на лету" без перезапуска сервера.

Значение по умолчанию - 3600.

**Пример**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<a name="server_settings-compression"></a>

## compression

Настройки компрессии данных.

!!! warning "Внимание"
    Лучше не использовать, если вы только начали работать с ClickHouse.

Общий вид конфигурации:

```xml
<compression>
    <case>
      <parameters/>
    </case>
    ...
</compression>
```

Можно сконфигурировать несколько разделов `<case>`.

Поля блока `<case>`:

- ``min_part_size`` - Минимальный размер части таблицы.
- ``min_part_size_ratio`` - Отношение размера минимальной части таблицы к полному размеру таблицы.
- ``method`` - Метод сжатия. Возможные значения: ``lz4``, ``zstd`` (экспериментальный).

ClickHouse проверит условия `min_part_size` и `min_part_size_ratio` и выполнит те блоки `case`, для которых условия совпали. Если ни один `<case>` не подходит, то ClickHouse применит алгоритм сжатия `lz4`.

**Пример**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

<a name="server_settings-default_database"></a>

## default_database

База данных по умолчанию.

Перечень баз данных можно получить запросом [SHOW DATABASES](../../query_language/misc.md#query_language_queries_show_databases).

**Пример**

```xml
<default_database>default</default_database>
```

<a name="server_settings-default_profile"></a>

## default_profile

Профиль настроек по умолчанию.

Профили настроек находятся в файле, указанном в параметре [user_config](#server_settings-users_config).

**Пример**

```xml
<default_profile>default</default_profile>
```

<a name="server_settings-dictionaries_config"></a>

## dictionaries_config

Путь к конфигурации внешних словарей.

Путь:

-   Указывается абсолютным или относительно конфигурационного файла сервера.
-   Может содержать wildcard-ы \* и ?.

Смотрите также "[Внешние словари](../../query_language/dicts/external_dicts.md#dicts-external_dicts)".

**Пример**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<a name="server_settings-dictionaries_lazy_load"></a>

## dictionaries_lazy_load

Отложенная загрузка словарей.

Если `true`, то каждый словарь создаётся при первом использовании. Если словарь не удалось создать, то вызов функции, использующей словарь, сгенерирует исключение.

Если `false`, то все словари создаются при старте сервера, и в случае ошибки сервер завершает работу.

По умолчанию - `true`.

**Пример**

```xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

<a name="server_settings-format_schema_path"></a>

## format_schema_path

Путь к каталогу со схемами для входных данных. Например со схемами для формата [CapnProto](../../interfaces/formats.md#format_capnproto).

**Пример**

```xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```


<a name="server_settings-graphite"></a>

## graphite

Отправка даных в [Graphite](https://github.com/graphite-project).

Настройки:

- host - Сервер Graphite.
- port - Порт сервера Graphite.
- interval - Период отправки в секундах.
- timeout - Таймаут отправки данных в секундах.
- root_path - Префикс для ключей.
- metrics - Отправка данных из таблицы :ref:`system_tables-system.metrics`.
- events - Отправка данных из таблицы :ref:`system_tables-system.events`.
- asynchronous_metrics - Отправка данных из таблицы :ref:`system_tables-system.asynchronous_metrics`.

Можно определить несколько секций `<graphite>`, например, для передачи различных данных с различной частотой.

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
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<a name="server_settings-graphite_rollup"></a>

## graphite_rollup

Настройка прореживания данных для Graphite.

Подробнее читайте в разделе [GraphiteMergeTree](../../operations/table_engines/graphitemergetree.md#table_engines-graphitemergetree).

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

<a name="server_settings-http_port"></a>

## http_port/https_port

Порт для обращений к серверу по протоколу HTTP(s).

Если указан `https_port`, то требуется конфигурирование [openSSL](#server_settings-openSSL).

Если указан `http_port`, то настройка openSSL игнорируется, даже если она задана.

**Пример**

```xml
<https>0000</https>
```

<a name="server_settings-http_server_default_response"></a>

## http_server_default_response

Страница, показываемая по умолчанию, при обращении к HTTP(s) серверу ClickHouse.

**Пример**

Показывает `https://tabix.io/` при обращенинии к `http://localhost:http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<a name="server_settings-include_from"></a>

## include_from

Путь к файлу с подстановками.

Подробности смотрите в разделе "[Конфигурационный файлы](../configuration_files.md#configuration_files)".

**Пример**

```xml
<include_from>/etc/metrica.xml</include_from>
```

<a name="server_settings-interserver_http_port"></a>

## interserver_http_port

Порт для обмена между серверами ClickHouse.

**Пример**

```xml
<interserver_http_port>9009</interserver_http_port>
```

<a name="server_settings-interserver_http_host"></a>

## interserver_http_host

Имя хоста, которое могут использовать другие серверы для обращения к этому.

Если не указано, то определяется аналогично команде `hostname -f`.

Удобно использовать, чтобы отвязаться от конкретного сетевого интерфейса.

**Пример**

```xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

<a name="server_settings-keep_alive_timeout"></a>

## keep_alive_timeout

Время в миллисекундах, в течение которого ClickHouse ожидает входящих запросов прежде, чем закрыть соединение.

**Пример**

```xml
<keep_alive_timeout>3</keep_alive_timeout>
```

<a name="server_settings-listen_host"></a>

## listen_host

Ограничение по хостам, с которых может прийти запрос. Если необходимо, чтобы сервер отвечал всем, то надо указать `::`.

Примеры:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<a name="server_settings-logger"></a>

## logger

Настройки логгирования.

Ключи:

- level - Уровень логгирования. Допустимые значения: ``trace``, ``debug``, ``information``, ``warning``, ``error``.
- log - Файл лога. Содержит все записи согласно ``level``.
- errorlog - Файл лога ошибок.
- size - Размер файла. Действует для ``log`` и ``errorlog``. Как только файл достиг размера ``size``, ClickHouse архивирует и переименовывает его, а на его месте создает новый файл лога.
- count - Количество заархивированных файлов логов, которые сохраняет ClickHouse.

**Пример**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Также, существует поддержка записи в syslog. Пример конфига:
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

Ключи:
- user_syslog - обязательная настройка, если требуется запись в syslog
- address - хост[:порт] демона syslogd. Если не указан, используется локальный
- hostname - опционально, имя хоста, с которого отсылаются логи
- facility - [категория syslog](https://en.wikipedia.org/wiki/Syslog#Facility),
записанная в верхнем регистре, с префиксом "LOG_": (``LOG_USER``, ``LOG_DAEMON``, ``LOG_LOCAL3`` и прочие).
Значения по умолчанию: при указанном ``address`` - ``LOG_USER``, иначе - ``LOG_DAEMON``
- format - формат сообщений. Возможные значения - ``bsd`` и ``syslog``


<a name="server_settings-macros"></a>

## macros

Подстановки параметров реплицируемых таблиц.

Можно не указывать, если реплицируемых таблицы не используются.

Подробнее смотрите в разделе "[Создание реплицируемых таблиц](../../operations/table_engines/replication.md#table_engines-replication-creation_of_rep_tables)".

**Пример**

```xml
<macros incl="macros" optional="true" />
```

<a name="server_settings-mark_cache_size"></a>

## mark_cache_size

Приблизительный размер (в байтах) кеша "засечек", используемых движками таблиц семейства [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree).

Кеш общий для сервера, память выделяется по мере необходимости. Кеш не может быть меньше, чем 5368709120.

**Пример**

```xml
<mark_cache_size>5368709120</mark_cache_size>
```

<a name="server_settings-max_concurrent_queries"></a>

## max_concurrent_queries

Максимальное количество одновременно обрабатываемых запросов.

**Пример**

```xml
<max_concurrent_queries>100</max_concurrent_queries>
```

<a name="server_settings-max_connections"></a>

## max_connections

Максимальное количество входящих соединений.

**Пример**

```xml
<max_connections>4096</max_connections>
```

<a name="server_settings-max_open_files"></a>

## max_open_files

Максимальное количество открытых файлов.

По умолчанию - `maximum`.

Рекомендуется использовать в Mac OS X, поскольу функция `getrlimit()` возвращает некорректное значение.

**Пример**

```xml
<max_open_files>262144</max_open_files>
```

<a name="server_settings-max_table_size_to_drop"></a>

## max_table_size_to_drop

Ограничение на удаление таблиц.

Если размер таблицы семейства [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree) превышает `max_table_size_to_drop` (в байтах), то ее нельзя удалить запросом DROP.

Если таблицу все же необходимо удалить, не перезапуская при этом сервер ClickHouse, то необходимо создать файл `<clickhouse-path>/flags/force_drop_table` и выполнить запрос DROP.

Значение по умолчанию - 50GB.

Значение 0 означает, что можно удалять все таблицы без ограничений.

**Пример**

```xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

<a name="server_settings-merge_tree"></a>

## merge_tree

Тонкая настройка таблиц семейства [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree).

Подробнее смотрите в заголовочном файле MergeTreeSettings.h.

**Пример**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<a name="server_settings-openSSL"></a>

## openSSL

Настройки клиента/сервера SSL.

Поддержку SSL обеспечивает библиотека ``libpoco``. Описание интерфейса находится в файле [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Ключи настроек сервера/клиента:

- privateKeyFile - Путь к файлу с секретным ключем сертификата в формате PEM. Файл может содержать ключ и сертификат одновременно.
- certificateFile - Путь к файлу сертификата клиента/сервера в формате PEM. Можно не указывать, если ``privateKeyFile`` содержит сертификат.
- caConfig - Путь к файлу или каталогу, которые содержат доверенные корневые сертификаты.
- verificationMode - Способ проверки сертификатов узла. Подробности находятся в описании класса [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Допустимые значения: ``none``, ``relaxed``, ``strict``, ``once``.
- verificationDepth - Максимальная длина верификационой цепи. Верификация завершится ошибкой, если длина цепи сертификатов превысит установленное значение.
- loadDefaultCAFile - Признак того, что будут использоваться встроенные CA-сертификаты для OpenSSL. Допустимые значения: ``true``, ``false``.  |
- cipherList - Поддерживаемые OpenSSL-шифры. Например, ``ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH``.
- cacheSessions - Включение/выключение кеширования сессии. Использовать обязательно вместе с ``sessionIdContext``. Допустимые значения: ``true``, ``false``.
- sessionIdContext - Уникальный набор произвольных символов, которые сервер добавляет к каждому сгенерированному идентификатору. Длина строки не должна превышать ``SSL_MAX_SSL_SESSION_ID_LENGTH``. Рекомендуется к использованию всегда, поскольку позволяет избежать проблем как в случае, если сервер кеширует сессию, так и если клиент затребовал кеширование. По умолчанию ``${application.name}``.
- sessionCacheSize - Максимальное количество сессий, которые кэширует сервер. По умолчанию - 1024\*20. 0 - неограниченное количество сессий.
- sessionTimeout - Время кеширования сессии на севрере.
- extendedVerification - Автоматическая расширенная проверка сертификатов после завершении сессии. Допустимые значения: ``true``, ``false``.
- requireTLSv1 - Требование соединения TLSv1. Допустимые значения: ``true``, ``false``.
- requireTLSv1_1 - Требование соединения TLSv1.1. Допустимые значения: ``true``, ``false``.
- requireTLSv1_2 - Требование соединения TLSv1.2. Допустимые значения: ``true``, ``false``.
- fips - Активация режима OpenSSL FIPS. Поддерживается, если версия OpenSSL, с которой собрана библиотека поддерживает fips.
- privateKeyPassphraseHandler - Класс (подкласс PrivateKeyPassphraseHandler)запрашивающий кодовую фразу доступа к секретному ключу. Например, ``<privateKeyPassphraseHandler>``, ``<name>KeyFileHandler</name>``, ``<options><password>test</password></options>``, ``</privateKeyPassphraseHandler>``.
- invalidCertificateHandler - Класс (подкласс CertificateHandler) для подтвеждения невалидных сертификатов. Например,  ``<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>``.
- disableProtocols - Запрещенные к искользованию протоколы.
- preferServerCiphers - Предпочтение серверных шифров на клиенте.

**Пример настройки:**

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

<a name="server_settings-part_log"></a>

## part_log

Логгирование событий, связанных с данными типа [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree). Например, события добавления или мержа данных. Лог можно использовать для симуляции алгоритмов слияния, чтобы сравнивать их характеристики. Также, можно визуализировать процесс слияния.

Запросы логгируются не в отдельный файл, а в таблицу ClickHouse.

Столбцы лога:

- event_time - Дата события.
- duration_ms - Время события.
- event_type - Тип события. 1 - кусок новый, 2 - результат мержа, 3 - кусок скачан с реплики, 4 - кусок удаляется.
- database_name - Имя базы даных.
- table_name - Имя таблицы.
- part_name - Имя куска данных.
- partition_id - Идентификатор партиции.
- size_in_bytes - Размер куска данных в байтах.
- merged_from - Массив имён кусков, из которых он образован при мерже (так же заполняется в случае скачивания уже смерженного куска).
- merge_time_ms - Время, потраченное на мерж.

При настройке логгирования используются следующие параметры:

- database - Имя базы данных.
- table - Имя таблицы.
- partition_by - Устанавливает [произвольный ключ партиционирования](../../operations/table_engines/custom_partitioning_key.md#custom-partitioning-key).
- flush_interval_milliseconds - Период сброса данных из оперативной памяти на диск.


**Пример**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

<a name="server_settings-path"></a>

## path

Путь к каталогу с данными.

!!! warning "Обратите внимание"
    Завершающий слеш обязателен.

**Пример**

```xml
<path>/var/lib/clickhouse/</path>
```

<a name="server_settings-query_log"></a>

## query_log

Настройка логгирования запросов, принятых с настройкой [log_queries=1](../settings/settings.md#settings_settings-log_queries).

Запросы логгируются не в отдельный файл, а в таблицу ClickHouse.

При настройке логгирования используются следующие параметры:

- database - Имя базы данных.
- table - Имя таблицы.
- partition_by - Устанавливает [произвольный ключ партиционирования](../../operations/table_engines/custom_partitioning_key.md#custom-partitioning-key).
- flush_interval_milliseconds - Период сброса данных из оперативной памяти на диск.

Если таблица не существует, то ClickHouse создаст её. Если структура журнала запросов изменилась при обновлении сервера ClickHouse, то таблица со старой структурой переименовывается, а новая таблица создается автоматически.

**Пример**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

<a name="server_settings-remote_servers"></a>

## remote_servers

Конфигурация кластеров, которые использует движок таблиц Distributed.

Пример настройки смотрите в разделе "[Движки таблиц/Distributed](../../operations/table_engines/distributed.md#table_engines-distributed)".

**Пример**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

Значение атрибута `incl` смотрите в разделе "[Конфигурационные файлы](../configuration_files.md#configuration_files)".


<a name="server_settings-timezone"></a>

## timezone

Временная зона сервера.

Указывается идентификатором IANA в виде часового пояса UTC или географического положения (например, Africa/Abidjan).

Временная зона необходима при преобразованиях между форматами String и DateTime, которые возникают при выводе полей DateTime в текстовый формат (на экран или в файл) и при получении DateTime из строки. Также, временная зона используется в функциях, которые работают со временем и датой, если они не получили временную зону в параметрах вызова.

**Пример**

```xml
<timezone>Europe/Moscow</timezone>
```

<a name="server_settings-tcp_port"></a>

## tcp_port

Порт для взаимодействия с клиентами по протоколу TCP.

**Пример**

```xml
<tcp_port>9000</tcp_port>
```

<a name="server_settings-tmp_path"></a>

## tmp_path

Путь ко временным данным для обработки больших запросов.

!!! warning "Обратите внимание"
    Завершающий слеш обязателен.

**Пример**

```xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

<a name="server_settings-uncompressed_cache_size"></a>

## uncompressed_cache_size

Размер кеша (в байтах) для несжатых данных, используемых движками таблиц семейства [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree).

Кеш единый для сервера. Память выделяется по-требованию. Кеш используется в том случае, если включена опция [use_uncompressed_cache](../settings/settings.md#settings-use_uncompressed_cache).

Несжатый кеш выгодно использовать для очень коротких запросов в отдельных случаях.

**Пример**

```xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path

Каталог с пользовательскими файлами. Используется в табличной функции [file()](../../query_language/table_functions/file.md#table_functions-file).

**Пример**

```xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

<a name="server_settings-users_config"></a>

## users_config

Путь к файлу, который содержит:

-   Конфигурации пользователей.
-   Права доступа.
-   Профили настроек.
-   Настройки квот.

**Пример**

```xml
<users_config>users.xml</users_config>
```

<a name="server_settings-zookeeper"></a>

## zookeeper

Конфигурация серверов ZooKeeper.

ClickHouse использует ZooKeeper для хранения метаданных о репликах при использовании реплицированных таблиц.

Параметр можно не указывать, если реплицированные таблицы не используются.

Подробно читайте в разделе "[Репликация](../../operations/table_engines/replication.md#table_engines-replication)".

**Пример**

```xml
<zookeeper incl="zookeeper-servers" optional="true" />
```
