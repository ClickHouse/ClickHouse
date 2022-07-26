---
sidebar_position: 43
sidebar_label: "Источники внешних словарей"
---

# Источники внешних словарей {#dicts-external-dicts-dict-sources}

Внешний словарь можно подключить из множества источников.

Общий вид XML-конфигурации:

``` xml
<clickhouse>
  <dictionary>
    ...
    <source>
      <source_type>
        <!-- Source configuration -->
      </source_type>
    </source>
    ...
  </dictionary>
  ...
</clickhouse>
```

Аналогичный [DDL-запрос](../../statements/create/dictionary.md#create-dictionary-query):

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

Источник настраивается в разделе `source`.

Для типов источников [Локальный файл](#dicts-external_dicts_dict_sources-local_file), [Исполняемый файл](#dicts-external_dicts_dict_sources-executable), [HTTP(s)](#dicts-external_dicts_dict_sources-http), [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
доступны дополнительные настройки:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
  <settings>
      <format_csv_allow_single_quotes>0</format_csv_allow_single_quotes>
  </settings>
</source>
```

или

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Типы источников (`source_type`):

-   [Локальный файл](#dicts-external_dicts_dict_sources-local_file)
-   [Исполняемый файл](#dicts-external_dicts_dict_sources-executable)
-   [Исполняемый пул](#dicts-external_dicts_dict_sources-executable_pool)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   СУБД:
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)
    -   [Cassandra](#dicts-external_dicts_dict_sources-cassandra)
    -   [PostgreSQL](#dicts-external_dicts_dict_sources-postgresql)

## Локальный файл {#dicts-external_dicts_dict_sources-local_file}

Пример настройки:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

или

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

Поля настройки:

-   `path` — абсолютный путь к файлу.
-   `format` — формат файла. Поддерживаются все форматы, описанные в разделе [Форматы](../../../interfaces/formats.md#formats).

Если словарь с источником `FILE` создается с помощью DDL-команды (`CREATE DICTIONARY ...`), источник словаря должен быть расположен в каталоге `user_files`. Иначе пользователи базы данных будут иметь доступ к произвольному файлу на узле ClickHouse.

**Смотрите также**

-   [Функция dictionary](../../../sql-reference/table-functions/dictionary.md#dictionary-function)

## Исполняемый файл {#dicts-external_dicts_dict_sources-executable}

Работа с исполняемым файлом зависит от [размещения словаря в памяти](external-dicts-dict-layout.md). Если тип размещения словаря `cache` и `complex_key_cache`, то ClickHouse запрашивает необходимые ключи, отправляя запрос в `STDIN` исполняемого файла.

Пример настройки:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
        <implicit_key>false</implicit_key>
    </executable>
</source>
```

Поля настройки:

-   `command` — абсолютный путь к исполняемому файлу или имя файла (если каталог программы прописан в `PATH`).
-   `format` — формат файла. Поддерживаются все форматы, описанные в разделе [Форматы](../../../interfaces/formats.md#formats).
-   `implicit_key` — исходный исполняемый файл может возвращать только значения, а соответствие запрошенным ключам определено неявно — порядком строк в результате. Значение по умолчанию: false. Необязательный параметр.

Этот источник словаря может быть настроен только с помощью XML-конфигурации. Создание словарей с исполняемым источником с помощью DDL запрещено. Иначе пользователь сможет выполнить произвольный бинарный файл на сервере ClickHouse.

## Исполняемый пул {#dicts-external_dicts_dict_sources-executable_pool}

Исполняемый пул позволяет загружать данные из пула процессов. Этот источник не работает со словарями, которые требуют загрузки всех данных из источника. Исполняемый пул работает словарями, которые размещаются [следующими способами](external-dicts-dict-layout.md#ways-to-store-dictionaries-in-memory): `cache`, `complex_key_cache`, `ssd_cache`, `complex_key_ssd_cache`, `direct`, `complex_key_direct`.

Исполняемый пул генерирует пул процессов с помощью указанной команды и оставляет их активными, пока они не завершатся. Программа считывает данные из потока STDIN пока он доступен и выводит результат в поток STDOUT, а затем ожидает следующего блока данных из STDIN. ClickHouse не закрывает поток STDIN после обработки блока данных и отправляет в него следующую порцию данных, когда это требуется. Исполняемый скрипт должен быть готов к такому способу обработки данных — он должен заранее опрашивать STDIN и отправлять данные в STDOUT.

Пример настройки:

``` xml
<source>
    <executable_pool>
        <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
        <format>TabSeparated</format>
        <pool_size>10</pool_size>
        <max_command_execution_time>10<max_command_execution_time>
        <implicit_key>false</implicit_key>
    </executable_pool>
</source>
```

Поля настройки:

-   `command` — абсолютный путь к файлу или имя файла (если каталог программы записан в `PATH`).
-   `format` — формат файла. Поддерживаются все форматы, описанные в “[Форматы](../../../interfaces/formats.md#formats)”.
-   `pool_size` — размер пула. Если в поле `pool_size` указан 0, то размер пула не ограничен.
-   `command_termination_timeout` — скрипт исполняемого пула должен включать основной цикл чтения-записи. После уничтожения словаря канал закрывается. При этом исполняемый файл имеет `command_termination_timeout` секунд для завершения работы, прежде чем ClickHouse пошлет сигнал SIGTERM дочернему процессу. Указывается в секундах. Значение по умолчанию: 10. Необязательный параметр.
-   `max_command_execution_time` — максимальное количество времени для исполняемого скрипта на обработку блока данных. Указывается в секундах. Значение по умолчанию: 10. Необязательный параметр.
-   `implicit_key` — исходный исполняемый файл может возвращать только значения, а соответствие запрошенным ключам определено неявно — порядком строк в результате. Значение по умолчанию: false. Необязательный параметр.

Этот источник словаря может быть настроен только с помощью XML-конфигурации. Создание словарей с исполняемым источником с помощью DDL запрещено. Иначе пользователь сможет выполнить произвольный бинарный файл на сервере ClickHouse.

## HTTP(s) {#dicts-external_dicts_dict_sources-http}

Работа с HTTP(s) сервером зависит от [размещения словаря в памяти](external-dicts-dict-layout.md). Если тип размещения словаря `cache` и `complex_key_cache`, то ClickHouse запрашивает необходимые ключи, отправляя запрос методом `POST`.

Пример настройки:

``` xml
<source>
    <http>
        <url>http://[::1]/os.tsv</url>
        <format>TabSeparated</format>
        <credentials>
            <user>user</user>
            <password>password</password>
        </credentials>
        <headers>
            <header>
                <name>API-KEY</name>
                <value>key</value>
            </header>
        </headers>
    </http>
</source>
```

или

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

Чтобы ClickHouse смог обратиться к HTTPS-ресурсу, необходимо [настроить openSSL](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) в конфигурации сервера.

Поля настройки:

-   `url` — URL источника.
-   `format` — формат файла. Поддерживаются все форматы, описанные в разделе «[Форматы](../../../interfaces/formats.md#formats)».
-   `credentials` – базовая HTTP-аутентификация. Необязательный параметр.
-   `user` – имя пользователя, необходимое для аутентификации.
-   `password` – пароль, необходимый для аутентификации.
-   `headers` – все пользовательские записи HTTP-заголовков, используемые для HTTP-запроса. Необязательный параметр.
-   `header` – одна запись HTTP-заголовка.
-   `name` – идентифицирующее имя, используемое для отправки заголовка запроса.
-   `value` – значение, заданное для конкретного идентифицирующего имени.

При создании словаря с помощью DDL-команды (`CREATE DICTIONARY ...`) удаленные хосты для HTTP-словарей проверяются в разделе `remote_url_allow_hosts` из конфигурации сервера. Иначе пользователи базы данных будут иметь доступ к произвольному HTTP-серверу.

### Выявленная уязвимость в функционировании ODBC словарей {#vyiavlennaia-uiazvimost-v-funktsionirovanii-odbc-slovarei}

    :::note "Внимание"
    При соединении с базой данных через ODBC можно заменить параметр соединения `Servername`. В этом случае, значения `USERNAME` и `PASSWORD` из `odbc.ini` отправляются на удаленный сервер и могут быть скомпрометированы.
    :::
**Пример небезопасного использования**

Сконфигурируем unixODBC для работы с PostgreSQL. Содержимое `/etc/odbc.ini`:

``` text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

Если выполнить запрос вида:

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

то ODBC драйвер отправит значения `USERNAME` и `PASSWORD` из `odbc.ini` на `some-server.com`.

### Пример подключения PostgreSQL {#primer-podkliucheniia-postgresql}

ОС Ubuntu.

Установка unixODBC и ODBC-драйвера для PostgreSQL: :

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Настройка `/etc/odbc.ini` (или `~/.odbc.ini`):

``` text
    [DEFAULT]
    Driver = myconnection

    [myconnection]
    Description         = PostgreSQL connection to my_db
    Driver              = PostgreSQL Unicode
    Database            = my_db
    Servername          = 127.0.0.1
    UserName            = username
    Password            = password
    Port                = 5432
    Protocol            = 9.3
    ReadOnly            = No
    RowVersioning       = No
    ShowSystemTables    = No
    ConnSettings        =
```

Конфигурация словаря в ClickHouse:

``` xml
<clickhouse>
    <dictionary>
        <name>table_name</name>
        <source>
            <odbc>
                <!-- в connection_string можно указывать следующие параметры: -->
                <!-- DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db -->
                <connection_string>DSN=myconnection</connection_string>
                <table>postgresql_table</table>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>some_column</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

или

``` sql
CREATE DICTIONARY table_name (
    id UInt64,
    some_column UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360)
```

Может понадобиться в `odbc.ini` указать полный путь до библиотеки с драйвером `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Пример подключения MS SQL Server

ОС Ubuntu.

Установка драйвера:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Настройка драйвера:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # тестирование TDS соединения
    $ sqsh -S MSSQL -D database -U user -P password


    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # если вы вошли из под пользователя из под которого запущен ClickHouse

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433


    # (не обязательно) тест ODBC соединения (используйте isql поставляемый вместе с [unixodbc](https://packages.debian.org/sid/unixodbc)-package)
    $ isql -v MSSQL "user" "password"
```

Примечание:
- чтобы определить самую раннюю версию TDS, которая поддерживается определенной версией SQL Server, обратитесь к документации продукта или посмотрите на [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Настройка словаря в ClickHouse:

``` xml
<clickhouse>
    <dictionary>
        <name>test</name>
        <source>
            <odbc>
                <table>dict</table>
                <connection_string>DSN=MSSQL;UID=test;PWD=test</connection_string>
            </odbc>
        </source>

        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>

        <layout>
            <flat />
        </layout>

        <structure>
            <id>
                <name>k</name>
            </id>
            <attribute>
                <name>s</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

или

``` sql
CREATE DICTIONARY test (
    k UInt64,
    s String DEFAULT ''
)
PRIMARY KEY k
SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 360)
```

## СУБД {#subd}

### ODBC {#dicts-external_dicts_dict_sources-odbc}

Этим способом можно подключить любую базу данных, имеющую ODBC драйвер.

Пример настройки:

``` xml
<source>
    <odbc>
        <db>DatabaseName</db>
        <table>ShemaName.TableName</table>
        <connection_string>DSN=some_parameters</connection_string>
        <invalidate_query>SQL_QUERY</invalidate_query>
        <query>SELECT id, value_1, value_2 FROM ShemaName.TableName</query>
    </odbc>
</source>
```

или

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Поля настройки:

-   `db` — имя базы данных. Не указывать, если имя базы задано в параметрах. `<connection_string>`.
-   `table` — имя таблицы и схемы, если она есть.
-   `connection_string` — строка соединения.
-   `invalidate_query` — запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external-dicts-dict-lifetime.md).
-   `query` – пользовательский запрос. Необязательный параметр.

:::info "Примечание"
    Поля `table` и `query` не могут быть использованы вместе. Также обязательно должен быть один из источников данных: `table` или `query`.

ClickHouse получает от ODBC-драйвера информацию о квотировании и квотирует настройки в запросах к драйверу, поэтому имя таблицы нужно указывать в соответствии с регистром имени таблицы в базе данных.

Если у вас есть проблемы с кодировками при использовании Oracle, ознакомьтесь с соответствующим разделом [FAQ](../../../faq/integration/oracle-odbc.md).

### MySQL {#dicts-external_dicts_dict_sources-mysql}

Пример настройки:

``` xml
<source>
  <mysql>
      <port>3306</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <replica>
          <host>example01-1</host>
          <priority>1</priority>
      </replica>
      <replica>
          <host>example01-2</host>
          <priority>1</priority>
      </replica>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

или

``` sql
SOURCE(MYSQL(
    port 3306
    user 'clickhouse'
    password 'qwerty'
    replica(host 'example01-1' priority 1)
    replica(host 'example01-2' priority 1)
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Поля настройки:

-   `port` — порт сервера MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).

-   `user` — имя пользователя MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).

-   `password` — пароль пользователя MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).

-   `replica` — блок конфигурации реплики. Блоков может быть несколько.

        - `replica/host` — хост MySQL.
        - `replica/priority` — приоритет реплики. При попытке соединения ClickHouse обходит реплики в соответствии с приоритетом. Чем меньше цифра, тем выше приоритет.

-   `db` — имя базы данных.

-   `table` — имя таблицы.

-   `where` — условие выбора. Синтаксис условия совпадает с синтаксисом секции `WHERE` в MySQL, например, `id > 10 AND id < 20`. Необязательный параметр.

-   `invalidate_query` — запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external-dicts-dict-lifetime.md).

-   `fail_on_connection_loss` – параметр конфигурации, контролирующий поведение сервера при потере соединения. Если значение `true`, то исключение генерируется сразу же, если соединение между клиентом и сервером было потеряно. Если значение `false`, то сервер повторно попытается выполнить запрос три раза прежде чем сгенерировать исключение. Имейте в виду, что повторные попытки могут увеличить время выполнения запроса. Значение по умолчанию: `false`.

-   `query` – пользовательский запрос. Необязательный параметр.

:::info "Примечание"
    Поля `table` или `where` не могут быть использованы вместе с полем `query`. Также обязательно должен быть один из источников данных: `table` или `query`.
    Явный параметр `secure` отсутствует. Автоматически поддержана работа в обоих случаях: когда установка SSL-соединения необходима и когда нет.

MySQL можно подключить на локальном хосте через сокеты, для этого необходимо задать `host` и `socket`.

Пример настройки:

``` xml
<source>
  <mysql>
      <host>localhost</host>
      <socket>/path/to/socket/file.sock</socket>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

или

``` sql
SOURCE(MYSQL(
    host 'localhost'
    socket '/path/to/socket/file.sock'
    user 'clickhouse'
    password 'qwerty'
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

### ClickHouse {#dicts-external_dicts_dict_sources-clickhouse}

Пример настройки:

``` xml
<source>
    <clickhouse>
        <host>example01-01-1</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>default</db>
        <table>ids</table>
        <where>id=10</where>
        <secure>1</secure>
        <query>SELECT id, value_1, value_2 FROM default.ids</query>
    </clickhouse>
</source>
```

или

``` sql
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
    secure 1
    query 'SELECT id, value_1, value_2 FROM default.ids'
));
```

Поля настройки:

-   `host` — хост ClickHouse. Если host локальный, то запрос выполняется без сетевого взаимодействия. Чтобы повысить отказоустойчивость решения, можно создать таблицу типа [Distributed](../../../engines/table-engines/special/distributed.md) и прописать её в дальнейших настройках.
-   `port` — порт сервера ClickHouse.
-   `user` — имя пользователя ClickHouse.
-   `password` — пароль пользователя ClickHouse.
-   `db` — имя базы данных.
-   `table` — имя таблицы.
-   `where` — условие выбора. Может отсутствовать.
-   `invalidate_query` — запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external-dicts-dict-lifetime.md).
-   `secure` - флаг, разрешающий или не разрешающий защищённое SSL-соединение.
-   `query` – пользовательский запрос. Необязательный параметр.

:::info "Примечание"
    Поля `table` или `where` не могут быть использованы вместе с полем `query`. Также обязательно должен быть один из источников данных: `table` или `query`.

### MongoDB {#dicts-external_dicts_dict_sources-mongodb}

Пример настройки:

``` xml
<source>
    <mongodb>
        <host>localhost</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
    </mongodb>
</source>
```

или

``` sql
SOURCE(MONGODB(
    host 'localhost'
    port 27017
    user ''
    password ''
    db 'test'
    collection 'dictionary_source'
))
```

Поля настройки:

-   `host` — хост MongoDB.
-   `port` — порт сервера MongoDB.
-   `user` — имя пользователя MongoDB.
-   `password` — пароль пользователя MongoDB.
-   `db` — имя базы данных.
-   `collection` — имя коллекции.

### Redis {#dicts-external_dicts_dict_sources-redis}

Пример настройки:

``` xml
<source>
    <redis>
        <host>localhost</host>
        <port>6379</port>
        <storage_type>simple</storage_type>
        <db_index>0</db_index>
    </redis>
</source>
```

или

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Поля настройки:

-   `host` – хост Redis.
-   `port` – порт сервера Redis.
-   `storage_type` – способ хранения ключей. Необходимо использовать `simple` для источников с одним столбцом ключей, `hash_map` – для источников с двумя столбцами ключей. Источники с более, чем двумя столбцами ключей, не поддерживаются. Может отсутствовать, значение по умолчанию `simple`.
-   `db_index` – номер базы данных. Может отсутствовать, значение по умолчанию 0.

### Cassandra {#dicts-external_dicts_dict_sources-cassandra}

Пример настройки:

``` xml
<source>
    <cassandra>
        <host>localhost</host>
        <port>9042</port>
        <user>username</user>
        <password>qwerty123</password>
        <keyspase>database_name</keyspase>
        <column_family>table_name</column_family>
        <allow_filering>1</allow_filering>
        <partition_key_prefix>1</partition_key_prefix>
        <consistency>One</consistency>
        <where>"SomeColumn" = 42</where>
        <max_threads>8</max_threads>
        <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
    </cassandra>
</source>
```

Поля настройки:

-   `host` – имя хоста с установленной Cassandra или разделенный через запятую список хостов.
-   `port` – порт на серверах Cassandra. Если не указан, используется значение по умолчанию: 9042.
-   `user` – имя пользователя  для соединения с Cassandra.
-   `password` – пароль для соединения с Cassandra.
-   `keyspace` – имя keyspace (база данных).
-   `column_family` – имя семейства столбцов (таблица).
-   `allow_filering` – флаг, разрешающий или не разрешающий потенциально дорогостоящие условия на кластеризации ключевых столбцов. Значение по умолчанию: 1.
-   `partition_key_prefix` – количество партиций ключевых столбцов в первичном ключе таблицы Cassandra.
Необходимо для составления ключей словаря. Порядок ключевых столбцов в определении словаря должен быть таким же, как в Cassandra.
Значение по умолчанию: 1 (первый ключевой столбец - это ключ партицирования, остальные ключевые столбцы - ключи кластеризации).
-   `consistency` – уровень консистентности. Возможные значения: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Значение по умолчанию: `One`.
-   `where` – опциональный критерий выборки.
-   `max_threads` – максимальное количество тредов для загрузки данных из нескольких партиций в словарь.
-   `query` – пользовательский запрос. Необязательный параметр.

:::info "Примечание"
    Поля `column_family` или `where` не могут быть использованы вместе с полем `query`. Также обязательно должен быть один из источников данных: `column_family` или `query`.

### PostgreSQL {#dicts-external_dicts_dict_sources-postgresql}

Пример настройки:

``` xml
<source>
  <postgresql>
      <port>5432</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </postgresql>
</source>
```

или

``` sql
SOURCE(POSTGRESQL(
    port 5432
    host 'postgresql-hostname'
    user 'postgres_user'
    password 'postgres_password'
    db 'db_name'
    table 'table_name'
    replica(host 'example01-1' port 5432 priority 1)
    replica(host 'example01-2' port 5432 priority 2)
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Поля настройки:

-   `host` – хост для соединения с PostgreSQL. Вы можете указать его для всех реплик или задать индивидуально для каждой релпики (внутри `<replica>`).
-   `port` – порт для соединения с PostgreSQL. Вы можете указать его для всех реплик или задать индивидуально для каждой релпики (внутри `<replica>`).
-   `user` – имя пользователя для соединения с PostgreSQL. Вы можете указать его для всех реплик или задать индивидуально для каждой релпики (внутри `<replica>`).
-   `password` – пароль для пользователя PostgreSQL.
-   `replica` – раздел конфигурации реплик. Может быть несколько.
    -   `replica/host` – хост PostgreSQL.
    -   `replica/port` – порт PostgreSQL .
    -   `replica/priority` – приоритет реплики. Во время попытки соединения ClickHouse будет перебирать реплики в порядке приоритета. Меньшее значение означает более высокий приоритет.
-   `db` – имя базы данных.
-   `table` – имя таблицы.
-   `where` – условие выборки. Синтаксис для условий такой же, как для выражения `WHERE` в PostgreSQL. Например, `id > 10 AND id < 20`. Необязательный параметр.
-   `invalidate_query` – запрос для проверки условия загрузки словаря. Необязательный параметр. Более подробную информацию смотрите в разделе [обновление словарей](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).
-   `query` – пользовательский запрос. Необязательный параметр.

:::info "Примечание"
    Поля `table` или `where` не могут быть использованы вместе с полем `query`. Также обязательно должен быть один из источников данных: `table` или `query`.
