# Источники внешних словарей {#dicts-external-dicts-dict-sources}

Внешний словарь можно подключить из множества источников.

Общий вид XML-конфигурации:

``` xml
<yandex>
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
</yandex>
```

Аналогичный [DDL-запрос](../../statements/create.md#create-dictionary-query):

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
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Типы источников (`source_type`):

-   [Локальный файл](#dicts-external_dicts_dict_sources-local_file)
-   [Исполняемый файл](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   СУБД:
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

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
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Поля настройки:

-   `path` — Абсолютный путь к файлу.
-   `format` — Формат файла. Поддерживаются все форматы, описанные в разделе «[Форматы](../../../interfaces/formats.md#formats)».

## Исполняемый файл {#dicts-external_dicts_dict_sources-executable}

Работа с исполняемым файлом зависит от [размещения словаря в памяти](external-dicts-dict-layout.md). Если тип размещения словаря `cache` и `complex_key_cache`, то ClickHouse запрашивает необходимые ключи, отправляя запрос в `STDIN` исполняемого файла.

Пример настройки:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

или

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Поля настройки:

-   `command` — Абсолютный путь к исполняемому файлу или имя файла (если каталог программы прописан в `PATH`).
-   `format` — Формат файла. Поддерживаются все форматы, описанные в разделе «[Форматы](../../../interfaces/formats.md#formats)».

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
-   `format` — Формат файла. Поддерживаются все форматы, описанные в разделе «[Форматы](../../../interfaces/formats.md#formats)».

## ODBC {#dicts-external_dicts_dict_sources-odbc}

Этим способом можно подключить любую базу данных, имеющую ODBC драйвер.

Пример настройки:

``` xml
<source>
    <odbc>
        <db>DatabaseName</db>
        <table>ShemaName.TableName</table>
        <connection_string>DSN=some_parameters</connection_string>
        <invalidate_query>SQL_QUERY</invalidate_query>
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
))
```

Поля настройки:

-   `db` — имя базы данных. Не указывать, если имя базы задано в параметрах. `<connection_string>`.
-   `table` — имя таблицы и схемы, если она есть.
-   `connection_string` — строка соединения.
-   `invalidate_query` — запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external-dicts-dict-lifetime.md).

ClickHouse получает от ODBC-драйвера информацию о квотировании и квотирует настройки в запросах к драйверу, поэтому имя таблицы нужно указывать в соответствии с регистром имени таблицы в базе данных.

Если у вас есть проблемы с кодировками при использовании Oracle, ознакомьтесь с соответствующим разделом [FAQ](../../../faq/general.md#oracle-odbc-encodings).

### Выявленная уязвимость в функционировании ODBC словарей {#vyiavlennaia-uiazvimost-v-funktsionirovanii-odbc-slovarei}

!!! attention "Attention"
    При соединении с базой данных через ODBC можно заменить параметр соединения `Servername`. В этом случае, значения `USERNAME` и `PASSWORD` из `odbc.ini` отправляются на удаленный сервер и могут быть скомпрометированы.

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
<yandex>
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
</yandex>
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

Может понадобиться в `odbc.ini` указать полный путь до библиотеки с драйвером `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Пример подключения MS SQL Server

ОС Ubuntu.

Установка драйвера: :

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Настройка драйвера: :

``` bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    $ cat /etc/odbcinst.ini
    ...

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat ~/.odbc.ini
    ...

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433
```

Настройка словаря в ClickHouse:

``` xml
<yandex>
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
</yandex>
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
))
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
SOURCE(MONGO(
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

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
