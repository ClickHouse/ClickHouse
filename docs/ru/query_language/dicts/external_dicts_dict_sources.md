<a name="dicts-external_dicts_dict_sources"></a>

# Источники внешних словарей

Внешний словарь можно подключить из множества источников.

Общий вид конфигурации:

```xml
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

Источник настраивается в разделе `source`.

Типы источников (`source_type`):

- [Локальный файл](#dicts-external_dicts_dict_sources-local_file)
- [Исполняемый файл](#dicts-external_dicts_dict_sources-executable)
- [HTTP(s)](#dicts-external_dicts_dict_sources-http)
- [ODBC](#dicts-external_dicts_dict_sources-odbc)
-   СУБД:
    - [MySQL](#dicts-external_dicts_dict_sources-mysql)
    - [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    - [MongoDB](#dicts-external_dicts_dict_sources-mongodb)

<a name="dicts-external_dicts_dict_sources-local_file"></a>

## Локальный файл

Пример настройки:

```xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

Поля настройки:

-   `path` - Абсолютный путь к файлу.
-   `format` - Формат файла. Поддерживаются все форматы, описанные в разделе "[Форматы](../../interfaces/formats.md#formats)".

<a name="dicts-external_dicts_dict_sources-executable"></a>

## Исполняемый файл

Работа с исполняемым файлом зависит от [размещения словаря в памяти](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout). Если тип размещения словаря `cache` и `complex_key_cache`, то ClickHouse запрашивает необходимые ключи, отправляя запрос в `STDIN` исполняемого файла.

Пример настройки:

```xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

Поля настройки:

-   `command` - Абсолютный путь к исполняемому файлу или имя файла (если каталог программы прописан в `PATH`).
-   `format` - Формат файла. Поддерживаются все форматы, описанные в разделе "[Форматы](../../interfaces/formats.md#formats)".

<a name="dicts-external_dicts_dict_sources-http"></a>

## HTTP(s)

Работа с HTTP(s) сервером зависит от [размещения словаря в памяти](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout). Если тип размещения словаря `cache` и `complex_key_cache`, то ClickHouse запрашивает необходимые ключи, отправляя запрос методом `POST`.

Пример настройки:

```xml
<source>
    <http>
        <url>http://[::1]/os.tsv</url>
        <format>TabSeparated</format>
    </http>
</source>
```

Чтобы ClickHouse смог обратиться к HTTPS-ресурсу, необходимо [настроить openSSL](../../operations/server_settings/settings.md#server_settings-openSSL) в конфигурации сервера.

Поля настройки:

-   `url` - URL источника.
-   `format` - Формат файла. Поддерживаются все форматы, описанные в разделе "[Форматы](../../interfaces/formats.md#formats)".

<a name="dicts-external_dicts_dict_sources-odbc"></a>

## ODBC

Этим способом можно подключить любую базу данных, имеющую ODBC драйвер.

Пример настройки:

```xml
<odbc>
    <db>DatabaseName</db>
    <table>TableName</table>
    <connection_string>DSN=some_parameters</connection_string>
    <invalidate_query>SQL_QUERY</invalidate_query>
</odbc>
```

Поля настройки:

-   `db` - имя базы данных. Не указывать, если имя базы задано в параметрах `<connection_string>`.
-   `table` - имя таблицы.
-   `connection_string` - строка соединения.
-   `invalidate_query` - запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external_dicts_dict_lifetime.md#dicts-external_dicts_dict_lifetime).

## Пример подключения PostgreSQL

ОС Ubuntu.

Установка unixODBC и ODBC-драйвера для PostgreSQL: :

    sudo apt-get install -y unixodbc odbcinst odbc-postgresql

Настройка `/etc/odbc.ini` (или `~/.odbc.ini`):

```
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

```xml
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

Может понадобиться в `odbc.ini` указать полный путь до библиотеки с драйвером `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Пример подключения MS SQL Server

ОС Ubuntu.

Установка драйвера: :

```
    sudo apt-get install tdsodbc freetds-bin sqsh
```

Настройка драйвера: :

```
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

```xml
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

## СУБД

<a name="dicts-external_dicts_dict_sources-mysql"></a>

### MySQL

Пример настройки:

```xml
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

Поля настройки:

-   `port` - порт сервера MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).
-   `user` - имя пользователя MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).
-   `password` - пароль пользователя MySQL. Можно указать для всех реплик или для каждой в отдельности (внутри `<replica>`).
-   `replica` - блок конфигурации реплики. Блоков может быть несколько.

    -   `replica/host` - хост MySQL.

    \* `replica/priority` - приоритет реплики. При попытке соединения ClickHouse обходит реплики в соответствии с приоритетом. Чем меньше цифра, тем выше приоритет.
-   `db` - имя базы данных.
-   `table` - имя таблицы.
-   `where` - условие выбора. Необязательный параметр.
-   `invalidate_query` - запрос для проверки статуса словаря. Необязательный параметр. Читайте подробнее в разделе [Обновление словарей](external_dicts_dict_lifetime.md#dicts-external_dicts_dict_lifetime).

MySQL можно подключить на локальном хосте через сокеты, для этого необходимо задать `host` и `socket`.

Пример настройки:

```xml
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

<a name="dicts-external_dicts_dict_sources-clickhouse"></a>

### ClickHouse

Пример настройки:

```xml
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

Поля настройки:

-   `host` - хост ClickHouse. Если host локальный, то запрос выполняется без сетевого взаимодействия. Чтобы повысить отказоустойчивость решения, можно создать таблицу типа [Distributed](../../operations/table_engines/distributed.md#table_engines-distributed) и прописать её в дальнейших настройках.
-   `port` - порт сервера ClickHouse.
-   `user` - имя пользователя ClickHouse.
-   `password` - пароль пользователя ClickHouse.
-   `db` - имя базы данных.
-   `table` - имя таблицы.
-   `where` - условие выбора. Может отсутствовать.

<a name="dicts-external_dicts_dict_sources-mongodb"></a>

### MongoDB

Пример настройки:

```xml
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

Поля настройки:

-   `host` - хост MongoDB.
-   `port` - порт сервера MongoDB.
-   `user` - имя пользователя MongoDB.
-   `password` - пароль пользователя MongoDB.
-   `db` - имя базы данных.
-   `collection` - имя коллекции.
