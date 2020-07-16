---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "\u0645\u0646\u0627\u0628\u0639 \u0644\u063A\u062A \u0646\u0627\u0645\u0647\
  \ \u0647\u0627\u06CC \u062E\u0627\u0631\u062C\u06CC"
---

# منابع لغت نامه های خارجی {#dicts-external-dicts-dict-sources}

فرهنگ لغت خارجی را می توان از بسیاری از منابع مختلف متصل می شود.

اگر فرهنگ لغت پیکربندی شده است با استفاده از فایل های فشرده, پیکربندی به نظر می رسد مثل این:

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

در صورت [توصیف](../../statements/create.md#create-dictionary-query), پیکربندی برابر خواهد شد مانند به نظر می رسد:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

منبع در پیکربندی `source` بخش.

برای انواع منبع [پرونده محلی](#dicts-external_dicts_dict_sources-local_file), [پرونده اجرایی](#dicts-external_dicts_dict_sources-executable), [HTTP(s)](#dicts-external_dicts_dict_sources-http), [فاحشه خانه](#dicts-external_dicts_dict_sources-clickhouse)
تنظیمات اختیاری در دسترس هستند:

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

یا

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

انواع منابع (`source_type`):

-   [پرونده محلی](#dicts-external_dicts_dict_sources-local_file)
-   [پرونده اجرایی](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [فاحشه خانه](#dicts-external_dicts_dict_sources-clickhouse)
    -   [مانگودیبی](#dicts-external_dicts_dict_sources-mongodb)
    -   [ردیس](#dicts-external_dicts_dict_sources-redis)

## پرونده محلی {#dicts-external_dicts_dict_sources-local_file}

مثال تنظیمات:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

یا

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
```

تنظیم فیلدها:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[فرشها](../../../interfaces/formats.md#formats)” پشتیبانی می شوند.

## پرونده اجرایی {#dicts-external_dicts_dict_sources-executable}

کار با فایل های اجرایی بستگی دارد [چگونه فرهنگ لغت در حافظه ذخیره می شود](external-dicts-dict-layout.md). اگر فرهنگ لغت با استفاده از ذخیره می شود `cache` و `complex_key_cache` کلیک هاوس کلید های لازم را با ارسال درخواست به فایل اجرایی درخواست می کند. در غیر این صورت, تاتر شروع می شود فایل اجرایی و خروجی خود را به عنوان داده فرهنگ لغت رفتار.

مثال تنظیمات:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

یا

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

تنظیم فیلدها:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[فرشها](../../../interfaces/formats.md#formats)” پشتیبانی می شوند.

## قام) {#dicts-external_dicts_dict_sources-http}

کار با سرور اچ تی پی بستگی دارد [چگونه فرهنگ لغت در حافظه ذخیره می شود](external-dicts-dict-layout.md). اگر فرهنگ لغت با استفاده از ذخیره می شود `cache` و `complex_key_cache`, کلیک درخواست کلید های لازم با ارسال یک درخواست از طریق `POST` روش.

مثال تنظیمات:

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

یا

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

برای دسترسی به یک منبع اچ تی پی باید از اینجا کلیک کنید [پیکربندی اپنسسل](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) در پیکربندی سرور.

تنظیم فیلدها:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[فرشها](../../../interfaces/formats.md#formats)” پشتیبانی می شوند.
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

شما می توانید از این روش برای اتصال هر پایگاه داده است که یک راننده بی سی استفاده کنید.

مثال تنظیمات:

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

یا

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

تنظیم فیلدها:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` پارامترها
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [بهروزرسانی واژهنامهها](external-dicts-dict-lifetime.md).

تاتر دریافت به نقل از علامت از او بی سی راننده و نقل قول تمام تنظیمات در نمایش داده شد به راننده, بنابراین لازم است به مجموعه ای از نام جدول بر این اساس به نام جدول مورد در پایگاه داده.

اگر شما یک مشکل با کدگذاریها در هنگام استفاده از اوراکل, دیدن مربوطه [FAQ](../../../faq/general.md#oracle-odbc-encodings) مقاله.

### قابلیت پذیری شناخته شده از قابلیت او بی سی فرهنگ لغت {#known-vulnerability-of-the-odbc-dictionary-functionality}

!!! attention "توجه"
    هنگام اتصال به پایگاه داده از طریق پارامتر اتصال درایور او بی سی `Servername` می تواند جایگزین شود. در این مورد ارزش `USERNAME` و `PASSWORD` از `odbc.ini` به سرور از راه دور ارسال می شود و می تواند به خطر بیافتد.

**نمونه ای از استفاده نا امن**

اجازه می دهد تا پیکربندی unixODBC برای PostgreSQL. محتوای `/etc/odbc.ini`:

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

اگر شما پس از ایجاد یک پرس و جو مانند

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

درایور او بی سی خواهد ارزش ارسال `USERNAME` و `PASSWORD` از `odbc.ini` به `some-server.com`.

### به عنوان مثال از اتصال شل {#example-of-connecting-postgresql}

سیستم عامل اوبونتو.

نصب unixODBC و ODBC driver for PostgreSQL:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

پیکربندی `/etc/odbc.ini` (یا `~/.odbc.ini`):

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

پیکربندی فرهنگ لغت در کلیک:

``` xml
<yandex>
    <dictionary>
        <name>table_name</name>
        <source>
            <odbc>
                <!-- You can specify the following parameters in connection_string: -->
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

یا

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

شما ممکن است نیاز به ویرایش `odbc.ini` برای مشخص کردن مسیر کامل به کتابخانه با راننده `DRIVER=/usr/local/lib/psqlodbcw.so`.

### به عنوان مثال اتصال سرور کارشناسی ارشد گذاشتن {#example-of-connecting-ms-sql-server}

سیستم عامل اوبونتو.

نصب درایور: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

پیکربندی راننده:

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

پیکربندی فرهنگ لغت در کلیک:

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

یا

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

## DBMS {#dbms}

### Mysql {#dicts-external_dicts_dict_sources-mysql}

مثال تنظیمات:

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

یا

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

تنظیم فیلدها:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` بند در خروجی زیر, مثلا, `id > 10 AND id < 20`. پارامتر اختیاری.

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [بهروزرسانی واژهنامهها](external-dicts-dict-lifetime.md).

خروجی زیر را می توان در یک میزبان محلی از طریق سوکت متصل. برای انجام این کار, تنظیم `host` و `socket`.

مثال تنظیمات:

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

یا

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

### فاحشه خانه {#dicts-external_dicts_dict_sources-clickhouse}

مثال تنظیمات:

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

یا

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

تنظیم فیلدها:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [توزیع شده](../../../engines/table-engines/special/distributed.md) جدول و در تنظیمات بعدی وارد کنید.
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [بهروزرسانی واژهنامهها](external-dicts-dict-lifetime.md).

### مانگودیبی {#dicts-external_dicts_dict_sources-mongodb}

مثال تنظیمات:

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

یا

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

تنظیم فیلدها:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### ردیس {#dicts-external_dicts_dict_sources-redis}

مثال تنظیمات:

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

یا

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

تنظیم فیلدها:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` برای منابع ساده و برای منابع تک کلیدی درهم, `hash_map` برای منابع درهم با دو کلید. منابع در بازه زمانی و منابع کش با کلید پیچیده پشتیبانی نشده است. ممکن است حذف شود, مقدار پیش فرض است `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
