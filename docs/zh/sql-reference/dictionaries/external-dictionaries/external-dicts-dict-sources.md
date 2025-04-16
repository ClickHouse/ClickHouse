---
slug: /zh/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 43
sidebar_label: "\u5916\u90E8\u5B57\u5178\u7684\u6765\u6E90"
---

# 外部字典的来源 {#dicts-external-dicts-dict-sources}

外部字典可以从许多不同的来源连接。

如果使用xml-file配置字典，则配置如下所示:

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

在情况下 [DDL-查询](../../statements/create.md#create-dictionary-query)，相等的配置将看起来像:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

源配置在 `source` 科。

对于源类型 [本地文件](#dicts-external_dicts_dict_sources-local_file), [可执行文件](#dicts-external_dicts_dict_sources-executable), [HTTP(s)](#dicts-external_dicts_dict_sources-http), [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
可选设置:

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

或

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

来源类型 (`source_type`):

-   [本地文件](#dicts-external_dicts_dict_sources-local_file)
-   [可执行文件](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

## 本地文件 {#dicts-external_dicts_dict_sources-local_file}

设置示例:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

或

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

设置字段:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[格式](../../../interfaces/formats.md#formats)” 支持。

## 可执行文件 {#dicts-external_dicts_dict_sources-executable}

使用可执行文件取决于 [字典如何存储在内存中](external-dicts-dict-layout.md). 如果字典存储使用 `cache` 和 `complex_key_cache`，ClickHouse通过向可执行文件的STDIN发送请求来请求必要的密钥。 否则，ClickHouse将启动可执行文件并将其输出视为字典数据。

设置示例:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

或

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

设置字段:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[格式](../../../interfaces/formats.md#formats)” 支持。

## Http(s) {#dicts-external_dicts_dict_sources-http}

使用HTTP（s）服务器取决于 [字典如何存储在内存中](external-dicts-dict-layout.md). 如果字典存储使用 `cache` 和 `complex_key_cache`，ClickHouse通过通过发送请求请求必要的密钥 `POST` 方法。

设置示例:

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

或

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

为了让ClickHouse访问HTTPS资源，您必须 [配置openSSL](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) 在服务器配置中。

设置字段:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[格式](../../../interfaces/formats.md#formats)” 支持。
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

您可以使用此方法连接具有ODBC驱动程序的任何数据库。

设置示例:

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

或

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

设置字段:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` 参数。
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [更新字典](external-dicts-dict-lifetime.md).

ClickHouse接收来自ODBC-driver的引用符号，并将查询中的所有设置引用到driver，因此有必要根据数据库中的表名大小写设置表名。

如果您在使用Oracle时遇到编码问题，请参阅相应的 [FAQ](../../../faq/general.md#oracle-odbc-encodings) 文章.

### ODBC字典功能的已知漏洞 {#known-vulnerability-of-the-odbc-dictionary-functionality}

:::info "注意"
通过ODBC驱动程序连接参数连接到数据库时 `Servername` 可以取代。 在这种情况下，值 `USERNAME` 和 `PASSWORD` 从 `odbc.ini` 被发送到远程服务器，并且可能会受到损害。
:::

**不安全使用示例**

让我们为PostgreSQL配置unixODBC。 的内容 `/etc/odbc.ini`:

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

如果然后进行查询，例如

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC驱动程序将发送的值 `USERNAME` 和 `PASSWORD` 从 `odbc.ini` 到 `some-server.com`.

### 连接Postgresql的示例 {#example-of-connecting-postgresql}

Ubuntu操作系统。

为PostgreSQL安装unixODBC和ODBC驱动程序:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

配置 `/etc/odbc.ini` （或 `~/.odbc.ini`):

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

ClickHouse中的字典配置:

``` xml
<clickhouse>
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
</clickhouse>
```

或

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

您可能需要编辑 `odbc.ini` 使用驱动程序指定库的完整路径 `DRIVER=/usr/local/lib/psqlodbcw.so`.

### 连接MS SQL Server的示例 {#example-of-connecting-ms-sql-server}

Ubuntu操作系统。

安装驱动程序: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

配置驱动程序:

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

在ClickHouse中配置字典:

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

或

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

设置示例:

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

或

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

设置字段:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` 例如，mysql中的子句, `id > 10 AND id < 20`. 可选参数。

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [更新字典](external-dicts-dict-lifetime.md).

MySQL可以通过套接字在本地主机上连接。 要做到这一点，设置 `host` 和 `socket`.

设置示例:

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

或

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

设置示例:

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

或

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

设置字段:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [分布](../../../engines/table-engines/special/distributed.md) 表并在后续配置中输入它。
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [更新字典](external-dicts-dict-lifetime.md).

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

设置示例:

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

或

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

设置字段:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

设置示例:

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

或

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

设置字段:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` 适用于简单源和散列单键源, `hash_map` 用于具有两个键的散列源。 不支持具有复杂键的范围源和缓存源。 可以省略，默认值为 `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.
