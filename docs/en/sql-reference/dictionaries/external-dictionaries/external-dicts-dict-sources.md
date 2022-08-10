---
sidebar_position: 43
sidebar_label: Sources of External Dictionaries
---

# Sources of External Dictionaries

An external dictionary can be connected from many different sources.

If dictionary is configured using xml-file, the configuration looks like this:

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

In case of [DDL-query](../../../sql-reference/statements/create/dictionary.md), equal configuration will looks like:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

The source is configured in the `source` section.

For source types [Local file](#dicts-external_dicts_dict_sources-local_file), [Executable file](#dicts-external_dicts_dict_sources-executable), [HTTP(s)](#dicts-external_dicts_dict_sources-http), [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
optional settings are available:

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

or

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Types of sources (`source_type`):

-   [Local file](#dicts-external_dicts_dict_sources-local_file)
-   [Executable File](#dicts-external_dicts_dict_sources-executable)
-   [Executable Pool](#dicts-external_dicts_dict_sources-executable_pool)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)
    -   [Cassandra](#dicts-external_dicts_dict_sources-cassandra)
    -   [PostgreSQL](#dicts-external_dicts_dict_sources-postgresql)

## Local File {#dicts-external_dicts_dict_sources-local_file}

Example of settings:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

or

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

Setting fields:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in [Formats](../../../interfaces/formats.md#formats) are supported.

When dictionary with source `FILE` is created via DDL command (`CREATE DICTIONARY ...`), the source file needs to be located in `user_files` directory, to prevent DB users accessing arbitrary file on ClickHouse node.

**See Also**

-   [Dictionary function](../../../sql-reference/table-functions/dictionary.md#dictionary-function)

## Executable File {#dicts-external_dicts_dict_sources-executable}

Working with executable files depends on [how the dictionary is stored in memory](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request to the executable file’s STDIN. Otherwise, ClickHouse starts executable file and treats its output as dictionary data.

Example of settings:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
        <implicit_key>false</implicit_key>
    </executable>
</source>
```

Setting fields:

-   `command` — The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` — The file format. All the formats described in [Formats](../../../interfaces/formats.md#formats) are supported.
-   `command_termination_timeout` — executable script should contain main read-write loop. After dictionary is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown, before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is 10. Optional parameter.
-   `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
-   `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
-   `implicit_key` — The executable source file can return only values, and the correspondence to the requested keys is determined implicitly — by the order of rows in the result. Default value is false.
-   `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `0`. Optional parameter.
-   `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled, otherwise, the DB user would be able to execute arbitrary binary on ClickHouse node.

## Executable Pool {#dicts-external_dicts_dict_sources-executable_pool}

Executable pool allows loading data from pool of processes. This source does not work with dictionary layouts that need to load all data from source. Executable pool works if the dictionary [is stored](external-dicts-dict-layout.md#ways-to-store-dictionaries-in-memory) using `cache`, `complex_key_cache`, `ssd_cache`, `complex_key_ssd_cache`, `direct`, `complex_key_direct` layouts.

Executable pool will spawn pool of processes with specified command and keep them running until they exit. The program should read data from STDIN while it is available and output result to STDOUT, and it can wait for next block of data on STDIN. ClickHouse will not close STDIN after processing a block of data but will pipe another chunk of data when needed. The executable script should be ready for this way of data processing — it should poll STDIN and flush data to STDOUT early.

Example of settings:

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

Setting fields:

-   `command` — The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` — The file format. All the formats described in “[Formats](../../../interfaces/formats.md#formats)” are supported.
-   `pool_size` — Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`.
-   `command_termination_timeout` — executable script should contain main read-write loop. After dictionary is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown, before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is 10. Optional parameter.
-   `max_command_execution_time` — Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is 10. Optional parameter.
-   `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
-   `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
-   `implicit_key` — The executable source file can return only values, and the correspondence to the requested keys is determined implicitly — by the order of rows in the result. Default value is false. Optional parameter.
-   `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional parameter.
-   `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled, otherwise, the DB user would be able to execute arbitrary binary on ClickHouse node.

## Http(s) {#dicts-external_dicts_dict_sources-http}

Working with an HTTP(s) server depends on [how the dictionary is stored in memory](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request via the `POST` method.

Example of settings:

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

or

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

In order for ClickHouse to access an HTTPS resource, you must [configure openSSL](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) in the server configuration.

Setting fields:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[Formats](../../../interfaces/formats.md#formats)” are supported.
-   `credentials` – Basic HTTP authentication. Optional parameter.
-   `user` – Username required for the authentication.
-   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
-   `header` – Single HTTP header entry.
-   `name` – Identifiant name used for the header send on the request.
-   `value` – Value set for a specific identifiant name.

When creating a dictionary using the DDL command (`CREATE DICTIONARY ...`) remote hosts for HTTP dictionaries are checked against the contents of `remote_url_allow_hosts` section from config to prevent database users to access arbitrary HTTP server.

### Known Vulnerability of the ODBC Dictionary Functionality {#known-vulnerability-of-the-odbc-dictionary-functionality}

:::note
When connecting to the database through the ODBC driver connection parameter `Servername` can be substituted. In this case values of `USERNAME` and `PASSWORD` from `odbc.ini` are sent to the remote server and can be compromised.
:::

**Example of insecure use**

Let’s configure unixODBC for PostgreSQL. Content of `/etc/odbc.ini`:

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

If you then make a query such as

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC driver will send values of `USERNAME` and `PASSWORD` from `odbc.ini` to `some-server.com`.

### Example of Connecting Postgresql {#example-of-connecting-postgresql}

Ubuntu OS.

Installing unixODBC and the ODBC driver for PostgreSQL:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuring `/etc/odbc.ini` (or `~/.odbc.ini` if you signed in under a user that runs ClickHouse):

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

The dictionary configuration in ClickHouse:

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

or

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

You may need to edit `odbc.ini` to specify the full path to the library with the driver `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Example of Connecting MS SQL Server {#example-of-connecting-ms-sql-server}

Ubuntu OS.

Installing the ODBC driver for connecting to MS SQL:

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuring the driver:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # test TDS connection
    $ sqsh -S MSSQL -D database -U user -P password


    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # if you signed in under a user that runs ClickHouse

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433


    # (optional) test ODBC connection (to use isql-tool install the [unixodbc](https://packages.debian.org/sid/unixodbc)-package)
    $ isql -v MSSQL "user" "password"
```

Remarks:
- to determine the earliest TDS version that is supported by a particular SQL Server version, refer to the product documentation or look at [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Configuring the dictionary in ClickHouse:

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

or

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

### ODBC {#dicts-external_dicts_dict_sources-odbc}

You can use this method to connect any database that has an ODBC driver.

Example of settings:

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

or

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Setting fields:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` parameters.
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).
-   `query` – The custom query. Optional parameter.

:::note
The `table` and `query` fields cannot be used together. And either one of the `table` or `query` fields must be declared.
:::

ClickHouse receives quoting symbols from ODBC-driver and quote all settings in queries to driver, so it’s necessary to set table name accordingly to table name case in database.

If you have a problems with encodings when using Oracle, see the corresponding [FAQ](../../../faq/integration/oracle-odbc.md) item.

### Mysql {#dicts-external_dicts_dict_sources-mysql}

Example of settings:

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

or

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

Setting fields:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` clause in MySQL, for example, `id > 10 AND id < 20`. Optional parameter.

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

-   `fail_on_connection_loss` – The configuration parameter that controls behavior of the server on connection loss. If `true`, an exception is thrown immediately if the connection between client and server was lost. If `false`, the ClickHouse server retries to execute the query three times before throwing an exception. Note that retrying leads to increased response times. Default value: `false`.

-   `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

MySQL can be connected on a local host via sockets. To do this, set `host` and `socket`.

Example of settings:

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

or

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

Example of settings:

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

or

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

Setting fields:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distributed](../../../engines/table-engines/special/distributed.md) table and enter it in subsequent configurations.
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).
-   `secure` - Use ssl for connection.
-   `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

Example of settings:

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

or

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

Setting fields:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

Example of settings:

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

or

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Setting fields:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` is for simple sources and for hashed single key sources, `hash_map` is for hashed sources with two keys. Ranged sources and cache sources with complex key are unsupported. May be omitted, default value is `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

### Cassandra {#dicts-external_dicts_dict_sources-cassandra}

Example of settings:

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

Setting fields:

-   `host` – The Cassandra host or comma-separated list of hosts.
-   `port` – The port on the Cassandra servers. If not specified, default port 9042 is used.
-   `user` – Name of the Cassandra user.
-   `password` – Password of the Cassandra user.
-   `keyspace` – Name of the keyspace (database).
-   `column_family` – Name of the column family (table).
-   `allow_filering` – Flag to allow or not potentially expensive conditions on clustering key columns. Default value is 1.
-   `partition_key_prefix` – Number of partition key columns in primary key of the Cassandra table. Required for compose key dictionaries. Order of key columns in the dictionary definition must be the same as in Cassandra. Default value is 1 (the first key column is a partition key and other key columns are clustering key).
-   `consistency` – Consistency level. Possible values: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Default value is `One`.
-   `where` – Optional selection criteria.
-   `max_threads` – The maximum number of threads to use for loading data from multiple partitions in compose key dictionaries.
-   `query` – The custom query. Optional parameter.

:::note
The `column_family` or `where` fields cannot be used together with the `query` field. And either one of the `column_family` or `query` fields must be declared.
:::

### PostgreSQL {#dicts-external_dicts_dict_sources-postgresql}

Example of settings:

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

or

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

Setting fields:

-   `host` – The host on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).
-   `port` – The port on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).
-   `user` – Name of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).
-   `password` – Password of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).
-   `replica` – Section of replica configurations. There can be multiple sections:
    -   `replica/host` – The PostgreSQL host.
    -   `replica/port` – The PostgreSQL port.
    -   `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` clause in PostgreSQL. For example, `id > 10 AND id < 20`. Optional parameter.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).
-   `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::