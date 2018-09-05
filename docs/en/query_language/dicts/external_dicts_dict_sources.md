<a name="dicts-external_dicts_dict_sources"></a>

# Sources of external dictionaries

An external dictionary can be connected from many different sources.

The configuration looks like this:

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

The source is configured in the `source` section.

Types of sources (`source_type`):

- [Local file](#dicts-external_dicts_dict_sources-local_file)
- [Executable file](#dicts-external_dicts_dict_sources-executable)
- [HTTP(s)](#dicts-external_dicts_dict_sources-http)
- [ODBC](#dicts-external_dicts_dict_sources-odbc)
- DBMS
   - [MySQL](#dicts-external_dicts_dict_sources-mysql)
   - [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
   - [MongoDB](#dicts-external_dicts_dict_sources-mongodb)

<a name="dicts-external_dicts_dict_sources-local_file"></a>

## Local file

Example of settings:

```xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

Setting fields:

- `path` – The absolute path to the file.
- `format` – The file format. All the formats described in "[Formats](../../interfaces/formats.md#formats)" are supported.

<a name="dicts-external_dicts_dict_sources-executable"></a>

## Executable file

Working with executable files depends on [how the dictionary is stored in memory](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request to the executable file's `STDIN`.

Example of settings:

```xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

Setting fields:

- `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
- `format` – The file format. All the formats described in "[Formats](../../interfaces/formats.md#formats)" are supported.

<a name="dicts-external_dicts_dict_sources-http"></a>

## HTTP(s)

Working with an HTTP(s) server depends on [how the dictionary is stored in memory](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request via the `POST` method.

Example of settings:

```xml
<source>
    <http>
        <url>http://[::1]/os.tsv</url>
        <format>TabSeparated</format>
    </http>
</source>
```

In order for ClickHouse to access an HTTPS resource, you must [configure openSSL](../../operations/server_settings/settings.md#server_settings-openSSL) in the server configuration.

Setting fields:

- `url` – The source URL.
- `format` – The file format. All the formats described in "[Formats](../../interfaces/formats.md#formats)" are supported.

<a name="dicts-external_dicts_dict_sources-odbc"></a>

## ODBC

You can use this method to connect any database that has an ODBC driver.

Example of settings:

```xml
<odbc>
    <db>DatabaseName</db>
    <table>TableName</table>
    <connection_string>DSN=some_parameters</connection_string>
    <invalidate_query>SQL_QUERY</invalidate_query>
</odbc>
```

Setting fields:

- `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` parameters.
- `table` – Name of the table.
- `connection_string` – Connection string.
- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](external_dicts_dict_lifetime.md#dicts-external_dicts_dict_lifetime).

## Example of connecting PostgreSQL

Ubuntu OS.

Installing unixODBC and the ODBC driver for PostgreSQL:

    sudo apt-get install -y unixodbc odbcinst odbc-postgresql

Configuring `/etc/odbc.ini` (or `~/.odbc.ini`):

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

The dictionary configuration in ClickHouse:

```xml
<yandex>
    <dictionary>
        <name>table_name</name>
        <source>
        <odbc>
            <!-- You can specifiy the following parameters in connection_string: -->
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

You may need to edit `odbc.ini` to specify the full path to the library with the driver `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Example of connecting MS SQL Server

Ubuntu OS.

Installing the driver: :

```
    sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuring the driver: :

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

Configuring the dictionary in ClickHouse:

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

## DBMS

<a name="dicts-external_dicts_dict_sources-mysql"></a>

### MySQL

Example of settings:

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

Setting fields:

- `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `replica` – Section of replica configurations. There can be multiple sections.
   - `replica/host` – The MySQL host.

   \* `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

- `db` – Name of the database.

- `table` – Name of the table.

- `where ` – The selection criteria. Optional parameter.

- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Updating dictionaries](external_dicts_dict_lifetime.md#dicts-external_dicts_dict_lifetime).

MySQL can be connected on a local host via sockets. To do this, set `host` and `socket`.

Example of settings:

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

Example of settings:

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

Setting fields:

- `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distributed](../../operations/table_engines/distributed.md#table_engines-distributed) table and enter it in subsequent configurations.
- `port` – The port on the ClickHouse server.
- `user` – Name of the ClickHouse user.
- `password` – Password of the ClickHouse user.
- `db` – Name of the database.
- `table` – Name of the table.
- `where ` – The selection criteria. May be omitted.

<a name="dicts-external_dicts_dict_sources-mongodb"></a>

### MongoDB

Example of settings:

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

Setting fields:

- `host` – The MongoDB host.
- `port` – The port on the MongoDB server.
- `user` – Name of the MongoDB user.
- `password` – Password of the MongoDB user.
- `db` – Name of the database.
- `collection` – Name of the collection.

