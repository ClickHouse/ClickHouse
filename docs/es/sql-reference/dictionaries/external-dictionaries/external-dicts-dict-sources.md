---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: Fuentes de diccionarios externos
---

# Fuentes de diccionarios externos {#dicts-external-dicts-dict-sources}

Un diccionario externo se puede conectar desde muchas fuentes diferentes.

Si el diccionario se configura usando xml-file, la configuración se ve así:

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

En caso de [Consulta DDL](../../statements/create.md#create-dictionary-query), la configuración igual parecerá:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

El origen está configurado en el `source` apartado.

Para tipos de origen [Archivo Local](#dicts-external_dicts_dict_sources-local_file), [Archivo ejecutable](#dicts-external_dicts_dict_sources-executable), [HTTP(s))](#dicts-external_dicts_dict_sources-http), [Haga clic en Casa](#dicts-external_dicts_dict_sources-clickhouse)
ajustes opcionales están disponibles:

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

o

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Tipos de fuentes (`source_type`):

-   [Archivo Local](#dicts-external_dicts_dict_sources-local_file)
-   [Archivo ejecutable](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(s))](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [Haga clic en Casa](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

## Archivo Local {#dicts-external_dicts_dict_sources-local_file}

Ejemplo de configuración:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

o

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Configuración de campos:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[Formato](../../../interfaces/formats.md#formats)” son compatibles.

## Archivo ejecutable {#dicts-external_dicts_dict_sources-executable}

Trabajar con archivos ejecutables depende de [cómo se almacena el diccionario en la memoria](external-dicts-dict-layout.md). Si el diccionario se almacena usando `cache` y `complex_key_cache`, ClickHouse solicita las claves necesarias enviando una solicitud al STDIN del archivo ejecutable. De lo contrario, ClickHouse inicia el archivo ejecutable y trata su salida como datos del diccionario.

Ejemplo de configuración:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

o

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Configuración de campos:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[Formato](../../../interfaces/formats.md#formats)” son compatibles.

## Http(s) {#dicts-external_dicts_dict_sources-http}

Trabajar con un servidor HTTP depende de [cómo se almacena el diccionario en la memoria](external-dicts-dict-layout.md). Si el diccionario se almacena usando `cache` y `complex_key_cache`, ClickHouse solicita las claves necesarias enviando una solicitud a través del `POST` método.

Ejemplo de configuración:

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

o

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

Para que ClickHouse tenga acceso a un recurso HTTPS, debe [configurar openSSL](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) en la configuración del servidor.

Configuración de campos:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[Formato](../../../interfaces/formats.md#formats)” son compatibles.
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

Puede utilizar este método para conectar cualquier base de datos que tenga un controlador ODBC.

Ejemplo de configuración:

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

o

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

Configuración de campos:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` parámetros.
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Actualización de diccionarios](external-dicts-dict-lifetime.md).

ClickHouse recibe símbolos de cotización del controlador ODBC y cita todas las configuraciones en las consultas al controlador, por lo que es necesario establecer el nombre de la tabla de acuerdo con el caso del nombre de la tabla en la base de datos.

Si tiene problemas con las codificaciones al utilizar Oracle, consulte el [FAQ](../../../faq/general.md#oracle-odbc-encodings) artículo.

### Vulnerabilidad conocida de la funcionalidad del diccionario ODBC {#known-vulnerability-of-the-odbc-dictionary-functionality}

!!! attention "Atención"
    Cuando se conecta a la base de datos a través del parámetro de conexión del controlador ODBC `Servername` puede ser sustituido. En este caso los valores de `USERNAME` y `PASSWORD` de `odbc.ini` se envían al servidor remoto y pueden verse comprometidos.

**Ejemplo de uso inseguro**

Vamos a configurar unixODBC para PostgreSQL. Contenido de `/etc/odbc.ini`:

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

Si luego realiza una consulta como

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

El controlador ODBC enviará valores de `USERNAME` y `PASSWORD` de `odbc.ini` a `some-server.com`.

### Ejemplo de Connecting Postgresql {#example-of-connecting-postgresql}

Sistema operativo Ubuntu.

Instalación de unixODBC y el controlador ODBC para PostgreSQL:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuración `/etc/odbc.ini` (o `~/.odbc.ini`):

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

La configuración del diccionario en ClickHouse:

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

o

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

Es posible que tenga que editar `odbc.ini` para especificar la ruta completa a la biblioteca con el controlador `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Ejemplo de conexión de MS SQL Server {#example-of-connecting-ms-sql-server}

Sistema operativo Ubuntu.

Instalación del controlador: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuración del controlador:

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

Configuración del diccionario en ClickHouse:

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

o

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

Ejemplo de configuración:

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

o

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

Configuración de campos:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` cláusula en MySQL, por ejemplo, `id > 10 AND id < 20`. Parámetro opcional.

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Actualización de diccionarios](external-dicts-dict-lifetime.md).

MySQL se puede conectar en un host local a través de sockets. Para hacer esto, establezca `host` y `socket`.

Ejemplo de configuración:

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

o

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

### Haga clic en Casa {#dicts-external_dicts_dict_sources-clickhouse}

Ejemplo de configuración:

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

o

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

Configuración de campos:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distribuido](../../../engines/table-engines/special/distributed.md) tabla e ingrésela en configuraciones posteriores.
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Actualización de diccionarios](external-dicts-dict-lifetime.md).

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

Ejemplo de configuración:

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

o

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

Configuración de campos:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

Ejemplo de configuración:

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

o

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Configuración de campos:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` es para fuentes simples y para fuentes de clave única hash, `hash_map` es para fuentes hash con dos teclas. Los orígenes a distancia y los orígenes de caché con clave compleja no son compatibles. Puede omitirse, el valor predeterminado es `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
