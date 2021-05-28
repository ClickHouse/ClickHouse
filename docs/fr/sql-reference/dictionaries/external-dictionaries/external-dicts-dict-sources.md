---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: Sources de dictionnaires externes
---

# Sources de dictionnaires externes {#dicts-external-dicts-dict-sources}

Externe dictionnaire peut être connecté à partir de nombreuses sources différentes.

Si dictionary est configuré à l'aide de xml-file, la configuration ressemble à ceci:

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

En cas de [DDL-requête](../../statements/create.md#create-dictionary-query), configuration égale ressemblera à:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

La source est configurée dans le `source` section.

Pour les types de source [Fichier Local](#dicts-external_dicts_dict_sources-local_file), [Fichier exécutable](#dicts-external_dicts_dict_sources-executable), [HTTP(S)](#dicts-external_dicts_dict_sources-http), [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
les paramètres optionnels sont disponibles:

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

ou

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Les Types de sources (`source_type`):

-   [Fichier Local](#dicts-external_dicts_dict_sources-local_file)
-   [Fichier exécutable](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(S)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

## Fichier Local {#dicts-external_dicts_dict_sources-local_file}

Exemple de paramètres:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

ou

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Définition des champs:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[Format](../../../interfaces/formats.md#formats)” sont pris en charge.

## Fichier Exécutable {#dicts-external_dicts_dict_sources-executable}

Travailler avec des fichiers exécutables en dépend [comment le dictionnaire est stocké dans la mémoire](external-dicts-dict-layout.md). Si le dictionnaire est stocké en utilisant `cache` et `complex_key_cache`, Clickhouse demande les clés nécessaires en envoyant une requête au STDIN du fichier exécutable. Sinon, ClickHouse démarre le fichier exécutable et traite sa sortie comme des données de dictionnaire.

Exemple de paramètres:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

ou

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Définition des champs:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[Format](../../../interfaces/formats.md#formats)” sont pris en charge.

## Http(s) {#dicts-external_dicts_dict_sources-http}

Travailler avec un serveur HTTP (S) dépend de [comment le dictionnaire est stocké dans la mémoire](external-dicts-dict-layout.md). Si le dictionnaire est stocké en utilisant `cache` et `complex_key_cache`, Clickhouse demande les clés nécessaires en envoyant une demande via le `POST` méthode.

Exemple de paramètres:

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

ou

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

Pour que ClickHouse accède à une ressource HTTPS, vous devez [configurer openSSL](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) dans la configuration du serveur.

Définition des champs:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[Format](../../../interfaces/formats.md#formats)” sont pris en charge.
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

Vous pouvez utiliser cette méthode pour connecter n'importe quelle base de données dotée d'un pilote ODBC.

Exemple de paramètres:

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

ou

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

Définition des champs:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` paramètre.
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Mise à jour des dictionnaires](external-dicts-dict-lifetime.md).

ClickHouse reçoit des symboles de citation D'ODBC-driver et cite tous les paramètres des requêtes au pilote, il est donc nécessaire de définir le nom de la table en conséquence sur le cas du nom de la table dans la base de données.

Si vous avez des problèmes avec des encodages lors de l'utilisation d'Oracle, consultez le [FAQ](../../../faq/general.md#oracle-odbc-encodings) article.

### Vulnérabilité connue de la fonctionnalité du dictionnaire ODBC {#known-vulnerability-of-the-odbc-dictionary-functionality}

!!! attention "Attention"
    Lors de la connexion à la base de données via le paramètre de connexion du pilote ODBC `Servername` peut être substitué. Dans ce cas, les valeurs de `USERNAME` et `PASSWORD` de `odbc.ini` sont envoyés au serveur distant et peuvent être compromis.

**Exemple d'utilisation non sécurisée**

Configurons unixODBC pour PostgreSQL. Le contenu de `/etc/odbc.ini`:

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

Si vous faites alors une requête telle que

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

Le pilote ODBC enverra des valeurs de `USERNAME` et `PASSWORD` de `odbc.ini` de `some-server.com`.

### Exemple de connexion Postgresql {#example-of-connecting-postgresql}

Ubuntu OS.

Installation d'unixODBC et du pilote ODBC pour PostgreSQL:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuration `/etc/odbc.ini` (ou `~/.odbc.ini`):

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

La configuration du dictionnaire dans ClickHouse:

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

ou

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

Vous devrez peut-être modifier `odbc.ini` pour spécifier le chemin d'accès complet à la bibliothèque avec le conducteur `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Exemple de connexion à MS SQL Server {#example-of-connecting-ms-sql-server}

Ubuntu OS.

Installation du pilote: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuration du pilote:

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

Configuration du dictionnaire dans ClickHouse:

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

ou

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

Exemple de paramètres:

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

ou

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

Définition des champs:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` clause dans MySQL, par exemple, `id > 10 AND id < 20`. Paramètre facultatif.

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Mise à jour des dictionnaires](external-dicts-dict-lifetime.md).

MySQL peut être connecté sur un hôte local via des sockets. Pour ce faire, définissez `host` et `socket`.

Exemple de paramètres:

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

ou

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

Exemple de paramètres:

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

ou

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

Définition des champs:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distribué](../../../engines/table-engines/special/distributed.md) table et entrez-le dans les configurations suivantes.
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Mise à jour des dictionnaires](external-dicts-dict-lifetime.md).

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

Exemple de paramètres:

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

ou

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

Définition des champs:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

Exemple de paramètres:

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

ou

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Définition des champs:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` est pour les sources simples et pour les sources à clé unique hachées, `hash_map` est pour les sources hachées avec deux clés. Les sources À Distance et les sources de cache à clé complexe ne sont pas prises en charge. Peut être omis, la valeur par défaut est `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
