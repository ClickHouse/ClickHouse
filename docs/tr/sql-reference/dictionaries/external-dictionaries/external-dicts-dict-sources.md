---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "D\u0131\u015F S\xF6zl\xFCklerin kaynaklar\u0131"
---

# Dış Sözlüklerin kaynakları {#dicts-external-dicts-dict-sources}

Harici bir sözlük birçok farklı kaynaktan bağlanabilir.

Sözlük xml dosyası kullanılarak yapılandırılmışsa, yapılandırma şöyle görünür:

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

Durumunda [DDL-sorgu](../../statements/create.md#create-dictionary-query), eşit yapılandırma gibi görünüyor olacak:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

Kaynak yapılandırılmış `source` bölme.

Kaynak türleri için [Yerel dosya](#dicts-external_dicts_dict_sources-local_file), [Yürütülebilir dosya](#dicts-external_dicts_dict_sources-executable), [HTTP (s)](#dicts-external_dicts_dict_sources-http), [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
isteğe bağlı ayarlar mevcuttur:

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

veya

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Kaynak türleri (`source_type`):

-   [Yerel dosya](#dicts-external_dicts_dict_sources-local_file)
-   [Yürütülebilir dosya](#dicts-external_dicts_dict_sources-executable)
-   [HTTP (s)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [ClickHouse](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

## Yerel Dosya {#dicts-external_dicts_dict_sources-local_file}

Ayarlar örneği:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

veya

``` sql
SOURCE(FILE(path '/opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Ayar alanları:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[Biçimliler](../../../interfaces/formats.md#formats)” desteklenir.

## Yürütülebilir Dosya {#dicts-external_dicts_dict_sources-executable}

Yürütülebilir dosyalarla çalışmak Aşağıdakilere bağlıdır [sözlük bellekte nasıl saklanır](external-dicts-dict-layout.md). Sözlük kullanılarak saklan theıyorsa `cache` ve `complex_key_cache` ClickHouse, yürütülebilir dosyanın STDIN'SİNE bir istek göndererek gerekli anahtarları ister. Aksi takdirde, clickhouse yürütülebilir dosyayı başlatır ve çıktısını sözlük verileri olarak değerlendirir.

Ayarlar örneği:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

veya

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

Ayar alanları:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[Biçimliler](../../../interfaces/formats.md#formats)” desteklenir.

## Http (s) {#dicts-external_dicts_dict_sources-http}

Bir HTTP (s) sunucusuyla çalışmak Aşağıdakilere bağlıdır [sözlük bellekte nasıl saklanır](external-dicts-dict-layout.md). Sözlük kullanılarak saklan theıyorsa `cache` ve `complex_key_cache`, ClickHouse aracılığıyla bir istek göndererek gerekli anahtarları ister `POST` yöntem.

Ayarlar örneği:

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

veya

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

Clickhouse'un bir HTTPS kaynağına erişebilmesi için şunları yapmanız gerekir [openssl'yi yapılandırma](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) sunucu yapılandırmasında.

Ayar alanları:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[Biçimliler](../../../interfaces/formats.md#formats)” desteklenir.
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

ODBC sürücüsü olan herhangi bir veritabanını bağlamak için bu yöntemi kullanabilirsiniz.

Ayarlar örneği:

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

veya

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

Ayar alanları:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` parametre.
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Sözlükleri güncelleme](external-dicts-dict-lifetime.md).

ClickHouse, ODBC sürücüsünden alıntı sembolleri alır ve sorgulardaki tüm ayarları sürücüye aktarır, bu nedenle tablo adını veritabanındaki tablo adı durumuna göre ayarlamak gerekir.

Oracle kullanırken kodlamalarla ilgili bir sorununuz varsa, ilgili [FAQ](../../../faq/general.md#oracle-odbc-encodings) Makale.

### ODBC Sözlük işlevselliği bilinen güvenlik açığı {#known-vulnerability-of-the-odbc-dictionary-functionality}

!!! attention "Dikkat"
    ODBC sürücüsü bağlantı parametresi aracılığıyla veritabanına bağlanırken `Servername` yerine. Bu durumda değerler `USERNAME` ve `PASSWORD` itibaren `odbc.ini` uzak sunucuya gönderilir ve tehlikeye girebilir.

**Güvensiz kullanım örneği**

PostgreSQL için unixodbc'yi yapılandıralım. İçeriği `/etc/odbc.ini`:

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

Daha sonra aşağıdaki gibi bir sorgu yaparsanız

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC sürücüsü değerleri gönderir `USERNAME` ve `PASSWORD` itibaren `odbc.ini` -e doğru `some-server.com`.

### PostgreSQL bağlanma örneği {#example-of-connecting-postgresql}

UB .untu OS.

PostgreSQL için UNİXODBC ve ODBC sürücüsünü yükleme:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Yapılandırma `/etc/odbc.ini` (veya `~/.odbc.ini`):

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

Clickhouse'da sözlük yapılandırması:

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

veya

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

Düzenlemeniz gerekebilir `odbc.ini` sürücü ile kitaplığın tam yolunu belirtmek için `DRIVER=/usr/local/lib/psqlodbcw.so`.

### MS SQL Server bağlanma örneği {#example-of-connecting-ms-sql-server}

UB .untu OS.

Sürücüyü yükleme: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Sürücüyü yapılandırma:

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

Clickhouse'da sözlüğü yapılandırma:

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

veya

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

Ayarlar örneği:

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

veya

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

Ayar alanları:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` MySQL, örneğin, `id > 10 AND id < 20`. İsteğe bağlı parametre.

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Sözlükleri güncelleme](external-dicts-dict-lifetime.md).

MySQL yuva üzerinden yerel bir ana bilgisayara bağlanabilir. Bunu yapmak için, ayarlayın `host` ve `socket`.

Ayarlar örneği:

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

veya

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

Ayarlar örneği:

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

veya

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

Ayar alanları:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Dağılı](../../../engines/table-engines/special/distributed.md) tablo ve sonraki yapılandırmalarda girin.
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Sözlükleri güncelleme](external-dicts-dict-lifetime.md).

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

Ayarlar örneği:

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

veya

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

Ayar alanları:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

Ayarlar örneği:

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

veya

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Ayar alanları:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` basit kaynaklar ve karma tek anahtar kaynaklar içindir, `hash_map` iki anahtarlı karma kaynaklar içindir. Ranged kaynakları ve karmaşık anahtarlı önbellek kaynakları desteklenmez. İhmal edilebilir, varsayılan değer `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
