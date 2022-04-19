---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "\u5916\u90E8\u8F9E\u66F8\u306E\u30BD\u30FC\u30B9"
---

# 外部辞書のソース {#dicts-external-dicts-dict-sources}

外部辞書は、さまざまなソースから接続できます。

辞書がxml-fileを使用して構成されている場合、構成は次のようになります:

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

の場合 [DDL-クエリ](../../statements/create.md#create-dictionary-query)、等しい構成は次のようになります:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

ソースは、 `source` セクション

ソースタイプの場合 [ローカル](#dicts-external_dicts_dict_sources-local_file), [実行可能ファイル](#dicts-external_dicts_dict_sources-executable), [HTTP(s)](#dicts-external_dicts_dict_sources-http), [クリックハウス](#dicts-external_dicts_dict_sources-clickhouse)
任意設定は利用できる:

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

または

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

ソースの種類 (`source_type`):

-   [ローカル](#dicts-external_dicts_dict_sources-local_file)
-   [実行可能ファイル](#dicts-external_dicts_dict_sources-executable)
-   [HTTP(s)](#dicts-external_dicts_dict_sources-http)
-   DBMS
    -   [ODBC](#dicts-external_dicts_dict_sources-odbc)
    -   [MySQL](#dicts-external_dicts_dict_sources-mysql)
    -   [クリックハウス](#dicts-external_dicts_dict_sources-clickhouse)
    -   [MongoDB](#dicts-external_dicts_dict_sources-mongodb)
    -   [Redis](#dicts-external_dicts_dict_sources-redis)

## ローカル {#dicts-external_dicts_dict_sources-local_file}

設定例:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

または

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

フィールドの設定:

-   `path` – The absolute path to the file.
-   `format` – The file format. All the formats described in “[形式](../../../interfaces/formats.md#formats)” サポートされます。

## 実行可能ファイル {#dicts-external_dicts_dict_sources-executable}

実行可能ファイルを操作するには [辞書をメモリに格納する方法](external-dicts-dict-layout.md). 辞書が以下を使用して格納されている場合 `cache` と `complex_key_cache`,ClickHouseは、実行可能ファイルのSTDINに要求を送信することによって、必要なキーを要求します。 その他、ClickHouse始まり実行可能ファイルを扱い、その出力としての辞書のデータです。

設定例:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
    </executable>
</source>
```

または

``` sql
SOURCE(EXECUTABLE(command 'cat /opt/dictionaries/os.tsv' format 'TabSeparated'))
```

フィールドの設定:

-   `command` – The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
-   `format` – The file format. All the formats described in “[形式](../../../interfaces/formats.md#formats)” サポートされます。

## Http(s) {#dicts-external_dicts_dict_sources-http}

HTTPサーバーでの作業は次のように依存します [辞書をメモリに格納する方法](external-dicts-dict-layout.md). 辞書が以下を使用して格納されている場合 `cache` と `complex_key_cache` クトを送信することにより、必要なキーを要求します。 `POST` 方法。

設定例:

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

または

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

ClickHouseがHTTPSリソースにアクセスするには、次の操作が必要です [openSSLの設定](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl) サーバー構成で。

フィールドの設定:

-   `url` – The source URL.
-   `format` – The file format. All the formats described in “[形式](../../../interfaces/formats.md#formats)” サポートされます。
-   `credentials` – Basic HTTP authentication. Optional parameter.
    -   `user` – Username required for the authentication.
    -   `password` – Password required for the authentication.
-   `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
    -   `header` – Single HTTP header entry.
    -   `name` – Identifiant name used for the header send on the request.
    -   `value` – Value set for a specific identifiant name.

## ODBC {#dicts-external_dicts_dict_sources-odbc}

このメソッドを使用して、ODBCドライバーを持つデータベースに接続できます。

設定例:

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

または

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
))
```

フィールドの設定:

-   `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` 変数。
-   `table` – Name of the table and schema if exists.
-   `connection_string` – Connection string.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [辞書の更新](external-dicts-dict-lifetime.md).

ClickHouseはODBC-driverから引用シンボルを受け取り、クエリ内のすべての設定をdriverに引用するため、データベース内のテーブル名の大文字と小文字に応じてテーブル名を

Oracleの使用時にエンコーディングに問題がある場合は、対応するものを参照してください [FAQ](../../../faq/general.md#oracle-odbc-encodings) 記事だ

### ODBCディクショナリ機能の既知の脆弱性 {#known-vulnerability-of-the-odbc-dictionary-functionality}

!!! attention "注意"
    ODBCドライバー接続パラメーターでデータベースに接続する場合 `Servername` 置換可能である。 この場合の値は `USERNAME` と `PASSWORD` から `odbc.ini` リモートサーバーに送信され、侵害される可能性があります。

**安全でない使用の例**

PostgreSQL用のunixODBCを設定しましょう。 の内容 `/etc/odbc.ini`:

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

次に、次のようなクエリを作成する場合

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBCドライバは、次の値を送信します `USERNAME` と `PASSWORD` から `odbc.ini` に `some-server.com`.

### Postgresqlの接続例 {#example-of-connecting-postgresql}

Ubuntu OS。

UnixodbcとPOSTGRESQL用ODBCドライバのインストール:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

設定 `/etc/odbc.ini` （または `~/.odbc.ini`):

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

ClickHouseの辞書構成:

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

または

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

編集が必要な場合があります `odbc.ini` ドライバを使用してライブラリへの完全パスを指定するには `DRIVER=/usr/local/lib/psqlodbcw.so`.

### MS SQL Serverの接続例 {#example-of-connecting-ms-sql-server}

Ubuntu OS。

ドライバの取り付け: :

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

ドライバの設定:

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

ClickHouseでの辞書の構成:

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

または

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

設定例:

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

または

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

フィールドの設定:

-   `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

-   `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

-   `db` – Name of the database.

-   `table` – Name of the table.

-   `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` MySQLの句、例えば, `id > 10 AND id < 20`. 任意パラメータ。

-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [辞書の更新](external-dicts-dict-lifetime.md).

MySQLはソケットを介してローカルホストに接続できます。 これを行うには、 `host` と `socket`.

設定例:

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

または

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

### クリックハウス {#dicts-external_dicts_dict_sources-clickhouse}

設定例:

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

または

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

フィールドの設定:

-   `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [分散](../../../engines/table-engines/special/distributed.md) テーブルと後続の構成でそれを入力します。
-   `port` – The port on the ClickHouse server.
-   `user` – Name of the ClickHouse user.
-   `password` – Password of the ClickHouse user.
-   `db` – Name of the database.
-   `table` – Name of the table.
-   `where` – The selection criteria. May be omitted.
-   `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [辞書の更新](external-dicts-dict-lifetime.md).

### Mongodb {#dicts-external_dicts_dict_sources-mongodb}

設定例:

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

または

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

フィールドの設定:

-   `host` – The MongoDB host.
-   `port` – The port on the MongoDB server.
-   `user` – Name of the MongoDB user.
-   `password` – Password of the MongoDB user.
-   `db` – Name of the database.
-   `collection` – Name of the collection.

### Redis {#dicts-external_dicts_dict_sources-redis}

設定例:

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

または

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

フィールドの設定:

-   `host` – The Redis host.
-   `port` – The port on the Redis server.
-   `storage_type` – The structure of internal Redis storage using for work with keys. `simple` は簡単な源のためのハッシュされたシングルキー源, `hash_map` 二つのキーを持つハッシュソース用です。 距源およびキャッシュ源の複雑な鍵サポートされていません。 省略可能であり、デフォルト値は `simple`.
-   `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

[元の記事](https://clickhouse.com/docs/en/query_language/dicts/external_dicts_dict_sources/) <!--hide-->
