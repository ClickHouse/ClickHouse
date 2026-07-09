---
slug: /sql-reference/statements/create/dictionary/sources/odbc
title: 'ODBC Dictionary ソース'
sidebar_position: 6
sidebar_label: 'ODBC'
description: 'ClickHouseで、ODBC 接続を Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

このメソッドを使用すると、ODBC ドライバを備えた任意のデータベースに接続できます。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(ODBC(
        db 'DatabaseName'
        table 'SchemaName.TableName'
        connection_string 'DSN=some_parameters'
        invalidate_query 'SQL_QUERY'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
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
  </TabItem>
</Tabs>

<br />

設定フィールド:

| Setting                | Description                                                                                                       |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `db`                   | データベース名です。データベース名が `<connection_string>` パラメータに設定されている場合は省略します。                                                   |
| `table`                | テーブル名と、存在する場合はスキーマ名です。                                                                                            |
| `connection_string`    | 接続文字列です。                                                                                                          |
| `invalidate_query`     | Dictionary のステータスを確認するためのクエリです。省略可能です。詳しくは [Refreshing dictionary data using LIFETIME](../lifetime.md) を参照してください。 |
| `background_reconnect` | 接続に失敗した場合、バックグラウンドでレプリカに再接続します。省略可能です。                                                                            |
| `query`                | カスタムクエリです。省略可能です。                                                                                                 |

:::note
`table` と `query` フィールドは同時に使用できません。また、`table` または `query` フィールドのいずれか一方を指定する必要があります。
:::

ClickHouse は ODBC ドライバからクォート文字を受け取り、ドライバに送るクエリ内のすべての設定値をクォートするため、データベース内のテーブル名の大文字・小文字に合わせて `table` 名を設定する必要があります。

Oracle の使用時にエンコーディングの問題が発生する場合は、対応する [よくある質問](/ja/knowledgebase/oracle-odbc) を参照してください。

<div id="known-vulnerability-of-the-odbc-dictionary-functionality">
  ### ODBC Dictionary 機能の既知の脆弱性
</div>

:::note
データベースへの接続時に、ODBC ドライバの接続パラメータ `Servername` は差し替えられる可能性があります。この場合、`odbc.ini` の `USERNAME` と `PASSWORD` の値がリモートサーバーに送信され、漏洩するおそれがあります。
:::

**安全でない使用例**

PostgreSQL 用に unixODBC を設定します。`/etc/odbc.ini` の内容:

```text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

その後、たとえば次のようなクエリを実行すると

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC ドライバは、`odbc.ini` の `USERNAME` と `PASSWORD` の値を `some-server.com` に送信します。

<div id="example-of-connecting-postgresql">
  ### PostgreSQL への接続例
</div>

Ubuntu OS。

PostgreSQL 用の unixODBC と ODBC ドライバをインストールします。

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

`/etc/odbc.ini` (ClickHouse を実行するユーザーでログインしている場合は `~/.odbc.ini`) を設定します:

```text
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

ClickHouse での Dictionary の設定:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY table_name (
        id UInt64,
        some_column UInt64 DEFAULT 0
    )
    PRIMARY KEY id
    SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
    LAYOUT(HASHED())
    LIFETIME(MIN 300 MAX 360)
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <clickhouse>
        <dictionary>
            <name>table_name</name>
            <source>
                <odbc>
                    <!-- connection_string には次のパラメータを指定できます: -->
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
  </TabItem>
</Tabs>

<br />

ドライバー `DRIVER=/usr/local/lib/psqlodbcw.so` としてライブラリのフルパスを指定するには、`odbc.ini` を編集する必要がある場合があります。

<div id="example-of-connecting-ms-sql-server">
  ### MS SQL Server への接続例
</div>

Ubuntu OS。

MS SQL Server に接続するための ODBC ドライバをインストールします:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

ドライバーの設定:

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

注意事項:

* 特定の SQL Server バージョンでサポートされる最も古い TDS バージョンを確認するには、製品ドキュメントを参照するか、[MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a) を確認してください

ClickHouse で Dictionary を設定するには、次のようにします。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY test (
        k UInt64,
        s String DEFAULT ''
    )
    PRIMARY KEY k
    SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
    LAYOUT(FLAT())
    LIFETIME(MIN 300 MAX 360)
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
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
  </TabItem>
</Tabs>