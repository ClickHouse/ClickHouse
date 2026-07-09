---
slug: /sql-reference/statements/create/dictionary/sources/odbc
title: 'مصدر قاموس ODBC'
sidebar_position: 6
sidebar_label: 'ODBC'
description: 'هيّئ اتصال ODBC كمصدر قاموس في ClickHouse.'
doc_type: 'مرجع'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

يمكنك استخدام هذه الطريقة للاتصال بأي قاعدة بيانات لديها برنامج تشغيل ODBC.

مثال على الإعدادات:

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

  <TabItem value="xml" label="ملف التهيئة">
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

حقول الإعدادات:

| الإعداد                | الوصف                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `db`                   | اسم قاعدة البيانات. احذفه إذا كان اسم قاعدة البيانات محددًا في معلمات `<connection_string>`.                          |
| `table`                | اسم الجدول والمخطط إن وُجد.                                                                                           |
| `connection_string`    | سلسلة الاتصال.                                                                                                        |
| `invalidate_query`     | استعلام للتحقق من حالة القاموس. اختياري. اقرأ المزيد في قسم [تحديث بيانات القاموس باستخدام LIFETIME](../lifetime.md). |
| `background_reconnect` | أعد الاتصال بالنسخة المتماثلة في الخلفية إذا فشل الاتصال. اختياري.                                                    |
| `query`                | الاستعلام المخصص. اختياري.                                                                                            |

:::note
لا يمكن استخدام الحقلين `table` و`query` معًا. ويجب التصريح بأحد الحقلين `table` أو `query`.
:::

يتلقى ClickHouse علامات الاقتباس من برنامج تشغيل ODBC، ويضع علامات اقتباس حول جميع الإعدادات في الاستعلامات المرسلة إلى برنامج التشغيل، لذلك من الضروري ضبط اسم الجدول بحيث يطابق حالة الأحرف المستخدمة لاسمه في قاعدة البيانات.

إذا واجهت مشكلات في الترميز عند استخدام Oracle، فراجع عنصر [FAQ](/ar/knowledgebase/oracle-odbc) ذي الصلة.

<div id="known-vulnerability-of-the-odbc-dictionary-functionality">
  ### ثغرة أمنية معروفة في وظيفة قاموس ODBC
</div>

:::note
عند الاتصال بقاعدة البيانات عبر برنامج تشغيل ODBC، يمكن استبدال parameter الاتصال `Servername`. في هذه الحالة، تُرسَل قيمتا `USERNAME` و`PASSWORD` من `odbc.ini` إلى الخادم البعيد، وقد تتعرضان للاختراق.
:::

**مثال على استخدام غير آمن**

لنُعِدّ unixODBC لـ PostgreSQL. محتوى `/etc/odbc.ini`:

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

إذا نفّذت بعد ذلك استعلامًا مثل

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

سيرسل برنامج تشغيل ODBC قيم `USERNAME` و`PASSWORD` من `odbc.ini` إلى `some-server.com`.

<div id="example-of-connecting-postgresql">
  ### مثال على الاتصال بـ PostgreSQL
</div>

نظام التشغيل Ubuntu.

تثبيت unixODBC وبرنامج تشغيل ODBC الخاص بـ PostgreSQL:

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

تهيئة `/etc/odbc.ini` (أو `~/.odbc.ini` إذا سجّلت الدخول بحساب المستخدم الذي يشغّل ClickHouse):

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

تهيئة القاموس في ClickHouse:

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

  <TabItem value="xml" label="ملف التهيئة">
    ```xml
    <clickhouse>
        <dictionary>
            <name>table_name</name>
            <source>
                <odbc>
                    <!-- يمكنك تحديد المعلمات التالية في connection_string: -->
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

قد تحتاج إلى تعديل `odbc.ini` لتحديد المسار الكامل إلى المكتبة التي تستخدم برنامج التشغيل `DRIVER=/usr/local/lib/psqlodbcw.so`.

<div id="example-of-connecting-ms-sql-server">
  ### مثال على الاتصال بـ MS SQL Server
</div>

نظام التشغيل Ubuntu.

تثبيت برنامج تشغيل ODBC للاتصال بـ MS SQL:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

إعداد برنامج التشغيل:

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

ملاحظات:

* لتحديد أقدم إصدار من TDS تدعمه نسخة معيّنة من SQL Server، ارجع إلى وثائق المنتج أو اطّلع على [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

تهيئة القاموس في ClickHouse:

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

  <TabItem value="xml" label="ملف التهيئة">
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