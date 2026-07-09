---
description: 'Documentación sobre colecciones con nombre'
sidebar_label: 'Colecciones con nombre'
sidebar_position: 69
slug: /operations/named-collections
title: 'Colecciones con nombre'
doc_type: 'Referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Las colecciones con nombre ofrecen una forma de almacenar colecciones de pares clave-valor para
utilizarlas en la configuración de integraciones con fuentes externas. Puede usar colecciones con nombre con
diccionarios, tablas, funciones de tabla y almacenamiento de objetos.

Las colecciones con nombre pueden configurarse con DDL o en archivos de configuración, y se aplican
cuando se inicia ClickHouse. Simplifican la creación de objetos y permiten ocultar credenciales
a los usuarios sin acceso administrativo.

Las claves de una colección con nombre deben coincidir con los nombres de los parámetros de la
función, el motor de tabla, la base de datos, etc. correspondientes. En los ejemplos siguientes, se
incluye un enlace a la lista de parámetros de cada tipo.

Los parámetros establecidos en una colección con nombre se pueden sobrescribir en SQL, como se muestra en los ejemplos
siguientes. Esta capacidad puede limitarse mediante las palabras clave `[NOT] OVERRIDABLE`, los atributos XML
y/o la opción de configuración `allow_named_collection_override_by_default`.

:::warning
Si se permite la sobrescritura, es posible que los usuarios sin acceso administrativo
puedan averiguar las credenciales que está intentando ocultar.
Si está usando colecciones con nombre con ese fin, debe deshabilitar
`allow_named_collection_override_by_default` (que está habilitada de forma predeterminada).
:::

<div id="storing-named-collections-in-the-system-database">
  ## Almacenamiento de colecciones con nombre en la base de datos del sistema
</div>

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

En el ejemplo anterior:

* `key_1` siempre se puede sobrescribir.
* `key_2` nunca se puede sobrescribir.
* `url` puede sobrescribirse o no según el valor de `allow_named_collection_override_by_default`.

<div id="permissions-to-create-named-collections-with-ddl">
  ### Permisos para crear colecciones con nombre con DDL
</div>

Para gestionar colecciones con nombre con DDL, un usuario debe tener el privilegio `named_collection_control`.  Esto se puede asignar añadiendo un archivo en `/etc/clickhouse-server/users.d/`.  El ejemplo otorga al usuario `default` los privilegios `access_management` y `named_collection_control`:

```xml title='/etc/clickhouse-server/users.d/user_default.xml'
<clickhouse>
  <users>
    <default>
      <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex replace=true>
      <access_management>1</access_management>
      <!-- highlight-start -->
      <named_collection_control>1</named_collection_control>
      <!-- highlight-end -->
    </default>
  </users>
</clickhouse>
```

:::tip
En el ejemplo anterior, el valor `password_sha256_hex` es la representación hexadecimal del hash SHA256 de la contraseña. Esta configuración para el usuario `default` tiene el atributo `replace=true`, ya que la configuración predeterminada establece una `password` en texto sin formato, y no es posible que un usuario tenga configuradas a la vez una contraseña en texto sin formato y una contraseña SHA256 en hexadecimal.
:::

<div id="storage-for-named-collections">
  ### Almacenamiento de colecciones con nombre
</div>

Las colecciones con nombre pueden almacenarse en el disco local o en ZooKeeper/Keeper. De forma predeterminada, se utiliza el almacenamiento local.
También pueden almacenarse cifradas con los mismos algoritmos usados para el [cifrado de disco](storing-data#encrypted-virtual-file-system),
donde `aes_128_ctr` se utiliza de forma predeterminada.

Para configurar el almacenamiento de colecciones con nombre, debe especificar un `type`. Puede ser `local` o `keeper`/`zookeeper`. Para almacenamiento cifrado,
puede usar `local_encrypted` o `keeper_encrypted`/`zookeeper_encrypted`.

Para usar ZooKeeper/Keeper, también es necesario configurar un `path` (ruta en ZooKeeper/Keeper donde se almacenarán las colecciones con nombre) en la
sección `named_collections_storage` del archivo de configuración. En el siguiente ejemplo se usan cifrado y ZooKeeper/Keeper:

```xml
<clickhouse>
  <named_collections_storage>
    <type>zookeeper_encrypted</type>
    <key_hex>bebec0cabebec0cabebec0cabebec0ca</key_hex>
    <algorithm>aes_128_ctr</algorithm>
    <path>/named_collections_path/</path>
    <update_timeout_ms>1000</update_timeout_ms>
  </named_collections_storage>
</clickhouse>
```

Un parámetro de configuración opcional, `update_timeout_ms`, tiene el valor `5000` de forma predeterminada.

<div id="storing-named-collections-in-configuration-files">
  ## Almacenar colecciones con nombre en archivos de configuración
</div>

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml title='/etc/clickhouse-server/config.d/named_collections.xml'
<clickhouse>
     <named_collections>
        <name>
            <key_1 overridable="true">value</key_1>
            <key_2 overridable="false">value_2</key_2>
            <url>https://connection.url/</url>
        </name>
     </named_collections>
</clickhouse>
```

En el ejemplo anterior:

* `key_1` siempre se puede sobrescribir.
* `key_2` no se puede sobrescribir nunca.
* `url` se puede sobrescribir o no, según el valor de `allow_named_collection_override_by_default`.

<div id="modifying-named-collections">
  ## Modificar colecciones con nombre
</div>

Las colecciones con nombre creadas con consultas DDL pueden modificarse o eliminarse mediante DDL. Las colecciones con nombre creadas con archivos XML pueden gestionarse editando o eliminando el archivo XML correspondiente.

<div id="alter-a-ddl-named-collection">
  ### Modificar una colección con nombre de DDL
</div>

Cambie o añada las claves `key1` y `key3` de la colección `collection2`
(esto no cambiará el valor del indicador `overridable` de esas claves):

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

Cambie o agregue la clave `key1` y permita sobrescribirla siempre:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

Quite la clave `key2` de `collection2`:

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

Modifique o agregue la clave `key1` y elimine la clave `key3` de la colección `collection2`:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

Para forzar que una clave use la configuración predeterminada del indicador `overridable`, debes
eliminarla y volver a añadirla.

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### Elimine la colección con nombre de DDL `collection2`:
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## Colecciones con nombre para acceder a S3
</div>

La descripción de los parámetros se encuentra en [función de tabla S3](../sql-reference/table-functions/s3.md).

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml
<clickhouse>
    <named_collections>
        <s3_mydata>
            <access_key_id>AKIAIOSFODNN7EXAMPLE</access_key_id>
            <secret_access_key>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</secret_access_key>
            <format>CSV</format>
            <url>https://s3.us-east-1.amazonaws.com/yourbucket/mydata/</url>
        </s3_mydata>
    </named_collections>
</clickhouse>
```

<div id="s3-function-and-s3-table-named-collection-examples">
  ### Ejemplos de colecciones con nombre para la función s3() y la tabla S3
</div>

Ambos ejemplos siguientes usan la misma colección con nombre `s3_mydata`:

<div id="s3-function">
  #### función s3()
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
El primer argumento de la función `s3()` anterior es el nombre de la colección, `s3_mydata`. Sin colección con nombre, el ID de la clave de acceso, el secret, el formato y la URL tendrían que incluirse en cada llamada a la función `s3()`.
:::

<div id="s3-table">
  #### Tabla de S3
</div>

```sql
CREATE TABLE s3_engine_table (number Int64)
ENGINE=S3(s3_mydata, url='https://s3.us-east-1.amazonaws.com/yourbucket/mydata/test_file.tsv.gz', format = 'TSV')
SETTINGS input_format_with_names_use_header = 0;

SELECT * FROM s3_engine_table LIMIT 3;
┌─number─┐
│      0 │
│      1 │
│      2 │
└────────┘
```

<div id="named-collections-for-accessing-mysql-database">
  ## Colecciones con nombre para acceder a la base de datos MySQL
</div>

Consulte la descripción de los parámetros en [mysql](../sql-reference/table-functions/mysql.md).

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION mymysql AS
user = 'myuser',
password = 'mypass',
host = '127.0.0.1',
port = 3306,
database = 'test',
connection_pool_size = 8,
replace_query = 1
```

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml
<clickhouse>
    <named_collections>
        <mymysql>
            <user>myuser</user>
            <password>mypass</password>
            <host>127.0.0.1</host>
            <port>3306</port>
            <database>test</database>
            <connection_pool_size>8</connection_pool_size>
            <replace_query>1</replace_query>
        </mymysql>
    </named_collections>
</clickhouse>
```

<div id="mysql-function-mysql-table-mysql-database-and-dictionary-named-collection-examples">
  ### Ejemplos de colecciones con nombre para la función mysql(), la tabla MySQL, la base de datos MySQL y el Diccionario
</div>

Los cuatro ejemplos siguientes usan la misma colección con nombre `mymysql`:

<div id="mysql-function">
  #### función mysql()
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
La named collection no especifica el parámetro `table`, así que este se indica en la llamada a la función como `table = 'test'`.
:::

<div id="mysql-table">
  #### Tabla de MySQL
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
El DDL sobrescribe el ajuste connection&#95;pool&#95;size de la colección con nombre.
:::

<div id="mysql-database">
  #### Base de datos MySQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

<div id="mysql-dictionary">
  #### Diccionario de MySQL
</div>

```sql
CREATE DICTIONARY dict (A Int64, B String)
PRIMARY KEY A
SOURCE(MYSQL(NAME mymysql TABLE 'source'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'B', 2);

┌─dictGet('dict', 'B', 2)─┐
│ two                     │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-postgresql-database">
  ## Colecciones con nombre para acceder a la base de datos PostgreSQL
</div>

Consulte la descripción de los parámetros en [postgresql](../sql-reference/table-functions/postgresql.md). Además, existen los siguientes alias:

* `username` para `user`
* `db` para `database`.

El parámetro `addresses_expr` se usa en una colección en lugar de `host:port`. Este parámetro es opcional, ya que hay otros parámetros opcionales: `host`, `hostname`, `port`. El siguiente pseudocódigo explica el orden de prioridad:

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

Ejemplo de creación:

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

Ejemplo de configuración:

```xml
<clickhouse>
    <named_collections>
        <mypg>
            <user>pguser</user>
            <password>jw8s0F4</password>
            <host>127.0.0.1</host>
            <port>5432</port>
            <database>test</database>
            <schema>test_schema</schema>
        </mypg>
    </named_collections>
</clickhouse>
```

<div id="example-of-using-named-collections-with-the-postgresql-function">
  ### Ejemplo de uso de colecciones con nombre con la función postgresql
</div>

```sql
SELECT * FROM postgresql(mypg, table = 'test');

┌─a─┬─b───┐
│ 2 │ two │
│ 1 │ one │
└───┴─────┘
SELECT * FROM postgresql(mypg, table = 'test', schema = 'public');

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

<div id="example-of-using-named-collections-with-database-with-engine-postgresql">
  ### Ejemplo de uso de colecciones con nombre con una base de datos que usa el motor PostgreSQL
</div>

```sql
CREATE TABLE mypgtable (a Int64) ENGINE = PostgreSQL(mypg, table = 'test', schema = 'public');

SELECT * FROM mypgtable;

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

:::note
PostgreSQL copia los datos de la colección con nombre cuando se crea la tabla. Los cambios en la colección no afectan a las tablas existentes.
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql-1">
  ### Ejemplo de uso de colecciones con nombre en una base de datos con motor PostgreSQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### Ejemplo de uso de colecciones con nombre con un diccionario con origen en POSTGRESQL
</div>

```sql
CREATE DICTIONARY dict (a Int64, b String)
PRIMARY KEY a
SOURCE(POSTGRESQL(NAME mypg TABLE test))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 2);

┌─dictGet('dict', 'b', 2)─┐
│ two                     │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-a-remote-clickhouse-database">
  ## Colecciones con nombre para acceder a una base de datos remota de ClickHouse
</div>

La descripción de los parámetros puede consultarse en [remote](../sql-reference/table-functions/remote.md/#parameters).

Ejemplo de configuración:

```sql
CREATE NAMED COLLECTION remote1 AS
host = 'remote_host',
port = 9000,
database = 'system',
user = 'foo',
password = 'secret',
secure = 1
```

```xml
<clickhouse>
    <named_collections>
        <remote1>
            <host>remote_host</host>
            <port>9000</port>
            <database>system</database>
            <user>foo</user>
            <password>secret</password>
            <secure>1</secure>
        </remote1>
    </named_collections>
</clickhouse>
```

`secure` no es necesario para la conexión gracias a `remoteSecure`, pero puede usarse para diccionarios.

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### Ejemplo de uso de colecciones con nombre en las funciones `remote`/`remoteSecure`
</div>

```sql
SELECT * FROM remote(remote1, table = one);
┌─dummy─┐
│     0 │
└───────┘

SELECT * FROM remote(remote1, database = merge(system, '^one'));
┌─dummy─┐
│     0 │
└───────┘

INSERT INTO FUNCTION remote(remote1, database = default, table = test) VALUES (1,'a');

SELECT * FROM remote(remote1, database = default, table = test);
┌─a─┬─b─┐
│ 1 │ a │
└───┴───┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-clickhouse">
  ### Ejemplo de uso de colecciones con nombre con un diccionario de origen ClickHouse
</div>

```sql
CREATE DICTIONARY dict(a Int64, b String)
PRIMARY KEY a
SOURCE(CLICKHOUSE(NAME remote1 TABLE test DB default))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 1);
┌─dictGet('dict', 'b', 1)─┐
│ a                       │
└─────────────────────────┘
```

<div id="named-collections-for-accessing-kafka">
  ## Colecciones con nombre para acceder a Kafka
</div>

La descripción de los parámetros se encuentra en [Kafka](../engines/table-engines/integrations/kafka.md).

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION my_kafka_cluster AS
kafka_broker_list = 'localhost:9092',
kafka_topic_list = 'kafka_topic',
kafka_group_name = 'consumer_group',
kafka_format = 'JSONEachRow',
kafka_max_block_size = '1048576';

```

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml
<clickhouse>
    <named_collections>
        <my_kafka_cluster>
            <kafka_broker_list>localhost:9092</kafka_broker_list>
            <kafka_topic_list>kafka_topic</kafka_topic_list>
            <kafka_group_name>consumer_group</kafka_group_name>
            <kafka_format>JSONEachRow</kafka_format>
            <kafka_max_block_size>1048576</kafka_max_block_size>
        </my_kafka_cluster>
    </named_collections>
</clickhouse>
```

<div id="example-of-using-named-collections-with-a-kafka-table">
  ### Ejemplo de uso de colecciones con nombre con una tabla de Kafka
</div>

Los dos ejemplos siguientes usan la misma colección con nombre `my_kafka_cluster`:

```sql
CREATE TABLE queue
(
    timestamp UInt64,
    level String,
    message String
)
ENGINE = Kafka(my_kafka_cluster)

CREATE TABLE queue
(
    timestamp UInt64,
    level String,
    message String
)
ENGINE = Kafka(my_kafka_cluster)
SETTINGS kafka_num_consumers = 4,
         kafka_thread_per_consumer = 1;
```

<div id="named-collections-for-backups">
  ## Colecciones con nombre para copias de seguridad
</div>

Para obtener una descripción de los parámetros, consulte [Copia de seguridad y restauración](/es/operations/backup/overview).

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml
<clickhouse>
    <named_collections>
        <named_collection_s3_backups>
            <url>https://my-s3-bucket.s3.amazonaws.com/backup-S3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </named_collection_s3_backups>
    </named_collections>
</clickhouse>
```

<div id="named-collections-for-accessing-mongodb-table-and-dictionary">
  ## Colecciones con nombre para acceder a la tabla y al diccionario de MongoDB
</div>

Consulte la descripción de los parámetros en [mongodb](../sql-reference/table-functions/mongodb.md).

<div id="ddl-example">
  ### Ejemplo de DDL
</div>

```sql
CREATE NAMED COLLECTION mymongo AS
user = '',
password = '',
host = '127.0.0.1',
port = 27017,
database = 'test',
collection = 'my_collection',
options = 'connectTimeoutMS=10000'
```

<div id="xml-example">
  ### Ejemplo en XML
</div>

```xml
<clickhouse>
    <named_collections>
        <mymongo>
            <user></user>
            <password></password>
            <host>127.0.0.1</host>
            <port>27017</port>
            <database>test</database>
            <collection>my_collection</collection>
            <options>connectTimeoutMS=10000</options>
        </mymongo>
    </named_collections>
</clickhouse>
```

<div id="mongodb-table">
  #### Tabla de MongoDB
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
El DDL sobrescribe la configuración de opciones de la colección con nombre.
:::

<div id="mongodb-dictionary">
  #### Diccionario de MongoDB
</div>

```sql
CREATE DICTIONARY dict
(
    `a` Int64,
    `b` String
)
PRIMARY KEY a
SOURCE(MONGODB(NAME mymongo COLLECTION my_dict))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED())

SELECT dictGet('dict', 'b', 2);

┌─dictGet('dict', 'b', 2)─┐
│ two                     │
└─────────────────────────┘
```

:::note
La colección con nombre especifica `my_collection` como nombre de la colección. En la llamada a la función, se sustituye por `collection = 'my_dict'` para seleccionar otra colección.
:::