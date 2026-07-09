---
description: 'Документация по именованным коллекциям'
sidebar_label: 'Именованные коллекции'
sidebar_position: 69
slug: /operations/named-collections
title: 'Именованные коллекции'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Именованные коллекции позволяют хранить наборы пар ключ-значение,
которые используются для настройки интеграций с внешними источниками. Именованные коллекции можно использовать со
словарями, таблицами, табличными функциями и Объектным хранилищем.

Именованные коллекции можно настраивать с помощью DDL или в файлах конфигурации; они применяются
при запуске ClickHouse. Они упрощают создание объектов и позволяют скрывать учетные данные
от пользователей без административного доступа.

Ключи в именованной коллекции должны совпадать с именами параметров соответствующей
функции, движка таблицы, базы данных и т. д. В примерах ниже для каждого типа
приведена ссылка на список параметров.

Параметры, заданные в именованной коллекции, можно переопределять в SQL — это показано в примерах
ниже. Эту возможность можно ограничить с помощью ключевых слов `[NOT] OVERRIDABLE`, XML-атрибутов
и/или параметра конфигурации `allow_named_collection_override_by_default`.

:::warning
Если переопределение разрешено, пользователи без административного доступа могут
выяснить учетные данные, которые вы пытаетесь скрыть.
Если вы используете именованные коллекции для этой цели, следует отключить
`allow_named_collection_override_by_default` (по умолчанию он включен).
:::

<div id="storing-named-collections-in-the-system-database">
  ## Хранение именованных коллекций в системной базе данных
</div>

<div id="ddl-example">
  ### Пример DDL
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

В приведённом выше примере:

* `key_1` всегда можно переопределить.
* `key_2` нельзя переопределить ни при каких условиях.
* `url` можно переопределить или нельзя — в зависимости от значения `allow_named_collection_override_by_default`.

<div id="permissions-to-create-named-collections-with-ddl">
  ### Разрешения на создание именованных коллекций с помощью DDL
</div>

Чтобы управлять именованными коллекциями с помощью DDL, пользователь должен иметь привилегию `named_collection_control`. Ее можно назначить, добавив файл в `/etc/clickhouse-server/users.d/`. В примере пользователю `default` назначаются обе привилегии: `access_management` и `named_collection_control`:

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
В приведённом выше примере значение `password_sha256_hex` — это шестнадцатеричное представление SHA256-хэша пароля. В этой конфигурации для пользователя `default` указан атрибут `replace=true`, поскольку в конфигурации по умолчанию задан `password` в открытом виде, а для одного пользователя нельзя одновременно задать пароль в открытом виде и пароль в виде sha256 hex.
:::

<div id="storage-for-named-collections">
  ### Хранилище для именованных коллекций
</div>

Именованные коллекции можно хранить как на локальном диске, так и в ZooKeeper/Keeper. По умолчанию используется локальное хранилище.
Их также можно хранить в зашифрованном виде с использованием тех же алгоритмов, что и для [шифрования диска](storing-data#encrypted-virtual-file-system),
при этом по умолчанию используется `aes_128_ctr`.

Чтобы настроить хранилище именованных коллекций, необходимо указать `type`. Это может быть `local` или `keeper`/`zookeeper`. Для зашифрованного хранилища
можно использовать `local_encrypted` или `keeper_encrypted`/`zookeeper_encrypted`.

Чтобы использовать ZooKeeper/Keeper, также нужно задать `path` (путь в ZooKeeper/Keeper, где будут храниться именованные коллекции) в
разделе `named_collections_storage` файла конфигурации. В следующем примере используются шифрование и ZooKeeper/Keeper:

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

Необязательный параметр конфигурации `update_timeout_ms` по умолчанию имеет значение `5000`.

<div id="storing-named-collections-in-configuration-files">
  ## Хранение именованных коллекций в конфигурационных файлах
</div>

<div id="xml-example">
  ### Пример XML
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

В приведенном выше примере:

* `key_1` можно переопределить всегда.
* `key_2` нельзя переопределить никогда.
* `url` можно переопределить или нет в зависимости от значения `allow_named_collection_override_by_default`.

<div id="modifying-named-collections">
  ## Изменение именованных коллекций
</div>

Именованные коллекции, созданные с помощью DDL-запросов, можно изменять или удалять с помощью DDL. Именованными коллекциями, созданными в XML-файлах, можно управлять, редактируя или удаляя соответствующий XML-файл.

<div id="alter-a-ddl-named-collection">
  ### Изменить именованную DDL-коллекцию
</div>

Измените или добавьте ключи `key1` и `key3` в коллекции `collection2`
(это не изменит значение флага `overridable` для этих ключей):

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

Измените или добавьте ключ `key1` и разрешите его всегда переопределять:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

Удалите ключ `key2` из `collection2`:

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

Измените или добавьте ключ `key1` и удалите ключ `key3` в коллекции `collection2`:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

Чтобы принудительно вернуть для ключа настройки по умолчанию для флага `overridable`, необходимо
удалить ключ и добавить его заново.

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### Удалите именованную коллекцию DDL `collection2`:
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## Именованные коллекции для доступа к S3
</div>

Описание параметров приведено в [табличной функции S3](../sql-reference/table-functions/s3.md).

<div id="ddl-example">
  ### Пример DDL
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example">
  ### Пример XML
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
  ### Примеры использования именованной коллекции в функции s3() и таблице S3
</div>

В обоих следующих примерах используется одна и та же именованная коллекция `s3_mydata`:

<div id="s3-function">
  #### Функция s3()
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
Первый аргумент функции `s3()`, показанной выше, — имя коллекции `s3_mydata`. Без именованных коллекций при каждом вызове функции `s3()` пришлось бы передавать идентификатор ключа доступа, секретный ключ, формат и URL.
:::

<div id="s3-table">
  #### Таблица S3
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
  ## Именованные коллекции для доступа к базе данных MySQL
</div>

Описание параметров приведено в [mysql](../sql-reference/table-functions/mysql.md).

<div id="ddl-example">
  ### Пример DDL
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
  ### Пример XML
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
  ### Примеры именованной коллекции для функции `mysql()`, таблицы MySQL, базы данных MySQL и словаря
</div>

В следующих четырех примерах используется одна и та же именованная коллекция `mymysql`:

<div id="mysql-function">
  #### Функция mysql()
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
В именованной коллекции не указан параметр `table`, поэтому в вызове функции он задаётся как `table = 'test'`.
:::

<div id="mysql-table">
  #### Таблица MySQL
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
DDL переопределяет параметр connection&#95;pool&#95;size в именованной коллекции.
:::

<div id="mysql-database">
  #### База данных MySQL
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
  #### Словарь MySQL
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
  ## Именованные коллекции для доступа к базе данных PostgreSQL
</div>

Описание параметров см. в разделе [postgresql](../sql-reference/table-functions/postgresql.md). Кроме того, доступны псевдонимы:

* `username` для `user`
* `db` для `database`.

Параметр `addresses_expr` в именованной коллекции используется вместо `host:port`. Этот параметр необязателен, поскольку есть и другие необязательные параметры: `host`, `hostname`, `port`. Приоритет иллюстрируется следующим псевдокодом:

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

Пример создания:

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

Пример конфигурации:

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
  ### Пример использования именованных коллекций с функцией postgresql
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
  ### Пример использования именованных коллекций с базой данных на движке PostgreSQL
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
PostgreSQL копирует данные из именованной коллекции при создании таблицы. Изменения в коллекции не влияют на уже созданные таблицы.
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql">
  ### Пример использования именованных коллекций с базой данных на движке PostgreSQL
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### Пример использования именованных коллекций в словаре с источником POSTGRESQL
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
  ## Именованные коллекции для доступа к удалённой базе данных ClickHouse
</div>

Описание параметров приведено в [remote](../sql-reference/table-functions/remote.md/#parameters).

Пример конфигурации:

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

`secure` не нужен для соединения благодаря `remoteSecure`, но его можно использовать для словарей.

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### Пример использования именованных коллекций с функциями `remote`/`remoteSecure`
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
  ### Пример использования именованных коллекций со словарём, использующим ClickHouse в качестве источника
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
  ## Именованные коллекции для доступа к Kafka
</div>

Описание параметров приведено в разделе [Kafka](../engines/table-engines/integrations/kafka.md).

<div id="ddl-example">
  ### Пример DDL
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
  ### Пример XML
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
  ### Пример использования именованных коллекций с таблицей Kafka
</div>

В обоих примерах ниже используется одна и та же именованная коллекция `my_kafka_cluster`:

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
  ## Именованные коллекции для резервных копий
</div>

Описание параметров приведено в разделе [Backup and Restore](/ru/operations/backup/overview).

<div id="ddl-example">
  ### Пример DDL
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example">
  ### Пример XML
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
  ## Именованные коллекции для доступа к таблице и словарю MongoDB
</div>

Описание параметров см. в [mongodb](../sql-reference/table-functions/mongodb.md).

<div id="ddl-example">
  ### Пример DDL
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
  ### Пример XML
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
  #### Таблица MongoDB
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
DDL переопределяет параметр options из именованной коллекции.
:::

<div id="mongodb-dictionary">
  #### Словарь MongoDB
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
именованная коллекция указывает `my_collection` в качестве имени collection. В вызове функции это значение переопределяется через `collection = 'my_dict'`, чтобы выбрать другую collection.
:::