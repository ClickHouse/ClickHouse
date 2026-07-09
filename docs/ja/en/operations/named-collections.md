---
description: '名前付きコレクションに関するドキュメント'
sidebar_label: '名前付きコレクション'
sidebar_position: 69
slug: /operations/named-collections
title: '名前付きコレクション'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

名前付きコレクションは、外部ソースとのインテグレーションを設定するためのキー・バリューのペアの集合を保存する仕組みです。名前付きコレクションは、
辞書、テーブル、テーブル関数、オブジェクトストレージで使用できます。

名前付きコレクションは DDL または設定ファイルで構成でき、
ClickHouse の起動時に適用されます。これにより、オブジェクトの作成が簡単になり、
管理アクセス権のないユーザーに認証情報を見せずに済みます。

名前付きコレクション内のオプションは、対応する
関数、table engine、データベース などのパラメータ名と一致している必要があります。以下の例では、各タイプごとに
パラメータ一覧へのリンクを示しています。

名前付きコレクションで設定したパラメータは、SQL で上書きできます。これについては以下の例で
示しています。この機能は、`[NOT] OVERRIDABLE` キーワードと XML 属性
および/または設定オプション `allow_named_collection_override_by_default` を使用して制限できます。

:::warning
上書きが許可されている場合、管理アクセス権のないユーザーでも、
隠そうとしている認証情報を特定できてしまう可能性があります。
その目的で名前付きコレクションを使用している場合は、
`allow_named_collection_override_by_default` (デフォルトで有効) を無効にする必要があります。
:::

<div id="storing-named-collections-in-the-system-database">
  ## system database に 名前付きコレクション を格納する
</div>

<div id="ddl-example">
  ### DDLの例
</div>

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

上記の例では：

* `key_1` は常に上書きできます。
* `key_2` は一切上書きできません。
* `url` は、`allow_named_collection_override_by_default` の値に応じて、上書きできる場合とできない場合があります。

<div id="permissions-to-create-named-collections-with-ddl">
  ### DDL で 名前付きコレクション を作成するための権限
</div>

DDL で 名前付きコレクション を管理するには、ユーザーに `named_collection_control` 権限が必要です。これは、`/etc/clickhouse-server/users.d/` にファイルを追加することで付与できます。以下の例では、ユーザー `default` に `access_management` と `named_collection_control` の両方の権限を付与しています。

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
上記の例では、`password_sha256_hex` の値は、パスワードの SHA256 ハッシュを16進数で表したものです。ユーザー `default` のこの設定には `replace=true` 属性が指定されています。これは、デフォルト設定では平文の `password` が設定されており、1人のユーザーに対して平文パスワードと sha256 hex パスワードの両方を同時に設定することはできないためです。
:::

<div id="storage-for-named-collections">
  ### 名前付きコレクション の保存先
</div>

名前付きコレクション は、ローカルディスクまたは ZooKeeper/Keeper に保存できます。デフォルトではローカルストレージが使用されます。
また、[ディスク暗号化](storing-data#encrypted-virtual-file-system)で使用されるものと同じアルゴリズムを使って暗号化して保存することもでき、
デフォルトでは `aes_128_ctr` が使用されます。

名前付きコレクション の保存先を設定するには、`type` を指定する必要があります。指定できる値は `local` または `keeper`/`zookeeper` です。暗号化ストレージの場合は、
`local_encrypted` または `keeper_encrypted`/`zookeeper_encrypted` を使用できます。

ZooKeeper/Keeper を使用するには、設定ファイルの `named_collections_storage` セクションに `path` (名前付きコレクション が保存される ZooKeeper/Keeper 内のパス) も
設定する必要があります。次の例では、暗号化と ZooKeeper/Keeper を使用しています:

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

オプションの設定パラメーター `update_timeout_ms` のデフォルト値は `5000` です。

<div id="storing-named-collections-in-configuration-files">
  ## 名前付きコレクション を設定ファイルに保存する
</div>

<div id="xml-example">
  ### XML の例
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

上記の例では：

* `key_1` は常に上書きできます。
* `key_2` は一切上書きできません。
* `url` は `allow_named_collection_override_by_default` の値に応じて、上書きできる場合とできない場合があります。

<div id="modifying-named-collections">
  ## 名前付きコレクションの変更
</div>

DDLクエリで作成された名前付きコレクションは、DDLを使って変更または削除できます。XMLファイルで作成された名前付きコレクションは、対応するXMLを編集または削除することで管理できます。

<div id="alter-a-ddl-named-collection">
  ### DDL 名前付きコレクション を変更する
</div>

コレクション `collection2` のキー `key1` と `key3` を変更または追加します
(これらのキーの `overridable` フラグの値は変更されません) :

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

`key1` キーを変更または追加し、常に上書き可能にします:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

`collection2` から `key2` キーを削除します:

```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

コレクション `collection2` の `key1` を変更または追加し、`key3` を削除します:

```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

キーで `overridable` フラグの既定の設定を使うようにするには、
そのキーを削除してから追加し直す必要があります。

```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

<div id="drop-the-ddl-named-collection-collection2">
  ### DDL 用の名前付きコレクション `collection2` を削除します:
</div>

```sql
DROP NAMED COLLECTION collection2
```

<div id="named-collections-for-accessing-s3">
  ## S3 にアクセスするための 名前付きコレクション
</div>

パラメータの説明は、[s3 テーブル関数](../sql-reference/table-functions/s3.md)を参照してください。

<div id="ddl-example">
  ### DDLの例
</div>

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

<div id="xml-example-1">
  ### XMLの例
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
  ### s3() 関数と S3 テーブルの名前付きコレクションの例
</div>

以下の 2 つの例では、どちらも同じ名前付きコレクション `s3_mydata` を使用しています。

<div id="s3-function">
  #### s3() 関数
</div>

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
上記の `s3()` 関数の最初の引数は、コレクション名 `s3_mydata` です。名前付きコレクションを使用しない場合は、アクセスキー ID、シークレットキー、フォーマット、URL を毎回 `s3()` 関数の呼び出し時に渡す必要があります。
:::

<div id="s3-table">
  #### S3 テーブル
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
  ## MySQL データベースにアクセスするための名前付きコレクション
</div>

各パラメーターの説明については、[mysql](../sql-reference/table-functions/mysql.md) を参照してください。

<div id="ddl-example">
  ### DDLの例
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

<div id="xml-example-1">
  ### XMLの例
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
  ### mysql() 関数、MySQL テーブル、MySQL データベース、および Dictionary の 名前付きコレクション の例
</div>

以下の 4 つの例では、同じ 名前付きコレクション `mymysql` を使用します:

<div id="mysql-function">
  #### mysql() 関数
</div>

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

:::note
名前付きコレクション では `table` パラメータが指定されていないため、関数呼び出しでは `table = 'test'` と指定します。
:::

<div id="mysql-table">
  #### MySQLテーブル
</div>

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
DDL によって、connection&#95;pool&#95;size に対する名前付きコレクションの設定が上書きされます。
:::

<div id="mysql-database">
  #### MySQL データベース
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
  #### MySQL Dictionary
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
  ## PostgreSQL データベースにアクセスするための名前付きコレクション
</div>

パラメータの説明は [postgresql](../sql-reference/table-functions/postgresql.md) を参照してください。さらに、以下の別名があります。

* `username` は `user` の別名
* `db` は `database` の別名

パラメータ `addresses_expr` は、コレクションでは `host:port` の代わりに使用します。このパラメータは省略可能です。これは、`host`、`hostname`、`port` という他の省略可能なパラメータがあるためです。優先順位は次の擬似コードで示します。

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

作成例:

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

設定例:

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
  ### `postgresql` 関数で 名前付きコレクション を使用する例
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
  ### PostgreSQL エンジンのデータベースで 名前付きコレクション を使用する例
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
PostgreSQL は、テーブルの作成時に 名前付きコレクション からデータをコピーします。名前付きコレクション を変更しても、既存のテーブルには影響しません。
:::

<div id="example-of-using-named-collections-with-database-with-engine-postgresql-1">
  ### PostgreSQLエンジンのデータベースで名前付きコレクションを使用する例
</div>

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

<div id="example-of-using-named-collections-with-a-dictionary-with-source-postgresql">
  ### ソースが POSTGRESQL の Dictionary で 名前付きコレクション を使用する例
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
  ## リモート ClickHouse データベースにアクセスするための名前付きコレクション
</div>

パラメータの説明は、[remote](../sql-reference/table-functions/remote.md/#parameters)を参照してください。

設定例:

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

`remoteSecure` を使用するため、接続には `secure` は不要ですが、辞書では使用できます。

<div id="example-of-using-named-collections-with-the-remoteremotesecure-functions">
  ### `remote`/`remoteSecure` 関数での 名前付きコレクション の使用例
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
  ### ソースが ClickHouse の Dictionary で named collections を使用する例
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
  ## Kafka にアクセスするための名前付きコレクション
</div>

パラメータの説明は [Kafka](../engines/table-engines/integrations/kafka.md) を参照してください。

<div id="ddl-example">
  ### DDLの例
</div>

```sql
CREATE NAMED COLLECTION my_kafka_cluster AS
kafka_broker_list = 'localhost:9092',
kafka_topic_list = 'kafka_topic',
kafka_group_name = 'consumer_group',
kafka_format = 'JSONEachRow',
kafka_max_block_size = '1048576';

```

<div id="xml-example-1">
  ### XMLの例
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
  ### Kafka テーブルで名前付きコレクションを使用する例
</div>

次の 2 つの例では、どちらも同じ名前付きコレクション `my_kafka_cluster` を使用します。

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
  ## バックアップ用の名前付きコレクション
</div>

パラメーターの説明は、[バックアップと復元](/ja/operations/backup/overview)を参照してください。

<div id="ddl-example">
  ### DDLの例
</div>

```sql
BACKUP TABLE default.test to S3(named_collection_s3_backups, 'directory')
```

<div id="xml-example-1">
  ### XMLの例
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
  ## MongoDB Table と Dictionary にアクセスするための 名前付きコレクション
</div>

パラメーターの説明については、[mongodb](../sql-reference/table-functions/mongodb.md)を参照してください。

<div id="ddl-example">
  ### DDLの例
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

<div id="xml-example-1">
  ### XMLの例
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
  #### MongoDB テーブル
</div>

```sql
CREATE TABLE mytable(log_type VARCHAR, host VARCHAR, command VARCHAR) ENGINE = MongoDB(mymongo, options='connectTimeoutMS=10000&compressors=zstd')
SELECT count() FROM mytable;

┌─count()─┐
│       2 │
└─────────┘
```

:::note
DDL で指定したオプションは、名前付きコレクション の設定より優先されます。
:::

<div id="mongodb-dictionary">
  #### MongoDB Dictionary
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
名前付きコレクション では、コレクション名に `my_collection` を指定しています。関数呼び出しでは、別のコレクションを選択するため、`collection = 'my_dict'` でこの値を上書きしています。
:::