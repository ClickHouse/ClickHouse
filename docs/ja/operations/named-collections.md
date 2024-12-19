---
slug: /ja/operations/named-collections
sidebar_position: 69
sidebar_label: "Named collections"
title: "Named collections"
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Named collectionsは外部ソースとの統合を構成するためのキー・バリューのペアのコレクションを保存する方法を提供します。Dictionary、テーブル、テーブル関数、およびオブジェクトストレージでNamed collectionsを使用できます。

Named collectionsはDDLまたは構成ファイルで構成可能で、ClickHouse起動時に適用されます。オブジェクトの作成を簡素化し、管理アクセス権のないユーザーから資格情報を隠すことができます。

Named collectionのキーは、対応する関数、テーブルエンジン、データベースなどのパラメータ名と一致する必要があります。以下の例では、各タイプにリンクされたパラメータリストがあります。

Named collectionで設定されたパラメータはSQLで上書き可能であることが、以下の例で示されています。この機能は`[NOT] OVERRIDABLE`キーワードおよびXML属性、または構成オプション`allow_named_collection_override_by_default`を使用して制限できます。

:::warning
上書きを許可すると、管理アクセス権のないユーザーが隠そうとしている資格情報を推測する可能性があります。
Named collectionsをその目的で使用している場合、`allow_named_collection_override_by_default`（デフォルトで有効）は無効にしてください。
:::

## システムデータベースでのNamed collectionsの保存

### DDL例

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value' OVERRIDABLE,
key_2 = 'value2' NOT OVERRIDABLE,
url = 'https://connection.url/'
```

上記の例では：

 * `key_1`は常に上書き可能です。
 * `key_2`は上書きできません。
 * `url`は、`allow_named_collection_override_by_default`の値に応じて上書き可能です。

### DDLを使用したNamed collections作成のための権限

DDLによってNamed collectionsを管理するには、ユーザーが`named_collection_control`特権を持っている必要があります。これを割り当てるには、`/etc/clickhouse-server/users.d/`にファイルを追加します。以下の例では、ユーザー`default`に`access_management`および`named_collection_control`の両方の特権を与えています：

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
上記の例では、`password_sha256_hex`の値はパスワードのSHA256ハッシュの16進数表現です。この設定では、ユーザー`default`の`replace=true`属性があります。そのため、デフォルト設定ではプレーンテキストの`password`が設定されており、プレーンテキストとsha256 hexの両方のパスワードを1人のユーザーに設定することはできません。
:::

### Named collectionsの保存

Named collectionsは、ローカルディスクまたはZooKeeper/Keeperに保存できます。デフォルトではローカルストレージが使用されます。
また、[disk encryption](storing-data#encrypted-virtual-file-system)で使用されるのと同じアルゴリズムで暗号化して保存することもできます。デフォルトでは`aes_128_ctr`が使用されます。

Named collectionsのストレージを構成するには、`type`を指定する必要があります。これは`local`または`keeper`/`zookeeper`のいずれかです。暗号化されたストレージの場合、`local_encrypted`または`keeper_encrypted`/`zookeeper_encrypted`を使用できます。

ZooKeeper/Keeperを使用するには、ZooKeeper/KeeperでNamed collectionsが保存されるパスである`path`を設定する必要があります。以下の例では、暗号化とZooKeeper/Keeperを使用しています：
```
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

オプションの構成パラメータである`update_timeout_ms`のデフォルトは`5000`です。

## 構成ファイルでのNamed collectionsの保存

### XML例

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

 * `key_1`は常に上書き可能です。
 * `key_2`は上書きできません。
 * `url`は、`allow_named_collection_override_by_default`の値に応じて上書き可能です。

## Named collectionsの修正

DDLクエリで作成されたNamed collectionsは、DDLを使用して変更または削除できます。XMLファイルで作成されたNamed collectionsは、対応するXMLを編集または削除して管理できます。

### DDL Named collectionの変更

コレクション`collection2`のキー`key1`と`key3`を変更または追加します
（これにより、そのキーの`overridable`フラグの値は変更されません）：
```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

キー`key1`を変更または追加し、常に上書き可能にします：
```sql
ALTER NAMED COLLECTION collection2 SET key1=4 OVERRIDABLE
```

コレクション`collection2`からキー`key2`を削除します：
```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

コレクション`collection2`のキー`key1`を変更または追加し、キー`key3`を削除します：
```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

`overridable`フラグのデフォルト設定を使用するようにキーを強制するには、キーを削除して再追加する必要があります。
```sql
ALTER NAMED COLLECTION collection2 DELETE key1;
ALTER NAMED COLLECTION collection2 SET key1=4;
```

### DDL Named collection `collection2`の削除：
```sql
DROP NAMED COLLECTION collection2
```

## S3へのアクセスのためのNamed collections

パラメータの説明は[s3 Table Function](../sql-reference/table-functions/s3.md)を参照してください。

### DDL例

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

### XML例

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

### s3()関数とS3テーブルのNamed collection例

以下の例はどちらも同じNamed collection `s3_mydata`を使用しています：

#### s3()関数

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
上記の`s3()`関数の最初の引数はコレクション名、`s3_mydata`です。Named collectionsがなければ、アクセスキーID、シークレット、フォーマット、URLを`s3()`関数への各呼び出しで渡す必要があります。
:::

#### S3テーブル

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

## MySQLデータベースへのアクセスのためのNamed collections

パラメータの説明は[mysql](../sql-reference/table-functions/mysql.md)を参照してください。

### DDL例

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

### XML例

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

### mysql()関数、MySQLテーブル、MySQLデータベース、およびDictionaryのNamed collection例

以下の4つの例は、すべて同じNamed collection `mymysql`を使用しています：

#### mysql()関数

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```
:::note
Named collectionは`table`パラメータを指定していないため、関数呼び出しで`table = 'test'`として指定されています。
:::

#### MySQLテーブル

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
DDLがconnection_pool_sizeのNamed collection設定を上書きします。
:::

#### MySQLデータベース

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

#### MySQL Dictionary

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

## PostgreSQLデータベースへのアクセスのためのNamed collections

パラメータの説明は[postgresql](../sql-reference/table-functions/postgresql.md)を参照してください。さらに、以下のエイリアスがあります：

- `username`は`user`のエイリアス
- `db`は`database`のエイリアス

パラメータ`addresses_expr`は、コレクション内で`host:port`の代わりに使用されます。パラメータはオプションであり、他のオプションもあります：`host`、`hostname`、`port`。以下の疑似コードは優先順位を説明します：

```sql
CASE
    WHEN collection['addresses_expr'] != '' THEN collection['addresses_expr']
    WHEN collection['host'] != ''           THEN collection['host'] || ':' || if(collection['port'] != '', collection['port'], '5432')
    WHEN collection['hostname'] != ''       THEN collection['hostname'] || ':' || if(collection['port'] != '', collection['port'], '5432')
END
```

作成例：
```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema'
```

構成例：
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

### postgresql関数でのNamed collectionsの使用例

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

### エンジンPostgreSQLを使用したデータベースでのNamed collectionsの使用例

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
PostgreSQLはテーブル作成時にNamed collectionからデータをコピーします。コレクションの変更は既存のテーブルに影響を与えません。
:::

### エンジンPostgreSQLを使用したデータベースでのNamed collectionsの使用例

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

### ソースPOSTGRESQLを使用したDictionaryでのNamed collectionsの使用例

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

## リモートClickHouseデータベースへのアクセスのためのNamed collections

パラメータの説明は[remote](../sql-reference/table-functions/remote.md/#parameters)を参照してください。

構成例：

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
`secure`は接続に必要ありませんが、dictionariesに使用できます。

### `remote`/`remoteSecure`関数でのNamed collectionsの使用例

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

### ソースClickHouseを使用したDictionaryでのNamed collectionsの使用例

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

## KafkaへのアクセスのためのNamed collections

パラメータの説明は[Kafka](../engines/table-engines/integrations/kafka.md)を参照してください。

### DDL例

```sql
CREATE NAMED COLLECTION my_kafka_cluster AS
kafka_broker_list = 'localhost:9092',
kafka_topic_list = 'kafka_topic',
kafka_group_name = 'consumer_group',
kafka_format = 'JSONEachRow',
kafka_max_block_size = '1048576';

```
### XML例

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

### KafkaテーブルでのNamed collectionsの使用例

以下の例はどちらも同じNamed collection `my_kafka_cluster`を使用しています：

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
