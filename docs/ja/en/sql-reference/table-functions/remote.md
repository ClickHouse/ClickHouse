---
description: '`remote` テーブル関数を使用すると、[Distributed](../../engines/table-engines/special/distributed.md)
  テーブルを作成しなくても、リモートサーバーに動的にアクセスできます。`remoteSecure`
  テーブル関数は `remote` と同じですが、セキュアな接続を使用します。'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

`remote` テーブル関数を使用すると、[Distributed](../../engines/table-engines/special/distributed.md) テーブルを作成しなくても、リモートサーバーに動的にアクセスできます。`remoteSecure` テーブル関数は `remote` と同じですが、セキュアな接続を使用します。

どちらの関数も `SELECT` クエリと `INSERT` クエリで使用できます。

<div id="syntax">
  ## 構文
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## パラメーター
</div>

| Argument         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `addresses_expr` | リモートサーバーのアドレス、または複数のリモートサーバーのアドレスを生成する式です。フォーマットは `host` または `host:port` です。<br /><br />    `host` には、server名、IPv4アドレス、またはIPv6アドレスを指定できます。IPv6アドレスは `[]` で囲んで指定する必要があります。<br /><br />    `port` はリモートサーバーのTCPポートです。ポートを省略した場合、テーブル関数 `remote` では server config file の [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) (デフォルトは 9000) 、テーブル関数 `remoteSecure` では [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) (デフォルトは 9440) を使用します。<br /><br />    IPv6アドレスでは、ポートの指定が必須です。<br /><br />    パラメーター `addresses_expr` のみを指定した場合、`db` と `table` にはデフォルトで `system.one` が使用されます。<br /><br />    Type: [String](../../sql-reference/data-types/string.md)。 |
| `db`             | Database name。Type: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `table`          | テーブル名。Type: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `user`           | ユーザー名。指定しない場合は `default` が使用されます。Type: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `password`       | ユーザーパスワード。指定しない場合は空のパスワードが使用されます。Type: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `sharding_key`   | ノード間でデータを分散するための分片キーです。例: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`。Type: [UInt32](../../sql-reference/data-types/int-uint.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

引数は [named collections](/ja/operations/named-collections.md) を使用して渡すこともできます。

<div id="returned-value">
  ## 戻り値
</div>

リモートサーバー上にあるテーブル。

<div id="usage">
  ## 使用
</div>

テーブル関数 `remote` および `remoteSecure` は、リクエストごとに接続を再確立するため、代わりに `Distributed` テーブルを使用することを推奨します。また、ホスト名が設定されている場合は名前解決が行われ、複数のレプリカを扱う際にエラーはカウントされません。大量のクエリを処理する場合は、必ず事前に `Distributed` テーブルを作成し、`remote` テーブル関数は使用しないでください。

`remote` テーブル関数は、次のような場合に役立ちます。

* あるシステムから別のシステムへの一回限りのデータ移行
* データの比較、デバッグ、テストのために特定のサーバーへアクセスする場合、つまりアドホックな接続
* 調査目的で複数の ClickHouse クラスター間でクエリを実行する場合
* 手動で実行する、頻度の低い分散リクエスト
* 対象サーバーの構成を毎回定義し直す分散リクエスト

<div id="addresses">
  ### アドレス
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

複数のアドレスは、カンマ区切りで指定できます。この場合、ClickHouse は分散処理を行い、指定したすべてのアドレス (異なるデータを持つ分片のようなもの) にクエリを送信します。例:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## 例
</div>

<div id="selecting-data-from-a-remote-server">
  ### リモートサーバーからデータを取得する:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

または、[named collections](/ja/operations/named-collections.md) を使用します：

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### リモートサーバー上のテーブルにデータを挿入する:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### あるシステムから別のシステムへのテーブル移行:
</div>

この例では、サンプルデータセット内の1つのテーブルを使用します。データベースは `imdb`、テーブルは `actors` です。

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### ソース側の ClickHouse システム上 (現在データをホストしているシステム)
</div>

* ソースデータベースとテーブル名 (`imdb.actors`) を確認します

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* ソース側で CREATE TABLE 文を取得します:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

出力

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### 宛先の ClickHouse システム上で
</div>

* 宛先データベースを作成します:

  ```sql
  CREATE DATABASE imdb
  ```

* ソース側の CREATE TABLE 文を使って、宛先側にテーブルを作成します:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### ソース側のデプロイメントに戻る
</div>

リモートシステム上に作成した新しいデータベースとテーブルにデータを挿入します。必要なのは、ホスト、ポート、ユーザー名、パスワード、宛先データベース、宛先テーブルです。

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## グロブ展開
</div>

`{ }` 内のパターンは、分片の集合を生成したり、レプリカを指定したりするために使用されます。`{ }` の組が複数ある場合は、対応する集合の直積が生成されます。

次の種類のパターンがサポートされています。

* `{a,b,c}` - 候補となる文字列 `a`、`b`、`c` のいずれかを表します。このパターンは、最初の分片アドレスでは `a` に、2 番目の分片アドレスでは `b` に、以降も同様に置き換えられます。たとえば、`example0{1,2}-1` は `example01-1` と `example02-1` というアドレスを生成します。
* `{N..M}` - 数値の範囲です。このパターンは、`N` から `M` まで (`M` を含む) 連番のインデックスを持つ分片アドレスを生成します。たとえば、`example0{1..2}-1` は `example01-1` と `example02-1` を生成します。
* `{0n..0m}` - 先頭にゼロを付けた数値の範囲です。このパターンでは、インデックスの先頭のゼロが保持されます。たとえば、`example{01..03}-1` は `example01-1`、`example02-1`、`example03-1` を生成します。
* `{a|b}` - `|` で区切られた任意個のバリアントです。このパターンはレプリカを指定します。たとえば、`example01-{1|2}` は `example01-1` と `example01-2` というレプリカを生成します。

クエリは最初に正常と判断されたレプリカに送信されます。ただし、`remote` の場合、レプリカは現在の [load&#95;balancing](../../operations/settings/settings.md#load_balancing) 設定で指定されている順序で順番に試行されます。
生成されるアドレス数は、[table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses) 設定によって制限されます。