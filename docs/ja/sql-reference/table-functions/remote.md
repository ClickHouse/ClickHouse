---
slug: /ja/sql-reference/table-functions/remote
sidebar_position: 175
sidebar_label: remote
---

# remote, remoteSecure

テーブル関数 `remote` は、[分散テーブル](../../engines/table-engines/special/distributed.md)を作成することなく、リモートサーバーに即座にアクセスすることを可能にします。テーブル関数 `remoteSecure` は、セキュアな接続を用いた `remote` と同様の機能を持ちます。

どちらの関数も `SELECT` および `INSERT` クエリで使用することができます。

## 構文

``` sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

## パラメータ

- `addresses_expr` — リモートサーバーのアドレスまたはリモートサーバーの複数のアドレスを生成する式。フォーマット: `host` または `host:port`。

    `host` はサーバー名として、あるいは IPv4 または IPv6 アドレスとして指定できます。IPv6 アドレスは角括弧で囲む必要があります。

    `port` はリモートサーバーのTCPポートです。ポートが省略された場合、テーブル関数 `remote` にはサーバー構成ファイルの [tcp_port](../../operations/server-configuration-parameters/settings.md#tcp_port)（デフォルトは9000）、テーブル関数 `remoteSecure` には [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure)（デフォルトは9440）が使用されます。

    IPv6 アドレスの場合、ポートの指定が必須です。

    パラメータ `addresses_expr` のみが指定された場合、`db` と `table` はデフォルトで `system.one` が使用されます。

    型: [String](../../sql-reference/data-types/string.md)。

- `db` — データベース名。型: [String](../../sql-reference/data-types/string.md)。
- `table` — テーブル名。型: [String](../../sql-reference/data-types/string.md)。
- `user` — ユーザー名。指定しない場合は `default` が使用されます。型: [String](../../sql-reference/data-types/string.md)。
- `password` — ユーザーパスワード。指定しない場合は空のパスワードが使用されます。型: [String](../../sql-reference/data-types/string.md)。
- `sharding_key` — ノード間でデータを分散するためのシャーディングキー。例: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`。型: [UInt32](../../sql-reference/data-types/int-uint.md)。

引数は[named collections](/docs/ja/operations/named-collections.md)を使って渡すこともできます。

## 戻り値

リモートサーバーに存在するテーブル。

## 使用法

テーブル関数 `remote` および `remoteSecure` は各リクエストごとに接続を再確立するため、`Distributed` テーブルを使用することをお勧めします。また、ホスト名が設定されている場合、名前の解決が行われ、さまざまなレプリカと作業する際にはエラーがカウントされません。多数のクエリを処理する場合は、事前に `Distributed` テーブルを作成し、`remote` テーブル関数を使用しないようにしてください。

`remote` テーブル関数は以下のケースで便利です:

- システム間の一回限りのデータ移行
- データ比較、デバッグ、およびテストのために特定のサーバーにアクセス（アドホック接続）
- 調査目的での複数の ClickHouse クラスター間のクエリ
- 手動で行われるまれな分散リクエスト
- サーバーのセットが毎回再定義される分散リクエスト

### アドレス

``` text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

複数のアドレスはカンマで区切ることができます。この場合、ClickHouse は分散処理を使用して、指定したすべてのアドレス（異なるデータを持つシャードのように）にクエリを送信します。例:

``` text
example01-01-1,example01-02-1
```

## 例

### リモートサーバーからデータを選択する：

``` sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

または[named collections](/docs/ja/operations/named-collections.md)を使用する場合：

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

### リモートサーバー上のテーブルにデータを挿入する：

``` sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

### システムからシステムへのテーブルの移行：

この例では、サンプルデータセットの1つのテーブルを使用します。データベースは `imdb`、テーブルは `actors` です。

#### ソース ClickHouse システムで（現在データをホストしているシステム）

- ソースデータベースおよびテーブル名を確認する（`imdb.actors`）

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

- ソースから CREATE TABLE ステートメントを取得する：

  ```
  select create_table_query
  from system.tables
  where database = 'imdb' and table = 'actors'
  ```

  レスポンス

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

#### 送信先 ClickHouse システムで：

- 送信先データベースを作成する：

  ```sql
  CREATE DATABASE imdb
  ```

- ソースからの CREATE TABLE ステートメントを使用し、送信先を作成する：

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

#### ソース展開に戻って：

リモートシステム上で作成した新しいデータベースおよびテーブルに挿入します。ホスト、ポート、ユーザー名、パスワード、送信先データベース、および送信先テーブルが必要です。

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

## グロビング {#globs-in-addresses}

波括弧 `{ }` 内のパターンは、シャードのセットを生成したり、レプリカを指定したりするのに使用されます。複数の波括弧ペアがある場合、対応するセットの直積が生成されます。

次のパターンタイプがサポートされています。

- `{a,b,c}` - 代替文字列のいずれかである `a`、`b` または `c` を表します。このパターンは最初のシャードアドレスでは `a` に置き換えられ、次のシャードアドレスでは `b`、その後も同様に置き換えられます。例: `example0{1,2}-1` はアドレス `example01-1` および `example02-1` を生成します。
- `{N..M}` - 数値の範囲。このパターンは、`N` から `M`（含む）へのインデックスを持つシャードアドレスを生成します。例: `example0{1..2}-1` は `example01-1` および `example02-1` を生成します。
- `{0n..0m}` - 先頭にゼロのある数値の範囲。このパターンはインデックスにおける先頭のゼロを維持します。例: `example{01..03}-1` は `example01-1`、`example02-1`、`example03-1` を生成します。
- `{a|b}` - `|` で区切られた任意の数のバリアント。パターンはレプリカを指定します。例: `example01-{1|2}` はレプリカ `example01-1` および `example01-2` を生成します。

クエリは最初の正常なレプリカに送信されます。しかし、`remote` では、レプリカは現在の[ロードバランシング](../../operations/settings/settings.md#load_balancing)設定に示されている順序で反復されます。生成されたアドレスの数は、[table_function_remote_max_addresses](../../operations/settings/settings.md#table_function_remote_max_addresses)設定によって制限されます。
