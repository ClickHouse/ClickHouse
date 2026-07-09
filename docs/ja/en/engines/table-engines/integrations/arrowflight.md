---
description: 'このエンジンでは、Apache Arrow Flight プロトコルを介して、リモートデータセットに対するクエリの実行やデータの挿入を行えます。'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'ArrowFlight テーブルエンジン'
doc_type: 'リファレンス'
---

ArrowFlight テーブルエンジンを使用すると、ClickHouse は [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) プロトコル経由でリモートデータセットの読み取りと書き込みを行えます。
このインテグレーションにより、ClickHouse は列指向の Arrow フォーマットで外部の Flight 対応サーバーと高性能にやり取りできます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**エンジンパラメータ**

* `host:port` — リモート Arrow Flight サーバーのアドレス。ポートを省略した場合は、デフォルトのポート `8815` が使用されます。[String](../../../sql-reference/data-types/string.md)。
* `dataset_name` — Flight サーバー上のデータセットの識別子 (`arrow_flight_request_descriptor_type` 設定に応じて、PATHディスクリプタとして、または `SELECT *` クエリで使用されます) 。[String](../../../sql-reference/data-types/string.md)。
* `username` — Basic HTTP認証用のユーザー名。[String](../../../sql-reference/data-types/string.md)。
* `password` — Basic HTTP認証用のパスワード。[String](../../../sql-reference/data-types/string.md)。

`username` と `password` を省略した場合、認証は使用されません (これは Arrow Flight サーバーが未認証アクセスを許可している場合にのみ機能します) 。

カラムリストは省略可能です。省略した場合、スキーマは `GetSchema` を介してリモート Arrow Flight サーバーから推論されます。

<div id="named-collections">
  ## 名前付きコレクション
</div>

このエンジンは、接続パラメータを保存するための[名前付きコレクション](/ja/operations/named-collections)をサポートしています。

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

名前付きコレクションのパラメータ:

| パラメータ                      | 必須    | デフォルト   | 説明                         |
| -------------------------- | ----- | ------- | -------------------------- |
| `host` or `hostname`       | いいえ   | `""`    | サーバーのホスト名。                 |
| `port`                     | はい    | —       | サーバーのポート。                  |
| `dataset`                  | いいえ   | `""`    | データセット名またはディスクリプタ。         |
| `use_basic_authentication` | いいえ   | `true`  | 基本認証を有効にします。               |
| `user` or `username`       | 認証有効時 | —       | 認証に使用するユーザー名。              |
| `password`                 | いいえ   | `""`    | 認証に使用するパスワード。              |
| `enable_ssl`               | いいえ   | `false` | TLS 暗号化を有効にします。            |
| `ssl_ca`                   | いいえ   | `""`    | TLS 検証に使用する CA 証明書ファイルのパス。 |
| `ssl_override_hostname`    | いいえ   | `""`    | TLS 検証時に確認するホスト名を上書きします。   |

<div id="settings">
  ## 設定
</div>

* `arrow_flight_request_descriptor_type` — データセット名を Flight サーバーにどのように送信するかを制御します。設定可能な値: `path` (デフォルト。PATH ディスクリプタとして送信) または `command` (`SELECT * FROM <dataset>` を含む CMD ディスクリプタとして送信) 。SQL コマンドを想定している Flight サーバー (例: Dremio) の場合は、`command` を使用してください。

<div id="usage-example">
  ## 使用例
</div>

リモートの Arrow Flight サーバーからデータを読み取る場合：

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

リモートのArrow Flight サーバーへのデータ挿入:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## 注意事項
</div>

* `CREATE TABLE` ステートメントでカラムを指定する場合は、Flight サーバー から返されるスキーマと一致している必要があります。
* カラムを省略した場合、スキーマはリモートサーバーから自動的に推論されます。
* 読み取り (`SELECT`) と書き込み (`INSERT`) の両方に対応しています。
* `arrow_flight_request_descriptor_type` 設定では、データセット名を PATH ディスクリプタとして送信するか、`SELECT *` クエリを含む CMD ディスクリプタとして送信するかを制御します。

<div id="see-also">
  ## 関連項目
</div>

* [arrowFlight テーブル関数](/ja/sql-reference/table-functions/arrowflight)
* [Arrow Flight インターフェイス](/ja/interfaces/arrowflight)
* [Apache Arrow Flight SQL 仕様](https://arrow.apache.org/docs/format/FlightSql.html)
* [ClickHouse の Arrow フォーマット](/ja/interfaces/formats/Arrow)