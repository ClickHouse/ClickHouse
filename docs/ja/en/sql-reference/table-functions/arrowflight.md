---
description: 'Apache Arrow Flight サーバーで公開されるデータの読み取りと書き込みを行えます。'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

[Apache Arrow Flight](/ja/interfaces/arrowflight) サーバーで公開されるデータの読み取りと書き込みを行えます。

**構文**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**引数**

* `host:port` — Arrow Flight サーバーのアドレスです。ポートが省略された場合は、デフォルトのポート `8815` が使用されます。[String](../../sql-reference/data-types/string.md)。
* `dataset_name` — Arrow Flight サーバーで利用可能なデータセットまたはディスクリプタの名前です。[String](../../sql-reference/data-types/string.md)。
* `username` — Basic HTTP 認証用のユーザー名です。[String](../../sql-reference/data-types/string.md)。
* `password` — Basic HTTP 認証用のパスワードです。[String](../../sql-reference/data-types/string.md)。

`username` と `password` が指定されていない場合は、認証は使用されません (これは Arrow Flight サーバーが認証なしのアクセスを許可している場合にのみ機能します) 。

この関数は [named collections](/ja/operations/named-collections) もサポートしています。サポートされているパラメータの一覧については、[ArrowFlight table engine](/ja/engines/table-engines/integrations/arrowflight#named-collections) を参照してください。

**戻り値**

リモートのデータセットを表すテーブルオブジェクトです。スキーマは Arrow Flight サーバーから推論されます。

**設定**

* `arrow_flight_request_descriptor_type` — データセット名を Flight サーバーに送信する方法を制御します。値: `path` (デフォルト) または `command`。詳細については、[ArrowFlight table engine](/ja/engines/table-engines/integrations/arrowflight#settings) を参照してください。

**例**

リモートの Arrow Flight サーバーから読み取る場合:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

リモートのArrow Flight サーバーにデータを挿入する:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

named collection を使う場合:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**関連項目**

* [ArrowFlight テーブルエンジン](/ja/engines/table-engines/integrations/arrowflight)
* [Arrow Flight インターフェイス](/ja/interfaces/arrowflight)
* [Apache Arrow Flight SQL 仕様書](https://arrow.apache.org/docs/format/FlightSql.html)