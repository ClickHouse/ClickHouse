---
slug: /ja/native-protocol/client
sidebar_position: 2
---

# クライアントパケット

| 値    | 名前                | 説明                      |
|-------|---------------------|---------------------------|
| 0     | [Hello](#hello)     | クライアントハンドシェイク開始 |
| 1     | [Query](#query)     | クエリリクエスト           |
| 2     | [Data](#data)       | データを含むブロック       |
| 3     | [Cancel](#cancel)   | クエリをキャンセル         |
| 4     | [Ping](#ping)       | Pingリクエスト             |
| 5     | TableStatus         | テーブルステータスリクエスト |

`Data`は圧縮可能です。

## Hello

たとえば、`Go Client` v1.10で`54451`プロトコルバージョンをサポートし、`default`データベースに`default`ユーザーで`secret`パスワードを使って接続したい場合です。

| フィールド           | タイプ     | 値             | 説明                       |
|----------------------|------------|----------------|----------------------------|
| client_name          | String     | `"Go Client"`  | クライアント実装名         |
| version_major        | UVarInt    | `1`            | クライアントメジャーバージョン |
| version_minor        | UVarInt    | `10`           | クライアントマイナーバージョン |
| protocol_version     | UVarInt    | `54451`        | TCPプロトコルバージョン    |
| database             | String     | `"default"`    | データベース名             |
| username             | String     | `"default"`    | ユーザー名                 |
| password             | String     | `"secret"`     | パスワード                 |

### プロトコルバージョン

プロトコルバージョンはクライアント側のTCPプロトコルバージョンです。

通常は最新の互換性のあるサーバーリビジョンと同じですが、これと混同しないでください。

### デフォルト

すべての値は**明示的に設定**する必要があります。サーバー側にデフォルト値はありません。クライアント側では、デフォルトとして`"default"`データベース、`"default"`ユーザー名、および`""`（空の文字列）パスワードを使用します。

## クエリ

| フィールド          | タイプ                        | 値         | 説明                         |
|---------------------|-------------------------------|------------|------------------------------|
| query_id            | String                        | `1ff-a123` | クエリID, UUIDv4可能          |
| client_info         | [ClientInfo](#client-info)    | タイプを参照| クライアントに関するデータ    |
| settings            | [Settings](#settings)         | タイプを参照| 設定のリスト                  |
| secret              | String                        | `secret`   | サーバー間の秘密               |
| [stage](#stage)     | UVarInt                       | `2`        | クエリステージまで実行         |
| compression         | UVarInt                       | `0`        | 無効=0、有効=1                |
| body                | String                        | `SELECT 1` | クエリテキスト                 |

### クライアント情報

| フィールド            | タイプ            | 説明                            |
|-----------------------|-------------------|---------------------------------|
| query_kind            | byte              | None=0, Initial=1, Secondary=2  |
| initial_user          | String            | 初期ユーザー                     |
| initial_query_id      | String            | 初期クエリID                     |
| initial_address       | String            | 初期アドレス                     |
| initial_time          | Int64             | 初期時間                         |
| interface             | byte              | TCP=1, HTTP=2                   |
| os_user               | String            | OSユーザー                       |
| client_hostname       | String            | クライアントホスト名             |
| client_name           | String            | クライアント名                   |
| version_major         | UVarInt           | クライアントメジャーバージョン     |
| version_minor         | UVarInt           | クライアントマイナーバージョン     |
| protocol_version      | UVarInt           | クライアントプロトコルバージョン   |
| quota_key             | String            | クオータキー                     |
| distributed_depth     | UVarInt           | 分散深度                         |
| version_patch         | UVarInt           | クライアントパッチバージョン       |
| otel                  | Bool              | トレースフィールドの有無          |
| trace_id              | FixedString(16)   | トレースID                       |
| span_id               | FixedString(8)    | スパンID                         |
| trace_state           | String            | トレース状態                     |
| trace_flags           | Byte              | トレースフラグ                   |

### 設定

| フィールド    | タイプ  | 値               | 説明                 |
|---------------|---------|------------------|----------------------|
| key           | String  | `send_logs_level`| 設定のキー           |
| value         | String  | `trace`          | 設定の値             |
| important     | Bool    | `true`           | 無視可能かどうか     |

リストとしてエンコードされ、キーと値が空の場合はリストの終わりを示します。

### ステージ

| 値    | 名前                | 説明                                           |
|-------|---------------------|-----------------------------------------------|
| 0     | FetchColumns        | カラムタイプのみ取得                           |
| 1     | WithMergeableState  | マージ可能な状態まで                           |
| 2     | Complete            | 完全な完了まで（デフォルトが推奨）              |

## データ

| フィールド | タイプ              | 説明                    |
|-----------|--------------------|------------------------|
| info      | BlockInfo          | エンコードされたブロック情報 |
| columns   | UVarInt            | カラム数                |
| rows      | UVarInt            | 行数                    |
| columns   | [[]Column](#column)| データを含むカラム      |

### カラム

| フィールド | タイプ   | 値              | 説明               |
|-----------|---------|-----------------|-------------------|
| name      | String  | `foo`           | カラム名           |
| type      | String  | `DateTime64(9)` | カラムタイプ       |
| data      | bytes   | ~               | カラムデータ       |

## キャンセル

パケットボディなし。サーバーはクエリをキャンセルする必要があります。

## ピング

パケットボディなし。サーバーは[pongで応答する](./server.md#pong)必要があります。
