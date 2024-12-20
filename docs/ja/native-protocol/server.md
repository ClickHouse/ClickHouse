---
slug: /ja/native-protocol/server
sidebar_position: 3
---

# サーバーパケット

| 値    | 名前                            | 説明                                                           |
|-------|--------------------------------|----------------------------------------------------------------|
| 0     | [Hello](#hello)                | サーバーのハンドシェイク応答                                     |
| 1     | Data                           | [クライアントデータ](./client.md#data) と同様                   |
| 2     | [Exception](#exception)        | クエリ処理の例外                                               |
| 3     | [Progress](#progress)          | クエリの進捗                                                   |
| 4     | [Pong](#pong)                  | ピン応答                                                       |
| 5     | [EndOfStream](#end-of-stream)  | すべてのパケットが転送された                                    |
| 6     | [ProfileInfo](#profile-info)   | プロファイリングデータ                                         |
| 7     | Totals                         | 合計値                                                         |
| 8     | Extremes                       | 極値 (最小, 最大)                                              |
| 9     | TablesStatusResponse           | TableStatus リクエストへの応答                                  |
| 10    | [Log](#log)                    | クエリシステムログ                                             |
| 11    | TableColumns                   | カラムの詳細                                                   |
| 12    | UUIDs                          | 一意なパーツIDのリスト                                          |
| 13    | ReadTaskRequest                | 次に必要なタスクを記述するためのリクエストを説明する文字列(UUID)|
| 14    | [ProfileEvents](#profile-events) | サーバーからのプロファイルイベントを含むパケット               |

`Data`、`Totals`、および `Extremes` は圧縮可能です。

## Hello

[クライアントの hello](./client.md#hello) に対する応答。

| フィールド       | タイプ   | 値             | 説明                   |
|----------------|----------|----------------|-----------------------|
| name           | String   | `Clickhouse`   | サーバー名             |
| version_major  | UVarInt  | `21`           | サーバーメジャーバージョン |
| version_minor  | UVarInt  | `12`           | サーバーマイナーバージョン |
| revision       | UVarInt  | `54452`        | サーバーリビジョン       |
| tz             | String   | `Europe/Moscow`| サーバータイムゾーン     |
| display_name   | String   | `Clickhouse`   | UI用サーバー名          |
| version_patch  | UVarInt  | `3`            | サーバーパッチバージョン |

## Exception

クエリ処理中のサーバー例外。

| フィールド     | タイプ   | 値                                     | 説明                          |
|---------------|----------|----------------------------------------|------------------------------|
| code          | Int32    | `60`                                   | [ErrorCodes.cpp][codes] を参照してください。|
| name          | String   | `DB::Exception`                        | サーバーメジャーバージョン    |
| message       | String   | `DB::Exception: Table X doesn't exist` | サーバーマイナーバージョン    |
| stack_trace   | String   | ~                                      | C++ スタックトレース          |
| nested        | Bool     | `true`                                 | 追加のエラー                  |

`nested` が `false` になるまで、例外の連続リストになることがあります。

[codes]: https://clickhouse.com/codebrowser/ClickHouse/src/Common/ErrorCodes.cpp.html "エラーコードのリスト"

## Progress

サーバーが定期的に報告するクエリ実行の進捗。

:::tip
**デルタ**で報告されます。合計はクライアント側で蓄積してください。
:::

| フィールド     | タイプ   | 値      | 説明                 |
|---------------|----------|--------|---------------------|
| rows          | UVarInt  | `65535`| 行数                 |
| bytes         | UVarInt  | `871799`| バイト数             |
| total_rows    | UVarInt  | `0`    | 合計行数             |
| wrote_rows    | UVarInt  | `0`    | クライアントからの行数|
| wrote_bytes   | UVarInt  | `0`    | クライアントからのバイト数 |

## Pong

[クライアントの ping](./client.md#ping) に対する応答、パケットボディなし。

## End of stream

**Data** パケットはもう送信されなくなり、クエリ結果が完全にサーバーからクライアントへストリームされました。

パケットボディなし。

## Profile info

| フィールド                  | タイプ   |
|---------------------------|----------|
| rows                      | UVarInt  |
| blocks                    | UVarInt  |
| bytes                     | UVarInt  |
| applied_limit             | Bool     |
| rows_before_limit         | UVarInt  |
| calculated_rows_before_limit | Bool  |

## Log

サーバーログを含む **Data block**。

:::tip
カラムの **data block** としてエンコードされますが、圧縮されません。
:::

| カラム        | タイプ     |
|--------------|-----------|
| time         | DateTime  |
| time_micro   | UInt32    |
| host_name    | String    |
| query_id     | String    |
| thread_id    | UInt64    |
| priority     | Int8      |
| source       | String    |
| text         | String    |

## Profile events

プロファイルイベントを含む **Data block**。

:::tip
カラムの **data block** としてエンコードされますが、圧縮されません。

`value` のタイプはサーバーリビジョンによって `UInt64` または `Int64` です。
:::

| カラム        | タイプ           |
|--------------|------------------|
| host_name    | String           |
| current_time | DateTime         |
| thread_id    | UInt64           |
| type         | Int8             |
| name         | String           |
| value        | UInt64 または Int64 |
