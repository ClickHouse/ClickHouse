---
slug: /ja/operations/system-tables/quotas_usage
---
# quotas_usage

全ユーザーのクオータ使用量。

カラム:
- `quota_name` ([String](../../sql-reference/data-types/string.md)) — クオータ名。
- `quota_key` ([String](../../sql-reference/data-types/string.md)) — キーの値。
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 現在のユーザーのクオータ使用量。
- `start_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — リソース消費の計算開始時刻。
- `end_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — リソース消費の計算終了時刻。
- `duration` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt32](../../sql-reference/data-types/int-uint.md))) — リソース消費計算のための時間間隔の長さ（秒単位）。
- `queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — この間隔でのリクエスト総数。
- `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — リクエストの最大数。
- `query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — この間隔でのSELECTリクエスト総数。
- `max_query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — SELECTリクエストの最大数。
- `query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — この間隔でのINSERTリクエスト総数。
- `max_query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — INSERTリクエストの最大数。
- `errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 例外を発生させたクエリ数。
- `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — エラーの最大数。
- `result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 結果として返された行の総数。
- `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — テーブルから読み取られたソース行の最大数。
- `result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリの結果を保存するために使用されたRAMバイト数。
- `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリの結果を保存するために使用されたRAMの最大バイト数。
- `read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md)))) — すべてのリモートサーバー上でクエリを実行するためにテーブルから読み取られたソース行の総数。
- `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリに関与したすべてのテーブルおよびテーブル関数から読み取られた行の最大数。
- `read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリに関与したすべてのテーブルおよびテーブル関数から読み取られたバイト数の総数。
- `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリに関与したすべてのテーブルおよびテーブル関数から読み取られたバイト数の最大値。
- `failed_sequential_authentications` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — 連続した認証失敗の総数。ユーザーが`failed_sequential_authentications`のしきい値を超える前に正しいパスワードを入力した場合、カウンタはリセットされます。
- `max_failed_sequential_authentications` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — 連続した認証失敗の最大数。
- `execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/float.md))) — クエリの総実行時間（秒単位の壁時計時間）。
- `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/float.md))) — クエリの実行時間の最大値。

## 参照 {#see-also}

- [SHOW QUOTA](../../sql-reference/statements/show.md#show-quota-statement)
