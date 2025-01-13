---
slug: /ja/operations/system-tables/quota_limits
---
# quota_limits

すべてのクォータのすべての間隔に対する最大値の情報を含みます。クォータには、任意の数またはゼロの行が対応します。

カラム:
- `quota_name` ([String](../../sql-reference/data-types/string.md)) — クォータ名。
- `duration` ([UInt32](../../sql-reference/data-types/int-uint.md)) — リソース消費を計算するための時間間隔の長さ（秒単位）。
- `is_randomized_interval` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 論理値。この間隔がランダム化されているかを示します。ランダム化されていない場合、間隔は常に同じ時間に開始されます。たとえば、1分間の間隔は、整数の分数で常に開始されます（例: 11:20:00に開始することはありますが、11:20:01に開始することはありません）。1日の間隔は常にUTCの午前0時に開始されます。ランダム化されている場合、最初の間隔はランダムな時間に開始され、その後は連続して開始します。値:
- `0` — 間隔はランダム化されていません。
- `1` — 間隔はランダム化されています。
- `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリの最大数。
- `max_query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — SELECTクエリの最大数。
- `max_query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — INSERTクエリの最大数。
- `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — エラーの最大数。
- `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 結果の行の最大数。
- `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリの結果を保存するために使用されるRAMボリュームの最大バイト数。
- `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリに参加したすべてのテーブルおよびテーブル関数から読み取られた行の最大数。
- `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — クエリに参加したすべてのテーブルおよびテーブル関数から読み取られたバイト数の最大値。
- `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — クエリの実行時間の最大値（秒単位）。
