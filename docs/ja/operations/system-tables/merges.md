---
slug: /ja/operations/system-tables/merges
---
# merges

MergeTree ファミリーのテーブルに対して、現在進行中のマージやパートのミューテーションに関する情報を含んでいます。

カラム:

- `database` (String) — テーブルが属するデータベースの名前。
- `table` (String) — テーブル名。
- `elapsed` (Float64) — マージが開始されてからの経過時間（秒）。
- `progress` (Float64) — 完了した作業の割合で、0から1までの値。
- `num_parts` (UInt64) — マージされるパーツの数。
- `result_part_name` (String) — マージの結果として形成されるパートの名前。
- `is_mutation` (UInt8) — このプロセスがパートのミューテーションである場合は1。
- `total_size_bytes_compressed` (UInt64) — マージされたチャンクの圧縮データの合計サイズ。
- `total_size_marks` (UInt64) — マージされたパーツのマークの総数。
- `bytes_read_uncompressed` (UInt64) — 読み込まれた未圧縮のバイト数。
- `rows_read` (UInt64) — 読み込まれた行数。
- `bytes_written_uncompressed` (UInt64) — 書き込まれた未圧縮のバイト数。
- `rows_written` (UInt64) — 書き込まれた行数。
- `memory_usage` (UInt64) — マージプロセスのメモリ消費量。
- `thread_id` (UInt64) — マージプロセスのスレッドID。
- `merge_type` — 現在のマージのタイプ。ミューテーションの場合は空。
- `merge_algorithm` — 現在のマージで使用されているアルゴリズム。ミューテーションの場合は空。
