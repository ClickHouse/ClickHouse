---
slug: /ja/operations/system-tables/distribution_queue
---
# distribution_queue

このテーブルはシャードに送信されるキューにあるローカルファイルに関する情報を含んでいます。これらのローカルファイルは、新しいデータを非同期モードで分散テーブルに挿入することによって生成された新しいパーツを含んでいます。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — データベースの名前。

- `table` ([String](../../sql-reference/data-types/string.md)) — テーブルの名前。

- `data_path` ([String](../../sql-reference/data-types/string.md)) — ローカルファイルが格納されているフォルダへのパス。

- `is_blocked` ([UInt8](../../sql-reference/data-types/int-uint.md)) — ローカルファイルをサーバーに送信することがブロックされているかを示すフラグ。

- `error_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — エラーの回数。

- `data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — フォルダ内のローカルファイルの数。

- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ローカルファイル内の圧縮データのサイズ（バイト単位）。

- `broken_data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 壊れている（エラーがあるため）とマークされたファイルの数。

- `broken_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 壊れたファイル内の圧縮データのサイズ（バイト単位）。

- `last_exception` ([String](../../sql-reference/data-types/string.md)) — 発生した最後のエラーに関するテキストメッセージ（もしあれば）。

**例**

``` sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:
```

**関連項目**

- [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)
