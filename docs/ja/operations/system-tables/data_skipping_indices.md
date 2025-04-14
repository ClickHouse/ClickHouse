---
slug: /ja/operations/system-tables/data_skipping_indices
---
# data_skipping_indices

全てのテーブルに存在するデータスキッピングインデックスに関する情報を含みます。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — データベース名。
- `table` ([String](../../sql-reference/data-types/string.md)) — テーブル名。
- `name` ([String](../../sql-reference/data-types/string.md)) — インデックス名。
- `type` ([String](../../sql-reference/data-types/string.md)) — インデックスタイプ。
- `type_full` ([String](../../sql-reference/data-types/string.md)) — 作成文からのインデックスタイプ式。
- `expr` ([String](../../sql-reference/data-types/string.md)) — インデックス計算のための式。
- `granularity` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ブロック内のグラニュール数。
- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 圧縮されたデータのサイズ（バイト単位）。
- `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 解凍されたデータのサイズ（バイト単位）。
- `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — マークのサイズ（バイト単位）。

**例**

```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       user_actions
name:        clicks_idx
type:        minmax
type_full:   minmax
expr:        clicks
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48

Row 2:
──────
database:    default
table:       users
name:        contacts_null_idx
type:        minmax
type_full:   minmax
expr:        assumeNotNull(contacts_null)
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48
```
