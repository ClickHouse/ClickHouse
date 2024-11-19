---
slug: /ja/operations/system-tables/columns
---
# columns

すべてのテーブルのカラムに関する情報を含んでいます。

このテーブルを使用すると、複数のテーブルに対して一度に[DESCRIBE TABLE](../../sql-reference/statements/describe-table.md)クエリに似た情報を取得できます。

[一時テーブル](../../sql-reference/statements/create/table.md#temporary-tables)のカラムは、それが作成されたセッションでのみ `system.columns` に表示されます。これらは `database` フィールドが空の状態で表示されます。

`system.columns` テーブルには以下のカラムが含まれています（カラムタイプは括弧内に示されています）：

- `database` ([String](../../sql-reference/data-types/string.md)) — データベース名。
- `table` ([String](../../sql-reference/data-types/string.md)) — テーブル名。
- `name` ([String](../../sql-reference/data-types/string.md)) — カラム名。
- `type` ([String](../../sql-reference/data-types/string.md)) — カラムタイプ。
- `position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — テーブル内のカラムの序数、1から開始。
- `default_kind` ([String](../../sql-reference/data-types/string.md)) — デフォルト値の式タイプ (`DEFAULT`, `MATERIALIZED`, `ALIAS`)、未定義の場合は空の文字列。
- `default_expression` ([String](../../sql-reference/data-types/string.md)) — デフォルト値の式、未定義の場合は空の文字列。
- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 圧縮されたデータのサイズ（バイト単位）。
- `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 解凍されたデータのサイズ（バイト単位）。
- `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — マークのサイズ（バイト単位）。
- `comment` ([String](../../sql-reference/data-types/string.md)) — カラムに関するコメント、未定義の場合は空の文字列。
- `is_in_partition_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — カラムがパーティション式に含まれているかどうかを示すフラグ。
- `is_in_sorting_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — カラムがソートキー式に含まれているかどうかを示すフラグ。
- `is_in_primary_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — カラムが主キー式に含まれているかどうかを示すフラグ。
- `is_in_sampling_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — カラムがサンプリングキー式に含まれているかどうかを示すフラグ。
- `compression_codec` ([String](../../sql-reference/data-types/string.md)) — 圧縮コーデック名。
- `character_octet_length` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — バイナリデータ、文字データ、またはテキストデータと画像の最大バイト長。ClickHouseでは `FixedString` データ型にのみ意味があります。それ以外の場合は `NULL` 値が返されます。
- `numeric_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 近似数値データ、正確な数値データ、整数データ、または貨幣データの精度。ClickHouseでは整数型に対するビット幅と `Decimal` 型に対する小数精度です。それ以外の場合は `NULL` 値が返されます。
- `numeric_precision_radix` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 近似数値データ、正確な数値データ、整数データ、または貨幣データの正確さを表す数値システムの基数です。ClickHouseでは整数型に対しては2、`Decimal` 型に対しては10です。それ以外の場合は `NULL` 値が返されます。
- `numeric_scale` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 近似数値データ、正確な数値データ、整数データ、または貨幣データのスケール。ClickHouseでは `Decimal` 型にのみ意味があります。それ以外の場合は `NULL` 値が返されます。
- `datetime_precision` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — `DateTime64` データ型の小数精度。他のデータ型の場合は `NULL` 値が返されます。

**例**

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_catalog
type:                    String
position:                1
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ

Row 2:
──────
database:                INFORMATION_SCHEMA
table:                   COLUMNS
name:                    table_schema
type:                    String
position:                2
default_kind:
default_expression:
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:
character_octet_length:  ᴺᵁᴸᴸ
numeric_precision:       ᴺᵁᴸᴸ
numeric_precision_radix: ᴺᵁᴸᴸ
numeric_scale:           ᴺᵁᴸᴸ
datetime_precision:      ᴺᵁᴸᴸ
```
