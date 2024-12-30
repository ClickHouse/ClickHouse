---
slug: /ja/operations/system-tables/settings_changes
---
# settings_changes

以前のClickHouseバージョンでの設定の変更に関する情報を含みます。

カラム:

- `version` ([String](../../sql-reference/data-types/string.md)) — 設定が変更されたClickHouseバージョン
- `changes` ([Array](../../sql-reference/data-types/array.md) of [Tuple](../../sql-reference/data-types/tuple.md)) — 設定変更の説明: (設定名、以前の値、新しい値、変更理由)

**例**

``` sql
SELECT *
FROM system.settings_changes
WHERE version = '23.5'
FORMAT Vertical
```

``` text
行 1:
──────
version: 23.5
changes: [('input_format_parquet_preserve_order','1','0','Parquetリーダーがより良い並列性のために行を並べ替えることを許可する。'),('parallelize_output_from_storages','0','1','ファイル/url/s3/etcから読み取るクエリの実行時に並列性を許可する。これにより行を並べ替える可能性がある。'),('use_with_fill_by_sorting_prefix','0','1','ORDER BY句でWITH FILLカラムの前にあるカラムはソートプレフィックスを形成する。ソートプレフィックスで異なる値を持つ行は独立して埋められる。'),('output_format_parquet_compliant_nested_types','0','1','出力Parquetファイルスキーマの内部フィールド名を変更する。')]
```

**関連項目**

- [設定](../../operations/settings/index.md#session-settings-intro)
- [system.settings](settings.md)
