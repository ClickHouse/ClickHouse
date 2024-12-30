---
slug: /ja/operations/system-tables/parts
---
# parts {#system_tables-parts}

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルのパーツに関する情報を含みます。

各行が一つのデータパーツを説明します。

カラム:

- `partition` ([String](../../sql-reference/data-types/string.md)) – パーティション名。パーティションとは何かを知るには、[ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter) クエリの説明をご覧ください。

    フォーマット:

    - `YYYYMM` 自動的に月ごとにパーティション分割されます。
    - `any_string` 手動でパーティション分割された場合。

- `name` ([String](../../sql-reference/data-types/string.md)) – データパーツの名前。

- `part_type` ([String](../../sql-reference/data-types/string.md)) — データパーツの保存フォーマット。

    値の可能性:

    - `Wide` — 各カラムがファイルシステム内の別々のファイルに保存されます。
    - `Compact` — すべてのカラムがファイルシステム内の1つのファイルに保存されます。

    データの保存フォーマットは、[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの `min_bytes_for_wide_part` および `min_rows_for_wide_part` 設定によって制御されます。

- `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) – データパーツがアクティブであるかを示すフラグ。データパーツがアクティブであれば、テーブルで使用されます。アクティブでない場合は削除されます。非アクティブなデータパーツはマージ後に残ります。

- `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) – マークの数。データパーツ内の行数をおおよそ求めるには、`marks` にインデックス粒度（通常8192）を掛けます（このヒントはアダプティブ粒度には適用されません）。

- `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 行数。

- `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツファイル全体のバイト単位のサイズ。

- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツ内の圧縮データの総サイズ。補助ファイル（例: マーク付きのファイル）は含まれていません。

- `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツ内の非圧縮データの総サイズ。補助ファイル（例: マーク付きのファイル）は含まれていません。

- `primary_key_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) – ディスク上の primary.idx/cidx ファイル内で主キー値に使用されるメモリ量（バイト単位）。

- `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – マーク付きファイルのサイズ。

- `secondary_indices_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツ内のセカンダリインデックス用の圧縮データの総サイズ。補助ファイル（例: マーク付きのファイル）は含まれていません。

- `secondary_indices_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツ内のセカンダリインデックス用の非圧縮データの総サイズ。補助ファイル（例: マーク付きのファイル）は含まれていません。

- `secondary_indices_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – セカンダリインデックス用のマーク付きファイルのサイズ。

- `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – データパーツのディレクトリが変更された時間。通常はデータパーツの作成時間に対応します。

- `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – データパーツが非アクティブになった時間。

- `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) – データパーツが使用されている場所の数。2以上の値は、データパーツがクエリやマージで使用されていることを示します。

- `min_date` ([Date](../../sql-reference/data-types/date.md)) – データパーツ内の日付キーの最小値。

- `max_date` ([Date](../../sql-reference/data-types/date.md)) – データパーツ内の日付キーの最大値。

- `min_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – データパーツ内の日付と時間キーの最小値。

- `max_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – データパーツ内の日付と時間キーの最大値。

- `partition_id` ([String](../../sql-reference/data-types/string.md)) – パーティションのID。

- `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – マージ後の現在のパーツを構成する最小のデータパーツ数。

- `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – マージ後の現在のパーツを構成する最大のデータパーツ数。

- `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) – マージツリーの深さ。0は、現在のパーツが他のパーツとのマージではなく挿入によって作成されたことを意味します。

- `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) – データパーツに適用すべきミューテーションを判断するために使用される番号（`data_version` より高いバージョンのミューテーションが適用される）。

- `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 主キー値のメモリ使用量（バイト単位）。

- `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 主キー値のために予約されたメモリ量（バイト単位）。

- `is_frozen` ([UInt8](../../sql-reference/data-types/int-uint.md)) – パーティションデータのバックアップが存在することを示すフラグ。1は存在することを示し、0は存在しないことを示します。詳細は [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md/#alter_freeze-partition) を参照してください。

- `database` ([String](../../sql-reference/data-types/string.md)) – データベースの名前。

- `table` ([String](../../sql-reference/data-types/string.md)) – テーブルの名前。

- `engine` ([String](../../sql-reference/data-types/string.md)) – パラメータを含まないテーブルエンジンの名前。

- `path` ([String](../../sql-reference/data-types/string.md)) – データパーツファイルのフォルダへの絶対パス。

- `disk_name` ([String](../../sql-reference/data-types/string.md)) – データパーツを保存しているディスクの名前。

- `hash_of_all_files` ([String](../../sql-reference/data-types/string.md)) – 圧縮ファイルの [sipHash128](../../sql-reference/functions/hash-functions.md/#hash_functions-siphash128)。

- `hash_of_uncompressed_files` ([String](../../sql-reference/data-types/string.md)) – 非圧縮ファイル（マーク付きのファイル、インデックスファイルなど）の [sipHash128](../../sql-reference/functions/hash-functions.md/#hash_functions-siphash128)。

- `uncompressed_hash_of_compressed_files` ([String](../../sql-reference/data-types/string.md)) – 圧縮ファイルが非圧縮であるかのようにデータ内の [sipHash128](../../sql-reference/functions/hash-functions.md/#hash_functions-siphash128)。

- `delete_ttl_info_min` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE ルール](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)のための日付と時間キーの最小値。

- `delete_ttl_info_max` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE ルール](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)のための日付と時間キーの最大値。

- `move_ttl_info.expression` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 式の配列。各式は[TLL MOVE ルール](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)を定義します。

:::note
`move_ttl_info.expression` 配列は主に後方互換性のために保持されています。現在、`TTL MOVE` ルールを確認する最も簡単な方法は `move_ttl_info.min` と `move_ttl_info.max` フィールドを使用することです。
:::

- `move_ttl_info.min` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日付と時間の値の配列。各要素は [TTL MOVE ルール](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)のためのキー値の最小値を示します。

- `move_ttl_info.max` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日付と時間の値の配列。各要素は [TTL MOVE ルール](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)のためのキー値の最大値を示します。

- `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – `bytes_on_disk` の別名。

- `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) – `marks_bytes` の別名。

**例**

``` sql
SELECT * FROM system.parts LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_4_1_6
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  6
bytes_on_disk:                         310
data_compressed_bytes:                 157
data_uncompressed_bytes:               91
secondary_indices_compressed_bytes:    58
secondary_indices_uncompressed_bytes:  6
secondary_indices_marks_bytes:         48
marks_bytes:                           144
modification_time:                     2020-06-18 13:01:49
remove_time:                           1970-01-01 00:00:00
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
min_time:                              1970-01-01 00:00:00
max_time:                              1970-01-01 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      4
level:                                 1
data_version:                          6
primary_key_bytes_in_memory:           8
primary_key_bytes_in_memory_allocated: 64
is_frozen:                             0
database:                              default
table:                                 months
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/months/all_1_4_1_6/
hash_of_all_files:                     2d0657a16d9430824d35e327fcbd87bf
hash_of_uncompressed_files:            84950cc30ba867c77a408ae21332ba29
uncompressed_hash_of_compressed_files: 1ad78f1c6843bbfb99a2c931abe7df7d
delete_ttl_info_min:                   1970-01-01 00:00:00
delete_ttl_info_max:                   1970-01-01 00:00:00
move_ttl_info.expression:              []
move_ttl_info.min:                     []
move_ttl_info.max:                     []
```

**関連項目**

- [MergeTree ファミリー](../../engines/table-engines/mergetree-family/mergetree.md)
- [カラムとテーブルのTTL](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-ttl)
