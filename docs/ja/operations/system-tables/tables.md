---
slug: /ja/operations/system-tables/tables
---
# tables

サーバーが認識している各テーブルのメタデータを含みます。

[Detach](../../sql-reference/statements/detach.md)されたテーブルは`system.tables`には表示されません。

[一時テーブル](../../sql-reference/statements/create/table.md#temporary-tables)は、それが作成されたセッション内でのみ`system.tables`に表示されます。それらは空の`database`フィールドと`is_temporary`フラグがオンの状態で示されます。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — テーブルが所属するデータベースの名前。

- `name` ([String](../../sql-reference/data-types/string.md)) — テーブル名。

- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — テーブルのUUID（Atomicデータベース）。

- `engine` ([String](../../sql-reference/data-types/string.md)) — テーブルエンジン名（パラメータなし）。

- `is_temporary` ([UInt8](../../sql-reference/data-types/int-uint.md)) - テーブルが一時的であるかを示すフラグ。

- `data_paths` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - ファイルシステム内のテーブルデータのパス。

- `metadata_path` ([String](../../sql-reference/data-types/string.md)) - ファイルシステム内のテーブルメタデータのパス。

- `metadata_modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - テーブルメタデータの最新修正時間。

- `metadata_version` ([Int32](../../sql-reference/data-types/int-uint.md)) - ReplicatedMergeTreeテーブルのメタデータバージョン、非ReplicatedMergeTreeテーブルの場合は0。

- `dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - データベースの依存関係。

- `dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - テーブルの依存関係（現行テーブルの[Materialized View](../../sql-reference/statements/create/view.md#materialized-view)）。

- `create_table_query` ([String](../../sql-reference/data-types/string.md)) - テーブルを作成するために使用されたクエリ。

- `engine_full` ([String](../../sql-reference/data-types/string.md)) - テーブルエンジンのパラメータ。

- `as_select` ([String](../../sql-reference/data-types/string.md)) - ビュー用の`SELECT`クエリ。

- `partition_key` ([String](../../sql-reference/data-types/string.md)) - テーブルで指定されたパーティションキー表現。

- `sorting_key` ([String](../../sql-reference/data-types/string.md)) - テーブルで指定されたソーティングキー表現。

- `primary_key` ([String](../../sql-reference/data-types/string.md)) - テーブルで指定された主キー表現。

- `sampling_key` ([String](../../sql-reference/data-types/string.md)) - テーブルで指定されたサンプリングキー表現。

- `storage_policy` ([String](../../sql-reference/data-types/string.md)) - ストレージポリシー:

    - [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    - [Distributed](../../engines/table-engines/special/distributed.md#distributed)

- `total_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - テーブル内の正確な行数を迅速に特定できる場合、合計行数。できない場合は`NULL`（基礎となる`Buffer`テーブルを含む）。

- `total_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - ストレージ上のテーブルに対して正確なバイト数を迅速に特定できる場合、合計バイト数。できない場合は`NULL`（基礎となるストレージを含まない）。

    - ディスクにデータを保存するテーブルの場合、ディスク上の使用済みスペース（圧縮状態）を返します。
    - メモリにデータを保存するテーブルの場合、メモリで使用されているおおよそのバイト数を返します。

- `total_bytes_uncompressed` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - ストレージ上のテーブルに対してパートのチェックサムから正確なバイト数を迅速に特定できる場合の非圧縮バイト総数。できない場合は`NULL`（基礎となるストレージを考慮しない）。

- `lifetime_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - サーバ起動以来INSERTされた行の総数（`Buffer`テーブルのみ）。

- `lifetime_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - サーバ起動以来INSERTされたバイトの総数（`Buffer`テーブルのみ）。

- `comment` ([String](../../sql-reference/data-types/string.md)) - テーブルへのコメント。

- `has_own_data` ([UInt8](../../sql-reference/data-types/int-uint.md)) — テーブル自身がディスク上にデータを保存しているか、別のソースにのみアクセスしているかを示すフラグ。

- `loading_dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - データベースの読み込み依存関係（現行オブジェクトの前に読み込まれるべきオブジェクトのリスト）。

- `loading_dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - テーブルの読み込み依存関係（現行オブジェクトの前に読み込まれるべきオブジェクトのリスト）。

- `loading_dependent_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - 依存しているデータベースの読み込み。

- `loading_dependent_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - 依存しているテーブルの読み込み。

`system.tables` テーブルは`SHOW TABLES`クエリの実装で使用されます。

**例**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
name:                       t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/store/81b/81b1c20a-b7c6-4116-a2ce-7583fb6b6736/']
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
metadata_modification_time: 2021-01-25 19:14:32
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE base.t1 (`n` UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY n SETTINGS index_granularity = 8192
as_select:                  SELECT database AS table_catalog
partition_key:              
sorting_key:                n
primary_key:                n
sampling_key:               
storage_policy:             default
total_rows:                 1
total_bytes:                99
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:                    
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []

Row 2:
──────
database:                   default
name:                       53r93yleapyears
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/data/default/53r93yleapyears/']
metadata_path:              /var/lib/clickhouse/metadata/default/53r93yleapyears.sql
metadata_modification_time: 2020-09-23 09:05:36
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE default.`53r93yleapyears` (`id` Int8, `febdays` Int8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY id SETTINGS index_granularity = 8192
as_select:                  SELECT name AS catalog_name
partition_key:              
sorting_key:                id
primary_key:                id
sampling_key:               
storage_policy:             default
total_rows:                 2
total_bytes:                155
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:                    
has_own_data:               0
loading_dependencies_database: []
loading_dependencies_table:    []
loading_dependent_database:    []
loading_dependent_table:       []
```
