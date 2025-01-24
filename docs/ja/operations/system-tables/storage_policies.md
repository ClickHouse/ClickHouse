---
slug: /ja/operations/system-tables/storage_policies
---
# storage_policies

[サーバー設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure)で定義されたストレージポリシーとボリュームに関する情報を含みます。

カラム:

- `policy_name` ([String](../../sql-reference/data-types/string.md)) — ストレージポリシーの名前。
- `volume_name` ([String](../../sql-reference/data-types/string.md)) — ストレージポリシーで定義されたボリュームの名前。
- `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 設定内でのボリューム順序番号、この順序に従ってデータがボリュームに記入されます。すなわち、データの挿入とマージ時に優先度の低いボリュームにデータが書き込まれます（他のルール: 有効期限 (TTL)、`max_data_part_size`、`move_factor`も考慮）。
- `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — ストレージポリシーで定義されたディスク名。
- `volume_type` ([Enum8](../../sql-reference/data-types/enum.md))  — ボリュームのタイプ。以下の値のいずれかを持つことができます:
    - `JBOD` 
    - `SINGLE_DISK`
    - `UNKNOWN`
- `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ボリュームディスクに格納可能なデータパートの最大サイズ（0 — 制限なし）。
- `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — 空きディスクスペースの比率。この比率が設定パラメーターの値を超えると、ClickHouseはデータを次のボリュームへ移動し始めます。
- `prefer_not_to_merge` ([UInt8](../../sql-reference/data-types/int-uint.md)) — `prefer_not_to_merge`設定の値。常にfalseであるべきです。この設定が有効の場合、誤りました。
- `perform_ttl_move_on_insert` ([UInt8](../../sql-reference/data-types/int-uint.md)) — `perform_ttl_move_on_insert`設定の値。データパートINSERT時のTTL移動を無効化します。デフォルトでは、TTL移動ルールによってすでに期限切れのデータパートを挿入すると、それは即座に移動ルールで宣言されたボリューム/ディスクに移動します。移動先のボリューム/ディスクが遅い場合（例えばS3）、これが挿入を著しく遅くする可能性があります。
- `load_balancing` ([Enum8](../../sql-reference/data-types/enum.md))  — ディスクバランジングのポリシー。以下の値のいずれかを持つことができます:
    - `ROUND_ROBIN`
    - `LEAST_USED`

ストレージポリシーに複数のボリュームが含まれる場合、各ボリュームの情報はテーブルの個別の行に保存されます。
