---
slug: /ja/operations/system-tables/graphite_retentions
---
# graphite_retentions

[graphite_rollup](../../operations/server-configuration-parameters/settings.md#graphite)パラメータの情報を含んでおり、[\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md)エンジンを使用したテーブルで利用されます。

カラム:

- `config_name` (String) - `graphite_rollup`パラメータ名。
- `regexp` (String) - メトリック名のパターン。
- `function` (String) - 集約関数の名前。
- `age` (UInt64) - データが存在する最低年齢（秒単位）。
- `precision` (UInt64) - データの年齢を秒単位でどれだけ正確に定義するか。
- `priority` (UInt16) - パターンの優先度。
- `is_default` (UInt8) - パターンがデフォルトであるかどうか。
- `Tables.database` (Array(String)) - `config_name`パラメータを使用するデータベーステーブルの名前の配列。
- `Tables.table` (Array(String)) - `config_name`パラメータを使用するテーブル名の配列。
