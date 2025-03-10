---
slug: /ja/operations/system-tables/settings_profiles
---
# settings_profiles

設定プロファイルのプロパティを含みます。

カラム:
- `name` ([String](../../sql-reference/data-types/string.md)) — 設定プロファイル名。

- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 設定プロファイルID。

- `storage` ([String](../../sql-reference/data-types/string.md)) — 設定プロファイルの保存場所へのパス。`access_control_path` パラメータで設定されます。

- `num_elements` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `system.settings_profile_elements` テーブル内のこのプロファイルに対する要素の数。

- `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — すべてのロールおよび/またはユーザーに設定プロファイルが適用されることを示します。

- `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 設定プロファイルが適用されるロールおよび/またはユーザーのリスト。

- `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — このリストに記載されているものを除くすべてのロールおよび/またはユーザーに設定プロファイルが適用されます。

## 参照 {#see-also}

- [SHOW PROFILES](../../sql-reference/statements/show.md#show-profiles-statement)
