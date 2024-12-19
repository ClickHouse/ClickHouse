---
slug: /ja/operations/system-tables/row_policies
---
# row_policies

特定のテーブルに対するフィルターと、この行ポリシーを使用するべき役割やユーザーのリストを含んでいます。

カラム:
- `name` ([String](../../sql-reference/data-types/string.md)) — 行ポリシーの名前。

- `short_name` ([String](../../sql-reference/data-types/string.md)) — 行ポリシーの短縮名。行ポリシーの名前は複合的であり、例えば: myfilter ON mydb.mytable。ここでの「myfilter ON mydb.mytable」が行ポリシーの名前であり、「myfilter」がその短縮名です。

- `database` ([String](../../sql-reference/data-types/string.md)) — データベース名。

- `table` ([String](../../sql-reference/data-types/string.md)) — テーブル名。データベースに対するポリシーの場合は空。

- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 行ポリシーID。

- `storage` ([String](../../sql-reference/data-types/string.md)) — 行ポリシーが保存されているディレクトリの名前。

- `select_filter` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 行をフィルターするために使用される条件。

- `is_restrictive` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 行ポリシーが行へのアクセスを制限するかを示します。[CREATE ROW POLICY](../../sql-reference/statements/create/row-policy.md#create-row-policy-as)を参照してください。値:
  - `0` — 行ポリシーが `AS PERMISSIVE` 句で定義されている。
  - `1` — 行ポリシーが `AS RESTRICTIVE` 句で定義されている。

- `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — すべての役割および/またはユーザーに設定された行ポリシーを示します。

- `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 行ポリシーが適用される役割および/またはユーザーのリスト。

- `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 記載されている役割および/またはユーザーを除き、行ポリシーが適用されているすべての役割および/またはユーザー。

## 参照 {#see-also}

- [SHOW POLICIES](../../sql-reference/statements/show.md#show-policies-statement)
