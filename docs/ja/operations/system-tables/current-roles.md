---
slug: /ja/operations/system-tables/current-roles
---
# current_roles

このテーブルには、現在のユーザーのアクティブなロールが含まれています。`SET ROLE` により、このテーブルの内容が変更されます。

カラム:

 - `role_name` ([String](../../sql-reference/data-types/string.md)) — ロール名。
 - `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `current_role` が `ADMIN OPTION` 権限を持つロールかどうかを示すフラグ。
 - `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `current_role` がデフォルトのロールかどうかを示すフラグ。
