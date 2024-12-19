---
slug: /ja/operations/system-tables/enabled-roles
---
# enabled_roles

現在アクティブなすべてのロールを含み、現在のユーザーの現在のロールやそのロールに対して付与されたロールも含まれます。

カラム:

- `role_name` ([String](../../sql-reference/data-types/string.md)) — ロール名。
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `enabled_role` が `ADMIN OPTION` 特権を持つロールかどうかを示すフラグ。
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `enabled_role` が現在のユーザーの現在のロールかどうかを示すフラグ。
- `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `enabled_role` がデフォルトのロールかどうかを示すフラグ。
