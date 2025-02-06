---
slug: /ja/operations/system-tables/role-grants
---
# role_grants

ユーザーとロールのためのロール付与情報を含みます。このテーブルにエントリを追加するには、`GRANT role TO user`を使用します。

カラム:

- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — ユーザー名。

- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — ロール名。

- `granted_role_name` ([String](../../sql-reference/data-types/string.md)) — `role_name`ロールに付与されたロールの名前。あるロールに別のロールを付与するには、`GRANT role1 TO role2`を使用します。

- `granted_role_is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `granted_role`がデフォルトロールであるかを示すフラグ。可能な値:
    - 1 — `granted_role`はデフォルトロールです。
    - 0 — `granted_role`はデフォルトロールではありません。

- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `granted_role`が[ADMIN OPTION](../../sql-reference/statements/grant.md#admin-option-privilege)特権を持つロールかを示すフラグ。可能な値:
    - 1 — ロールは`ADMIN OPTION`特権を持ちます。
    - 0 — ロールは`ADMIN OPTION`特権を持ちません。
