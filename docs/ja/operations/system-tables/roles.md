---
slug: /ja/operations/system-tables/roles
---
# roles

設定された[ロール](../../guides/sre/user-management/index.md#role-management)に関する情報を含みます。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — ロール名。
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ロールID。
- `storage` ([String](../../sql-reference/data-types/string.md)) — ロールのストレージへのパス。`access_control_path` パラメータで設定されています。

## 参照 {#see-also}

- [SHOW ROLES](../../sql-reference/statements/show.md#show-roles-statement)
