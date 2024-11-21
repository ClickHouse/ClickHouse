---
slug: /ja/operations/system-tables/grants
---
# grants

ClickHouseユーザーアカウントに付与された特権。

カラム:
- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — ユーザー名。

- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — ユーザーアカウントに割り当てられたロール。

- `access_type` ([Enum8](../../sql-reference/data-types/enum.md)) — ClickHouseユーザーアカウントのアクセスパラメーター。

- `database` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — データベースの名前。

- `table` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — テーブルの名前。

- `column` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — アクセスが許可されたカラムの名前。

- `is_partial_revoke` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 論理値。いくつかの特権が取り消されたかどうかを示します。可能な値:
- `0` — 行は付与を示します。
- `1` — 行は部分的な取り消しを示します。

- `grant_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `WITH GRANT OPTION`とともに許可が付与されていることを示します。[GRANT](../../sql-reference/statements/grant.md#granting-privilege-syntax)を参照してください。
