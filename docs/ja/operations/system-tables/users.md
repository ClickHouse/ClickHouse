---
slug: /ja/operations/system-tables/users
---
# users

サーバーで設定されている[ユーザーアカウント](../../guides/sre/user-management/index.md#user-account-management)の一覧を含みます。

カラム:
- `name` ([String](../../sql-reference/data-types/string.md)) — ユーザー名。

- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ユーザー ID。

- `storage` ([String](../../sql-reference/data-types/string.md)) — ユーザーのストレージへのパス。`access_control_path` パラメーターで設定されます。

- `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)('no_password' = 0, 'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3, 'ldap' = 4, 'kerberos' = 5, 'ssl_certificate' = 6, 'bcrypt_password' = 7)) — 認証タイプを示します。ユーザー認証には、パスワードなし、プレーンテキストパスワード、[SHA256](https://en.wikipedia.org/wiki/SHA-2) エンコードされたパスワード、[ダブルSHA-1](https://en.wikipedia.org/wiki/SHA-1) エンコードされたパスワード、または [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) エンコードされたパスワードの方法があります。

- `auth_params` ([String](../../sql-reference/data-types/string.md)) — `auth_type` に応じたJSON形式の認証パラメーター。

- `host_ip` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — ClickHouseサーバーに接続することを許可されているホストのIPアドレス。

- `host_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — ClickHouseサーバーに接続することを許可されているホストの名前。

- `host_names_regexp` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — ClickHouseサーバーに接続することを許可されているホスト名の正規表現。

- `host_names_like` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — LIKE述語を使って設定された、ClickHouseサーバーへの接続が許可されているホストの名前。

- `default_roles_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — デフォルトでユーザーに設定されたすべての付与されたロールを示します。

- `default_roles_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — デフォルトで提供される付与されたロールのリスト。

- `default_roles_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — デフォルトで設定された付与されたすべてのロール。ただし、指定されたものを除きます。

## See Also {#see-also}

- [SHOW USERS](../../sql-reference/statements/show.md#show-users-statement)
