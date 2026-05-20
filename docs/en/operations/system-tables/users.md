---
description: 'System table containing a list of user accounts configured on the server.'
keywords: ['system table', 'users']
slug: /operations/system-tables/users
title: 'system.users'
---

# system.users

Contains a list of [user accounts](../../guides/sre/user-management/index.md#user-account-management) configured on the server.

Columns:
- `name` ([String](../../sql-reference/data-types/string.md)) — User name.

- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — User ID.

- `storage` ([String](../../sql-reference/data-types/string.md)) — Path to the storage of users. Configured in the `access_control_path` parameter.

- `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)('no_password' = 0, 'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3, 'ldap' = 4, 'kerberos' = 5, 'ssl_certificate' = 6, 'bcrypt_password' = 7)) — Shows the authentication type. There are multiple ways of user identification: with no password, with plain text password, with [SHA256](https://en.wikipedia.org/wiki/SHA-2)-encoded password, with [double SHA-1](https://en.wikipedia.org/wiki/SHA-1)-encoded password or with [bcrypt](https://en.wikipedia.org/wiki/Bcrypt)-encoded password.

- `auth_params` ([String](../../sql-reference/data-types/string.md)) — Authentication parameters in the JSON format depending on the `auth_type`.

- `host_ip` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — IP addresses of hosts that are allowed to connect to the ClickHouse server.

- `host_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Names of hosts that are allowed to connect to the ClickHouse server.

- `host_names_regexp` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Regular expression for host names that are allowed to connect to the ClickHouse server.

- `host_names_like` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Names of hosts that are allowed to connect to the ClickHouse server, set using the LIKE predicate.

- `default_roles_all` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — Shows that all granted roles set for user by default.

- `default_roles_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of granted roles provided by default.

- `default_roles_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — All the granted roles set as default excepting of the listed ones.

## See Also {#see-also}

- [SHOW USERS](/sql-reference/statements/show#show-users)
