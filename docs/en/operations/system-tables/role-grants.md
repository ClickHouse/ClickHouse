# role_grants 

Contains the role grants for users and roles. To add entries to this table, use `GRANT role TO user`.

Columns:

- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — User name.

- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Role name.

- `granted_role_name` ([String](../../sql-reference/data-types/string.md)) — Name of role granted to the `role_name` role. To grant one role to another one use `GRANT role1 TO role2`.

- `granted_role_is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `granted_role` is a default role. Possible values:
    -   1 — `granted_role` is a default role.
    -   0 — `granted_role` is not a default role.

- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `granted_role` is a role with [ADMIN OPTION](../../sql-reference/statements/grant.md#admin-option-privilege) privilege. Possible values:
    -   1 — The role has `ADMIN OPTION` privilege.
    -   0 — The role without `ADMIN OPTION` privilege.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/role-grants) <!--hide-->
