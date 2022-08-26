# system.enabled_roles {#system_tables-enabled_roles}

Contains all active roles at the moment, including current role of the current user and granted roles for current role.

Columns:

- `role_name` ([String](../../sql-reference/data-types/string.md))) — Role name.
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a role with `ADMIN OPTION` privilege.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a current role of a current user.
- `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `enabled_role` is a default role.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/enabled-roles) <!--hide-->
