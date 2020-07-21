#system.current_roles {#system_tables-current_roles}
Contains active roles of a current user. `SET ROLE` changes the contents of this table.

Columns:

 - `role_name` ([String](../../sql-reference/data-types/string.md))) — Role name.
 - `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `current_role` is a role with `ADMIN OPTION` privilege.
 - `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Flag that shows whether `current_role` is a default role.

 [Original article](https://clickhouse.tech/docs/en/operations/system_tables/current-roles) <!--hide-->