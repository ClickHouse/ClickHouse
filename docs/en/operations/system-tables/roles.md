# roles {#system_tables-roles}

Contains information about configured [roles](../../operations/access-rights.md#role-management).

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Role name.
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Role ID.
- `storage` ([String](../../sql-reference/data-types/string.md)) — Path to the storage of roles. Configured in the `access_control_path` parameter.

## See Also {#see-also}

-   [SHOW ROLES](../../sql-reference/statements/show.md#show-roles-statement)

[Original article](https://clickhouse.com/docs/en/operations/system-tables/roles) <!--hide-->
