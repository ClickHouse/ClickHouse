# GRANT

Grants privileges to a ClickHouse [user account](../operations/settings/settings_profiles.md).

To revoke privileges, use the [REVOKE](revoke.md) statement. Also you can list privileges by the [SHOW GRANTS](show.md#show-grants) statement.

## Syntax

```sql
GRANT privilege TO user [WITH GRANT OPTION]
```

- `privilege` — Type of privilege.
- `user` — ClickHouse user.

The `WITH GRANT OPTION` clause sets `GRANT OPTION` privilege for `user`.


## Usage

To use `GRANT`, your account must have the `GRANT OPTION` privilege. You can grant any privilege inside the scope of your privileges.

For example, administrator has granted privileges to the user `vasia` by the query:

```sql
GRANT SELECT(x,y) ON db.table TO vasya WITH GRANT OPTION
```

It means that `vasya` has the permission to perform the `SELECT x,y FROM db.tabel` query and it can grant other users with the following privileges:
    
- Performing the same queries as `vasya` itself.
- Performing only `SELECT x FROM db.tabel`.
- Performing only `SELECT y FROM db.tabel`.

Also `vasya` has the `GRANT OPTION` privilege, so it can grant other users with this privilege.

## Privileges
