---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: REVOKE
---

# REVOKE {#revoke}

Revoca privilegios de usuarios o roles.

## Sintaxis {#revoke-syntax}

**Revocación de privilegios de usuarios**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Revocación de roles de usuarios**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Descripci {#revoke-description}

Para revocar algún privilegio, puede usar un privilegio de alcance más amplio que planea revocar. Por ejemplo, si un usuario tiene `SELECT (x,y)` privilegio, administrador puede realizar `REVOKE SELECT(x,y) ...`, o `REVOKE SELECT * ...` o incluso `REVOKE ALL PRIVILEGES ...` consulta para revocar este privilegio.

### Revocaciones parciales {#partial-revokes-dscr}

Puede revocar una parte de un privilegio. Por ejemplo, si un usuario tiene `SELECT *.*` privilegio puede revocarle un privilegio para leer datos de alguna tabla o una base de datos.

## Ejemplos {#revoke-example}

Otorgue el `john` cuenta de usuario con un privilegio para seleccionar de todas las bases de datos `accounts` una:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Otorgue el `mira` cuenta de usuario con un privilegio para seleccionar entre todas las columnas `accounts.staff` excepción de la tabla `wage` una.

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

{## [Artículo Original](https://clickhouse.tech/docs/en/operations/settings/settings/) ##}
