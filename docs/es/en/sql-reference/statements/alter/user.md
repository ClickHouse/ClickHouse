---
description: 'Documentación sobre USER'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'reference'
---

Modifica las cuentas de usuario de ClickHouse.

Sintaxis:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

Para usar `ALTER USER`, debe tener el privilegio [ALTER USER](../../../sql-reference/statements/grant.md#access-management).

`SET variable = value` es un alias de `MODIFY SETTING variable = value`: modifica una sola configuración sin afectar las demás. Es preferible usarlo (o `MODIFY SETTING`) en lugar de la cláusula `SETTINGS` por sí sola, ya que reemplaza toda la lista de configuraciones y también elimina todos los perfiles heredados.

<div id="grantees-clause">
  ## Cláusula GRANTEES
</div>

Especifica los usuarios o roles que pueden recibir [privilegios](../../../sql-reference/statements/grant.md#privileges) de este usuario, siempre que este usuario también tenga concedidos todos los permisos de acceso necesarios con [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). Opciones de la cláusula `GRANTEES`:

* `user` — Especifica un usuario al que este usuario puede conceder privilegios.
* `role` — Especifica un rol al que este usuario puede conceder privilegios.
* `ANY` — Este usuario puede conceder privilegios a cualquiera. Es la configuración predeterminada.
* `NONE` — Este usuario no puede conceder privilegios a nadie.

Puede excluir cualquier usuario o rol mediante la expresión `EXCEPT`. Por ejemplo, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. Esto significa que, si a `user1` se le han concedido algunos privilegios con `GRANT OPTION`, podrá conceder esos privilegios a cualquiera excepto a `user2`.

<div id="examples">
  ## Ejemplos
</div>

Establecer los roles asignados como predeterminados:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

Si a un usuario no se le han asignado roles previamente, ClickHouse lanza una excepción.

Establezca todos los roles asignados como predeterminados:

```sql
ALTER USER user DEFAULT ROLE ALL
```

Si en el futuro se asigna un rol a un usuario, pasará a ser el rol predeterminado automáticamente.

Establezca como predeterminados todos los roles asignados, excepto `role1` y `role2`:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

Permite al usuario de la cuenta `john` otorgar sus privilegios al usuario de la cuenta `jack`:

```sql
ALTER USER john GRANTEES jack;
```

Agrega nuevos métodos de autenticación al usuario, manteniendo los existentes:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Notas:

1. Es posible que las versiones anteriores de ClickHouse no admitan la sintaxis de varios métodos de autenticación. Por lo tanto, si el servidor de ClickHouse contiene esos usuarios y se degrada a una versión que no la admite, esos usuarios dejarán de ser utilizables y algunas operaciones relacionadas con los usuarios dejarán de funcionar. Para realizar la reversión de versión sin problemas, es necesario configurar todos los usuarios para que tengan un único método de autenticación antes de degradar la versión. Como alternativa, si el servidor se degradó sin seguir el procedimiento adecuado, se deben eliminar los usuarios defectuosos.
2. `no_password` no puede coexistir con otros métodos de autenticación por motivos de seguridad.
   Por ello, no es posible `ADD` un método de autenticación `no_password`. La consulta siguiente generará un error:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

Si desea eliminar los métodos de autenticación de un usuario y usar `no_password`, debe especificarlo mediante el siguiente formato de reemplazo.

Restablece los métodos de autenticación y añade los especificados en la consulta (efecto de un IDENTIFIED inicial sin la palabra clave ADD):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

Restablezca los métodos de autenticación y conserve el último añadido:

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## Cláusula VALID UNTIL
</div>

Permite especificar la fecha de vencimiento y, opcionalmente, la hora de un método de autenticación. Acepta una cadena como parámetro. Se recomienda usar el formato `YYYY-MM-DD [hh:mm:ss] [timezone]` para la fecha y hora. De forma predeterminada, este parámetro es `'infinity'`.
La cláusula `VALID UNTIL` solo puede especificarse junto con un método de autenticación, excepto en el caso de que no se haya especificado ningún método de autenticación en la consulta. En este escenario, la cláusula `VALID UNTIL` se aplicará a todos los métodos de autenticación existentes.

Ejemplos:

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`