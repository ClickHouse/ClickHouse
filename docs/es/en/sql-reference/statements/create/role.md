---
description: 'Documentación de rol'
sidebar_label: 'ROLE'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

Crea nuevos [roles](../../../guides/sre/user-management/index.md#role-management). Un rol es un conjunto de [privilegios](/es/sql-reference/statements/grant#granting-privilege-syntax). Un [usuario](../../../sql-reference/statements/create/user.md) al que se le asigna un rol obtiene todos los privilegios de dicho rol.

Sintaxis:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## Gestión de roles
</div>

A un usuario se le pueden asignar varios roles. Los usuarios pueden aplicar sus roles asignados en combinaciones arbitrarias mediante la sentencia [SET ROLE](../../../sql-reference/statements/set-role.md). El alcance final de los privilegios es la combinación de todos los privilegios de todos los roles aplicados. Si un usuario tiene privilegios otorgados directamente a su cuenta de usuario, estos también se combinan con los privilegios otorgados por los roles.

Un usuario puede tener roles predeterminados que se aplican al iniciar sesión. Para establecer roles predeterminados, use la sentencia [SET DEFAULT ROLE](/es/sql-reference/statements/set-role#set-default-role) o la sentencia [ALTER USER](/es/sql-reference/statements/alter/user).

Para revocar un rol, use la sentencia [REVOKE](../../../sql-reference/statements/revoke.md).

Para eliminar un rol, use la sentencia [DROP ROLE](/es/sql-reference/statements/drop#drop-role). El rol eliminado se revoca automáticamente a todos los usuarios y roles a los que se había asignado.

<div id="examples">
  ## Ejemplos
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Esta secuencia de consultas crea el rol `accountant`, que tiene el privilegio de leer datos de la base de datos `db`.

Asignación del rol al usuario `mira`:

```sql
GRANT accountant TO mira;
```

Una vez asignado el rol, el usuario puede activarlo y ejecutar las consultas permitidas. Por ejemplo:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```