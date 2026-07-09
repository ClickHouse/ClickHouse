---
description: 'Documentación de la sentencia EXECUTE AS'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'Sentencia EXECUTE AS'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # Sentencia EXECUTE AS
</div>

Permite ejecutar consultas en nombre de otro usuario.

<div id="syntax">
  ## Sintaxis
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

La primera forma (sin `subquery`) establece que todas las consultas posteriores de la sesión actual se ejecutarán en nombre del `target_user` especificado.

La segunda forma (con `subquery`) ejecuta únicamente la `subquery` especificada en nombre del `target_user` especificado.

Para que ambas formas funcionen, es necesario que la configuración `access_control_improvements.allow_impersonate_user`
esté establecida en `1` y que se haya concedido el privilegio `IMPERSONATE`. Por ejemplo, los siguientes comandos

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

permitir que el usuario `user2` ejecute comandos `EXECUTE AS user1 ...` y también que el usuario `user3` ejecute comandos como cualquier usuario.

Mientras se suplanta a otro usuario, la función [currentUser()](/es/sql-reference/functions/other-functions#currentUser) devuelve el nombre de ese otro usuario,
y la función [authenticatedUser()](/es/sql-reference/functions/other-functions#authenticatedUser) devuelve el nombre del usuario que realmente se ha autenticado.

<div id="examples">
  ## Ejemplos
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```