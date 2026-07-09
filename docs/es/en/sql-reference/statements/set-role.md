---
description: 'Documentación de SET ROLE'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'Sentencia SET ROLE'
doc_type: 'referencia'
---

Activa los roles del usuario actual.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

Establece los roles predeterminados para un usuario.

Los roles predeterminados se activan automáticamente al iniciar sesión. Solo se pueden establecer como predeterminados los roles concedidos previamente. Si el rol no se ha concedido a un usuario, ClickHouse lanza una excepción.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## Ejemplos
</div>

Asigne varios roles predeterminados a un usuario:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Establezca como predeterminados para un usuario todos los roles concedidos:

```sql
SET DEFAULT ROLE ALL TO user
```

Quitar los roles predeterminados de un usuario:

```sql
SET DEFAULT ROLE NONE TO user
```

Establece como predeterminados todos los roles concedidos, excepto los roles específicos `role1` y `role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```