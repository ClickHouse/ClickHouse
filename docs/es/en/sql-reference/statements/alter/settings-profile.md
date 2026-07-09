---
description: 'Documentación de SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'referencia'
---

Modifica los perfiles de configuración.

Sintaxis:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

La cláusula `ON CLUSTER` permite modificar perfiles de configuración en un clúster; consulta [DDL distribuido](../../../sql-reference/distributed-ddl.md).

<div id="replacing-vs-modifying">
  ## Reemplazar o modificar la configuración
</div>

`ALTER SETTINGS PROFILE` admite dos formas distintas de cambiar la configuración y los perfiles padre (heredados) de un perfil. Su funcionamiento es muy diferente, por lo que es importante elegir la correcta.

<div id="replacing-form">
  ### Forma de reemplazo: `SETTINGS` / `INHERIT` simple
</div>

Una cláusula `SETTINGS` simple (sin `ADD`, `MODIFY` ni `DROP`) **reemplaza por completo la lista de settings y todos los perfiles heredados** del perfil por exactamente lo que indiques. Todo lo que estuviera presente antes pero no figure en la lista se descarta sin previo aviso; no hay ninguna advertencia.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
Como la forma simple `SETTINGS` reemplaza todo por completo, usarla para &quot;sobrescribir una configuración&quot; sobre un perfil base ya poblado eliminará todas las demás configuraciones (y todos los perfiles de los que hereda) de ese perfil. Si solo quieres cambiar una configuración y conservar el resto, usa la forma incremental `MODIFY`/`ADD`/`DROP` que se describe a continuación.
:::

Este es el mismo comportamiento que `SETTINGS` en [`CREATE SETTINGS PROFILE`](../create/settings-profile.md): la cláusula define la lista completa de configuraciones.

<div id="incremental-form">
  ### Forma incremental: `ADD` / `MODIFY` / `DROP`
</div>

Las palabras clave `ADD`, `MODIFY` y `DROP` cambian entradas individuales sin modificar el resto del perfil:

* `ADD SETTINGS variable = value [constraints]` — agrega una configuración que todavía no está presente.
* `MODIFY SETTINGS variable = value [constraints]` — reemplaza la entrada de una sola configuración. Se sobrescribe la entrada completa (valor y restricciones), así que debe volver a especificar `MIN`/`MAX`/`READONLY`/etc. si quiere conservarlos.
* `DROP SETTINGS variable [,...]` — elimina las configuraciones indicadas.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — agregan o eliminan perfiles padre (heredados).
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — eliminan todas las configuraciones o todos los perfiles padre.

Varias de estas cláusulas pueden combinarse en una sola sentencia, por ejemplo, `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` es un alias de `MODIFY SETTINGS variable = value`. Se ofrece porque `SET` resulta natural y porque escribir la cláusula `SETTINGS` de reemplazo cuando en realidad se pretendía hacer un cambio incremental es un error habitual.

<div id="examples">
  ## Ejemplos
</div>

Sobrescriba un solo ajuste y conserve el resto de un perfil ya configurado:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

Añada un nuevo ajuste restringido y elimine otro:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

Gestione los perfiles padre de forma incremental:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

Compruebe siempre el resultado con [`SHOW CREATE SETTINGS PROFILE`](../show.md):

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## Incremental vs reemplazo completo
</div>

:::warning
Una cláusula `SETTINGS` sin modificadores **elimina todos los ajustes existentes y todos los perfiles heredados (padre)** del perfil antes de aplicar los nuevos.
:::

Para cambiar un solo ajuste y conservar el resto, usa `ADD SETTINGS` o `MODIFY SETTINGS` (consulta los ejemplos a continuación).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

Tanto `ADD SETTINGS` como `MODIFY SETTINGS` conservan los demás ajustes del perfil, pero tratan de forma distinta una entrada existente para el *mismo* ajuste:

* `ADD SETTINGS variable = value ...` primero elimina cualquier entrada existente para `variable` y luego inserta la nueva. Por tanto, **reemplaza el valor junto con todas las restricciones** de ese ajuste. Cualquier `MIN`, `MAX` o propiedad de escritura (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) definida previamente para `variable` que no repitas se descarta.
* `MODIFY SETTINGS variable = value ...` **combina campo por campo**: sobrescribe solo los campos que realmente especifiques (el valor, `MIN`, `MAX` o la propiedad de escritura) y conserva los demás campos de ese ajuste tal como estaban.

:::tip
En resumen, usa `MODIFY SETTINGS` cuando solo quieras ajustar un aspecto de un ajuste (por ejemplo, solo el valor, conservando un `MAX` existente); usa `ADD SETTINGS` cuando quieras redefinir un ajuste desde cero.
:::

<div id="examples">
  ## Ejemplos
</div>

Cree un perfil para usarlo en los ejemplos siguientes:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

Añada o cambie una sola configuración sin alterar las demás:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Como `MODIFY` fusiona campo por campo, cambiar solo el valor de una configuración conserva sus restricciones existentes:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

Añade una configuración (sin eliminar las demás), redefiniéndola por completo si ya existe:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

A diferencia de `MODIFY`, si se vuelve a ejecutar `ADD` solo con un valor, se eliminan las restricciones definidas previamente para ese ajuste:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

Elimina una o más configuraciones especificadas por nombre:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Elimine todos los ajustes de una vez:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### Trabajar con perfiles heredados
</div>

Agregue o elimine perfiles padre (heredados) sin afectar la configuración del propio perfil:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```