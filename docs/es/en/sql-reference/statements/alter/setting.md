---
description: 'Documentación sobre la modificación de la configuración de tablas'
sidebar_label: 'SETTING'
sidebar_position: 38
slug: /sql-reference/statements/alter/setting
title: 'Modificación de la configuración de tablas'
doc_type: 'reference'
---

Hay varias consultas para cambiar la configuración de las tablas. Puede modificar la configuración o restablecerla a sus valores predeterminados. Una sola consulta puede cambiar varias configuraciones a la vez.
Si no existe una configuración con el nombre especificado, la consulta genera una excepción.

**Sintaxis**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
Estas consultas solo se pueden aplicar a las tablas [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).
:::

<div id="modify-setting">
  ## MODIFY SETTING
</div>

Modifica la configuración de la tabla.

**Sintaxis**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Ejemplo**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

<div id="reset-setting">
  ## RESET SETTING
</div>

Restablece la configuración de la tabla a sus valores predeterminados. Si una configuración ya está en su estado predeterminado, no se realiza ninguna acción.

**Sintaxis**

```sql
RESET SETTING setting_name [, ...]
```

**Ejemplo**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**Véase también**

* [Configuración de MergeTree](../../../operations/settings/merge-tree-settings.md)