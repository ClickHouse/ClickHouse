---
description: 'Documentación de la sentencia ALTER TABLE ... DELETE'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'Sentencia ALTER TABLE ... DELETE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Elimina los datos que coinciden con la expresión de filtrado especificada. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

:::note
El prefijo `ALTER TABLE` hace que esta sintaxis difiera de la de la mayoría de los demás sistemas compatibles con SQL. Su objetivo es indicar que, a diferencia de consultas similares en bases de datos OLTP, esta es una operación costosa que no está diseñada para usarse con frecuencia. `ALTER TABLE` se considera una operación pesada que requiere que los datos subyacentes se fusionen antes de eliminarse. Para las tablas MergeTree, considere usar la [consulta `DELETE FROM`](/es/sql-reference/statements/delete.md), que realiza una eliminación ligera y puede ser considerablemente más rápida.
:::

`filter_expr` debe ser de tipo `UInt8`. La consulta elimina las filas de la tabla para las que esta expresión toma un valor distinto de cero.

Una consulta puede contener varios comandos separados por comas.

La sincronía del procesamiento de la consulta viene definida por la configuración [mutations&#95;sync](/es/operations/settings/settings.md/#mutations_sync). De forma predeterminada, es asíncrona.

**Véase también**

* [Mutaciones](/es/sql-reference/statements/alter/index.md#mutations)
* [Sincronía de las consultas ALTER](/es/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* configuración [mutations&#95;sync](/es/operations/settings/settings.md/#mutations_sync)

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo gestionar actualizaciones y eliminaciones en ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)