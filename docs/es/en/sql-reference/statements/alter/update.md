---
description: 'Documentación de las sentencias ALTER TABLE ... UPDATE'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'Sentencias ALTER TABLE ... UPDATE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

Manipula los datos que coinciden con la expresión de filtrado especificada. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

:::note
El prefijo `ALTER TABLE` hace que esta sintaxis sea diferente de la de la mayoría de los demás sistemas compatibles con SQL. Su objetivo es indicar que, a diferencia de consultas similares en bases de datos OLTP, esta es una operación costosa que no está pensada para usarse con frecuencia.
:::

`filter_expr` debe ser de tipo `UInt8`. Esta consulta actualiza los valores de las columnas especificadas con los valores de las expresiones correspondientes en las filas para las que `filter_expr` toma un valor distinto de cero. Los valores se convierten al tipo de la columna mediante el operador `CAST`. No se admite la actualización de columnas que se utilizan en el cálculo de la clave primaria o de la clave de partición.

Una consulta puede contener varios comandos separados por comas.

El carácter síncrono del procesamiento de la consulta lo define la configuración [mutations&#95;sync](/es/operations/settings/settings.md/#mutations_sync). De forma predeterminada, es asíncrono.

**Véase también**

* [Mutations](/es/sql-reference/statements/alter/index.md#mutations)
* [Sincronía de las consultas ALTER](/es/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* Configuración [mutations&#95;sync](/es/operations/settings/settings.md/#mutations_sync)
* [Actualización ligera `UPDATE`](/es/sql-reference/statements/update) - Alternativa de actualización ligera que usa partes de parche
* [`APPLY PATCHES`](/es/sql-reference/statements/alter/apply-patches) - Aplicar manualmente parches de actualizaciones ligeras

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo gestionar actualizaciones y eliminaciones en ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)