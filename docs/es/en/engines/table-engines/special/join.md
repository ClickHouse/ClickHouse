---
description: 'Estructura de datos preparada opcional para usar en operaciones JOIN.'
sidebar_label: 'Join'
sidebar_position: 70
slug: /engines/table-engines/special/join
title: 'Motor de tabla Join'
doc_type: 'reference'
---

Estructura de datos preparada opcional para usar en operaciones [JOIN](/es/sql-reference/statements/select/join).

:::note
En ClickHouse Cloud, si el servicio se creó con una versión anterior a la 25.4, deberá establecer la compatibilidad en al menos 25.4 mediante `SET compatibility=25.4`.
:::

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

Consulte la descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

<div id="engine-parameters">
  ## Parámetros del motor
</div>

<div id="join_strictness">
  ### `join_strictness`
</div>

`join_strictness` – [criterio de coincidencia de JOIN](/es/sql-reference/statements/select/join#supported-types-of-join).

<div id="join_type">
  ### `join_type`
</div>

`join_type` – [tipo de JOIN](/es/sql-reference/statements/select/join#supported-types-of-join).

<div id="key-columns">
  ### Columnas clave
</div>

`k1[, k2, ...]` – Columnas clave de la cláusula `USING` con las que se realiza la operación `JOIN`.

Especifique los parámetros `join_strictness` y `join_type` sin comillas; por ejemplo, `Join(ANY, LEFT, col1)`. Deben coincidir con la operación `JOIN` para la que se va a usar la tabla. Si los parámetros no coinciden, ClickHouse no lanza ninguna excepción y puede devolver datos incorrectos.

<div id="specifics-and-recommendations">
  ## Particularidades y recomendaciones
</div>

<div id="data-storage">
  ### Almacenamiento de datos
</div>

Los datos de la tabla `Join` siempre están en la RAM. Al insertar filas en una tabla, ClickHouse escribe bloques de datos en el directorio del disco para poder restaurarlos cuando el servidor se reinicie.

Si el servidor se reinicia incorrectamente, el bloque de datos del disco puede perderse o dañarse. En ese caso, puede que sea necesario eliminar manualmente el archivo con los datos dañados.

<div id="selecting-and-inserting-data">
  ### Selección e inserción de datos
</div>

Puede usar consultas `INSERT` para agregar datos a las tablas con motor `Join`. Si la tabla se creó con strictness `ANY`, se omiten los datos con claves duplicadas. Con strictness `ALL`, se agregan todas las filas.

Los principales casos de uso de las tablas con motor `Join` son los siguientes:

* Coloque la tabla en el lado derecho de una cláusula `JOIN`.
* Llame a la función [joinGet](/es/sql-reference/functions/other-functions.md/#joinGet), que le permite extraer datos de la tabla del mismo modo que de un diccionario.

<div id="deleting-data">
  ### Eliminación de datos
</div>

Las consultas `ALTER DELETE` para las tablas con motor `Join` se implementan como [mutaciones](/es/sql-reference/statements/alter/index.md#mutations). La mutación `DELETE` lee los datos filtrados y sobrescribe los datos en memoria y en disco.

<div id="join-limitations-and-settings">
  ### Limitaciones y ajustes
</div>

Al crear una tabla, se aplican los siguientes ajustes:

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

[join&#95;use&#95;nulls](/es/operations/settings/settings.md/#join_use_nulls)

<div id="max_rows_in_join">
  #### `max_rows_in_join`
</div>

[max&#95;rows&#95;in&#95;join](/es/operations/settings/settings#max_rows_in_join)

<div id="max_bytes_in_join">
  #### `max_bytes_in_join`
</div>

[max&#95;bytes&#95;in&#95;join](/es/operations/settings/settings#max_bytes_in_join)

<div id="join_overflow_mode">
  #### `join_overflow_mode`
</div>

[join&#95;overflow&#95;mode](/es/operations/settings/settings#join_overflow_mode)

<div id="join_any_take_last_row">
  #### `join_any_take_last_row`
</div>

[join&#95;any&#95;take&#95;last&#95;row](/es/operations/settings/settings.md/#join_any_take_last_row)

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

<div id="persistent">
  #### Persistencia
</div>

Deshabilita la persistencia para los motores de tabla Join y [Set](/es/engines/table-engines/special/set.md).

Reduce la sobrecarga de E/S. Es adecuado para escenarios que priorizan el rendimiento y no requieren persistencia.

Valores posibles:

* 1 — Habilitado.
* 0 — Deshabilitado.

Valor predeterminado: `1`.

Las tablas con motor `Join` no pueden usarse en operaciones `GLOBAL JOIN`.

El motor `Join` permite especificar la opción [join&#95;use&#95;nulls](/es/operations/settings/settings.md/#join_use_nulls) en la sentencia `CREATE TABLE`. La consulta [SELECT](/es/sql-reference/statements/select/index.md) debe tener el mismo valor de `join_use_nulls`.

<div id="example">
  ## Ejemplos de uso
</div>

Creación de la tabla de la izquierda:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

Creación de la tabla `Join` de la derecha:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

Unión de las tablas:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

Como alternativa, puede obtener datos de la tabla `Join` especificando el valor de la clave de unión:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

Eliminar una fila de la tabla `Join`:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```