---
description: 'Documentación de PARTITION'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'Gestión de particiones y partes'
doc_type: 'reference'
---

Están disponibles las siguientes operaciones con [particiones](/es/engines/table-engines/mergetree-family/custom-partitioning-key.md):

* [DETACH PARTITION|PART](#detach-partitionpart) — Mueve una partición o parte al directorio `detached` y deja de registrarla.
* [DROP PARTITION|PART](#drop-partitionpart) — Elimina una partición o parte.
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - Elimina una parte o todas las partes de una partición de `detached`.
* [FORGET PARTITION](#forget-partition) — Elimina los metadatos de una partición de ZooKeeper si está vacía.
* [ATTACH PARTITION|PART](#attach-partitionpart) — Añade a la tabla una partición o parte desde el directorio `detached`.
* [ATTACH PARTITION FROM](#attach-partition-from) — Copia la partición de datos de una tabla a otra y la añade.
* [REPLACE PARTITION](#replace-partition) — Copia la partición de datos de una tabla a otra y la reemplaza.
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — Mueve la partición de datos de una tabla a otra.
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — Restablece el valor de una columna especificada en una partición.
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — Restablece el índice secundario especificado en una partición.
* [FREEZE PARTITION](#freeze-partition) — Crea una copia de seguridad de una partición.
* [UNFREEZE PARTITION](#unfreeze-partition) — Elimina una copia de seguridad de una partición.
* [FETCH PARTITION|PART](#fetch-partitionpart) — Descarga una parte o partición desde otro servidor.
* [MOVE PARTITION|PART](#move-partitionpart) — Mueve una partición o parte de datos a otro disco o volumen.
* [UPDATE IN PARTITION](#update-in-partition) — Actualiza los datos dentro de la partición según una condición.
* [DELETE IN PARTITION](#delete-in-partition) — Elimina los datos dentro de la partición según una condición.
* [REWRITE PARTS](#rewrite-parts) — Reescribe por completo las partes de la tabla (o de una partición específica).

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Mueve todos los datos de la partición especificada al directorio `detached`. El servidor deja de reconocer la partición de datos detached como si no existiera. El servidor no reconocerá estos datos hasta que ejecute la consulta [ATTACH](#attach-partitionpart).

Ejemplo:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Consulte cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

Una vez ejecutada la consulta, puede hacer lo que quiera con los datos del directorio `detached`: eliminarlos del sistema de archivos o simplemente dejarlos.

Esta consulta se replica: mueve los datos al directorio `detached` en todas las réplicas. Tenga en cuenta que solo puede ejecutar esta consulta en una réplica líder. Para averiguar si una réplica es líder, ejecute la consulta `SELECT` sobre la tabla [system.replicas](/es/operations/system-tables/replicas). Como alternativa, es más fácil ejecutar una consulta `DETACH` en todas las réplicas: todas las réplicas lanzan una excepción, excepto las réplicas líderes (ya que se permiten varios líderes).

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Elimina la partición especificada de la tabla. Esta consulta marca la partición como inactiva y elimina los datos por completo en aproximadamente 10 minutos.

Consulte cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

La consulta se replica: elimina los datos de todas las réplicas.

Ejemplo:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

Elimina la parte especificada o todas las partes de la partición especificada de `detached`.
Obtenga más información sobre cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

Elimina todos los metadatos de una partición vacía en ZooKeeper. La consulta falla si la partición no está vacía o no existe. Asegúrese de ejecutar esto solo para particiones que nunca volverán a usarse.

Lea cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

Ejemplo:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Añade datos a la tabla desde el directorio `detached`. Es posible añadir datos de una partición completa o de una parte por separado. Ejemplos:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Obtenga más información sobre cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

Esta consulta está replicada. La réplica iniciadora comprueba si hay datos en el directorio `detached`.
Si existen datos, la consulta comprueba su integridad. Si todo es correcto, la consulta agrega los datos a la tabla.

Si la réplica no iniciadora, al recibir el comando ATTACH, encuentra la parte con las sumas de comprobación correctas en su propio directorio `detached`, adjunta los datos sin obtenerlos de otras réplicas.
Si no hay ninguna parte con las sumas de comprobación correctas, los datos se descargan desde cualquier réplica que tenga la parte.

Puede colocar datos en el directorio `detached` de una réplica y usar la consulta `ALTER ... ATTACH` para agregarlos a la tabla en todas las réplicas.

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

Esta consulta copia la partición de datos de `table1` a `table2`.

Tenga en cuenta que:

* Los datos no se eliminarán ni de `table1` ni de `table2`.
* `table1` puede ser una tabla temporal.

Para que la consulta se ejecute correctamente, deben cumplirse las siguientes condiciones:

* Ambas tablas deben tener la misma estructura.
* Ambas tablas deben tener la misma clave de partición, la misma clave de ORDER BY y la misma clave primaria.
* Ambas tablas deben tener la misma política de almacenamiento.
* La tabla de destino debe incluir todos los índices y las proyecciones de la tabla de origen. Si la configuración `enforce_index_structure_match_on_partition_manipulation` está habilitada para la tabla de destino, los índices y las proyecciones deben ser idénticos. De lo contrario, la tabla de destino puede tener un superconjunto de los índices y las proyecciones de la tabla de origen.

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

Esta consulta copia la partición de datos de `table1` a `table2` y reemplaza la partición existente en `table2`. La operación es atómica.

Tenga en cuenta que:

* Los datos no se eliminarán de `table1`.
* `table1` puede ser una tabla temporal.

Para que la consulta se ejecute correctamente, deben cumplirse las siguientes condiciones:

* Ambas tablas deben tener la misma estructura.
* Ambas tablas deben tener la misma clave de partición, la misma clave de ORDER BY y la misma clave primaria.
* Ambas tablas deben tener la misma política de almacenamiento.
* La tabla de destino debe incluir todos los índices y las proyecciones de la tabla de origen. Si el ajuste `enforce_index_structure_match_on_partition_manipulation` está habilitado en la tabla de destino, los índices y las proyecciones deben ser idénticos. De lo contrario, la tabla de destino puede tener un superconjunto de los índices y las proyecciones de la tabla de origen.

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

Esta consulta mueve la partición de datos de `table_source` a `table_dest` y elimina los datos de `table_source`.

Para que la consulta se ejecute correctamente, se deben cumplir las siguientes condiciones:

* Ambas tablas deben tener la misma estructura.
* Ambas tablas deben tener la misma clave de partición, la misma clave de ORDER BY y la misma clave primaria.
* Ambas tablas deben tener la misma política de almacenamiento.
* Ambas tablas deben pertenecer a la misma familia de motores (replicados o no replicados).
* La tabla de destino debe incluir todos los índices y proyecciones de la tabla de origen. Si el ajuste `enforce_index_structure_match_on_partition_manipulation` está habilitado en la tabla de destino, los índices y las proyecciones deben ser idénticos. De lo contrario, la tabla de destino puede tener un superconjunto de los índices y proyecciones de la tabla de origen.

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Restablece todos los valores de la columna especificada en una partición. Si se definió la cláusula `DEFAULT` al crear la tabla, esta consulta establece el valor de la columna en el valor predeterminado especificado.

Ejemplo:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

Esta consulta crea una copia de seguridad local de una partición especificada. Si se omite la cláusula `PARTITION`, la consulta crea la copia de seguridad de todas las particiones a la vez.

:::note
Todo el proceso de copia de seguridad se realiza sin detener el servidor.
:::

Tenga en cuenta que, en las tablas de estilo antiguo, puede especificar el prefijo del nombre de la partición (por ejemplo, `2019`); en ese caso, la consulta crea la copia de seguridad de todas las particiones correspondientes. Lea cómo establecer la expresión de partición en la sección [establecer la expresión de partición](#how-to-set-partition-expression).

En el momento de la ejecución, para crear una instantánea de los datos, la consulta crea hardlinks a los datos de la tabla. Los hardlinks se colocan en el directorio `/var/lib/clickhouse/shadow/N/...`, donde:

* `/var/lib/clickhouse/` es el directorio de trabajo de ClickHouse especificado en la configuración.
* `N` es el número incremental de la copia de seguridad.
* si se especifica el parámetro `WITH NAME`, se usa el valor del parámetro `'backup_name'` en lugar del número incremental.

:::note
Si utiliza [un conjunto de discos para almacenar datos en una tabla](/es/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes), el directorio `shadow/N` aparece en cada disco y almacena las partes de datos que coinciden con la expresión `PARTITION`.
:::

Dentro de la copia de seguridad se crea la misma estructura de directorios que en `/var/lib/clickhouse/`. La consulta ejecuta `chmod` en todos los archivos para impedir que se escriba en ellos.

Después de crear la copia de seguridad, puede copiar los datos de `/var/lib/clickhouse/shadow/` al servidor remoto y luego eliminarlos del servidor local. Tenga en cuenta que la consulta `ALTER t FREEZE PARTITION` no se replica. Crea una copia de seguridad local solo en el servidor local.

La consulta crea la copia de seguridad casi al instante (aunque primero espera a que terminen de ejecutarse las consultas actuales sobre la tabla correspondiente).

`ALTER TABLE t FREEZE PARTITION` copia solo los datos, no los metadatos de la tabla. Para hacer una copia de seguridad de los metadatos de la tabla, copie el archivo `/var/lib/clickhouse/metadata/database/table.sql`

Para restaurar datos desde una copia de seguridad, haga lo siguiente:

1. Cree la tabla si no existe. Para ver la consulta, use el archivo .sql (reemplace `ATTACH` por `CREATE` en él).
2. Copie los datos del directorio `data/database/table/` dentro de la copia de seguridad al directorio `/var/lib/clickhouse/data/database/table/detached/`.
3. Ejecute consultas `ALTER TABLE t ATTACH PARTITION` para agregar los datos a la tabla.

La restauración desde una copia de seguridad no requiere detener el servidor.

La consulta procesa las partes en paralelo; el número de hilos está regulado por la configuración `max_threads`.

Para obtener más información sobre las copias de seguridad y la restauración de datos, consulte la sección [&quot;Backup and Restore in ClickHouse&quot;](/es/operations/backup/overview).

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Elimina del disco las particiones `frozen` con el nombre especificado. Si se omite la cláusula `PARTITION`, la consulta elimina la copia de seguridad de todas las particiones a la vez.

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

La consulta funciona de forma similar a `CLEAR COLUMN`, pero restablece un índice en lugar de los datos de una columna.

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Descarga una partición|parte de otro servidor. Esta consulta solo funciona para las tablas replicadas.

La consulta realiza lo siguiente:

1. Descarga la partición|parte del segmento especificado. En &#39;path-in-zookeeper&#39; debe especificar una ruta al segmento en ZooKeeper.
2. Después, la consulta coloca los datos descargados en el directorio `detached` de la tabla `table_name`. Use la consulta [ATTACH PARTITION|PART](#attach-partitionpart) para agregar los datos a la tabla.

Por ejemplo:

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Tenga en cuenta lo siguiente:

* La consulta `ALTER ... FETCH PARTITION|PART` no se replica. Coloca la parte o partición en el directorio `detached` solo en el servidor local.
* La consulta `ALTER TABLE ... ATTACH` sí se replica. Agrega los datos a todas las réplicas. Los datos se agregan a una de las réplicas desde el directorio `detached` y, a las demás, desde las réplicas vecinas.

Antes de descargarla, el sistema comprueba si la partición existe y si la estructura de la tabla coincide. La réplica más adecuada se selecciona automáticamente entre las réplicas en buen estado.

Aunque la consulta se llama `ALTER TABLE`, no cambia la estructura de la tabla ni modifica de inmediato los datos disponibles en ella.

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

Mueve particiones o partes de datos a otro volumen o disco en tablas con motor `MergeTree`. Consulte [Uso de varios dispositivos de bloque para almacenar datos](/es/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

La consulta `ALTER TABLE t MOVE`:

* No se replica, porque distintas réplicas pueden tener políticas de almacenamiento diferentes.
* Devuelve un error si el disco o volumen especificado no está configurado. La consulta también devuelve un error si no se pueden aplicar las condiciones de movimiento de datos especificadas en la política de almacenamiento.
* Puede devolver un error en caso de que los datos que se van a mover ya hayan sido movidos por un proceso en segundo plano, una consulta `ALTER TABLE t MOVE` concurrente o como resultado de la fusión de datos en segundo plano. El usuario no debe realizar ninguna acción adicional en este caso.

Ejemplo:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

Modifica los datos de la partición especificada que coinciden con la expresión de filtrado indicada. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

Sintaxis:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Ejemplo
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### Véase también
</div>

* [UPDATE](/es/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

Elimina los datos de la partición especificada que coinciden con la expresión de filtrado indicada. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

Sintaxis:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Ejemplo
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

Esto reescribirá las partes desde cero, usando toda la configuración nueva. Esto tiene sentido porque la configuración a nivel de tabla, como `use_const_adaptive_granularity`, por defecto solo se aplica a las partes recién escritas.

<div id="example">
  ### Ejemplo
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### Véase también
</div>

* [DELETE](/es/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## Cómo establecer la expresión de partición
</div>

Puede especificar la expresión de partición en las consultas `ALTER ... PARTITION` de distintas maneras:

* Como un valor de la columna `partition` de la tabla `system.parts`. Por ejemplo, `ALTER TABLE visits DETACH PARTITION 201901`.
* Usando la palabra clave `ALL`. Solo puede usarse con DROP/DETACH/ATTACH/ATTACH FROM. Por ejemplo, `ALTER TABLE visits ATTACH PARTITION ALL`.
* Como una tupla de expresiones o constantes que coincida (en tipos) con la tupla de la clave de particionamiento de la tabla. En el caso de una clave de particionamiento de un solo elemento, la expresión debe envolverse en la función `tuple (...)`. Por ejemplo, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
* Usando el ID de partición. El ID de partición es un identificador de cadena de la partición (legible para humanos, si es posible) que se usa como nombre de las particiones en el sistema de archivos y en ZooKeeper. El ID de partición debe especificarse en la cláusula `PARTITION ID`, entre comillas simples. Por ejemplo, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
* En las consultas [ALTER ATTACH PART](#attach-partitionpart) y [DROP DETACHED PART](#drop-detached-partitionpart), para especificar el nombre de una parte, use un literal de cadena con un valor de la columna `name` de la tabla [system.detached&#95;parts](/es/operations/system-tables/detached_parts). Por ejemplo, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

El uso de comillas al especificar la partición depende del tipo de expresión de partición. Por ejemplo, para el tipo `String`, debe especificar su nombre entre comillas (`'`). Para los tipos `Date` e `Int*`, no se necesitan comillas.

Todas las reglas anteriores también se aplican a la consulta [OPTIMIZE](/es/sql-reference/statements/optimize.md). Si necesita especificar la única partición al optimizar una tabla no particionada, establezca la expresión `PARTITION tuple()`. Por ejemplo:

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` especifica la partición a la que se aplican las expresiones [UPDATE](/es/sql-reference/statements/alter/update) o [DELETE](/es/sql-reference/statements/alter/delete) como resultado de la consulta `ALTER TABLE`. Solo se crean nuevas partes a partir de la partición especificada. De este modo, `IN PARTITION` ayuda a reducir la carga cuando la tabla está dividida en muchas particiones y solo necesita actualizar los datos de forma puntual.

Los ejemplos de consultas `ALTER ... PARTITION` se muestran en las pruebas [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) y [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).