---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: Otro
---

# Consultas Misceláneas {#miscellaneous-queries}

## ATTACH {#attach}

Esta consulta es exactamente la misma que `CREATE`, pero

-   En lugar de la palabra `CREATE` utiliza la palabra `ATTACH`.
-   La consulta no crea datos en el disco, pero supone que los datos ya están en los lugares apropiados, y simplemente agrega información sobre la tabla al servidor.
    Después de ejecutar una consulta ATTACH, el servidor sabrá sobre la existencia de la tabla.

Si la tabla se separó previamente (`DETACH`), lo que significa que su estructura es conocida, puede usar taquigrafía sin definir la estructura.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Esta consulta se utiliza al iniciar el servidor. El servidor almacena los metadatos de la tabla como archivos con `ATTACH` consultas, que simplemente se ejecuta en el lanzamiento (con la excepción de las tablas del sistema, que se crean explícitamente en el servidor).

## CHECK TABLE {#check-table}

Comprueba si los datos de la tabla están dañados.

``` sql
CHECK TABLE [db.]name
```

El `CHECK TABLE` query compara los tamaños de archivo reales con los valores esperados que se almacenan en el servidor. Si los tamaños de archivo no coinciden con los valores almacenados, significa que los datos están dañados. Esto puede deberse, por ejemplo, a un bloqueo del sistema durante la ejecución de la consulta.

La respuesta de consulta contiene el `result` columna con una sola fila. La fila tiene un valor de
[Booleana](../../sql-reference/data-types/boolean.md) tipo:

-   0 - Los datos de la tabla están dañados.
-   1 - Los datos mantienen la integridad.

El `CHECK TABLE` query admite los siguientes motores de tablas:

-   [Registro](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [Familia MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

Realizado sobre las tablas con otros motores de tabla causa una excepción.

Motores del `*Log` la familia no proporciona la recuperación automática de datos en caso de fallo. Utilice el `CHECK TABLE` consulta para rastrear la pérdida de datos de manera oportuna.

Para `MergeTree` motores familiares, el `CHECK TABLE` query muestra un estado de comprobación para cada parte de datos individual de una tabla en el servidor local.

**Si los datos están dañados**

Si la tabla está dañada, puede copiar los datos no dañados a otra tabla. Para hacer esto:

1.  Cree una nueva tabla con la misma estructura que la tabla dañada. Para ello, ejecute la consulta `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Establezca el [max\_threads](../../operations/settings/settings.md#settings-max_threads) valor a 1 para procesar la siguiente consulta en un único subproceso. Para ello, ejecute la consulta `SET max_threads = 1`.
3.  Ejecutar la consulta `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Esta solicitud copia los datos no dañados de la tabla dañada a otra tabla. Solo se copiarán los datos anteriores a la parte dañada.
4.  Reinicie el `clickhouse-client` para restablecer el `max_threads` valor.

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Devuelve lo siguiente `String` tipo columnas:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [expresión predeterminada](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` o `ALIAS`). La columna contiene una cadena vacía, si no se especifica la expresión predeterminada.
-   `default_expression` — Value specified in the `DEFAULT` clausula.
-   `comment_expression` — Comment text.

Las estructuras de datos anidadas se generan en “expanded” formato. Cada columna se muestra por separado, con el nombre después de un punto.

## DETACH {#detach}

Elimina información sobre el ‘name’ tabla desde el servidor. El servidor deja de saber sobre la existencia de la tabla.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Esto no elimina los datos ni los metadatos de la tabla. En el próximo lanzamiento del servidor, el servidor leerá los metadatos y volverá a conocer la tabla.
Del mismo modo, un “detached” se puede volver a conectar usando el `ATTACH` consulta (con la excepción de las tablas del sistema, que no tienen metadatos almacenados para ellas).

No hay `DETACH DATABASE` consulta.

## DROP {#drop}

Esta consulta tiene dos tipos: `DROP DATABASE` y `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Elimina todas las tablas dentro del ‘db’ base de datos, a continuación, elimina ‘db’ base de datos en sí.
Si `IF EXISTS` se especifica, no devuelve un error si la base de datos no existe.

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Elimina la tabla.
Si `IF EXISTS` se especifica, no devuelve un error si la tabla no existe o la base de datos no existe.

    DROP DICTIONARY [IF EXISTS] [db.]name

Elimina el diccionario.
Si `IF EXISTS` se especifica, no devuelve un error si la tabla no existe o la base de datos no existe.

## DROP USER {#drop-user-statement}

Elimina un usuario.

### Sintaxis {#drop-user-syntax}

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

Elimina un rol.

El rol eliminado se revoca de todas las entidades donde se concedió.

### Sintaxis {#drop-role-syntax}

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

Elimina una directiva de fila.

La directiva de filas eliminadas se revoca de todas las entidades a las que se asignó.

### Sintaxis {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

Elimina una cuota.

La cuota eliminada se revoca de todas las entidades a las que se asignó.

### Sintaxis {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

Elimina una cuota.

La cuota eliminada se revoca de todas las entidades a las que se asignó.

### Sintaxis {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Devuelve una sola `UInt8`columna -type, que contiene el valor único `0` si la tabla o base de datos no existe, o `1` si la tabla existe en la base de datos especificada.

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Intenta terminar por la fuerza las consultas que se están ejecutando actualmente.
Las consultas a finalizar se seleccionan en el sistema.tabla de procesos utilizando los criterios definidos en el `WHERE` cláusula de la `KILL` consulta.

Ejemplos:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

Los usuarios de solo lectura solo pueden detener sus propias consultas.

De forma predeterminada, se utiliza la versión asincrónica de las consultas (`ASYNC`), que no espera la confirmación de que las consultas se han detenido.

La versión síncrona (`SYNC`) espera a que se detengan todas las consultas y muestra información sobre cada proceso a medida que se detiene.
La respuesta contiene el `kill_status` columna, que puede tomar los siguientes valores:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

Una consulta de prueba (`TEST`) sólo comprueba los derechos del usuario y muestra una lista de consultas para detener.

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Intenta cancelar y quitar [mutación](alter.md#alter-mutations) que se están ejecutando actualmente. Las mutaciones para cancelar se seleccionan en el [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) utilizando el filtro especificado por el `WHERE` cláusula de la `KILL` consulta.

Una consulta de prueba (`TEST`) sólo comprueba los derechos del usuario y muestra una lista de consultas para detener.

Ejemplos:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Los cambios ya realizados por la mutación no se revierten.

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

Esta consulta intenta inicializar una combinación no programada de partes de datos para tablas con un motor de tablas [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) familia.

El `OPTMIZE` consulta también es compatible con el [Método de codificación de datos:](../../engines/table-engines/special/materializedview.md) y el [Búfer](../../engines/table-engines/special/buffer.md) motor. No se admiten otros motores de tabla.

Cuando `OPTIMIZE` se utiliza con el [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) la familia de motores de tablas, ClickHouse crea una tarea para fusionar y espera la ejecución en todos los nodos (si `replication_alter_partitions_sync` está habilitada la configuración).

-   Si `OPTIMIZE` no realiza una fusión por ningún motivo, no notifica al cliente. Para habilitar las notificaciones, [Optize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) configuración.
-   Si especifica un `PARTITION`, sólo la partición especificada está optimizada. [Cómo establecer la expresión de partición](alter.md#alter-how-to-specify-part-expr).
-   Si especifica `FINAL`, la optimización se realiza incluso cuando todos los datos ya están en una parte.
-   Si especifica `DEDUPLICATE`, luego se deduplicarán filas completamente idénticas (se comparan todas las columnas), tiene sentido solo para el motor MergeTree.

!!! warning "Advertencia"
    `OPTIMIZE` no se puede arreglar el “Too many parts” error.

## RENAME {#misc_operations-rename}

Cambia el nombre de una o más tablas.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Todas las tablas se renombran bajo bloqueo global. Cambiar el nombre de las tablas es una operación ligera. Si ha indicado otra base de datos después de TO, la tabla se moverá a esta base de datos. Sin embargo, los directorios con bases de datos deben residir en el mismo sistema de archivos (de lo contrario, se devuelve un error).

## SET {#query-set}

``` sql
SET param = value
```

Asignar `value` a la `param` [configuración](../../operations/settings/index.md) para la sesión actual. No se puede cambiar [configuración del servidor](../../operations/server-configuration-parameters/index.md) de esta manera.

También puede establecer todos los valores del perfil de configuración especificado en una sola consulta.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Para obtener más información, consulte [Configuración](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

Activa roles para el usuario actual.

### Sintaxis {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Establece roles predeterminados para un usuario.

Los roles predeterminados se activan automáticamente al iniciar sesión del usuario. Puede establecer como predeterminado sólo los roles concedidos anteriormente. Si el rol no se concede a un usuario, ClickHouse produce una excepción.

### Sintaxis {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

### Ejemplos {#set-default-role-examples}

Establecer varios roles predeterminados para un usuario:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Establezca todos los roles concedidos como predeterminados para un usuario:

``` sql
SET DEFAULT ROLE ALL TO user
```

Borrar roles predeterminados de un usuario:

``` sql
SET DEFAULT ROLE NONE TO user
```

Establezca todos los roles concedidos como predeterminados excepto algunos de ellos:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Elimina todos los datos de una tabla. Cuando la cláusula `IF EXISTS` se omite, la consulta devuelve un error si la tabla no existe.

El `TRUNCATE` consulta no es compatible con [Vista](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) y [Nulo](../../engines/table-engines/special/null.md) motores de mesa.

## USE {#use}

``` sql
USE db
```

Permite establecer la base de datos actual para la sesión.
La base de datos actual se utiliza para buscar tablas si la base de datos no está definida explícitamente en la consulta con un punto antes del nombre de la tabla.
Esta consulta no se puede realizar cuando se usa el protocolo HTTP, ya que no existe un concepto de sesión.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
