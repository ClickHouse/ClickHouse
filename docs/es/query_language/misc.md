---
machine_translated: true
---

# Consultas Misceláneas {#miscellaneous-queries}

## CONECTAR {#attach}

Esta consulta es exactamente la misma que `CREATE` pero

-   En lugar de la palabra `CREATE` utiliza la palabra `ATTACH`.
-   La consulta no crea datos en el disco, pero supone que los datos ya están en los lugares apropiados, y simplemente agrega información sobre la tabla al servidor.
    Después de ejecutar una consulta ATTACH, el servidor sabrá sobre la existencia de la tabla.

Si la tabla se separó previamente (`DETACH`), lo que significa que su estructura es conocida, puede usar taquigrafía sin definir la estructura.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Esta consulta se utiliza al iniciar el servidor. El servidor almacena los metadatos de la tabla como archivos con `ATTACH` consultas, que simplemente se ejecuta en el lanzamiento (con la excepción de las tablas del sistema, que se crean explícitamente en el servidor).

## MESA DE VERIFICACIÓN {#check-table}

Comprueba si los datos de la tabla están dañados.

``` sql
CHECK TABLE [db.]name
```

El `CHECK TABLE` query compara los tamaños de archivo reales con los valores esperados que se almacenan en el servidor. Si los tamaños de archivo no coinciden con los valores almacenados, significa que los datos están dañados. Esto puede deberse, por ejemplo, a un bloqueo del sistema durante la ejecución de la consulta.

La respuesta de consulta contiene el `result` columna con una sola fila. La fila tiene un valor de
[Booleana](../data_types/boolean.md) Tipo:

-   0 - Los datos de la tabla están dañados.
-   1 - Los datos mantienen la integridad.

El `CHECK TABLE` query admite los siguientes motores de tablas:

-   [Registro](../operations/table_engines/log.md)
-   [TinyLog](../operations/table_engines/tinylog.md)
-   [StripeLog](../operations/table_engines/stripelog.md)
-   [Familia MergeTree](../operations/table_engines/mergetree.md)

Realizado sobre las tablas con otros motores de tabla causa una excepción.

Motores del `*Log` La familia no proporciona recuperación automática de datos en caso de fallo. Utilice el `CHECK TABLE` consulta para rastrear la pérdida de datos de manera oportuna.

Para `MergeTree` motores familiares, el `CHECK TABLE` query muestra un estado de comprobación para cada parte de datos individual de una tabla en el servidor local.

**Si los datos están dañados**

Si la tabla está dañada, puede copiar los datos no dañados a otra tabla. Para hacer esto:

1.  Cree una nueva tabla con la misma estructura que la tabla dañada. Para ello, ejecute la consulta `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Establezca el [max\_threads](../operations/settings/settings.md#settings-max_threads) valor a 1 para procesar la siguiente consulta en un único subproceso. Para ello, ejecute la consulta `SET max_threads = 1`.
3.  Ejecutar la consulta `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. Esta solicitud copia los datos no dañados de la tabla dañada a otra tabla. Solo se copiarán los datos anteriores a la parte dañada.
4.  Reinicie el `clickhouse-client` para restablecer el `max_threads` valor.

## TABLA DE DESCRIBE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Devuelve lo siguiente `String` Tipo columnas:

-   `name` — Nombre de la columna.
-   `type`— Tipo de columna.
-   `default_type` — Cláusula utilizada en [expresión predeterminada](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` o `ALIAS`). La columna contiene una cadena vacía, si no se especifica la expresión predeterminada.
-   `default_expression` — Valor especificado en el `DEFAULT` clausula.
-   `comment_expression` — Texto de comentario.

Las estructuras de datos anidadas se generan en “expanded” formato. Cada columna se muestra por separado, con el nombre después de un punto.

## SEPARAR {#detach}

Elimina información sobre el ‘name’ tabla desde el servidor. El servidor deja de conocer la existencia de la tabla.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Esto no elimina los datos ni los metadatos de la tabla. En el próximo lanzamiento del servidor, el servidor leerá los metadatos y volverá a conocer la tabla.
Del mismo modo, un “detached” se puede volver a conectar usando el `ATTACH` consulta (con la excepción de las tablas del sistema, que no tienen metadatos almacenados para ellas).

No hay `DETACH DATABASE` consulta.

## GOTA {#drop}

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

## EXISTIR {#exists}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Devuelve una sola `UInt8`columna -type, que contiene el valor único `0` si la tabla o base de datos no existe, o `1` si la tabla existe en la base de datos especificada.

## Matar consulta {#kill-query}

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

1.  ‘finished’ – La consulta se ha finalizado correctamente.
2.  ‘waiting’ – Esperando a que finalice la consulta después de enviarle una señal para finalizar.
3.  Los otros valores consultan por qué no se puede detener.

Una consulta de prueba (`TEST`) sólo comprueba los derechos del usuario y muestra una lista de consultas para detener.

## MUTACIÓN DE MATAR {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Intenta cancelar y quitar [mutación](alter.md#alter-mutations) que se están ejecutando actualmente. Las mutaciones para cancelar se seleccionan en el [`system.mutations`](../operations/system_tables.md#system_tables-mutations) utilizando el filtro especificado por el `WHERE` cláusula de la `KILL` consulta.

Una consulta de prueba (`TEST`) sólo comprueba los derechos del usuario y muestra una lista de consultas para detener.

Ejemplos:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

La consulta es útil cuando una mutación está bloqueada y no puede finalizar (por ejemplo, si alguna función en la consulta de mutación arroja una excepción cuando se aplica a los datos contenidos en la tabla).

Los cambios ya realizados por la mutación no se revierten.

## OPTIMIZAR {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

Esta consulta intenta inicializar una combinación no programada de partes de datos para tablas con un motor de tablas [Método de codificación de datos:](../operations/table_engines/mergetree.md) Familia.

El `OPTMIZE` consulta también es compatible con el [Método de codificación de datos:](../operations/table_engines/materializedview.md) y el [Búfer](../operations/table_engines/buffer.md) motor. No se admiten otros motores de tabla.

Cuando `OPTIMIZE` se utiliza con el [ReplicatedMergeTree](../operations/table_engines/replication.md) la familia de motores de tablas, ClickHouse crea una tarea para fusionar y espera la ejecución en todos los nodos (si `replication_alter_partitions_sync` está habilitada la configuración).

-   Si `OPTIMIZE` no realiza una fusión por ningún motivo, no notifica al cliente. Para habilitar las notificaciones, [Optize\_throw\_if\_noop](../operations/settings/settings.md#setting-optimize_throw_if_noop) configuración.
-   Si especifica un `PARTITION`, sólo la partición especificada está optimizada. [Cómo establecer la expresión de partición](alter.md#alter-how-to-specify-part-expr).
-   Si especifica `FINAL`, la optimización se realiza incluso cuando todos los datos ya están en una parte.
-   Si especifica `DEDUPLICATE`, luego se deduplicarán filas completamente idénticas (se comparan todas las columnas), tiene sentido solo para el motor MergeTree.

!!! warning "Advertencia"
    `OPTIMIZE` no se puede arreglar el “Too many parts” error.

## Renombrar {#misc_operations-rename}

Cambia el nombre de una o más tablas.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Todas las tablas se renombran bajo bloqueo global. Cambiar el nombre de las tablas es una operación ligera. Si ha indicado otra base de datos después de TO, la tabla se moverá a esta base de datos. Sin embargo, los directorios con bases de datos deben residir en el mismo sistema de archivos (de lo contrario, se devuelve un error).

## ESTABLECER {#query-set}

``` sql
SET param = value
```

Asignar `value` Angeles `param` [configuración](../operations/settings/index.md) para la sesión actual. No se puede cambiar [configuración del servidor](../operations/server_settings/index.md) de esta manera.

También puede establecer todos los valores del perfil de configuración especificado en una sola consulta.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Para obtener más información, consulte [Configuración](../operations/settings/settings.md).

## TRUNCAR {#truncate}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Elimina todos los datos de una tabla. Cuando la cláusula `IF EXISTS` se omite, la consulta devuelve un error si la tabla no existe.

El `TRUNCATE` consulta no es compatible con [Vista](../operations/table_engines/view.md), [File](../operations/table_engines/file.md), [URL](../operations/table_engines/url.md) y [Nulo](../operations/table_engines/null.md) motores de mesa.

## UTILIZAR {#use}

``` sql
USE db
```

Permite establecer la base de datos actual para la sesión.
La base de datos actual se utiliza para buscar tablas si la base de datos no está definida explícitamente en la consulta con un punto antes del nombre de la tabla.
Esta consulta no se puede realizar cuando se utiliza el protocolo HTTP, ya que no existe un concepto de sesión.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/misc/) <!--hide-->
