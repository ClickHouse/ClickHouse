---
description: 'Descripción general de la replicación de datos con la familia Replicated* de motores de tabla en ClickHouse'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'Motores de tabla Replicated*'
doc_type: 'reference'
---

:::note
En ClickHouse Cloud, la replicación se gestiona automáticamente. Cree las tablas sin añadir argumentos. Por ejemplo, en el texto siguiente sustituiría:

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

con:

```sql
ENGINE = ReplicatedMergeTree
```

:::

La replicación solo es compatible con tablas de la familia MergeTree

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

La replicación funciona a nivel de cada tabla, no del servidor completo. Un servidor puede almacenar tablas replicadas y no replicadas al mismo tiempo.

La replicación no depende del sharding. Cada segmento tiene su propia replicación independiente.

Los datos comprimidos de las consultas `INSERT` y `ALTER` se replican (para más información, consulte la documentación de [ALTER](/es/sql-reference/statements/alter).

Las consultas `CREATE`, `DROP`, `ATTACH`, `DETACH` y `RENAME` se ejecutan en un único servidor y no se replican:

* La consulta `CREATE TABLE` crea una nueva tabla replicable en el servidor donde se ejecuta. Si esta tabla ya existe en otros servidores, añade una nueva réplica.
* La consulta `DROP TABLE` elimina la réplica ubicada en el servidor donde se ejecuta la consulta.
* La consulta `RENAME` cambia el nombre de la tabla en una de las réplicas. En otras palabras, las tablas replicadas pueden tener nombres distintos en diferentes réplicas.

ClickHouse usa [ClickHouse Keeper](/es/guides/sre/keeper/index.md) para almacenar los metadatos de las réplicas. Es posible usar ZooKeeper versión 3.4.5 o posterior, pero se recomienda ClickHouse Keeper.

Para usar la replicación, establezca los parámetros en la sección [zookeeper](/es/operations/server-configuration-parameters/settings#zookeeper) de la configuración del servidor.

:::note
No descuide la configuración de seguridad. ClickHouse admite el [esquema ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) `digest` del subsistema de seguridad de ZooKeeper.
:::

Ejemplo de configuración de las direcciones del clúster de ClickHouse Keeper:

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

ClickHouse también permite almacenar los metadatos de las réplicas en un clúster auxiliar de ZooKeeper. Para ello, proporcione el nombre y la ruta del clúster de ZooKeeper como argumentos del motor.
En otras palabras, permite almacenar los metadatos de distintas tablas en diferentes clústeres de ZooKeeper.

Ejemplo de configuración de las direcciones del clúster auxiliar de ZooKeeper:

```xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

Para almacenar los metadatos de la tabla en un clúster auxiliar de ZooKeeper en lugar del clúster de ZooKeeper predeterminado, podemos usar SQL para crear la tabla con el
motor ReplicatedMergeTree de la siguiente manera:

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

Puede especificar cualquier clúster de ZooKeeper existente, y el sistema usará un directorio en él para sus propios datos (el directorio se especifica al crear una tabla replicable).

Si ZooKeeper no está configurado en el archivo de configuración, no puede crear tablas replicadas y cualquier tabla replicada existente será de solo lectura.

ZooKeeper no se usa en las consultas `SELECT` porque la replicación no afecta al rendimiento de `SELECT`, y las consultas se ejecutan igual de rápido que en las tablas no replicadas. Al consultar tablas replicadas distribuidas, el comportamiento de ClickHouse está controlado por los ajustes [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/es/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) y [fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/es/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries).

Para cada consulta `INSERT`, se añaden aproximadamente diez entradas a ZooKeeper mediante varias transacciones. (Más concretamente, esto ocurre por cada bloque de datos insertado; una consulta `INSERT` contiene un bloque, o un bloque por cada `max_insert_block_size = 1048576` filas). Esto provoca latencias ligeramente mayores para `INSERT` en comparación con las tablas no replicadas. Pero si sigues la recomendación de insertar datos en lotes de no más de un `INSERT` por segundo, no supone ningún problema. Todo el clúster de ClickHouse que utiliza un clúster de ZooKeeper para coordinarse tiene en total varios cientos de `INSERTs` por segundo. El rendimiento de las inserciones de datos (el número de filas por segundo) es igual de alto que en los datos no replicados.

Para clústeres muy grandes, puedes usar distintos clústeres de ZooKeeper para diferentes segmentos. Sin embargo, según nuestra experiencia, esto no ha resultado necesario en clústeres de producción de aproximadamente 300 servidores.

La replicación es asíncrona y multi-master. Las consultas `INSERT` (así como `ALTER`) pueden enviarse a cualquier servidor disponible. Los datos se insertan en el servidor donde se ejecuta la consulta y luego se copian a los demás servidores. Como es asíncrona, los datos insertados recientemente aparecen en las otras réplicas con cierta latencia. Si parte de las réplicas no está disponible, los datos se escriben cuando vuelven a estar disponibles. Si una réplica está disponible, la latencia es el tiempo que se tarda en transferir el bloque de datos comprimidos a través de la red. El número de hilos que realizan tareas en segundo plano para las tablas replicadas puede configurarse mediante el ajuste [background&#95;schedule&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size).

El motor `ReplicatedMergeTree` usa un grupo de hilos independiente para los replicated fetches. El tamaño del grupo está limitado por el ajuste [background&#95;fetches&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings#background_fetches_pool_size), que puede ajustarse reiniciando el servidor.

De forma predeterminada, una consulta `INSERT` espera la confirmación de escritura de los datos de una sola réplica. Si los datos se escribieron correctamente solo en una réplica y el servidor con esa réplica deja de existir, los datos almacenados se perderán. Para habilitar la confirmación de escritura de datos desde varias réplicas, usa la opción `insert_quorum`.

Cada bloque de datos se escribe de forma atómica. La consulta `INSERT` se divide en bloques de hasta `max_insert_block_size = 1048576` filas. En otras palabras, si la consulta `INSERT` tiene menos de 1048576 filas, se realiza de forma atómica.

Los bloques de datos se deduplican. Si el mismo bloque de datos se escribe varias veces (bloques de datos del mismo tamaño que contienen las mismas filas en el mismo orden), el bloque solo se escribe una vez. La razón es que, en caso de fallos de red, la aplicación cliente puede no saber si los datos se escribieron en la base de datos, por lo que la consulta `INSERT` puede repetirse sin más. No importa a qué réplica se enviaron los `INSERTs` con datos idénticos. Los `INSERTs` son idempotentes. Los parámetros de deduplicación están controlados por los ajustes del servidor de [merge&#95;tree](/es/operations/server-configuration-parameters/settings.md/#merge_tree).

Durante la replicación, solo los datos de origen que se van a insertar se transfieren por la red. La transformación posterior de los datos (merging) se coordina y se realiza de la misma manera en todas las réplicas. Esto minimiza el uso de la red, lo que significa que la replicación funciona bien cuando las réplicas se encuentran en distintos datacenters. (Ten en cuenta que duplicar datos en distintos datacenters es el objetivo principal de la replicación).

Puedes tener cualquier número de réplicas de los mismos datos. Según nuestra experiencia, una solución relativamente fiable y práctica podría usar replicación doble en producción, con cada servidor usando RAID-5 o RAID-6 (y RAID-10 en algunos casos).

El sistema supervisa la sincronización de los datos entre las réplicas y puede recuperarse tras un fallo. La conmutación por error es automática (cuando las diferencias en los datos son pequeñas) o semiautomática (cuando los datos difieren demasiado, lo que puede indicar un error de configuración).

<div id="creating-replicated-tables">
  ## Crear tablas replicadas
</div>

:::note
En ClickHouse Cloud, la replicación se gestiona automáticamente.

Cree las tablas con [`MergeTree`](/es/engines/table-engines/mergetree-family/mergetree) sin argumentos de replicación. El sistema convierte internamente [`MergeTree`](/es/engines/table-engines/mergetree-family/mergetree) en [`SharedMergeTree`](/es/cloud/reference/shared-merge-tree) para la replicación y la distribución de datos.

Evite usar `ReplicatedMergeTree` o especificar parámetros de replicación, ya que la plataforma se encarga de ella.

:::

<div id="replicatedmergetree-parameters">
  ### Parámetros de Replicated*MergeTree
</div>

| Parámetro          | Descripción                                                                                                      |
| ------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `zoo_path`         | La ruta de la tabla en ClickHouse Keeper.                                                                        |
| `replica_name`     | El nombre de la réplica en ClickHouse Keeper.                                                                    |
| `other_parameters` | Parámetros del motor utilizado para crear la versión replicada; por ejemplo, la versión en `ReplacingMergeTree`. |

Ejemplo:

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>Ejemplo con sintaxis obsoleta</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

Como muestra el ejemplo, estos parámetros pueden incluir sustituciones entre `{}`. Los valores sustituidos se toman de la sección [macros](/es/operations/server-configuration-parameters/settings.md/#macros) del archivo de configuración.

Ejemplo:

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

La ruta de la tabla en ClickHouse Keeper debe ser única para cada tabla replicada. Las tablas en distintos segmentos deben tener rutas diferentes.
En este caso, la ruta consta de las siguientes partes:

`/clickhouse/tables/` es el prefijo común. Recomendamos usar exactamente este.

`{shard}` se expandirá al identificador del segmento.

`table_name` es el nombre del nodo de la tabla en ClickHouse Keeper. Conviene que sea el mismo que el nombre de la tabla. Se define explícitamente porque, a diferencia del nombre de la tabla, no cambia después de una consulta RENAME.
*SUGERENCIA*: también puedes añadir un nombre de base de datos delante de `table_name`. P. ej., `db_name.table_name`

Se pueden usar las dos sustituciones integradas `{database}` y `{table}`; se expanden al nombre de la tabla y al nombre de la base de datos, respectivamente (a menos que estas macros estén definidas en la sección `macros`). Así, la ruta de ZooKeeper puede especificarse como `'/clickhouse/tables/{shard}/{database}/{table}'`.
Ten cuidado al cambiar el nombre de tablas cuando uses estas sustituciones integradas. La ruta en ClickHouse Keeper no puede cambiarse y, cuando se cambie el nombre de la tabla, las macros se expandirán a una ruta diferente; la tabla hará referencia a una ruta que no existe en ClickHouse Keeper y entrará en modo de solo lectura.

El nombre de la réplica identifica las distintas réplicas de la misma tabla. Puedes usar para ello el nombre del servidor, como en el ejemplo. El nombre solo tiene que ser único dentro de cada segmento.

Puedes definir los parámetros explícitamente en lugar de usar sustituciones. Esto puede resultar conveniente para pruebas y para configurar clústeres pequeños. Sin embargo, en este caso no puedes usar consultas DDL distribuidas (`ON CLUSTER`).

Al trabajar con clústeres grandes, recomendamos usar sustituciones porque reducen la probabilidad de error.

Puedes especificar argumentos predeterminados para el motor de tabla `Replicated` en el archivo de configuración del servidor. Por ejemplo:

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

En este caso, puede omitir argumentos al crear tablas:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

Equivale a:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

Ejecute la consulta `CREATE TABLE` en cada réplica. Esta consulta crea una nueva tabla replicada o añade una nueva réplica a una existente.

Si añade una nueva réplica cuando la tabla ya contiene datos en otras réplicas, los datos se copiarán desde las otras réplicas a la nueva después de ejecutar la consulta. En otras palabras, la nueva réplica se sincroniza automáticamente con las demás.

Para eliminar una réplica, ejecute `DROP TABLE`. Sin embargo, solo se elimina una réplica: la que se encuentra en el servidor donde ejecuta la consulta.

<div id="recovery-after-failures">
  ## Recuperación tras fallos
</div>

Si ClickHouse Keeper no está disponible cuando se inicia un servidor, las tablas replicadas pasan a modo de solo lectura. El sistema intenta conectarse periódicamente a ClickHouse Keeper.

Si ClickHouse Keeper no está disponible durante un `INSERT`, o se produce un error al interactuar con ClickHouse Keeper, se lanza una excepción.

Después de conectarse a ClickHouse Keeper, el sistema comprueba si el conjunto de datos del sistema de archivos local coincide con el conjunto de datos esperado (ClickHouse Keeper almacena esta información). Si hay pequeñas inconsistencias, el sistema las resuelve sincronizando los datos con las réplicas.

Si el sistema detecta partes de datos dañadas (con tamaños de archivo incorrectos) o partes no reconocidas (partes escritas en el sistema de archivos pero no registradas en ClickHouse Keeper), las mueve al subdirectorio `detached` (no se eliminan). Las partes que falten se copian desde las réplicas.

Tenga en cuenta que ClickHouse no realiza acciones destructivas, como eliminar automáticamente una gran cantidad de datos.

Cuando se inicia el servidor (o establece una nueva sesión con ClickHouse Keeper), solo comprueba el número y el tamaño de todos los archivos. Si los tamaños de los archivos coinciden pero se han modificado bytes en algún punto intermedio, esto no se detecta de inmediato, sino solo al intentar leer los datos para una consulta `SELECT`. La consulta lanza una excepción por una suma de comprobación que no coincide o por el tamaño de un bloque comprimido. En este caso, las partes de datos se añaden a la cola de verificación y se copian desde las réplicas si es necesario.

Si el conjunto local de datos difiere demasiado del esperado, se activa un mecanismo de seguridad. El servidor lo registra en el log y se niega a iniciarse. Esto se debe a que este caso puede indicar un error de configuración, por ejemplo, si una réplica de un segmento se configuró accidentalmente como una réplica de otro segmento. Sin embargo, los umbrales de este mecanismo están configurados bastante bajos, y esta situación puede producirse durante una recuperación normal tras un fallo. En este caso, los datos se restauran de forma semiautomática, &quot;pulsando un botón&quot;.

Para iniciar la recuperación, cree el nodo `/path_to_table/replica_name/flags/force_restore_data` en ClickHouse Keeper con cualquier contenido, o ejecute el comando para restaurar todas las tablas replicadas:

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Luego, reinicie el servidor. Al arrancar, el servidor elimina estos indicadores e inicia la recuperación.

<div id="recovery-after-complete-data-loss">
  ## Recuperación tras una pérdida total de datos
</div>

Si todos los datos y los metadatos desaparecieron de uno de los servidores, siga estos pasos para la recuperación:

1. Instale ClickHouse en el servidor. Defina correctamente las sustituciones en el archivo de configuración que contiene el identificador del segmento y las réplicas, si las usa.
2. Si tenía tablas no replicadas que deben duplicarse manualmente en los servidores, copie sus datos desde una réplica (en el directorio `/var/lib/clickhouse/data/db_name/table_name/`).
3. Copie las definiciones de las tablas ubicadas en `/var/lib/clickhouse/metadata/` desde una réplica. Si en las definiciones de las tablas se define explícitamente un identificador de segmento o de réplica, corríjalo para que corresponda a esta réplica. (Como alternativa, inicie el servidor y ejecute todas las consultas `ATTACH TABLE` que deberían haber estado en los archivos .sql de `/var/lib/clickhouse/metadata/`).
4. Para iniciar la recuperación, cree el nodo de ClickHouse Keeper `/path_to_table/replica_name/flags/force_restore_data` con cualquier contenido, o ejecute el comando para restaurar todas las tablas replicadas: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

A continuación, inicie el servidor (reinícielo si ya está en ejecución). Los datos se descargarán desde las réplicas.

Otra opción de recuperación es eliminar de ClickHouse Keeper la información sobre la réplica perdida (`/path_to_table/replica_name`) y, a continuación, volver a crear la réplica como se describe en &quot;[Creación de tablas replicadas](#creating-replicated-tables)&quot;.

No hay ninguna restricción de ancho de banda de red durante la recuperación. Téngalo en cuenta si está restaurando muchas réplicas a la vez.

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## Conversión de MergeTree a ReplicatedMergeTree
</div>

Usamos el término `MergeTree` para referirnos a todos los motores de tabla de la `familia MergeTree`, igual que hacemos con `ReplicatedMergeTree`.

Si tenía una tabla `MergeTree` que se replicaba manualmente, puede convertirla en una tabla replicada. Puede que necesite hacerlo si ya ha recopilado una gran cantidad de datos en una tabla `MergeTree` y ahora quiere habilitar la replicación.

La sentencia [ATTACH TABLE ... AS REPLICATED](/es/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) permite adjuntar como `ReplicatedMergeTree` una tabla `MergeTree` detached.

La tabla `MergeTree` puede convertirse automáticamente al reiniciar el servidor si el indicador `convert_to_replicated` está establecido en el directorio de datos de la tabla (`/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` para la base de datos `Atomic`).
Cree un archivo vacío `convert_to_replicated` y la tabla se cargará como replicada en el siguiente reinicio del servidor.

Esta consulta puede usarse para obtener la ruta de datos de la tabla. Si la tabla tiene varias rutas de datos, debe usar la primera.

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

Tenga en cuenta que la tabla ReplicatedMergeTree se creará con los valores de la configuración `default_replica_path` y `default_replica_name`.
Para crear la tabla convertida en otras réplicas, deberá especificar explícitamente su ruta en el primer argumento del motor `ReplicatedMergeTree`. La siguiente consulta puede usarse para obtener esa ruta.

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

También existe una forma manual de hacerlo.

Si los datos difieren entre varias réplicas, primero sincronícelos o elimínelos de todas las réplicas excepto de una.

Cambie el nombre de la tabla MergeTree existente y, a continuación, cree una tabla `ReplicatedMergeTree` con el nombre anterior.
Mueva los datos de la tabla anterior al subdirectorio `detached` dentro del directorio con los datos de la nueva tabla (`/var/lib/clickhouse/data/db_name/table_name/`).
Luego ejecute `ALTER TABLE ATTACH PARTITION` en una de las réplicas para añadir estas partes de datos al conjunto de trabajo.

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## Conversión de ReplicatedMergeTree a MergeTree
</div>

Use la sentencia [ATTACH TABLE ... AS NOT REPLICATED](/es/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) para adjuntar una tabla `ReplicatedMergeTree` en estado `detached` como `MergeTree` en un único servidor.

Otra forma de hacerlo requiere reiniciar el servidor. Cree una tabla `MergeTree` con un nombre diferente. Mueva todos los datos del directorio con los datos de la tabla `ReplicatedMergeTree` al directorio de datos de la nueva tabla. Luego elimine la tabla `ReplicatedMergeTree` y reinicie el servidor.

Si quiere deshacerse de una tabla `ReplicatedMergeTree` sin iniciar el servidor:

* Elimine el archivo `.sql` correspondiente en el directorio de metadatos (`/var/lib/clickhouse/metadata/`).
* Elimine la ruta correspondiente en ClickHouse Keeper (`/path_to_table/replica_name`).

Después de esto, puede iniciar el servidor, crear una tabla `MergeTree`, mover los datos a su directorio y luego reiniciar el servidor.

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## Recuperación cuando se pierden o se dañan los metadatos del clúster de ClickHouse Keeper
</div>

Si los datos de ClickHouse Keeper se perdieron o se dañaron, puede conservarlos moviéndolos a una tabla no replicada, como se describió anteriormente.

**Vea también**

* [background&#95;schedule&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/es/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/es/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/es/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)