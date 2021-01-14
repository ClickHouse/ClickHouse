---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "Replicaci\xF3n de datos"
---

# Replicación de datos {#table_engines-replication}

La replicación solo se admite para tablas de la familia MergeTree:

-   ReplicatedMergeTree
-   ReplicatedSummingMergeTree
-   ReplicatedReplacingMergeTree
-   ReplicatedAggregatingMergeTree
-   ReplicatedCollapsingMergeTree
-   ReplicatedVersionedCollapsingMergetree
-   ReplicatedGraphiteMergeTree

La replicación funciona a nivel de una tabla individual, no de todo el servidor. Un servidor puede almacenar tablas replicadas y no replicadas al mismo tiempo.

La replicación no depende de la fragmentación. Cada fragmento tiene su propia replicación independiente.

Datos comprimidos para `INSERT` y `ALTER` se replica (para obtener más información, consulte la documentación para [ALTER](../../../sql-reference/statements/alter.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` y `RENAME` las consultas se ejecutan en un único servidor y no se replican:

-   El `CREATE TABLE` query crea una nueva tabla replicable en el servidor donde se ejecuta la consulta. Si esta tabla ya existe en otros servidores, agrega una nueva réplica.
-   El `DROP TABLE` query elimina la réplica ubicada en el servidor donde se ejecuta la consulta.
-   El `RENAME` query cambia el nombre de la tabla en una de las réplicas. En otras palabras, las tablas replicadas pueden tener diferentes nombres en diferentes réplicas.

Uso de ClickHouse [Apache ZooKeeper](https://zookeeper.apache.org) para almacenar metainformación de réplicas. Utilice ZooKeeper versión 3.4.5 o posterior.

Para utilizar la replicación, establezca los parámetros [Zookeeper](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) sección de configuración del servidor.

!!! attention "Atención"
    No descuides la configuración de seguridad. ClickHouse soporta el `digest` [Esquema de ACL](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) del subsistema de seguridad ZooKeeper.

Ejemplo de configuración de las direcciones del clúster ZooKeeper:

``` xml
<zookeeper>
    <node index="1">
        <host>example1</host>
        <port>2181</port>
    </node>
    <node index="2">
        <host>example2</host>
        <port>2181</port>
    </node>
    <node index="3">
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

Puede especificar cualquier clúster ZooKeeper existente y el sistema utilizará un directorio en él para sus propios datos (el directorio se especifica al crear una tabla replicable).

Si ZooKeeper no está establecido en el archivo de configuración, no puede crear tablas replicadas y las tablas replicadas existentes serán de solo lectura.

ZooKeeper no se utiliza en `SELECT` consultas porque la replicación no afecta al rendimiento de `SELECT` y las consultas se ejecutan tan rápido como lo hacen para las tablas no replicadas. Al consultar tablas replicadas distribuidas, el comportamiento de ClickHouse se controla mediante la configuración [max\_replica\_delay\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) y [fallback\_to\_stale\_replicas\_for\_distributed\_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

Para cada `INSERT` consulta, aproximadamente diez entradas se agregan a ZooKeeper a través de varias transacciones. (Para ser más precisos, esto es para cada bloque de datos insertado; una consulta INSERT contiene un bloque o un bloque por `max_insert_block_size = 1048576` filas.) Esto conduce a latencias ligeramente más largas para `INSERT` en comparación con las tablas no replicadas. Pero si sigue las recomendaciones para insertar datos en lotes de no más de uno `INSERT` por segundo, no crea ningún problema. Todo el clúster ClickHouse utilizado para coordinar un clúster ZooKeeper tiene un total de varios cientos `INSERTs` por segundo. El rendimiento en las inserciones de datos (el número de filas por segundo) es tan alto como para los datos no replicados.

Para clústeres muy grandes, puede usar diferentes clústeres de ZooKeeper para diferentes fragmentos. Sin embargo, esto no ha demostrado ser necesario en el Yandex.Clúster Metrica (aproximadamente 300 servidores).

La replicación es asíncrona y multi-master. `INSERT` consultas (así como `ALTER`) se puede enviar a cualquier servidor disponible. Los datos se insertan en el servidor donde se ejecuta la consulta y, a continuación, se copian a los demás servidores. Debido a que es asincrónico, los datos insertados recientemente aparecen en las otras réplicas con cierta latencia. Si parte de las réplicas no está disponible, los datos se escriben cuando estén disponibles. Si hay una réplica disponible, la latencia es la cantidad de tiempo que tarda en transferir el bloque de datos comprimidos a través de la red.

De forma predeterminada, una consulta INSERT espera la confirmación de la escritura de los datos de una sola réplica. Si los datos fue correctamente escrito a sólo una réplica y el servidor con esta réplica deja de existir, los datos almacenados se perderán. Para habilitar la confirmación de las escrituras de datos de varias réplicas, utilice `insert_quorum` opcion.

Cada bloque de datos se escribe atómicamente. La consulta INSERT se divide en bloques hasta `max_insert_block_size = 1048576` filas. En otras palabras, si el `INSERT` consulta tiene menos de 1048576 filas, se hace atómicamente.

Los bloques de datos se deduplican. Para varias escrituras del mismo bloque de datos (bloques de datos del mismo tamaño que contienen las mismas filas en el mismo orden), el bloque solo se escribe una vez. La razón de esto es en caso de fallas de red cuando la aplicación cliente no sabe si los datos se escribieron en la base de datos, por lo que `INSERT` consulta simplemente se puede repetir. No importa a qué réplica se enviaron los INSERT con datos idénticos. `INSERTs` son idempotentes. Los parámetros de desduplicación son controlados por [merge\_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) configuración del servidor.

Durante la replicación, sólo los datos de origen que se van a insertar se transfieren a través de la red. La transformación de datos adicional (fusión) se coordina y se realiza en todas las réplicas de la misma manera. Esto minimiza el uso de la red, lo que significa que la replicación funciona bien cuando las réplicas residen en centros de datos diferentes. (Tenga en cuenta que la duplicación de datos en diferentes centros de datos es el objetivo principal de la replicación.)

Puede tener cualquier número de réplicas de los mismos datos. El Yandex.Metrica utiliza doble replicación en producción. Cada servidor utiliza RAID-5 o RAID-6, y RAID-10 en algunos casos. Esta es una solución relativamente confiable y conveniente.

El sistema supervisa la sincronicidad de los datos en las réplicas y puede recuperarse después de un fallo. La conmutación por error es automática (para pequeñas diferencias en los datos) o semiautomática (cuando los datos difieren demasiado, lo que puede indicar un error de configuración).

## Creación de tablas replicadas {#creating-replicated-tables}

El `Replicated` prefijo se agrega al nombre del motor de tabla. Por ejemplo:`ReplicatedMergeTree`.

**Replicated\*MergeTree parámetros**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.

Ejemplo:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

<details markdown="1">

<summary>Ejemplo en sintaxis obsoleta</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

</details>

Como muestra el ejemplo, estos parámetros pueden contener sustituciones entre llaves. Los valores sustituidos se toman de la ‘macros’ sección del archivo de configuración. Ejemplo:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

La ruta de acceso a la tabla en ZooKeeper debe ser única para cada tabla replicada. Las tablas en diferentes fragmentos deben tener rutas diferentes.
En este caso, la ruta consta de las siguientes partes:

`/clickhouse/tables/` es el prefijo común. Recomendamos usar exactamente este.

`{layer}-{shard}` es el identificador de fragmento. En este ejemplo consta de dos partes, ya que el Yandex.Metrica clúster utiliza sharding de dos niveles. Para la mayoría de las tareas, puede dejar solo la sustitución {shard}, que se expandirá al identificador de fragmento.

`table_name` es el nombre del nodo de la tabla en ZooKeeper. Es una buena idea hacerlo igual que el nombre de la tabla. Se define explícitamente, porque a diferencia del nombre de la tabla, no cambia después de una consulta RENAME.
*HINT*: podría agregar un nombre de base de datos delante de `table_name` También. Nivel de Cifrado WEP `db_name.table_name`

El nombre de réplica identifica diferentes réplicas de la misma tabla. Puede usar el nombre del servidor para esto, como en el ejemplo. El nombre solo tiene que ser único dentro de cada fragmento.

Puede definir los parámetros explícitamente en lugar de utilizar sustituciones. Esto podría ser conveniente para probar y para configurar clústeres pequeños. Sin embargo, no puede usar consultas DDL distribuidas (`ON CLUSTER` en este caso.

Cuando se trabaja con clústeres grandes, se recomienda utilizar sustituciones porque reducen la probabilidad de error.

Ejecute el `CREATE TABLE` consulta en cada réplica. Esta consulta crea una nueva tabla replicada o agrega una nueva réplica a una existente.

Si agrega una nueva réplica después de que la tabla ya contenga algunos datos en otras réplicas, los datos se copiarán de las otras réplicas a la nueva después de ejecutar la consulta. En otras palabras, la nueva réplica se sincroniza con las demás.

Para eliminar una réplica, ejecute `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## Recuperación después de fallos {#recovery-after-failures}

Si ZooKeeper no está disponible cuando se inicia un servidor, las tablas replicadas cambian al modo de solo lectura. El sistema intenta conectarse periódicamente a ZooKeeper.

Si ZooKeeper no está disponible durante un `INSERT`, o se produce un error al interactuar con ZooKeeper, se produce una excepción.

Después de conectarse a ZooKeeper, el sistema comprueba si el conjunto de datos en el sistema de archivos local coincide con el conjunto de datos esperado (ZooKeeper almacena esta información). Si hay incoherencias menores, el sistema las resuelve sincronizando datos con las réplicas.

Si el sistema detecta partes de datos rotas (con un tamaño incorrecto de archivos) o partes no reconocidas (partes escritas en el sistema de archivos pero no grabadas en ZooKeeper), las mueve al `detached` subdirectorio (no se eliminan). Las piezas que faltan se copian de las réplicas.

Tenga en cuenta que ClickHouse no realiza ninguna acción destructiva, como eliminar automáticamente una gran cantidad de datos.

Cuando el servidor se inicia (o establece una nueva sesión con ZooKeeper), solo verifica la cantidad y el tamaño de todos los archivos. Si los tamaños de los archivos coinciden pero los bytes se han cambiado en algún punto intermedio, esto no se detecta inmediatamente, sino solo cuando se intenta leer los datos `SELECT` consulta. La consulta produce una excepción sobre una suma de comprobación no coincidente o el tamaño de un bloque comprimido. En este caso, las partes de datos se agregan a la cola de verificación y se copian de las réplicas si es necesario.

Si el conjunto local de datos difiere demasiado del esperado, se activa un mecanismo de seguridad. El servidor ingresa esto en el registro y se niega a iniciarse. La razón de esto es que este caso puede indicar un error de configuración, como si una réplica en un fragmento se configurara accidentalmente como una réplica en un fragmento diferente. Sin embargo, los umbrales para este mecanismo se establecen bastante bajos, y esta situación puede ocurrir durante la recuperación de falla normal. En este caso, los datos se restauran semiautomáticamente, mediante “pushing a button”.

Para iniciar la recuperación, cree el nodo `/path_to_table/replica_name/flags/force_restore_data` en ZooKeeper con cualquier contenido, o ejecute el comando para restaurar todas las tablas replicadas:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

A continuación, reinicie el servidor. Al iniciar, el servidor elimina estos indicadores e inicia la recuperación.

## Recuperación después de la pérdida completa de datos {#recovery-after-complete-data-loss}

Si todos los datos y metadatos desaparecieron de uno de los servidores, siga estos pasos para la recuperación:

1.  Instale ClickHouse en el servidor. Defina correctamente las sustituciones en el archivo de configuración que contiene el identificador de fragmento y las réplicas, si las usa.
2.  Si tenía tablas no duplicadas que deben duplicarse manualmente en los servidores, copie sus datos desde una réplica (en el directorio `/var/lib/clickhouse/data/db_name/table_name/`).
3.  Copiar definiciones de tablas ubicadas en `/var/lib/clickhouse/metadata/` de una réplica. Si un identificador de fragmento o réplica se define explícitamente en las definiciones de tabla, corríjalo para que corresponda a esta réplica. (Como alternativa, inicie el servidor y `ATTACH TABLE` consultas que deberían haber estado en el .sql archivos en `/var/lib/clickhouse/metadata/`.)
4.  Para iniciar la recuperación, cree el nodo ZooKeeper `/path_to_table/replica_name/flags/force_restore_data` con cualquier contenido o ejecute el comando para restaurar todas las tablas replicadas: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Luego inicie el servidor (reinicie, si ya se está ejecutando). Los datos se descargarán de las réplicas.

Una opción de recuperación alternativa es eliminar información sobre la réplica perdida de ZooKeeper (`/path_to_table/replica_name`), luego vuelva a crear la réplica como se describe en “[Creación de tablas replicadas](#creating-replicated-tables)”.

No hay restricción en el ancho de banda de la red durante la recuperación. Tenga esto en cuenta si está restaurando muchas réplicas a la vez.

## La conversión de MergeTree a ReplicatedMergeTree {#converting-from-mergetree-to-replicatedmergetree}

Usamos el término `MergeTree` para referirse a todos los motores de mesa en el `MergeTree family`, lo mismo que para `ReplicatedMergeTree`.

Si usted tenía un `MergeTree` tabla replicada manualmente, puede convertirla en una tabla replicada. Es posible que tenga que hacer esto si ya ha recopilado una gran cantidad de datos `MergeTree` y ahora desea habilitar la replicación.

Si los datos difieren en varias réplicas, primero sincronícelos o elimínelos en todas las réplicas, excepto en una.

Cambie el nombre de la tabla MergeTree existente y, a continuación, cree un `ReplicatedMergeTree` mesa con el antiguo nombre.
Mueva los datos de la tabla antigua a la `detached` subdirectorio dentro del directorio con los nuevos datos de la tabla (`/var/lib/clickhouse/data/db_name/table_name/`).
Luego ejecuta `ALTER TABLE ATTACH PARTITION` en una de las réplicas para agregar estas partes de datos al conjunto de trabajo.

## La conversión de ReplicatedMergeTree a MergeTree {#converting-from-replicatedmergetree-to-mergetree}

Cree una tabla MergeTree con un nombre diferente. Mueva todos los datos del directorio con el `ReplicatedMergeTree` datos de la tabla al directorio de datos de la nueva tabla. A continuación, elimine el `ReplicatedMergeTree` y reinicie el servidor.

Si desea deshacerse de un `ReplicatedMergeTree` sin iniciar el servidor:

-   Eliminar el correspondiente `.sql` archivo en el directorio de metadatos (`/var/lib/clickhouse/metadata/`).
-   Eliminar la ruta correspondiente en ZooKeeper (`/path_to_table/replica_name`).

Después de esto, puede iniciar el servidor, crear un `MergeTree` tabla, mueva los datos a su directorio y, a continuación, reinicie el servidor.

## Recuperación cuando se pierden o se dañan los metadatos del clúster Zookeeper {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

Si los datos de ZooKeeper se perdieron o se dañaron, puede guardar los datos moviéndolos a una tabla no duplicada como se describió anteriormente.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
