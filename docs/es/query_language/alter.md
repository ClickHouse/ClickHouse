## ALTERAR {#query-language-queries-alter}

El `ALTER` consulta sólo se admite para `*MergeTree` mesas, así como `Merge`y`Distributed`. La consulta tiene varias variaciones.

### Manipulaciones de columna {#column-manipulations}

Cambiar la estructura de la tabla.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

En la consulta, especifique una lista de una o más acciones separadas por comas.
Cada acción es una operación en una columna.

Se admiten las siguientes acciones:

-   [AÑADIR COLUMNA](#alter_add-column) — Agrega una nueva columna a la tabla.
-   [COLUMNA DE GOTA](#alter_drop-column) — Elimina la columna.
-   [COLUMNA CLARA](#alter_clear-column) — Restablece los valores de las columnas.
-   [COLUMNA DE COMENTARIOS](#alter_comment-column) — Agrega un comentario de texto a la columna.
-   [MODIFICAR COLUMNA](#alter_modify-column) — Cambia el tipo de columna, la expresión predeterminada y el TTL.

Estas acciones se describen en detalle a continuación.

#### AÑADIR COLUMNA {#alter-add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

Agrega una nueva columna a la tabla con el `name`, `type`, [`codec`](create.md#codecs) y `default_expr` (ver la sección [Expresiones predeterminadas](create.md#create-default-values)).

Si el `IF NOT EXISTS` se incluye una cláusula, la consulta no devolverá un error si la columna ya existe. Si especifica `AFTER name_after` (el nombre de otra columna), la columna se agrega después de la especificada en la lista de columnas de tabla. De lo contrario, la columna se agrega al final de la tabla. Tenga en cuenta que no hay forma de agregar una columna al principio de una tabla. Para una cadena de acciones, `name_after` puede ser el nombre de una columna que se agrega en una de las acciones anteriores.

Agregar una columna solo cambia la estructura de la tabla, sin realizar ninguna acción con datos. Los datos no aparecen en el disco después de `ALTER`. Si faltan los datos para una columna al leer de la tabla, se rellena con valores predeterminados (realizando la expresión predeterminada si hay una, o usando ceros o cadenas vacías). La columna aparece en el disco después de fusionar partes de datos (consulte [Método de codificación de datos:](../operations/table_engines/mergetree.md)).

Este enfoque nos permite completar el `ALTER` consulta al instante, sin aumentar el volumen de datos antiguos.

Ejemplo:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### COLUMNA DE GOTA {#alter-drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Elimina la columna con el nombre `name`. Si el `IF EXISTS` Si se especifica una cláusula, la consulta no devolverá un error si la columna no existe.

Elimina datos del sistema de archivos. Dado que esto elimina archivos completos, la consulta se completa casi al instante.

Ejemplo:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### COLUMNA CLARA {#alter-clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Restablece todos los datos de una columna para una partición especificada. Obtenga más información sobre cómo configurar el nombre de la partición en la sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

Si el `IF EXISTS` Si se especifica una cláusula, la consulta no devolverá un error si la columna no existe.

Ejemplo:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COLUMNA DE COMENTARIOS {#alter-comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

Agrega un comentario a la columna. Si el `IF EXISTS` Si se especifica una cláusula, la consulta no devolverá un error si la columna no existe.

Cada columna puede tener un comentario. Si ya existe un comentario para la columna, un nuevo comentario sobrescribe el comentario anterior.

Los comentarios se almacenan en el `comment_expression` columna devuelta por el [TABLA DE DESCRIBE](misc.md#misc-describe-table) consulta.

Ejemplo:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFICAR COLUMNA {#alter-modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

Esta consulta cambia el `name` propiedades de la columna:

-   Tipo

-   Expresión predeterminada

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../operations/table_engines/mergetree.md#mergetree-column-ttl).

Si el `IF EXISTS` Si se especifica una cláusula, la consulta no devolverá un error si la columna no existe.

Al cambiar el tipo, los valores se convierten como si [ToType](functions/type_conversion_functions.md) se les aplicaron funciones. Si solo se cambia la expresión predeterminada, la consulta no hace nada complejo y se completa casi al instante.

Ejemplo:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Cambiar el tipo de columna es la única acción compleja: cambia el contenido de los archivos con datos. Para mesas grandes, esto puede llevar mucho tiempo.

Hay varias etapas de procesamiento:

-   Preparación de archivos temporales (nuevos) con datos modificados.
-   Cambiar el nombre de los archivos antiguos.
-   Cambiar el nombre de los archivos temporales (nuevos) a los nombres antiguos.
-   Eliminar los archivos antiguos.

Solo la primera etapa lleva tiempo. Si hay un error en esta etapa, los datos no se cambian.
Si hay un error durante una de las etapas sucesivas, los datos se pueden restaurar manualmente. La excepción es si los archivos antiguos se eliminaron del sistema de archivos, pero los datos de los nuevos archivos no se escribieron en el disco y se perdieron.

El `ALTER` se replica la consulta para cambiar columnas. Las instrucciones se guardan en ZooKeeper, luego cada réplica las aplica. Todo `ALTER` las consultas se ejecutan en el mismo orden. La consulta espera a que se completen las acciones adecuadas en las otras réplicas. Sin embargo, una consulta para cambiar columnas en una tabla replicada se puede interrumpir y todas las acciones se realizarán de forma asincrónica.

#### Limitaciones de consulta ALTER {#alter-query-limitations}

El `ALTER` query le permite crear y eliminar elementos separados (columnas) en estructuras de datos anidadas, pero no en estructuras de datos anidadas completas. Para agregar una estructura de datos anidada, puede agregar columnas con un nombre como `name.nested_name` y el tipo `Array(T)`. Una estructura de datos anidada es equivalente a varias columnas de matriz con un nombre que tiene el mismo prefijo antes del punto.

No hay soporte para eliminar columnas en la clave principal o la clave de muestreo (columnas que se utilizan en el `ENGINE` expresion). Solo es posible cambiar el tipo de las columnas que se incluyen en la clave principal si este cambio no provoca que se modifiquen los datos (por ejemplo, puede agregar valores a un Enum o cambiar un tipo de `DateTime` a `UInt32`).

Si el `ALTER` la consulta no es suficiente para realizar los cambios en la tabla que necesita, puede crear una nueva tabla, copiar los datos [INSERTAR SELECCIONAR](insert_into.md#insert_query_insert-select) consulta, luego cambie las tablas usando el [Renombrar](misc.md#misc_operations-rename) consulta y elimina la tabla anterior. Puede usar el [Método de codificación de datos:](../operations/utils/clickhouse-copier.md) como una alternativa a la `INSERT SELECT` consulta.

El `ALTER` query bloquea todas las lecturas y escrituras para la tabla. En otras palabras, si un largo `SELECT` se está ejecutando en el momento de la `ALTER` consulta, el `ALTER` la consulta esperará a que se complete. Al mismo tiempo, todas las consultas nuevas a la misma tabla esperarán `ALTER` se está ejecutando.

Para tablas que no almacenan datos por sí mismas (como `Merge` y `Distributed`), `ALTER` simplemente cambia la estructura de la tabla, y no cambia la estructura de las tablas subordinadas. Por ejemplo, cuando se ejecuta ALTER para un `Distributed` mesa, también tendrá que ejecutar `ALTER` para las tablas en todos los servidores remotos.

### Manipulaciones con expresiones clave {#manipulations-with-key-expressions}

Se admite el siguiente comando:

``` sql
MODIFY ORDER BY new_expression
```

Solo funciona para tablas en el [`MergeTree`](../operations/table_engines/mergetree.md) familia (incluyendo
[repetición](../operations/table_engines/replication.md) tabla). El comando cambia el
[clave de clasificación](../operations/table_engines/mergetree.md) de la mesa
a `new_expression` (una expresión o una tupla de expresiones). La clave principal sigue siendo la misma.

El comando es liviano en el sentido de que solo cambia los metadatos. Para mantener la propiedad esa parte de datos
las filas están ordenadas por la expresión de clave de ordenación, no puede agregar expresiones que contengan columnas existentes
a la clave de ordenación (sólo las columnas añadidas `ADD COLUMN` comando en el mismo `ALTER` consulta).

### Manipulaciones con índices de saltos de datos {#manipulations-with-data-skipping-indices}

Solo funciona para tablas en el [`*MergeTree`](../operations/table_engines/mergetree.md) familia (incluyendo
[repetición](../operations/table_engines/replication.md) tabla). Las siguientes operaciones
están disponibles:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Agrega la descripción del índice a los metadatos de las tablas.

-   `ALTER TABLE [db].name DROP INDEX name` - Elimina la descripción del índice de los metadatos de las tablas y elimina los archivos de índice del disco.

Estos comandos son livianos en el sentido de que solo cambian los metadatos o eliminan archivos.
Además, se replican (sincronizando metadatos de índices a través de ZooKeeper).

### Manipulaciones con restricciones {#manipulations-with-constraints}

Ver más en [limitación](create.md#constraints)

Las restricciones se pueden agregar o eliminar utilizando la siguiente sintaxis:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Las consultas agregarán o eliminarán metadatos sobre restricciones de la tabla para que se procesen inmediatamente.

Comprobación de restricciones *no se ejecutará* en los datos existentes si se agregaron.

Todos los cambios en las tablas replicadas se transmiten a ZooKeeper, por lo que se aplicarán en otras réplicas.

### Manipulaciones con particiones y piezas {#alter-manipulations-with-partitions}

Las siguientes operaciones con [partición](../operations/table_engines/custom_partitioning_key.md) están disponibles:

-   [DETACH PARTITION](#alter_detach-partition) – Mueve una partición a la `detached` directorio y olvidarlo.
-   [PARTICIÓN DE CAÍDA](#alter_drop-partition) – Elimina una partición.
-   [ADJUNTA PARTE\|PARTICIÓN](#alter_attach-partition) – Añade una pieza o partición desde el `detached` directorio a la tabla.
-   [REEMPLAZAR LA PARTICIÓN](#alter_replace-partition) - Copia la partición de datos de una tabla a otra.
-   [ADJUNTA PARTICIÓN DE](#alter_attach-partition-from) – Copia la partición de datos de una tabla a otra y añade.
-   [REEMPLAZAR LA PARTICIÓN](#alter_replace-partition) - Copia la partición de datos de una tabla a otra y reemplaza.
-   [MUEVA LA PARTICIÓN A LA MESA](#alter_move_to_table-partition) (\#alter\_move\_to\_table-partition) - Mover la partición de datos de una tabla a otra.
-   [COLUMNA CLARA EN PARTICIPACIÓN](#alter_clear-column-partition) - Restablece el valor de una columna especificada en una partición.
-   [ÍNDICE CLARO EN PARTICIPACIÓN](#alter_clear-index-partition) - Restablece el índice secundario especificado en una partición.
-   [CONGELAR PARTICIÓN](#alter_freeze-partition) – Crea una copia de seguridad de una partición.
-   [PARTICIÓN FETCH](#alter_fetch-partition) – Descarga una partición de otro servidor.
-   [PARTICIÓN DE MOVIMIENTO\|PARTE](#alter_move-partition) – Mover partición / parte de datos a otro disco o volumen.

<!-- -->

#### DETACH PARTITION {\#alter\_detach-partition} {#detach-partition-alter-detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Mueve todos los datos de la partición especificada `detached` directorio. El servidor se olvida de la partición de datos separada como si no existiera. El servidor no sabrá acerca de estos datos hasta que [CONECTAR](#alter_attach-partition) consulta.

Ejemplo:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

Lea cómo configurar la expresión de partición en una sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

Después de ejecutar la consulta, puede hacer lo que quiera con los datos en el `detached` directorio — eliminarlo del sistema de archivos, o simplemente dejarlo.

Esta consulta se replica – mueve los datos a la `detached` directorio en todas las réplicas. Tenga en cuenta que solo puede ejecutar esta consulta en una réplica de líder. Para averiguar si una réplica es un líder, realice `SELECT` consulta a la [sistema.Replica](../operations/system_tables.md#system_tables-replicas) tabla. Alternativamente, es más fácil hacer un `DETACH` consulta en todas las réplicas: todas las réplicas producen una excepción, excepto la réplica líder.

#### PARTICIÓN DE CAÍDA {#alter-drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Elimina la partición especificada de la tabla. Esta consulta etiqueta la partición como inactiva y elimina los datos por completo, aproximadamente en 10 minutos.

Lea cómo configurar la expresión de partición en una sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

La consulta se replica: elimina los datos de todas las réplicas.

#### CAÍDA DE DESPRENDIMIENTO DE LA PARTICIÓN\|PARTE {#alter-drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Quita la parte especificada o todas las partes de la partición especificada de `detached`.
Más información sobre cómo establecer la expresión de partición en una sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

#### ADJUNTA PARTICIÓN\|PARTE {#alter-attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Agrega datos a la tabla desde el `detached` directorio. Es posible agregar datos para una partición completa o para una parte separada. Ejemplos:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Más información sobre cómo establecer la expresión de partición en una sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

Esta consulta se replica. El iniciador de réplica comprueba si hay datos en el `detached` directorio. Si existen datos, la consulta comprueba su integridad. Si todo es correcto, la consulta agrega los datos a la tabla. Todas las demás réplicas descargan los datos del iniciador de réplica.

Entonces puedes poner datos en el `detached` en una réplica, y utilice el directorio `ALTER ... ATTACH` consulta para agregarlo a la tabla en todas las réplicas.

#### ADJUNTA PARTICIÓN DE {#alter-attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

Esta consulta copia la partición de datos `table1` a `table2` añade datos a los que existen en el `table2`. Tenga en cuenta que los datos no se eliminarán de `table1`.

Para que la consulta se ejecute correctamente, se deben cumplir las siguientes condiciones:

-   Ambas tablas deben tener la misma estructura.
-   Ambas tablas deben tener la misma clave de partición.

#### REEMPLAZAR LA PARTICIÓN {#alter-replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

Esta consulta copia la partición de datos `table1` a `table2` y reemplaza la partición existente en el `table2`. Tenga en cuenta que los datos no se eliminarán de `table1`.

Para que la consulta se ejecute correctamente, se deben cumplir las siguientes condiciones:

-   Ambas tablas deben tener la misma estructura.
-   Ambas tablas deben tener la misma clave de partición.

#### MUEVA LA PARTICIÓN A LA MESA {#alter-move-to-table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

Esta consulta mueve la partición de datos `table_source` a `table_dest` con la eliminación de los datos de `table_source`.

Para que la consulta se ejecute correctamente, se deben cumplir las siguientes condiciones:

-   Ambas tablas deben tener la misma estructura.
-   Ambas tablas deben tener la misma clave de partición.
-   Ambas tablas deben ser de la misma familia de motores. (replicado o no replicado)
-   Ambas tablas deben tener la misma política de almacenamiento.

#### COLUMNA CLARA EN PARTICIPACIÓN {#alter-clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Restablece todos los valores de la columna especificada en una partición. Si el `DEFAULT` cláusula se determinó al crear una tabla, esta consulta establece el valor de columna en un valor predeterminado especificado.

Ejemplo:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### CONGELAR PARTICIÓN {#alter-freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

Esta consulta crea una copia de seguridad local de una partición especificada. Si el `PARTITION` se omite la cláusula, la consulta crea la copia de seguridad de todas las particiones a la vez.

!!! note "Nota"
    Todo el proceso de copia de seguridad se realiza sin detener el servidor.

Tenga en cuenta que para las tablas de estilo antiguo puede especificar el prefijo del nombre de la partición (por ejemplo, ‘2019’) - entonces la consulta crea la copia de seguridad para todas las particiones correspondientes. Lea cómo configurar la expresión de partición en una sección [Cómo especificar la expresión de partición](#alter-how-to-specify-part-expr).

En el momento de la ejecución, para una instantánea de datos, la consulta crea vínculos rígidos a los datos de una tabla. Los enlaces duros se colocan en el directorio `/var/lib/clickhouse/shadow/N/...`, donde:

-   `/var/lib/clickhouse/` es el directorio ClickHouse de trabajo especificado en la configuración.
-   `N` es el número incremental de la copia de seguridad.

!!! note "Nota"
    Si usted usa [un conjunto de discos para el almacenamiento de datos en una tabla](../operations/table_engines/mergetree.md#table_engine-mergetree-multiple-volumes), el `shadow/N` directorio aparece en cada disco, almacenando partes de datos que coinciden con el `PARTITION` expresion.

La misma estructura de directorios se crea dentro de la copia de seguridad que dentro `/var/lib/clickhouse/`. La consulta realiza ‘chmod’ para todos los archivos, prohibiendo escribir en ellos.

Después de crear la copia de seguridad, puede copiar los datos desde `/var/lib/clickhouse/shadow/` al servidor remoto y, a continuación, elimínelo del servidor local. Tenga en cuenta que el `ALTER t FREEZE PARTITION` consulta no se replica. Crea una copia de seguridad local solo en el servidor local.

La consulta crea una copia de seguridad casi instantáneamente (pero primero espera a que las consultas actuales a la tabla correspondiente terminen de ejecutarse).

`ALTER TABLE t FREEZE PARTITION` copia solo los datos, no los metadatos de la tabla. Para hacer una copia de seguridad de los metadatos de la tabla, copie el archivo `/var/lib/clickhouse/metadata/database/table.sql`

Para restaurar los datos de una copia de seguridad, haga lo siguiente:

1.  Cree la tabla si no existe. Para ver la consulta, utilice el .archivo sql (reemplazar `ATTACH` en ella con `CREATE`).
2.  Copie los datos de la `data/database/table/` directorio dentro de la copia de seguridad a la `/var/lib/clickhouse/data/database/table/detached/` directorio.
3.  Ejecutar `ALTER TABLE t ATTACH PARTITION` consultas para agregar los datos a una tabla.

La restauración desde una copia de seguridad no requiere detener el servidor.

Para obtener más información sobre las copias de seguridad y la restauración de datos, consulte [Copia de seguridad de datos](../operations/backup.md) apartado.

#### ÍNDICE CLARO EN PARTICIPACIÓN {#alter-clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

La consulta funciona de forma similar a `CLEAR COLUMN`, pero restablece un índice en lugar de una columna de datos.

#### PARTICIÓN FETCH {#alter-fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Descarga una partición desde otro servidor. Esta consulta solo funciona para las tablas replicadas.

La consulta hace lo siguiente:

1.  Descarga la partición del fragmento especificado. En ‘path-in-zookeeper’ debe especificar una ruta al fragmento en ZooKeeper.
2.  Luego, la consulta coloca los datos descargados en el `detached` directorio de la `table_name` tabla. Utilice el [ADJUNTA PARTICIÓN\|PARTE](#alter_attach-partition) consulta para agregar los datos a la tabla.

Por ejemplo:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Tenga en cuenta que:

-   El `ALTER ... FETCH PARTITION` la consulta no está replicada. Coloca la partición en el `detached` sólo en el servidor local.
-   El `ALTER TABLE ... ATTACH` consulta se replica. Agrega los datos a todas las réplicas. Los datos se agregan a una de las réplicas desde el `detached` directorio, y para los demás - de réplicas vecinas.

Antes de descargar, el sistema verifica si la partición existe y la estructura de la tabla coincide. La réplica más adecuada se selecciona automáticamente de las réplicas en buen estado.

Aunque se llama a la consulta `ALTER TABLE`, no cambia la estructura de la tabla y no cambiar inmediatamente los datos disponibles en la tabla.

#### PARTICIÓN DE MOVIMIENTO\|PARTE {#alter-move-partition}

Mueve particiones o partes de datos a otro volumen o disco para `MergeTree`-mesas de motor. Ver [Uso de varios dispositivos de bloque para el almacenamiento de datos](../operations/table_engines/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

El `ALTER TABLE t MOVE` consulta:

-   No replicado, porque diferentes réplicas pueden tener diferentes directivas de almacenamiento.
-   Devuelve un error si el disco o volumen especificado no está configurado. La consulta también devuelve un error si no se pueden aplicar las condiciones de movimiento de datos especificadas en la directiva de almacenamiento.
-   Puede devolver un error en el caso, cuando los datos que se moverán ya se mueven por un proceso en segundo plano, concurrente `ALTER TABLE t MOVE` consulta o como resultado de la fusión de datos de fondo. Un usuario no debe realizar ninguna acción adicional en este caso.

Ejemplo:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### Cómo establecer la expresión de partición {#alter-how-to-specify-part-expr}

Puede especificar la expresión de partición en `ALTER ... PARTITION` de diferentes maneras:

-   Como valor de la `partition` columna de la `system.parts` tabla. Por ejemplo, `ALTER TABLE visits DETACH PARTITION 201901`.
-   Como la expresión de la columna de la tabla. Se admiten constantes y expresiones constantes. Por ejemplo, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   Usando el ID de partición. El ID de partición es un identificador de cadena de la partición (legible por humanos, si es posible) que se usa como nombres de particiones en el sistema de archivos y en ZooKeeper. El ID de partición debe especificarse en el `PARTITION ID` cláusula, entre comillas simples. Por ejemplo, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   En el [ALTERAR PIEZA DE ADJUNTO](#alter_attach-partition) y [PARTE DESMONTADA DE GOTA](#alter_drop-detached) consulta, para especificar el nombre de una parte, utilice un literal de cadena con un valor `name` columna de la [sistema.detached\_parts](../operations/system_tables.md#system_tables-detached_parts) tabla. Por ejemplo, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

El uso de comillas al especificar la partición depende del tipo de expresión de partición. Por ejemplo, para el `String` tipo, debe especificar su nombre entre comillas (`'`). Para el `Date` y `Int*` tipos no se necesitan comillas.

Para las tablas de estilo antiguo, puede especificar la partición como un número `201901` o una cadena `'201901'`. La sintaxis para las tablas de nuevo estilo es más estricta con los tipos (similar al analizador para el formato de entrada VALUES).

Todas las reglas anteriores también son ciertas para el [OPTIMIZAR](misc.md#misc_operations-optimize) consulta. Si necesita especificar la única partición al optimizar una tabla no particionada, establezca la expresión `PARTITION tuple()`. Por ejemplo:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

Los ejemplos de `ALTER ... PARTITION` las consultas se demuestran en las pruebas [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_local.sql) y [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Manipulaciones con Tabla TTL {#manipulations-with-table-ttl}

Usted puede cambiar [tabla TTL](../operations/table_engines/mergetree.md#mergetree-table-ttl) con una solicitud del siguiente formulario:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### Sincronicidad de las consultas ALTER {#synchronicity-of-alter-queries}

Para tablas no replicables, todas `ALTER` las consultas se realizan de forma sincrónica. Para las tablas replicables, la consulta solo agrega instrucciones para las acciones apropiadas para `ZooKeeper`, y las acciones mismas se realizan tan pronto como sea posible. Sin embargo, la consulta puede esperar a que estas acciones se completen en todas las réplicas.

Para `ALTER ... ATTACH|DETACH|DROP` consultas, puede utilizar el `replication_alter_partitions_sync` configuración para configurar la espera.
Valores posibles: `0` – no espere; `1` – sólo esperar a su propia ejecución (por defecto); `2` – esperar a todos.

### Mutación {#alter-mutations}

Las mutaciones son una variante de consulta ALTER que permite cambiar o eliminar filas en una tabla. En contraste con el estándar `UPDATE` y `DELETE` consultas destinadas a cambios de datos puntuales, las mutaciones están destinadas a operaciones pesadas que cambian muchas filas en una tabla. Apoyado para el `MergeTree` familia de motores de mesa, incluidos los motores con soporte de replicación.

Las tablas existentes están listas para las mutaciones tal como están (no es necesaria la conversión), pero después de que la primera mutación se aplica a una tabla, su formato de metadatos se vuelve incompatible con las versiones anteriores del servidor y volver a una versión anterior se vuelve imposible.

Comandos disponibles actualmente:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

El `filter_expr` debe ser de tipo `UInt8`. La consulta elimina las filas de la tabla para la que esta expresión toma un valor distinto de cero.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

El `filter_expr` debe ser de tipo `UInt8`. Esta consulta actualiza los valores de las columnas especificadas a los valores de las expresiones correspondientes `filter_expr` toma un valor distinto de cero. Los valores se convierten al tipo de columna utilizando el `CAST` operador. No se admite la actualización de columnas que se utilizan en el cálculo de la clave principal o de partición.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

La consulta reconstruye el índice secundario `name` en la partición `partition_name`.

Una consulta puede contener varios comandos separados por comas.

Para las tablas \*MergeTree, las mutaciones se ejecutan reescribiendo partes de datos completas. No hay atomicidad - las partes se sustituyen por partes mutadas tan pronto como están listas y una `SELECT` La consulta que comenzó a ejecutarse durante una mutación verá datos de partes que ya han sido mutadas junto con datos de partes que aún no han sido mutadas.

Las mutaciones están totalmente ordenadas por su orden de creación y se aplican a cada parte en ese orden. Las mutaciones también se ordenan parcialmente con INSERTs: los datos que se insertaron en la tabla antes de que se enviara la mutación se mutarán y los datos que se insertaron después de eso no se mutarán. Tenga en cuenta que las mutaciones no bloquean INSERTs de ninguna manera.

Una consulta de mutación regresa inmediatamente después de agregar la entrada de mutación (en el caso de tablas replicadas a ZooKeeper, para tablas no replicadas, al sistema de archivos). La mutación en sí se ejecuta de forma asíncrona utilizando la configuración del perfil del sistema. Para realizar un seguimiento del progreso de las mutaciones, puede usar el [`system.mutations`](../operations/system_tables.md#system_tables-mutations) tabla. Una mutación que se envió correctamente continuará ejecutándose incluso si se reinician los servidores ClickHouse. No hay forma de revertir la mutación una vez que se presenta, pero si la mutación está atascada por alguna razón, puede cancelarse con el [`KILL MUTATION`](misc.md#kill-mutation) consulta.

Las entradas de mutaciones terminadas no se eliminan de inmediato (el número de entradas conservadas viene determinado por el `finished_mutations_to_keep` parámetro del motor de almacenamiento). Las entradas de mutación más antiguas se eliminan.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/alter/) <!--hide-->
