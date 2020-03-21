# Distribuido {#distributed}

**Las tablas con motor distribuido no almacenan ningún dato por sí mismas**, pero permite el procesamiento de consultas distribuidas en varios servidores.
La lectura se paralela automáticamente. Durante una lectura, se utilizan los índices de tabla en servidores remotos, si los hay.

El motor distribuido acepta parámetros:

-   el nombre del clúster en el archivo de configuración del servidor

-   el nombre de una base de datos remota

-   el nombre de una tabla remota

-   (opcionalmente) clave de fragmentación

-   nombre de política (opcionalmente), se usará para almacenar archivos temporales para el envío asíncrono

    Ver también:

    -   `insert_distributed_sync` configuración
    -   [Método de codificación de datos:](mergetree.md#table_engine-mergetree-multiple-volumes) para los ejemplos

Ejemplo:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

Los datos se leerán desde todos los servidores ‘logs’ clúster, desde el valor predeterminado.tabla de éxitos ubicada en cada servidor del clúster.
Los datos no solo se leen sino que se procesan parcialmente en los servidores remotos (en la medida en que esto sea posible).
Por ejemplo, para una consulta con GROUP BY, los datos se agregarán en servidores remotos y los estados intermedios de las funciones agregadas se enviarán al servidor solicitante. Luego, los datos se agregarán más.

En lugar del nombre de la base de datos, puede usar una expresión constante que devuelva una cadena. Por ejemplo: currentDatabase().

logs: el nombre del clúster en el archivo de configuración del servidor.

Los clústeres se establecen así:

``` xml
<remote_servers>
    <logs>
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

Aquí se define un clúster con el nombre ‘logs’ que consta de dos fragmentos, cada uno de los cuales contiene dos réplicas.
Los fragmentos se refieren a los servidores que contienen diferentes partes de los datos (para leer todos los datos, debe acceder a todos los fragmentos).
Las réplicas están duplicando servidores (para leer todos los datos, puede acceder a los datos en cualquiera de las réplicas).

Los nombres de clúster no deben contener puntos.

Los parámetros `host`, `port`, y opcionalmente `user`, `password`, `secure`, `compression` se especifican para cada servidor:
- `host` – La dirección del servidor remoto. Puede utilizar el dominio o la dirección IPv4 o IPv6. Si especifica el dominio, el servidor realiza una solicitud DNS cuando se inicia y el resultado se almacena mientras el servidor se esté ejecutando. Si la solicitud DNS falla, el servidor no se inicia. Si cambia el registro DNS, reinicie el servidor.
- `port` – El puerto TCP para la actividad de mensajería (‘tcp\_port’ en la configuración, generalmente establecido en 9000). No lo confundas con http\_port.
- `user` – Nombre del usuario para conectarse a un servidor remoto. Valor predeterminado: predeterminado. Este usuario debe tener acceso para conectarse al servidor especificado. El acceso se configura en los usuarios.archivo xml. Para obtener más información, consulte la sección [Derechos de acceso](../../operations/access_rights.md).
- `password` – La contraseña para conectarse a un servidor remoto (no enmascarado). Valor predeterminado: cadena vacía.
- `secure` - Use ssl para la conexión, por lo general también debe definir `port` ¿Por qué? El servidor debe escuchar en <tcp_port_secure>Método de codificación de datos:</tcp_port_secure> y tener certificados correctos.
- `compression` - Utilice la compresión de datos. Valor predeterminado: true.

Al especificar réplicas, se seleccionará una de las réplicas disponibles para cada uno de los fragmentos al leer. Puede configurar el algoritmo para el equilibrio de carga (la preferencia para qué réplica acceder) [load\_balancing](../settings/settings.md#settings-load_balancing) configuración.
Si no se establece la conexión con el servidor, habrá un intento de conectarse con un breve tiempo de espera. Si la conexión falla, se seleccionará la siguiente réplica, y así sucesivamente para todas las réplicas. Si el intento de conexión falló para todas las réplicas, el intento se repetirá de la misma manera, varias veces.
Esto funciona a favor de la resiliencia, pero no proporciona una tolerancia completa a errores: un servidor remoto podría aceptar la conexión, pero podría no funcionar o funcionar mal.

Puede especificar solo uno de los fragmentos (en este caso, el procesamiento de consultas debe denominarse remoto, en lugar de distribuido) o hasta cualquier número de fragmentos. En cada fragmento, puede especificar entre una y cualquier número de réplicas. Puede especificar un número diferente de réplicas para cada fragmento.

Puede especificar tantos clústeres como desee en la configuración.

Para ver los clústeres, utilice el ‘system.clusters’ tabla.

El motor distribuido permite trabajar con un clúster como un servidor local. Sin embargo, el clúster es inextensible: debe escribir su configuración en el archivo de configuración del servidor (mejor aún, para todos los servidores del clúster).

No hay compatibilidad con las tablas distribuidas que miran a otras tablas distribuidas (excepto en los casos en que una tabla distribuida solo tiene un fragmento). Como alternativa, haga que la tabla Distribuida mire el “final” tabla.

El motor distribuido requiere escribir clústeres en el archivo de configuración. Los clústeres del archivo de configuración se actualizan sobre la marcha, sin reiniciar el servidor. Si necesita enviar una consulta a un conjunto desconocido de fragmentos y réplicas cada vez, no necesita crear una tabla ‘remote’ función de tabla en su lugar. Vea la sección [Funciones de tabla](../../query_language/table_functions/index.md).

Hay dos métodos para escribir datos en un clúster:

Primero, puede definir a qué servidores escribir en qué datos y realizar la escritura directamente en cada fragmento. En otras palabras, realice INSERT en las tablas que la tabla distribuida “looks at”. Esta es la solución más flexible, ya que puede usar cualquier esquema de fragmentación, que podría ser no trivial debido a los requisitos del área temática. Esta es también la solución más óptima ya que los datos se pueden escribir en diferentes fragmentos de forma completamente independiente.

En segundo lugar, puede realizar INSERT en una tabla distribuida. En este caso, la tabla distribuirá los datos insertados a través de los propios servidores. Para escribir en una tabla distribuida, debe tener un conjunto de claves de fragmentación (el último parámetro). Además, si solo hay un fragmento, la operación de escritura funciona sin especificar la clave de fragmentación, ya que no significa nada en este caso.

Cada fragmento puede tener un peso definido en el archivo de configuración. Por defecto, el peso es igual a uno. Los datos se distribuyen entre fragmentos en la cantidad proporcional al peso del fragmento. Por ejemplo, si hay dos fragmentos y el primero tiene un peso de 9 mientras que el segundo tiene un peso de 10, el primero se enviará 9 / 19 partes de las filas, y el segundo se enviará 10 / 19.

Cada fragmento puede tener el ‘internal\_replication’ parámetro definido en el archivo de configuración.

Si este parámetro se establece en ‘true’, la operación de escritura selecciona la primera réplica en buen estado y escribe datos en ella. Utilice esta alternativa si la tabla Distribuida “looks at” tablas replicadas. En otras palabras, si la tabla donde se escribirán los datos los replicará por sí misma.

Si se establece en ‘false’ (el valor predeterminado), los datos se escriben en todas las réplicas. En esencia, esto significa que la tabla distribuida replica los datos en sí. Esto es peor que usar tablas replicadas, porque no se verifica la consistencia de las réplicas y, con el tiempo, contendrán datos ligeramente diferentes.

Para seleccionar el fragmento al que se envía una fila de datos, se analiza la expresión de fragmentación y su resto se toma de dividirlo por el peso total de los fragmentos. La fila se envía al fragmento que corresponde al medio intervalo de los restos de ‘prev\_weight’ a ‘prev\_weights + weight’, donde ‘prev\_weights’ es el peso total de los fragmentos con el número más pequeño, y ‘weight’ es el peso de este fragmento. Por ejemplo, si hay dos fragmentos, y el primero tiene un peso de 9 mientras que el segundo tiene un peso de 10, la fila se enviará al primer fragmento para los restos del rango \[0, 9), y al segundo para los restos del rango \[9, 19).

La expresión de fragmentación puede ser cualquier expresión de constantes y columnas de tabla que devuelva un entero. Por ejemplo, puede usar la expresión ‘rand()’ para la distribución aleatoria de datos, o ‘UserID’ para la distribución por el resto de dividir el ID del usuario (entonces los datos de un solo usuario residirán en un solo fragmento, lo que simplifica la ejecución de IN y JOIN por los usuarios). Si una de las columnas no se distribuye lo suficientemente uniformemente, puede envolverla en una función hash: intHash64(UserID) .

Un simple recordatorio de la división es una solución limitada para la fragmentación y no siempre es apropiado. Funciona para volúmenes medianos y grandes de datos (docenas de servidores), pero no para volúmenes muy grandes de datos (cientos de servidores o más). En este último caso, use el esquema de fragmentación requerido por el área asunto, en lugar de usar entradas en Tablas distribuidas.

Las consultas SELECT se envían a todos los fragmentos y funcionan independientemente de cómo se distribuyen los datos entre los fragmentos (se pueden distribuir completamente aleatoriamente). Cuando agrega un nuevo fragmento, no tiene que transferirle los datos antiguos. Puede escribir nuevos datos con un peso más pesado: los datos se distribuirán de manera ligeramente desigual, pero las consultas funcionarán correcta y eficientemente.

Debería preocuparse por el esquema de fragmentación en los siguientes casos:

-   Se utilizan consultas que requieren unir datos (IN o JOIN) mediante una clave específica. Si esta clave fragmenta datos, puede usar IN local o JOIN en lugar de GLOBAL IN o GLOBAL JOIN, que es mucho más eficiente.
-   Se usa una gran cantidad de servidores (cientos o más) con una gran cantidad de consultas pequeñas (consultas de clientes individuales: sitios web, anunciantes o socios). Para que las pequeñas consultas no afecten a todo el clúster, tiene sentido ubicar datos para un solo cliente en un solo fragmento. Alternativamente, como lo hemos hecho en Yandex.Metrica, puede configurar sharding de dos niveles: divida todo el clúster en “layers”, donde una capa puede consistir en varios fragmentos. Los datos de un único cliente se encuentran en una sola capa, pero los fragmentos se pueden agregar a una capa según sea necesario y los datos se distribuyen aleatoriamente dentro de ellos. Las tablas distribuidas se crean para cada capa y se crea una única tabla distribuida compartida para consultas globales.

Los datos se escriben de forma asíncrona. Cuando se inserta en la tabla, el bloque de datos se acaba de escribir en el sistema de archivos local. Los datos se envían a los servidores remotos en segundo plano tan pronto como sea posible. El período de envío de datos está gestionado por el [Distributed\_directory\_monitor\_sleep\_time\_ms](../settings/settings.md#distributed_directory_monitor_sleep_time_ms) y [Distributed\_directory\_monitor\_max\_sleep\_time\_ms](../settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) configuración. El `Distributed` el motor envía cada archivo con datos insertados por separado, pero puede habilitar el envío por lotes de archivos [distributed\_directory\_monitor\_batch\_inserts](../settings/settings.md#distributed_directory_monitor_batch_inserts) configuración. Esta configuración mejora el rendimiento del clúster al utilizar mejor los recursos de red y servidor local. Debe comprobar si los datos se envían correctamente comprobando la lista de archivos (datos en espera de ser enviados) en el directorio de la tabla: `/var/lib/clickhouse/data/database/table/`.

Si el servidor dejó de existir o tuvo un reinicio aproximado (por ejemplo, después de un error de dispositivo) después de un INSERT en una tabla distribuida, es posible que se pierdan los datos insertados. Si se detecta un elemento de datos dañado en el directorio de la tabla, se transfiere al ‘broken’ subdirectorio y ya no se utiliza.

Cuando la opción max\_parallel\_replicas está habilitada, el procesamiento de consultas se paralela en todas las réplicas dentro de un solo fragmento. Para obtener más información, consulte la sección [max\_parallel\_replicas](../settings/settings.md#settings-max_parallel_replicas).

## Virtual Columnas {#virtual-columns}

-   `_shard_num` — Contiene el `shard_num` (de `system.clusters`). Tipo: [UInt32](../../data_types/int_uint.md).

!!! note "Nota"
    Ya [`remote`](../../query_language/table_functions/remote.md)/`cluster` funciones de tabla crean internamente instancia temporal del mismo motor distribuido, `_shard_num` está disponible allí también.

**Ver también**

-   [Virtual columnas](index.md#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/distributed/) <!--hide-->
