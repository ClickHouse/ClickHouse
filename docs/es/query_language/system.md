# Consultas del sistema {#query-language-system}

-   [Cargar DICCIONARIOS](#query_language-system-reload-dictionaries)
-   [Cargar DICCIONARIO](#query_language-system-reload-dictionary)
-   [CATEGORÍA](#query_language-system-drop-dns-cache)
-   [CACHÉ DE LA MARCA DE LA GOTA](#query_language-system-drop-mark-cache)
-   [REGISTROS DE FLUSH](#query_language-system-flush_logs)
-   [CONFIGURACIÓN DE Carga](#query_language-system-reload-config)
-   [APAGADO](#query_language-system-shutdown)
-   [MATAR](#query_language-system-kill)
-   [PARADA DE SENTIDOS DISTRIBUIDOS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUIDO](#query_language-system-flush-distributed)
-   [COMIENZAR SENTIDOS DISTRIBUIDOS](#query_language-system-start-distributed-sends)
-   [PARADA DE FUSIONES](#query_language-system-stop-merges)
-   [COMIENZAR FUSIONES](#query_language-system-start-merges)

## Cargar DICCIONARIOS {#query-language-system-reload-dictionaries}

Vuelve a cargar todos los diccionarios que se han cargado correctamente antes.
De forma predeterminada, los diccionarios se cargan perezosamente (ver [Diccionarios\_lazy\_load](../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load)), por lo que en lugar de cargarse automáticamente al inicio, se inicializan en el primer acceso a través de la función dictGet o SELECT desde tablas con ENGINE = Dictionary . El `SYSTEM RELOAD DICTIONARIES` Consulta vuelve a cargar dichos diccionarios (LOADED).
Siempre vuelve `Ok.` independientemente del resultado de la actualización del diccionario.

## RELOAD DICTIONARY dictionary\_name {#query-language-system-reload-dictionary}

Recarga completamente un diccionario `dictionary_name`, independientemente del estado del diccionario (LOADED / NOT\_LOADED / FAILED).
Siempre vuelve `Ok.` independientemente del resultado de la actualización del diccionario.
El estado del diccionario se puede comprobar consultando el `system.dictionaries` tabla.

``` sql
SELECT name, status FROM system.dictionaries;
```

## CATEGORÍA {#query-language-system-drop-dns-cache}

Restablece la caché DNS interna de ClickHouse. A veces (para versiones anteriores de ClickHouse) es necesario usar este comando al cambiar la infraestructura (cambiar la dirección IP de otro servidor de ClickHouse o el servidor utilizado por los diccionarios).

Para obtener una administración de caché más conveniente (automática), consulte disable\_internal\_dns\_cache, dns\_cache\_update\_period parameters.

## CACHÉ DE LA MARCA DE LA GOTA {#query-language-system-drop-mark-cache}

Restablece la caché de marcas. Utilizado en el desarrollo de ClickHouse y pruebas de rendimiento.

## REGISTROS DE FLUSH {#query-language-system-flush-logs}

Vuelca los búferes de los mensajes de registro a las tablas del sistema (por ejemplo, el sistema.query\_log). Le permite no esperar 7,5 segundos al depurar.

## CONFIGURACIÓN DE Carga {#query-language-system-reload-config}

Vuelve a cargar la configuración de ClickHouse. Se usa cuando la configuración se almacena en ZooKeeeper.

## APAGADO {#query-language-system-shutdown}

Normalmente se apaga ClickHouse (como `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## MATAR {#query-language-system-kill}

Anula el proceso de ClickHouse (como `kill -9 {$ pid_clickhouse-server}`)

## Administración de tablas distribuidas {#query-language-system-distributed}

ClickHouse puede administrar [distribuido](../operations/table_engines/distributed.md) tabla. Cuando un usuario inserta datos en estas tablas, ClickHouse primero crea una cola de los datos que se deben enviar a los nodos del clúster y, a continuación, los envía de forma asincrónica. Puede administrar el procesamiento de colas con el [PARADA DE SENTIDOS DISTRIBUIDOS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUIDO](#query_language-system-flush-distributed), y [COMIENZAR SENTIDOS DISTRIBUIDOS](#query_language-system-start-distributed-sends) consulta. También puede insertar sincrónicamente datos distribuidos con el `insert_distributed_sync` configuración.

### PARADA DE SENTIDOS DISTRIBUIDOS {#query-language-system-stop-distributed-sends}

Deshabilita la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUIDO {#query-language-system-flush-distributed}

Obliga a ClickHouse a enviar datos a nodos de clúster de forma sincrónica. Si algún nodo no está disponible, ClickHouse produce una excepción y detiene la ejecución de la consulta. Puede volver a intentar la consulta hasta que tenga éxito, lo que sucederá cuando todos los nodos estén nuevamente en línea.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### COMIENZAR SENTIDOS DISTRIBUIDOS {#query-language-system-start-distributed-sends}

Habilita la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### PARADA DE FUSIONES {#query-language-system-stop-merges}

Proporciona la posibilidad de detener las fusiones en segundo plano para las tablas de la familia MergeTree:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "Nota"
    `DETACH / ATTACH` la tabla comenzará las fusiones de fondo para la tabla, incluso en caso de que las fusiones se hayan detenido para todas las tablas MergeTree antes.

### COMIENZAR FUSIONES {#query-language-system-start-merges}

Proporciona la posibilidad de iniciar fusiones en segundo plano para tablas de la familia MergeTree:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[Artículo Original](https://clickhouse.tech/docs/es/query_language/system/) <!--hide-->
