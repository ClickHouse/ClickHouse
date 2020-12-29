---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: SYSTEM
---

# Consultas del sistema {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Vuelve a cargar todos los diccionarios que se han cargado correctamente antes.
De forma predeterminada, los diccionarios se cargan perezosamente (ver [Diccionarios\_lazy\_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)), por lo que en lugar de cargarse automáticamente al inicio, se inicializan en el primer acceso a través de la función dictGet o SELECT desde tablas con ENGINE = Dictionary . El `SYSTEM RELOAD DICTIONARIES` consulta vuelve a cargar dichos diccionarios (LOADED).
Siempre vuelve `Ok.` independientemente del resultado de la actualización del diccionario.

## RELOAD DICTIONARY Dictionary\_name {#query_language-system-reload-dictionary}

Recarga completamente un diccionario `dictionary_name`, independientemente del estado del diccionario (LOADED / NOT\_LOADED / FAILED).
Siempre vuelve `Ok.` independientemente del resultado de la actualización del diccionario.
El estado del diccionario se puede comprobar consultando el `system.dictionaries` tabla.

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Restablece la caché DNS interna de ClickHouse. A veces (para versiones anteriores de ClickHouse) es necesario usar este comando al cambiar la infraestructura (cambiar la dirección IP de otro servidor de ClickHouse o el servidor utilizado por los diccionarios).

Para obtener una administración de caché más conveniente (automática), consulte disable\_internal\_dns\_cache, dns\_cache\_update\_period parameters.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

Restablece la caché de marcas. Utilizado en el desarrollo de ClickHouse y pruebas de rendimiento.

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query\_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

Vuelve a cargar la configuración de ClickHouse. Se usa cuando la configuración se almacena en ZooKeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Normalmente se apaga ClickHouse (como `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Anula el proceso de ClickHouse (como `kill -9 {$ pid_clickhouse-server}`)

## Administración de tablas distribuidas {#query-language-system-distributed}

ClickHouse puede administrar [distribuido](../../engines/table-engines/special/distributed.md) tabla. Cuando un usuario inserta datos en estas tablas, ClickHouse primero crea una cola de los datos que se deben enviar a los nodos del clúster y, a continuación, los envía de forma asincrónica. Puede administrar el procesamiento de colas con el [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), y [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) consulta. También puede insertar sincrónicamente datos distribuidos con el `insert_distributed_sync` configuración.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Deshabilita la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Obliga a ClickHouse a enviar datos a nodos de clúster de forma sincrónica. Si algún nodo no está disponible, ClickHouse produce una excepción y detiene la ejecución de la consulta. Puede volver a intentar la consulta hasta que tenga éxito, lo que sucederá cuando todos los nodos estén nuevamente en línea.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Habilita la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

Proporciona la posibilidad de detener las fusiones en segundo plano para las tablas de la familia MergeTree:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "Nota"
    `DETACH / ATTACH` la tabla comenzará las fusiones de fondo para la tabla, incluso en caso de que las fusiones se hayan detenido para todas las tablas MergeTree antes.

### START MERGES {#query_language-system-start-merges}

Proporciona la posibilidad de iniciar fusiones en segundo plano para tablas de la familia MergeTree:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
