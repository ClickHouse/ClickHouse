---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 45
toc_title: Monitoreo
---

# Monitoreo {#monitoring}

Usted puede monitorear:

-   Utilización de recursos de hardware.
-   Métricas del servidor ClickHouse.

## Utilización de recursos {#resource-utilization}

ClickHouse no supervisa el estado de los recursos de hardware por sí mismo.

Se recomienda encarecidamente configurar la supervisión para:

-   Carga y temperatura en los procesadores.

    Usted puede utilizar [dmesg](https://en.wikipedia.org/wiki/Dmesg), [Turbostat](https://www.linux.org/docs/man8/turbostat.html) u otros instrumentos.

-   Utilización del sistema de almacenamiento, RAM y red.

## Métricas del servidor de Clickhouse {#clickhouse-server-metrics}

El servidor ClickHouse tiene instrumentos integrados para el monitoreo de estado propio.

Para realizar un seguimiento de los eventos del servidor, use los registros del servidor. Ver el [registrador](server_configuration_parameters/settings.md#server_configuration_parameters-logger) sección del archivo de configuración.

ClickHouse recoge:

-   Diferentes métricas de cómo el servidor utiliza recursos computacionales.
-   Común de la estadística en el procesamiento de la consulta.

Puede encontrar métricas en el [sistema.métricas](../operations/system_tables.md#system_tables-metrics), [sistema.evento](../operations/system_tables.md#system_tables-events), y [sistema.asynchronous\_metrics](../operations/system_tables.md#system_tables-asynchronous_metrics) tabla.

Puede configurar ClickHouse para exportar métricas a [Grafito](https://github.com/graphite-project). Ver el [Sección de grafito](server_configuration_parameters/settings.md#server_configuration_parameters-graphite) en el archivo de configuración del servidor ClickHouse. Antes de configurar la exportación de métricas, debe configurar Graphite siguiendo sus [guiar](https://graphite.readthedocs.io/en/latest/install.html).

Además, puede supervisar la disponibilidad del servidor a través de la API HTTP. Enviar el `HTTP GET` solicitud de `/ping`. Si el servidor está disponible, responde con `200 OK`.

Para supervisar servidores en una configuración de clúster, debe establecer [max\_replica\_delay\_for\_distributed\_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) parámetro y utilizar el recurso HTTP `/replicas_status`. Una solicitud para `/replicas_status` devoluciones `200 OK` si la réplica está disponible y no se retrasa detrás de las otras réplicas. Si una réplica se retrasa, devuelve `503 HTTP_SERVICE_UNAVAILABLE` con información sobre la brecha.
