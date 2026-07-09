---
description: 'Resumen de qué son las tablas del sistema y por qué son útiles.'
keywords: ['tablas del sistema', 'resumen']
sidebar_label: 'Resumen'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'Resumen de las tablas del sistema'
doc_type: 'referencia'
---

<div id="system-tables-introduction">
  ## Resumen de las tablas del sistema
</div>

Las tablas del sistema proporcionan información sobre:

* Los estados, procesos y entorno del servidor.
* Los procesos internos del servidor.
* Las opciones utilizadas al compilar el binario de ClickHouse.

Tablas del sistema:

* Están ubicadas en la base de datos `system`.
* Solo están disponibles para lectura de datos.
* No se pueden eliminar ni alterar, pero sí se pueden desvincular.

La mayoría de las tablas del sistema almacenan sus datos en RAM. Un servidor ClickHouse crea estas tablas del sistema al iniciarse.

A diferencia de otras tablas del sistema, las tablas de log del sistema [metric&#95;log](../../operations/system-tables/metric_log.md), [query&#95;log](../../operations/system-tables/query_log.md), [query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md), [trace&#95;log](../../operations/system-tables/trace_log.md), [part&#95;log](../../operations/system-tables/part_log.md), [crash&#95;log](../../operations/system-tables/crash_log.md), [text&#95;log](../../operations/system-tables/text_log.md) y [backup&#95;log](../../operations/system-tables/backup_log.md) usan el motor de tabla [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) y almacenan sus datos en un sistema de archivos de forma predeterminada. Si elimina una tabla del sistema de archivos, el servidor ClickHouse volverá a crear una vacía la próxima vez que se escriban datos. Si el esquema de una tabla del sistema cambia en una nueva versión, ClickHouse cambia el nombre de la tabla actual y crea una nueva.

Las tablas de log del sistema se pueden personalizar creando un archivo de configuración con el mismo nombre que la tabla en `/etc/clickhouse-server/config.d/`, o configurando los elementos correspondientes en `/etc/clickhouse-server/config.xml`. Los elementos que se pueden personalizar son:

* `database`: base de datos a la que pertenece la tabla de log del sistema. Esta opción está obsoleta actualmente. Todas las tablas de log del sistema están en la base de datos `system`.
* `table`: tabla en la que insertar datos.
* `partition_by`: especifica la expresión [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
* `ttl`: especifica la expresión [TTL](../../sql-reference/statements/alter/ttl.md) de la tabla.
* `flush_interval_milliseconds`: intervalo de vaciado de datos en disco.
* `engine`: proporciona la expresión completa del motor (empezando por `ENGINE =` ) con parámetros. Esta opción entra en conflicto con `partition_by` y `ttl`. Si se configuran juntas, el servidor generará una excepción y se cerrará.

Un ejemplo:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

Por defecto, el crecimiento de la tabla es ilimitado. Para controlar el tamaño de una tabla, puede usar la configuración de [TTL](/es/sql-reference/statements/alter/ttl) para eliminar registros de log obsoletos. También puede usar la función de particionamiento de las tablas con motor `MergeTree`.

<div id="system-tables-sources-of-system-metrics">
  ## Fuentes de las métricas del sistema
</div>

Para recopilar métricas del sistema, el servidor ClickHouse utiliza:

* La capacidad `CAP_NET_ADMIN`.
* [procfs](https://en.wikipedia.org/wiki/Procfs) (solo en Linux).

**procfs**

Si el servidor ClickHouse no tiene la capacidad `CAP_NET_ADMIN`, intenta usar `ProcfsMetricsProvider` como alternativa. `ProcfsMetricsProvider` permite recopilar métricas del sistema por consulta (de CPU y E/S).

Si procfs es compatible y está habilitado en el sistema, el servidor ClickHouse recopila estas métricas:

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` está deshabilitado de forma predeterminada en los kernels de Linux a partir de la versión 5.14.x.
Puede habilitarlo con `sudo sysctl kernel.task_delayacct=1` o creando un archivo `.conf` en `/etc/sysctl.d/` con `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## Tablas del sistema en ClickHouse Cloud
</div>

En ClickHouse Cloud, las tablas del sistema proporcionan información esencial sobre el estado y el rendimiento del servicio, igual que en las implementaciones autogestionadas. Algunas tablas del sistema funcionan a nivel de todo el clúster, especialmente las que obtienen sus datos de los nodos Keeper, que gestionan los metadatos distribuidos. Estas tablas reflejan el estado global del clúster y deben ser coherentes cuando se consultan desde nodos individuales. Por ejemplo, la tabla [`parts`](/es/operations/system-tables/parts) debe ser coherente independientemente del nodo desde el que se consulte:

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

Por el contrario, otras tablas del sistema son específicas de cada nodo, por ejemplo, las que están en memoria o las que conservan sus datos mediante el motor de tabla MergeTree. Esto es habitual en datos como logs y métricas. Esta persistencia garantiza que los datos históricos sigan estando disponibles para su análisis. Sin embargo, estas tablas específicas de cada nodo son intrínsecamente únicas para cada nodo.

En general, pueden aplicarse las siguientes reglas para determinar si una tabla del sistema es específica de un nodo:

* Tablas del sistema con el sufijo `_log`.
* Tablas del sistema que exponen métricas, por ejemplo `metrics`, `asynchronous_metrics`, `events`.
* Tablas del sistema que exponen procesos en curso, por ejemplo `processes`, `merges`.

Además, pueden crearse nuevas versiones de las tablas del sistema como resultado de actualizaciones o cambios en su esquema. Estas versiones se nombran con un sufijo numérico.

Por ejemplo, considere las tablas `system.query_log`, que contienen una fila para cada consulta ejecutada por el nodo:

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### Consultar varias versiones
</div>

Podemos consultar estas tablas de forma conjunta usando la función [`merge`](/es/sql-reference/table-functions/merge). Por ejemplo, la siguiente consulta identifica la consulta más reciente emitida al nodo de destino en cada tabla `query_log`:

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note No confíes en el sufijo numérico para determinar el orden
Aunque el sufijo numérico de las tablas puede sugerir el orden de los datos, nunca debe usarse como referencia. Por este motivo, usa siempre la función de tabla merge combinada con un filtro por fecha cuando quieras consultar rangos de fechas específicos.
:::

Es importante destacar que estas tablas siguen siendo **locales a cada nodo**.

<div id="querying-across-nodes">
  ### Consultas en todos los nodos
</div>

Para obtener una vista completa de todo el clúster, los usuarios pueden aprovechar la función [`clusterAllReplicas`](/es/sql-reference/table-functions/cluster) en combinación con la función `merge`. La función `clusterAllReplicas` permite consultar tablas del sistema en todas las réplicas del clúster &quot;default&quot;, consolidando los datos específicos de cada nodo en un resultado unificado. Cuando se combina con la función `merge`, puede utilizarse para abarcar todos los datos del sistema de una tabla específica en un clúster.

Este enfoque es especialmente valioso para la monitorización y la depuración de operaciones en todo el clúster, ya que permite a los usuarios analizar eficazmente el estado y el rendimiento de su implementación de ClickHouse Cloud.

:::note
ClickHouse Cloud proporciona clústeres con múltiples réplicas para redundancia y conmutación por error. Esto permite funciones como el escalado automático dinámico y las actualizaciones sin interrupciones. En un momento dado, es posible que se estén añadiendo nodos nuevos al clúster o eliminando nodos del clúster. Para omitir esos nodos, agregue `SETTINGS skip_unavailable_shards = 1` a las consultas que usan `clusterAllReplicas`, como se muestra a continuación.
:::

Por ejemplo, observe la diferencia al consultar la tabla `query_log`, que suele ser esencial para el análisis.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### Consultas entre nodos y versiones
</div>

Debido al versionado de las tablas del sistema, esto todavía no representa la totalidad de los datos del clúster. Al combinar lo anterior con la función `merge`, obtenemos un resultado preciso para nuestro intervalo de fechas:

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Tablas del sistema y un vistazo a los entresijos de ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* Blog: [Consultas esenciales de monitorización: parte 1: consultas INSERT](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* Blog: [Consultas esenciales de monitorización: parte 2: consultas SELECT](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)