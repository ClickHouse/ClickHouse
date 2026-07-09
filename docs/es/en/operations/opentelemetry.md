---
description: 'Guía para usar OpenTelemetry para el trazado distribuido y la recopilación de métricas
  en ClickHouse'
sidebar_label: 'Trazado de ClickHouse con OpenTelemetry'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'Trazado de ClickHouse con OpenTelemetry'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) es un estándar abierto para recopilar trazas y métricas de aplicaciones distribuidas. ClickHouse ofrece cierta compatibilidad con OpenTelemetry.

<div id="supplying-trace-context-to-clickhouse">
  ## Suministro de contexto de traza a ClickHouse
</div>

ClickHouse acepta encabezados HTTP de contexto de traza, como se describe en la [recomendación del W3C](https://www.w3.org/TR/trace-context/). También acepta contexto de traza a través de un protocolo nativo que se usa para la comunicación entre servidores ClickHouse o entre el cliente y el servidor. Para realizar pruebas manuales, se pueden proporcionar a `clickhouse-client` encabezados de contexto de traza conformes con la recomendación Trace Context mediante los indicadores `--opentelemetry-traceparent` y `--opentelemetry-tracestate`.

Si no se proporciona ningún contexto de traza padre o el contexto de traza proporcionado no cumple con el estándar W3C mencionado anteriormente, ClickHouse puede iniciar una nueva traza, con una probabilidad controlada por la configuración [opentelemetry&#95;start&#95;trace&#95;probability](/es/operations/settings/settings#opentelemetry_start_trace_probability).

<div id="propagating-the-trace-context">
  ## Propagación del contexto de traza
</div>

El contexto de traza se propaga a los servicios posteriores en los siguientes casos:

* Consultas a servidores remotos de ClickHouse, como al usar el motor de tabla [Distributed](../engines/table-engines/special/distributed.md).

* Función de tabla [url](../sql-reference/table-functions/url.md). La información del contexto de traza se envía en las cabeceras HTTP.

<div id="tracing-clickhouse-keeper-requests">
  ## Trazado de solicitudes de ClickHouse Keeper
</div>

ClickHouse admite el trazado de OpenTelemetry para las solicitudes de [ClickHouse Keeper](../guides/sre/keeper/index.md) (servicio de coordinación compatible con ZooKeeper). Esta funcionalidad ofrece visibilidad detallada del ciclo de vida de las operaciones de Keeper, desde el envío de la solicitud por parte del client hasta su procesamiento en el servidor.

<div id="enabling-keeper-tracing">
  ### Habilitar el trazado de Keeper
</div>

Para habilitar el trazado de las peticiones de Keeper, configure los siguientes ajustes en la configuración de su client de ZooKeeper/Keeper:

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Tipos de spans de Keeper
</div>

Cuando el trazado está habilitado, ClickHouse crea spans tanto para las operaciones de Keeper del lado del client como del lado del servidor:

**Spans del lado del client:**

* `zookeeper.create` — Crear un nodo nuevo
* `zookeeper.get` — Obtener los datos del nodo
* `zookeeper.set` — Establecer los datos del nodo
* `zookeeper.remove` — Eliminar un nodo
* `zookeeper.list` — Enumerar nodos hijo
* `zookeeper.exists` — Comprobar si existe un nodo
* `zookeeper.multi` — Ejecutar varias operaciones de forma atómica
* `zookeeper.client.requests_queue` — Tiempo dedicado al encolado de solicitudes antes de enviarlas

**Spans del lado del servidor (Keeper):**

* `keeper.receive_request` — Recepción y parsing de la solicitud del client
* `keeper.dispatcher.requests_queue` — Encolado de solicitudes en el despachador
* `keeper.write.pre_commit` — Preprocesamiento de solicitudes de escritura antes del Raft commit
* `keeper.write.commit` — Procesamiento de solicitudes de escritura después del Raft commit
* `keeper.read.wait_for_write` — Solicitudes de lectura en espera de escrituras dependientes
* `keeper.read.process` — Procesamiento de solicitudes de lectura
* `keeper.dispatcher.responses_queue` — Encolado de respuestas en el despachador
* `keeper.send_response` — Envío de la respuesta al client

<div id="sampling-and-performance">
  ### Muestreo y rendimiento
</div>

Para gestionar la sobrecarga del trazado, Keeper implementa muestreo dinámico. La tasa de muestreo se ajusta automáticamente entre 1/10,000 y 1/10 según el tamaño de la solicitud. De todas las solicitudes (muestreadas y no muestreadas), se registra la duración en métricas de histograma para supervisar el rendimiento.

<div id="tracing-the-clickhouse-itself">
  ## Trazas del propio ClickHouse
</div>

ClickHouse crea `trace spans` para cada consulta y para algunas de las etapas de ejecución de la consulta, como la planificación de consultas o las consultas distribuidas.

Para que sea útil, la información de trazado debe exportarse a un sistema de monitorización compatible con OpenTelemetry, como [Jaeger](https://jaegertracing.io/) o [Prometheus](https://prometheus.io/). ClickHouse evita depender de un sistema de monitorización concreto y, en su lugar, solo proporciona los datos de trazas a través de una tabla del sistema. La información de los spans de OpenTelemetry [requerida por el estándar](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) se almacena en la tabla [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md).

La tabla debe estar habilitada en la configuración del servidor; consulte el elemento `opentelemetry_span_log` en el archivo de configuración predeterminado `config.xml`. Está habilitada de forma predeterminada.

Las etiquetas o los atributos se guardan en dos arrays paralelos que contienen las claves y los valores. Use [ARRAY JOIN](../sql-reference/statements/select/array-join.md) para trabajar con ellos.

<div id="log-query-settings">
  ## Registro de configuración de consultas
</div>

La configuración [log&#95;query&#95;settings](settings/settings.md) permite registrar los cambios en la configuración de las consultas durante su ejecución. Cuando está habilitada, cualquier modificación realizada en la configuración de una consulta se registrará en el log del span de OpenTelemetry. Esta funcionalidad resulta especialmente útil en entornos de producción para hacer seguimiento de los cambios de configuración que pueden afectar al rendimiento de las consultas.

<div id="integration-with-monitoring-systems">
  ## Integración con sistemas de monitorización
</div>

Por el momento, no existe ninguna herramienta lista para usar que pueda exportar los datos de trazas de ClickHouse a un sistema de monitorización.

Para hacer pruebas, es posible configurar la exportación mediante una vista materializada con el motor [URL](../engines/table-engines/special/url.md) sobre la tabla [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md), que enviaría los datos de log a medida que lleguen a un endpoint HTTP de un collector de trazas. Por ejemplo, para enviar los datos mínimos de span a una instancia de Zipkin en ejecución en `http://localhost:9411`, en formato JSON v2 de Zipkin:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

En caso de error, se perderá sin previo aviso la parte de los datos de registro en la que se haya producido el error. Consulte el log del servidor para ver los mensajes de error si los datos no llegan.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Creación de una solución de observabilidad con ClickHouse - Parte 2 - Trazas](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)