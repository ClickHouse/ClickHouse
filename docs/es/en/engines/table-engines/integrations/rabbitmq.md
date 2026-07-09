---
description: 'Este motor permite integrar ClickHouse con RabbitMQ.'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'Motor de tabla de RabbitMQ'
doc_type: 'guide'
---

Este motor permite integrar ClickHouse con [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` le permite:

* Publicar o suscribirse a flujos de datos.
* Procesar flujos a medida que están disponibles.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

Parámetros obligatorios:

* `rabbitmq_host_port` – host:puerto (por ejemplo, `localhost:5672`).
* `rabbitmq_exchange_name` – Nombre del exchange de RabbitMQ.
* `rabbitmq_format` – Formato del mensaje. Utiliza la misma notación que la función SQL `FORMAT`, como `JSONEachRow`. Para obtener más información, consulta la sección [Formatos](../../../interfaces/formats.md).

Parámetros opcionales:

* `rabbitmq_exchange_type` – El tipo de exchange de RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. Predeterminado: `fanout`.
* `rabbitmq_routing_key_list` – Una lista de claves de enrutamiento separadas por comas.
* `rabbitmq_schema` – Parámetro que debe usarse si el formato requiere una definición de esquema. Por ejemplo, [Cap&#39;n Proto](https://capnproto.org/) requiere la ruta al archivo de esquema y el nombre del objeto raíz `schema.capnp:Message`.
* `rabbitmq_num_consumers` – El número de consumidores por tabla. Especifique más consumidores si el rendimiento de un consumidor no es suficiente. Predeterminado: `1`
* `rabbitmq_num_queues` – Número total de colas. Aumentar este número puede mejorar significativamente el rendimiento. Predeterminado: `1`.
* `rabbitmq_queue_base` - Especifique un prefijo para los nombres de las colas. Los casos de uso de esta configuración se describen a continuación.
* `rabbitmq_persistent` - Si se establece en 1 (true), el modo de entrega de la consulta de inserción se establecerá en 2 (marca los mensajes como &#39;persistent&#39;). Predeterminado: `0`.
* `rabbitmq_skip_broken_messages` – Tolerancia del analizador de mensajes de RabbitMQ a mensajes incompatibles con el esquema por bloque. Si `rabbitmq_skip_broken_messages = N`, el motor omite *N* mensajes de RabbitMQ que no pueden analizarse (un mensaje equivale a una fila de datos). Predeterminado: `0`.
* `rabbitmq_max_block_size` - Número de filas recopiladas antes de vaciar los datos de RabbitMQ. Predeterminado: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `rabbitmq_flush_interval_ms` - Tiempo de espera para vaciar los datos de RabbitMQ. Predeterminado: [stream&#95;flush&#95;interval&#95;ms](/es/operations/settings/settings#stream_flush_interval_ms).
* `rabbitmq_queue_settings_list` - permite establecer opciones de RabbitMQ al crear una cola. Opciones disponibles: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. La opción `durable` se habilita automáticamente para la cola.
* `rabbitmq_address` - Dirección para la conexión. Use esta configuración o `rabbitmq_host_port`.
* `rabbitmq_vhost` - vhost de RabbitMQ. Predeterminado: `'/'`.
* `rabbitmq_queue_consume` - Use colas definidas por el usuario y no realice ninguna configuración de RabbitMQ: declaración de exchanges, colas y bindings. Predeterminado: `false`.
* `rabbitmq_username` - Nombre de usuario de RabbitMQ.
* `rabbitmq_password` - Contraseña de RabbitMQ.
* `reject_unhandled_messages` - Rechaza mensajes (envía un acuse negativo de RabbitMQ) en caso de errores. Esta configuración se habilita automáticamente si hay un `x-dead-letter-exchange` definido en `rabbitmq_queue_settings_list`.
* `rabbitmq_commit_on_select` - Hace commit de los mensajes cuando se realiza una consulta SELECT. Predeterminado: `false`.
* `rabbitmq_max_rows_per_message` — El número máximo de filas escritas en un mensaje de RabbitMQ para formatos basados en filas. Predeterminado: `1`.
* `rabbitmq_empty_queue_backoff_start_ms` — Punto inicial de backoff para reprogramar la lectura si la cola de RabbitMQ está vacía.
* `rabbitmq_empty_queue_backoff_end_ms` — Punto final de backoff para reprogramar la lectura si la cola de RabbitMQ está vacía.
* `rabbitmq_empty_queue_backoff_step_ms` — Paso de backoff para reprogramar la lectura si la cola de RabbitMQ está vacía.
* `rabbitmq_handle_error_mode` — Cómo manejar los errores del motor RabbitMQ. Valores posibles: default (se lanzará la excepción si no se puede analizar un mensaje), stream (el mensaje de excepción y el mensaje sin procesar se guardarán en las columnas virtuales `_error` y `_raw_message`), dead&#95;letter&#95;queue (los datos relacionados con errores se guardarán en system.dead&#95;letter&#95;queue).

<div id="ssl-connection">
  ### Conexión SSL
</div>

Use `rabbitmq_secure = 1` o `amqps` en la dirección de conexión: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
El comportamiento predeterminado de la biblioteca utilizada no comprueba si la conexión TLS creada es lo bastante segura. Tanto si el certificado ha caducado como si es autofirmado, falta o no es válido, la conexión simplemente se permite. Es posible que en el futuro se implemente una comprobación más estricta de los certificados.

También se pueden añadir ajustes de formato junto con los ajustes relacionados con RabbitMQ.

Ejemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

La configuración del servidor de RabbitMQ debe añadirse en el archivo de configuración de ClickHouse.

Configuración requerida:

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Configuración adicional:

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## Descripción
</div>

`SELECT` no es especialmente útil para leer mensajes (salvo para depuración), porque cada mensaje solo puede leerse una vez. Es más práctico crear flujos en tiempo real mediante [vistas materializadas](../../../sql-reference/statements/create/view.md). Para ello:

1. Use el motor para crear un consumidor de RabbitMQ y trátelo como un flujo de datos.
2. Cree una tabla con la estructura deseada.
3. Cree una vista materializada que convierta los datos del motor y los inserte en una tabla creada previamente.

Cuando la `MATERIALIZED VIEW` se conecta al motor, empieza a recopilar datos en segundo plano. Esto le permite recibir continuamente mensajes de RabbitMQ y convertirlos al formato requerido mediante `SELECT`.
Una tabla RabbitMQ puede tener tantas vistas materializadas como desee.

Los datos pueden enrutarse según `rabbitmq_exchange_type` y la `rabbitmq_routing_key_list` especificada.
No puede haber más de un exchange por tabla. Un exchange puede compartirse entre varias tablas; esto permite enrutar datos a varias tablas al mismo tiempo.

Opciones de tipo de exchange:

* `direct` - El enrutamiento se basa en la coincidencia exacta de claves. Lista de claves de tabla de ejemplo: `key1,key2,key3,key4,key5`; la clave del mensaje puede ser igual a cualquiera de ellas.
* `fanout` - Enrutamiento a todas las tablas (donde el nombre del exchange es el mismo), independientemente de las claves.
* `topic` - El enrutamiento se basa en patrones con claves separadas por puntos. Ejemplos: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
* `headers` - El enrutamiento se basa en coincidencias `key=value` con una configuración `x-match=all` o `x-match=any`. Lista de claves de tabla de ejemplo: `x-match=all,format=logs,type=report,year=2020`.
* `consistent_hash` - Los datos se distribuyen uniformemente entre todas las tablas vinculadas (donde el nombre del exchange es el mismo). Tenga en cuenta que este tipo de exchange debe habilitarse con el plugin de RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

La configuración `rabbitmq_queue_base` puede usarse en los siguientes casos:

* para permitir que distintas tablas compartan colas, de modo que puedan registrarse varios consumidores para las mismas colas, lo que mejora el rendimiento. Si se usan las configuraciones `rabbitmq_num_consumers` y/o `rabbitmq_num_queues`, se logra una coincidencia exacta de las colas cuando estos parámetros son los mismos.
* para poder restaurar la lectura desde determinadas colas persistentes cuando no todos los mensajes se hayan consumido correctamente. Para reanudar el consumo desde una cola específica, establezca su nombre en la configuración `rabbitmq_queue_base` y no especifique `rabbitmq_num_consumers` ni `rabbitmq_num_queues` (el valor predeterminado es 1). Para reanudar el consumo desde todas las colas declaradas para una tabla específica, simplemente especifique la misma configuración: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. De forma predeterminada, los nombres de las colas serán únicos para cada tabla.
* para reutilizar colas, ya que se declaran como persistentes y no se eliminan automáticamente. (Pueden eliminarse mediante cualquiera de las herramientas CLI de RabbitMQ).

Para mejorar el rendimiento, los mensajes recibidos se agrupan en bloques del tamaño de [max&#95;insert&#95;block&#95;size](/es/operations/settings/settings#max_insert_block_size). Si el bloque no se formó dentro de [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) milisegundos, los datos se volcarán en la tabla independientemente de que el bloque esté completo.

Si las configuraciones `rabbitmq_num_consumers` y/o `rabbitmq_num_queues` se especifican junto con `rabbitmq_exchange_type`, entonces:

* el plugin `rabbitmq-consistent-hash-exchange` debe estar habilitado.
* debe especificarse la propiedad `message_id` de los mensajes publicados (única para cada mensaje/lote).

Para la consulta INSERT, hay metadatos del mensaje que se agregan a cada mensaje publicado: `messageID` y el indicador `republished` (true si se publicó más de una vez); puede accederse a ellos a través de los encabezados del mensaje.

No use la misma tabla para inserciones y vistas materializadas.

Ejemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_exchange_name` - Nombre del exchange de RabbitMQ. Tipo de dato: `String`.
* `_channel_id` - ChannelID en el que se declaró el consumidor que recibió el mensaje. Tipo de dato: `String`.
* `_delivery_tag` - DeliveryTag del mensaje recibido. Su ámbito se limita al canal. Tipo de dato: `UInt64`.
* `_redelivered` - indicador `redelivered` del mensaje. Tipo de dato: `UInt8`.
* `_message_id` - messageID del mensaje recibido; no está vacío si se estableció al publicar el mensaje. Tipo de dato: `String`.
* `_timestamp` - timestamp del mensaje recibido; no está vacío si se estableció al publicar el mensaje. Tipo de dato: `UInt64`.

Columnas virtuales adicionales cuando `rabbitmq_handle_error_mode='stream'`:

* `_raw_message` - Mensaje sin procesar que no pudo analizarse correctamente. Tipo de dato: `Nullable(String)`.
* `_error` - Mensaje de excepción generado durante un error de análisis. Tipo de dato: `Nullable(String)`.

Nota: las columnas virtuales `_raw_message` y `_error` se rellenan solo en caso de excepción durante el análisis; siempre son `NULL` cuando el mensaje se analiza correctamente.

<div id="caveats">
  ## Consideraciones
</div>

Aunque puede especificar [expresiones de columna predeterminadas](/es/sql-reference/statements/create/table.md/#default_values) (como `DEFAULT`, `MATERIALIZED`, `ALIAS`) en la definición de la tabla, estas se ignorarán. En su lugar, las columnas se rellenarán con los valores predeterminados correspondientes a sus tipos.

<div id="data-formats-support">
  ## Compatibilidad con formatos de datos
</div>

El motor RabbitMQ admite todos los [formatos](../../../interfaces/formats.md) que admite ClickHouse.
El número de filas de un mensaje de RabbitMQ depende de si el formato es por filas o por bloques:

* En los formatos por filas, el número de filas de un mensaje de RabbitMQ se puede controlar mediante la configuración `rabbitmq_max_rows_per_message`.
* En los formatos por bloques, no se puede dividir un bloque en partes más pequeñas, pero el número de filas de un bloque se puede controlar mediante la configuración general [max&#95;block&#95;size](/es/operations/settings/settings#max_block_size).