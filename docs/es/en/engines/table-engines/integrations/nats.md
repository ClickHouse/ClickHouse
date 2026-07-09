---
description: 'Este motor permite integrar ClickHouse con NATS para publicar o suscribirse
  a subjects de mensajes y procesar mensajes nuevos a medida que llegan.'
sidebar_label: 'NATS'
sidebar_position: 140
slug: /engines/table-engines/integrations/nats
title: 'Motor de tabla de NATS'
doc_type: 'guide'
---

Este motor permite integrar ClickHouse con [NATS](https://nats.io/).

`NATS` permite:

* Publicar o suscribirse a subjects de mensajes.
* Procesar mensajes nuevos a medida que llegan.

<div id="creating-a-table">
  ## Creación de una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

Parámetros obligatorios:

* `nats_url` – host:port (por ejemplo, `localhost:4222`)..
* `nats_subjects` – Lista de subjects a los que la tabla NATS se suscribe o publica. Admite subjects comodín como `foo.*.bar` o `baz.>`
* `nats_format` – Formato del mensaje. Usa la misma notación que la función SQL `FORMAT`, como `JSONEachRow`. Para obtener más información, consulte la sección [Formatos](../../../interfaces/formats.md).

Parámetros opcionales:

* `nats_schema` – Parámetro que debe usarse si el formato requiere una definición de esquema. Por ejemplo, [Cap&#39;n Proto](https://capnproto.org/) requiere la ruta al archivo de esquema y el nombre del objeto raíz `schema.capnp:Message`.
* `nats_stream` – El nombre de un stream existente en NATS JetStream.
* `nats_consumer_name` – El nombre de un durable pull consumer existente en NATS JetStream.
* `nats_num_consumers` – El número de consumers por tabla. Valor predeterminado: `1`. Especifique más consumers si el rendimiento de un consumer no es suficiente solo para NATS core.
* `nats_queue_group` – Nombre del grupo de cola de los suscriptores de NATS. El valor predeterminado es el nombre de la tabla.
* `nats_max_reconnect` – Obsoleto y sin efecto; la reconexión se realiza permanentemente con el tiempo de espera de `nats_reconnect_wait`.
* `nats_reconnect_wait` – Cantidad de tiempo, en milisegundos, que se espera entre cada intento de reconexión. Valor predeterminado: `2000`.
* `nats_server_list` - Lista de servidores para la conexión. Puede especificarse para conectarse a un clúster de NATS.
* `nats_skip_broken_messages` - Tolerancia del analizador de mensajes NATS a mensajes incompatibles con el esquema por bloque. Valor predeterminado: `0`. Si `nats_skip_broken_messages = N`, el motor omite *N* mensajes NATS que no pueden analizarse (un mensaje equivale a una fila de datos).
* `nats_max_block_size` - Número de filas recopiladas por sondeo(s) para vaciar datos desde NATS. Valor predeterminado: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `nats_flush_interval_ms` - Tiempo de espera para vaciar los datos leídos desde NATS. Valor predeterminado: [stream&#95;flush&#95;interval&#95;ms](/es/operations/settings/settings#stream_flush_interval_ms).
* `nats_username` - Nombre de usuario de NATS.
* `nats_password` - Contraseña de NATS.
* `nats_token` - Token de autenticación de NATS.
* `nats_credential_file` - Ruta a un archivo de credenciales de NATS.
* `nats_startup_connect_tries` - Número de intentos de conexión al inicio. Valor predeterminado: `5`.
* `nats_max_rows_per_message` — El número máximo de filas escritas en un mensaje NATS para formatos basados en filas. (valor predeterminado: `1`).
* `nats_handle_error_mode` — Cómo manejar los errores del motor NATS. Valores posibles: default (se lanzará una excepción si no se puede analizar un mensaje), stream (el mensaje de excepción y el mensaje sin procesar se guardarán en las columnas virtuales `_error` y `_raw_message`).

Conexión SSL:

Para una conexión segura, use `nats_secure = 1`.
La verificación del certificado se controla mediante la variable de entorno `CLICKHOUSE_NATS_TLS_SECURE`;
Si el certificado está vencido, es autofirmado, falta o no es válido por cualquier otro motivo, desactive la verificación estableciendo `CLICKHOUSE_NATS_TLS_SECURE=0`.

Escritura en la tabla NATS:

Si la tabla lee solo de un subject, cualquier inserción se publicará en ese mismo subject.
Sin embargo, si la tabla lee de varios subjects, es necesario especificar en qué subject queremos publicar.
Por eso, siempre que se inserten datos en una tabla con varios subjects, es necesario configurar `stream_like_engine_insert_queue`.
Puede seleccionar uno de los subjects de los que lee la tabla y publicar allí sus datos. Por ejemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

  INSERT INTO queue
  SETTINGS stream_like_engine_insert_queue = 'subject2'
  VALUES (1, 1);
```

También se pueden añadir ajustes de formato junto con los ajustes relacionados con nats.

Ejemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

La configuración del servidor NATS puede añadirse mediante el archivo de configuración de ClickHouse.
En concreto, puede añadir su contraseña para el motor NATS:

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

<div id="description">
  ## Descripción
</div>

`SELECT` no es especialmente útil para leer mensajes (excepto para tareas de depuración), porque cada mensaje solo puede leerse una vez. Es más práctico crear flujos en tiempo real mediante [vistas materializadas](../../../sql-reference/statements/create/view.md). Para ello:

1. Use el motor para crear un consumer de NATS y considérelo un flujo de datos.
2. Cree una tabla con la estructura deseada.
3. Cree una vista materializada que convierta los datos del motor y los inserte en una tabla creada previamente.

Cuando la `MATERIALIZED VIEW` se conecta al motor, empieza a recopilar datos en segundo plano. Esto le permite recibir continuamente mensajes de NATS y convertirlos al formato requerido mediante `SELECT`.
Una tabla de NATS puede tener tantas vistas materializadas como desee; no leen datos de la tabla directamente, sino que reciben nuevos registros (en bloques). De este modo, puede escribir en varias tablas con distintos niveles de detalle (con agrupación - agregación y sin ella).

Ejemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

Para dejar de recibir datos de streams o para cambiar la lógica de conversión, desvincule la vista materializada:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si quieres cambiar la tabla de destino mediante `ALTER`, te recomendamos deshabilitar la vista materializada para evitar discrepancias entre la tabla de destino y los datos de la vista.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_subject` - subject del mensaje de NATS. Tipo de dato: `String`.

Columnas virtuales adicionales cuando `nats_handle_error_mode='stream'`:

* `_raw_message` - Mensaje sin procesar que no pudo analizarse correctamente. Tipo de dato: `Nullable(String)`.
* `_error` - Mensaje de excepción generado durante un análisis fallido. Tipo de dato: `Nullable(String)`.

Nota: las columnas virtuales `_raw_message` y `_error` solo se rellenan en caso de excepción durante el análisis; siempre son `NULL` cuando el mensaje se analiza correctamente.

<div id="data-formats-support">
  ## Compatibilidad con formatos de datos
</div>

El motor NATS admite todos los [formatos](../../../interfaces/formats.md) compatibles con ClickHouse.
El número de filas en un mensaje de NATS depende de si el formato está basado en filas o en bloques:

* En los formatos basados en filas, el número de filas en un mensaje de NATS puede controlarse mediante la configuración `nats_max_rows_per_message`.
* En los formatos basados en bloques, no podemos dividir un bloque en partes más pequeñas, pero el número de filas de un bloque puede controlarse mediante la configuración general [max&#95;block&#95;size](/es/operations/settings/settings#max_block_size).

<div id="using-jetstream">
  ## Uso de JetStream
</div>

Antes de usar el motor NATS con NATS JetStream, debe crear un stream de NATS y un durable pull consumer. Para ello, puede usar, por ejemplo, la utilidad `nats` del paquete [NATS CLI](https://github.com/nats-io/natscli):

<details>
  <summary>creación de un stream</summary>

  ```bash
  $ nats stream add
  ? Stream Name stream_name
  ? Subjects stream_subject
  ? Storage file
  ? Replication 1
  ? Retention Policy Limits
  ? Discard Policy Old
  ? Stream Messages Limit -1
  ? Per Subject Messages Limit -1
  ? Total Stream Size -1
  ? Message TTL -1
  ? Max Message Size -1
  ? Duplicate tracking time window 2m0s
  ? Allow message Roll-ups No
  ? Allow message deletion Yes
  ? Allow purging subjects or the entire stream Yes
  Stream stream_name was created

  Information for Stream stream_name created 2025-10-03 14:12:51

                  Subjects: stream_subject
                  Replicas: 1
                   Storage: File

  Options:

                 Retention: Limits
           Acknowledgments: true
            Discard Policy: Old
          Duplicate Window: 2m0s
                Direct Get: true
         Allows Msg Delete: true
              Allows Purge: true
    Allows Per-Message TTL: false
            Allows Rollups: false

  Limits:

          Maximum Messages: unlimited
       Maximum Per Subject: unlimited
             Maximum Bytes: unlimited
               Maximum Age: unlimited
      Maximum Message Size: unlimited
         Maximum Consumers: unlimited

  State:

                  Messages: 0
                     Bytes: 0 B
            First Sequence: 0
             Last Sequence: 0
          Active Consumers: 0
  ```
</details>

<details>
  <summary>creación de un durable pull consumer</summary>

  ```bash
  $ nats consumer add
  ? Select a Stream stream_name
  ? Consumer name consumer_name
  ? Delivery target (empty for Pull Consumers) 
  ? Start policy (all, new, last, subject, 1h, msg sequence) all
  ? Acknowledgment policy explicit
  ? Replay policy instant
  ? Filter Stream by subjects (blank for all) 
  ? Maximum Allowed Deliveries -1
  ? Maximum Acknowledgments Pending 0
  ? Deliver headers only without bodies No
  ? Add a Retry Backoff Policy No
  Information for Consumer stream_name > consumer_name created 2025-10-03T14:13:51+03:00

  Configuration:

                      Name: consumer_name
                 Pull Mode: true
            Deliver Policy: All
                Ack Policy: Explicit
                  Ack Wait: 30.00s
             Replay Policy: Instant
           Max Ack Pending: 1,000
         Max Waiting Pulls: 512

  State:

    Last Delivered Message: Consumer sequence: 0 Stream sequence: 0
      Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0
          Outstanding Acks: 0 out of maximum 1,000
      Redelivered Messages: 0
      Unprocessed Messages: 0
             Waiting Pulls: 0 of maximum 512
  ```
</details>

Después de crear el stream y el durable pull consumer, puede crear una tabla con el motor NATS. Para ello, debe inicializar: nats&#95;stream, nats&#95;consumer&#95;name y nats&#95;subjects:

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS 
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```