---
description: 'El motor de tabla de Kafka se puede usar para trabajar con Apache Kafka y permite publicar o suscribirse
  a flujos de datos, organizar un almacenamiento tolerante a fallos y procesar flujos a medida que
  estén disponibles.'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Motor de tabla de Kafka'
keywords: ['Kafka', 'motor de tabla']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # Motor de tabla de Kafka
</div>

:::tip
Si usas ClickHouse Cloud, te recomendamos usar [ClickPipes](/es/integrations/clickpipes) en su lugar. ClickPipes admite de forma nativa conexiones de red privadas, el escalado independiente de la ingestión y de los recursos del clúster, y una monitorización exhaustiva para transmitir datos de Kafka en streaming a ClickHouse.
:::

* Publicar o suscribirse a flujos de datos.
* Organizar un almacenamiento tolerante a fallos.
* Procesar flujos a medida que estén disponibles.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

Parámetros obligatorios:

* `kafka_broker_list` — Una lista de brókeres separada por comas (por ejemplo, `localhost:9092`).
* `kafka_topic_list` — Una lista de topics de Kafka.
* `kafka_group_name` — Un grupo de consumidores de Kafka. Los offsets de lectura se registran por separado para cada grupo. Si no desea que los mensajes se dupliquen en el clúster, use el mismo nombre de grupo en todas partes.
* `kafka_format` — Formato del mensaje. Utiliza la misma notación que la función SQL `FORMAT`, como `JSONEachRow`. Para más información, consulte la sección [Formatos](../../../interfaces/formats.md).

Parámetros opcionales:

* `kafka_security_protocol` - Protocolo utilizado para comunicarse con los brókers. Valores posibles: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
* `kafka_sasl_mechanism` - Mecanismo SASL que se usará para la autenticación. Valores posibles: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`.
* `kafka_aws_region` - Región de AWS para la autenticación de MSK IAM. Se detecta automáticamente a partir de la dirección del bróker si no se especifica. Indíquelo explícitamente cuando use alias de PrivateLink o hostnames DNS personalizados que no contengan información de la región. Predeterminado: vacío (detección automática).
* `kafka_sasl_username` - Nombre de usuario de SASL para usar con los mecanismos `PLAIN` y `SASL-SCRAM-..`.
* `kafka_sasl_password` - Contraseña de SASL para usar con los mecanismos `PLAIN` y `SASL-SCRAM-..`.
* `kafka_schema` — Parámetro que debe usarse si el formato requiere una definición de schema. Por ejemplo, [Cap&#39;n Proto](https://capnproto.org/) requiere la ruta al archivo de schema y el nombre del objeto raíz `schema.capnp:Message`.
* `kafka_schema_registry_skip_bytes` — Número de bytes que se omiten desde el inicio de cada mensaje cuando se usa schema registry con headers de envoltura (por ejemplo, AWS Glue Schema Registry, que incluye una envoltura de 19 bytes). Rango: `[0, 255]`. Predeterminado: `0`.
* `kafka_num_consumers` — Número de consumers por tabla. Especifique más consumers si el throughput de un consumer es insuficiente. El número total de consumers no debe exceder el número de partitions del topic, ya que solo se puede asignar un consumer por partition, y no debe ser mayor que el número de núcleos físicos del servidor donde se implementa ClickHouse. Predeterminado: `1`.
* `kafka_max_block_size` — Tamaño máximo del lote (en mensajes) para `poll`. Predeterminado: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `kafka_skip_broken_messages` — Tolerancia del analizador de mensajes de Kafka a mensajes incompatibles con el schema por bloque. Si `kafka_skip_broken_messages = N`, el motor omite *N* mensajes de Kafka que no se pueden analizar (un mensaje equivale a una fila de datos). Predeterminado: `0`.
* `kafka_commit_every_batch` — Hace commit de cada lote consumido y procesado, en lugar de un único commit después de escribir un bloque completo. Predeterminado: `0`.
* `kafka_client_id` — Identificador del client. Vacío de forma predeterminada.
* `kafka_poll_timeout_ms` — Timeout para una única operación de `poll` desde Kafka. Predeterminado: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `kafka_poll_max_batch_size` — Cantidad máxima de mensajes que se recuperarán en una única operación de `poll` de Kafka. Predeterminado: [max&#95;block&#95;size](/es/operations/settings/settings#max_block_size).
* `kafka_flush_interval_ms` — Timeout para hacer flush de datos desde Kafka. Predeterminado: [stream&#95;flush&#95;interval&#95;ms](/es/operations/settings/settings#stream_flush_interval_ms).
* `kafka_consumer_reschedule_ms` — Intervalo de reprogramación cuando el procesamiento de flujo de Kafka se detiene (por ejemplo, cuando no hay mensajes disponibles para consumir). Esta configuración controla la demora antes de que el consumer vuelva a intentar el sondeo. No debe exceder `kafka_consumers_pool_ttl_ms`. Predeterminado: `500` milisegundos.
* `kafka_thread_per_consumer` — Proporciona un hilo independiente para cada consumer. Cuando está habilitado, cada consumer hace flush de los datos de forma independiente, en paralelo (de lo contrario, las filas de varios consumers se combinan para formar un bloque). Predeterminado: `0`.
* `kafka_handle_error_mode` — Cómo manejar los errores del motor Kafka. Valores posibles: default (se lanzará una excepción si no se puede analizar un mensaje), stream (el mensaje de excepción y el mensaje sin procesar se guardarán en las columnas virtuales `_error` y `_raw_message`), dead&#95;letter&#95;queue (los datos relacionados con el error se guardarán en system.dead&#95;letter&#95;queue).
* `kafka_commit_on_select` — Hace commit de los mensajes cuando se ejecuta una consulta `SELECT`. Predeterminado: `false`.
* `kafka_consumer_acquire_timeout_ms` — Timeout en milisegundos para adquirir un consumer de Kafka durante consultas `SELECT` directas en una tabla `Kafka2` (con almacenamiento de offsets basado en Keeper). Cuando se ejecutan varias consultas `SELECT` directas concurrentes sobre la misma tabla, cada una debe esperar a que haya consumers disponibles. El timeout evita interbloqueos cuando las consultas retienen distintos subconjuntos de consumers. Predeterminado: `30000`.
* `kafka_max_rows_per_message` — El número máximo de filas escritas en un mensaje de Kafka para formatos basados en filas. Valor predeterminado: `1`.
* `kafka_autodetect_client_rack` — Establece automáticamente el parámetro `client.rack` para `librdkafka` a fin de dar preferencia a las réplicas de Kafka más cercanas.
  Fuentes compatibles:
  `AWS_ZONE_ID` para el ID de la zona de disponibilidad de AWS IMDSv2, por ejemplo `euc1-az1`;
  `AWS_ZONE_NAME` para el nombre de la zona de disponibilidad de AWS IMDSv2, por ejemplo `eu-central-1a`;
  `GCP_ZONE` para la zona del servicio de metadatos de GCP, por ejemplo `europe-central2-a`;
  `CLICKHOUSE` para usar la detección interna de ClickHouse, que puede basarse en metadatos de la nube o en la configuración;
  `AWS_ZONE_NAME_THEN_GCP_ZONE` para probar `AWS_ZONE_NAME` y luego `GCP_ZONE`.
  Valor predeterminado: cadena vacía, deshabilitado.
  Consejo: los distintos entornos usan formatos de zona de disponibilidad diferentes. Amazon MSK normalmente usa ID de zona, así que prefiera `AWS_ZONE_ID`. Confluent Cloud normalmente usa nombres de zona, así que prefiera `AWS_ZONE_NAME`. Si no está seguro, use `AWS_ZONE_NAME_THEN_GCP_ZONE` o compruebe el valor de `broker.rack` en su clúster.
  Nota: los brókeres de Kafka deben estar configurados con `broker.rack` y `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
* `kafka_compression_codec` — Códec de compresión usado para producir mensajes. Valores admitidos: cadena vacía, `none`, `gzip`, `snappy`, `lz4`, `zstd`. Si se usa una cadena vacía, la tabla no establece el códec de compresión, por lo que se usarán los valores de los archivos de configuración o el valor predeterminado de `librdkafka`. Valor predeterminado: cadena vacía.
* `kafka_compression_level` — Parámetro de nivel de compresión para el algoritmo seleccionado por kafka&#95;compression&#95;codec. Los valores más altos darán como resultado una mejor compresión a costa de un mayor uso de CPU. El intervalo utilizable depende del algoritmo: `[0-9]` para `gzip`; `[0-12]` para `lz4`; solo `0` para `snappy`; `[0-12]` para `zstd`; `-1` = nivel de compresión predeterminado dependiente del códec. Valor predeterminado: `-1`.
* `kafka_map_virtual_columns_on_write` — Si está habilitado, las columnas con nombres especiales `_key`, `_timestamp`, `_headers.name` y `_headers.value` en el esquema de la tabla se asignan a los metadatos correspondientes del mensaje de Kafka en `INSERT` y se excluyen de la carga útil del mensaje. Consulte [Correspondencia de columnas con metadatos de mensajes de Kafka](#mapping-columns-to-kafka-message-metadata). Valor predeterminado: `false`.

Ejemplos:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos. Si es posible, migre los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
El motor de tabla de Kafka no admite columnas con [valor predeterminado](/es/sql-reference/statements/create/table#default_values). Si necesita columnas con valor predeterminado, puede añadirlas en la vista materializada (véase más abajo).
:::

<div id="description">
  ## Descripción
</div>

Los mensajes entregados se registran automáticamente, por lo que cada mensaje de un grupo solo se cuenta una vez. Si desea recibir los datos dos veces, cree una copia de la tabla con otro nombre de grupo.

Los grupos son flexibles y se sincronizan en todo el clúster. Por ejemplo, si tiene 10 topics y 5 copias de una tabla en un clúster, a cada copia se le asignan 2 topics. Si cambia el número de copias, los topics se redistribuyen automáticamente entre ellas. Lea más sobre esto en http://kafka.apache.org/intro.

Se recomienda que cada topic de Kafka tenga su propio grupo de consumidores dedicado, para garantizar una relación exclusiva entre el topic y el grupo, especialmente en entornos donde los topics pueden crearse y eliminarse dinámicamente (p. ej., en pruebas o staging).

`SELECT` no es especialmente útil para leer mensajes (excepto para depuración), porque cada mensaje solo puede leerse una vez. Es más práctico crear flujos en tiempo real mediante vistas materializadas. Para ello:

1. Use el motor para crear un consumidor de Kafka y trátelo como un flujo de datos.
2. Cree una tabla con la estructura deseada.
3. Cree una vista materializada que convierta los datos del motor y los inserte en una tabla creada previamente.

Cuando la `MATERIALIZED VIEW` se conecta al motor, empieza a recopilar datos en segundo plano. Esto le permite recibir continuamente mensajes de Kafka y convertirlos al formato requerido mediante `SELECT`.
Una tabla de Kafka puede tener tantas vistas materializadas como desee; no leen datos de la tabla de Kafka directamente, sino que reciben nuevos registros (en bloques). De este modo, puede escribir en varias tablas con distintos niveles de detalle (con agrupación - agregación y sin ella).

Ejemplo:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Para mejorar el rendimiento, los mensajes recibidos se agrupan en bloques del tamaño de [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size). Si el bloque no se forma en [stream&#95;flush&#95;interval&#95;ms](/es/operations/settings/settings#stream_flush_interval_ms) milisegundos, los datos se escribirán en la tabla independientemente de que el bloque esté completo.

Para dejar de recibir datos del topic o cambiar la lógica de conversión, desvincule la vista materializada:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si quieres cambiar la tabla de destino con `ALTER`, te recomendamos deshabilitar la vista materializada para evitar discrepancias entre la tabla de destino y los datos de la vista.

<div id="configuration">
  ## Configuración
</div>

Al igual que GraphiteMergeTree, el motor Kafka admite una configuración ampliada mediante el archivo de configuración de ClickHouse. Puede usar dos claves de configuración: global (debajo de `<kafka>`) y a nivel de topic (debajo de `<kafka><kafka_topic>`). La configuración global se aplica primero y, después, se aplica la configuración a nivel de topic (si existe).

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- Settings for producer -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

Para ver una lista de las posibles opciones de configuración, consulte la [referencia de configuración de librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). En la configuración de ClickHouse, use el guion bajo (`_`) en lugar del punto. Por ejemplo, `check.crcs=true` se convertirá en `<check_crcs>true</check_crcs>`.

<div id="kafka-aws-msk-iam">
  ### Autenticación IAM de AWS MSK
</div>

:::note
La autenticación IAM de AWS MSK requiere que ClickHouse esté compilado con compatibilidad con AWS S3 habilitada.
:::

AWS MSK admite autenticación basada en IAM, lo que permite conectarse a clusters de Kafka con credenciales de AWS en lugar de gestionar nombres de usuario y contraseñas por separado.

**Configuración básica:**

Establezca `kafka_sasl_mechanism = 'AWS_MSK_IAM'` en la configuración de la tabla:

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

La región de AWS se extrae automáticamente del endpoint del bróker mediante coincidencia de patrones:

* MSK aprovisionado: `b-X.cluster.kafka.<region>.amazonaws.com:9098`
* MSK sin servidor: `boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* VPC Endpoint: `vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**Credenciales de AWS:**

Las credenciales siempre se cargan desde `~/.aws/credentials` y `~/.aws/config` (archivos de perfil de AWS) si existen. Para habilitar también los perfiles de instancia de EC2, las variables de entorno (`AWS_ACCESS_KEY_ID`, etc.), los roles de tarea de ECS y otras fuentes automáticas de credenciales, añada lo siguiente a la configuración del servidor:

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

Esta configuración solo puede ser establecida por los administradores del servidor. Valor predeterminado: `false`.

**PrivateLink y DNS personalizado:**

Al usar alias de PrivateLink o nombres de host DNS personalizados que no incluyan información de la región, especifique explícitamente la región de AWS:

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**Permisos de IAM:**

Permisos para el consumidor (para leer mensajes):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

Permisos del productor (para escribir mensajes):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### Compatibilidad con Kerberos
</div>

Para trabajar con Kafka con compatibilidad con Kerberos, agregue el elemento secundario `security_protocol` con el valor `sasl_plaintext`. Basta con que el ticket de concesión de tickets de Kerberos se obtenga y quede almacenado en caché mediante las funciones del sistema operativo.
ClickHouse puede mantener las credenciales de Kerberos mediante un archivo keytab. Considere los elementos secundarios `sasl_kerberos_service_name`, `sasl_kerberos_keytab` y `sasl_kerberos_principal`.

Ejemplo:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_topic` — Topic de Kafka. Tipo de dato: `LowCardinality(String)`.
* `_key` — Clave del mensaje. Tipo de dato: `String`.
* `_offset` — Offset del mensaje. Tipo de dato: `UInt64`.
* `_timestamp` — Marca temporal del mensaje. Tipo de dato: `Nullable(DateTime)`.
* `_timestamp_ms` — Marca temporal del mensaje en milisegundos. Tipo de dato: `Nullable(DateTime64(3))`.
* `_partition` — Partición del topic de Kafka. Tipo de dato: `UInt64`.
* `_headers.name` — Array de claves de los encabezados del mensaje. Tipo de dato: `Array(String)`.
* `_headers.value` — Array de valores de los encabezados del mensaje. Tipo de dato: `Array(String)`.

Columnas virtuales adicionales cuando `kafka_handle_error_mode='stream'`:

* `_raw_message` - Mensaje sin procesar que no pudo analizarse correctamente. Tipo de dato: `String`.
* `_error` - Mensaje de excepción producido durante un error de análisis. Tipo de dato: `String`.

Nota: las columnas virtuales `_raw_message` y `_error` se rellenan solo en caso de excepción durante el análisis; siempre están vacías cuando el mensaje se ha analizado correctamente.

<div id="mapping-columns-to-kafka-message-metadata">
  ## Correspondencia entre columnas y metadatos de mensajes de Kafka
</div>

Al producir mensajes con `INSERT INTO`, el motor Kafka siempre usa una columna llamada `_key` (de tipo `String`) como clave del mensaje de Kafka y una columna llamada `_timestamp` (de tipo `DateTime`) como marca de tiempo del mensaje de Kafka, si esas columnas existen en la tabla. De forma predeterminada, estas columnas también aparecen en la carga útil del mensaje generado junto con las demás columnas.

Con `kafka_map_virtual_columns_on_write = 1`, el comportamiento cambia:

* `_key` (tipo `String`) — se asigna a la clave del mensaje de Kafka.
* `_timestamp` (tipo `DateTime`) — se asigna a la marca de tiempo del mensaje de Kafka.
* `_headers.name` (tipo `Array(String)`) y `_headers.value` (tipo `Array(String)`) — se asignan a los encabezados del mensaje de Kafka. Cada par `(_headers.name[i], _headers.value[i])` se convierte en un encabezado de Kafka. Como `_headers.name` y `_headers.value` comparten el prefijo Nested `_headers`, ClickHouse exige que ambos arrays tengan el mismo tamaño en cada fila.

Las columnas con estos nombres se **excluyen de la carga útil del mensaje** solo si sus tipos coinciden con los indicados arriba; de lo contrario, permanecen en la carga útil, por lo que los esquemas que reutilizan estos nombres casualmente para datos no relacionados siguen funcionando.

Ejemplo:

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

El mensaje de Kafka generado contiene el payload `{"event_json":"{\"a\":1}"}`, la clave `session-42`, la marca temporal actual y dos encabezados: `source=api` y `trace_id=abc-123`.

<div id="data-formats-support">
  ## Compatibilidad con formatos de datos
</div>

El motor Kafka admite todos los [formatos](../../../interfaces/formats.md) compatibles con ClickHouse.
El número de filas de un mensaje de Kafka depende de si el formato se basa en filas o en bloques:

* En los formatos basados en filas, el número de filas de un mensaje de Kafka puede controlarse con la configuración `kafka_max_rows_per_message`.
* En los formatos basados en bloques, no podemos dividir un bloque en partes más pequeñas, pero el número de filas de un bloque puede controlarse con la configuración general [max&#95;block&#95;size](/es/operations/settings/settings#max_block_size).

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## Motor para almacenar los desplazamientos confirmados en ClickHouse Keeper
</div>

<ExperimentalBadge />

Si `allow_experimental_kafka_offsets_storage_in_keeper` está habilitado, se pueden especificar dos ajustes adicionales para el motor de tabla de Kafka:

* `kafka_keeper_path` especifica la ruta de la tabla en ClickHouse Keeper
* `kafka_replica_name` especifica el nombre de la réplica en ClickHouse Keeper

Deben especificarse ambos ajustes o ninguno de los dos. Cuando se especifican ambos, se utilizará un motor Kafka nuevo y experimental. El nuevo motor no depende de almacenar los desplazamientos confirmados en Kafka, sino que los almacena en ClickHouse Keeper. Sigue intentando confirmar los desplazamientos en Kafka, pero solo depende de ellos cuando se crea la tabla. En cualquier otra circunstancia (si la tabla se reinicia o se recupera después de algún error), se usarán los desplazamientos almacenados en ClickHouse Keeper para reanudar el consumo de mensajes. Además del desplazamiento confirmado, también almacena cuántos mensajes se consumieron en el último lote, de modo que, si la inserción falla, se volverá a consumir la misma cantidad de mensajes, lo que permite la deduplicación si es necesario.

Ejemplo:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### Limitaciones conocidas
</div>

Como el nuevo motor es experimental, todavía no está listo para producción. La implementación presenta algunas limitaciones conocidas:

* Eliminar y volver a crear rápidamente la tabla, o especificar la misma ruta de ClickHouse Keeper para distintos motores, puede causar problemas. Como práctica recomendada, puedes usar `{uuid}` en `kafka_keeper_path` para evitar conflictos entre rutas.
* Para garantizar lecturas repetibles, los mensajes no pueden consumirse desde varias particiones en un solo hilo. Por otro lado, los consumidores de Kafka deben sondearse regularmente para mantenerlos activos. Como resultado de estos dos requisitos, decidimos permitir la creación de varios consumidores solo si `kafka_thread_per_consumer` está habilitado; de lo contrario, es demasiado complicado evitar problemas relacionados con el sondeo regular de los consumidores.

**Véase también**

* [Columnas virtuales](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/es/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)