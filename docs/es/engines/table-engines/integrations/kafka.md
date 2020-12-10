---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: Kafka
---

# Kafka {#kafka}

Este motor funciona con [Acerca de nosotros](http://kafka.apache.org/).

Kafka te permite:

-   Publicar o suscribirse a flujos de datos.
-   Organice el almacenamiento tolerante a fallos.
-   Secuencias de proceso a medida que estén disponibles.

## Creación de una tabla {#table_engine-kafka-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_row_delimiter = 'delimiter_symbol',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0]
```

Parámetros requeridos:

-   `kafka_broker_list` – A comma-separated list of brokers (for example, `localhost:9092`).
-   `kafka_topic_list` – A list of Kafka topics.
-   `kafka_group_name` – A group of Kafka consumers. Reading margins are tracked for each group separately. If you don't want messages to be duplicated in the cluster, use the same group name everywhere.
-   `kafka_format` – Message format. Uses the same notation as the SQL `FORMAT` función, tal como `JSONEachRow`. Para obtener más información, consulte [Formato](../../../interfaces/formats.md) apartado.

Parámetros opcionales:

-   `kafka_row_delimiter` – Delimiter character, which ends the message.
-   `kafka_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requiere la ruta de acceso al archivo de esquema y el nombre de la raíz `schema.capnp:Message` objeto.
-   `kafka_num_consumers` – The number of consumers per table. Default: `1`. Especifique más consumidores si el rendimiento de un consumidor es insuficiente. El número total de consumidores no debe exceder el número de particiones en el tema, ya que solo se puede asignar un consumidor por partición.
-   `kafka_max_block_size` - El tamaño máximo de lote (en mensajes) para la encuesta (predeterminado: `max_block_size`).
-   `kafka_skip_broken_messages` – Kafka message parser tolerance to schema-incompatible messages per block. Default: `0`. Si `kafka_skip_broken_messages = N` entonces el motor salta *N* Mensajes de Kafka que no se pueden analizar (un mensaje es igual a una fila de datos).
-   `kafka_commit_every_batch` - Confirmar cada lote consumido y manejado en lugar de una única confirmación después de escribir un bloque completo (predeterminado: `0`).

Ejemplos:

``` sql
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

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No utilice este método en nuevos proyectos. Si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```

</details>

## Descripci {#description}

Los mensajes entregados se realizan un seguimiento automático, por lo que cada mensaje de un grupo solo se cuenta una vez. Si desea obtener los datos dos veces, cree una copia de la tabla con otro nombre de grupo.

Los grupos son flexibles y se sincronizan en el clúster. Por ejemplo, si tiene 10 temas y 5 copias de una tabla en un clúster, cada copia obtiene 2 temas. Si el número de copias cambia, los temas se redistribuyen automáticamente entre las copias. Lea más sobre esto en http://kafka.apache.org/intro .

`SELECT` no es particularmente útil para leer mensajes (excepto para la depuración), ya que cada mensaje se puede leer solo una vez. Es más práctico crear subprocesos en tiempo real utilizando vistas materializadas. Para hacer esto:

1.  Use el motor para crear un consumidor de Kafka y considérelo como un flujo de datos.
2.  Crea una tabla con la estructura deseada.
3.  Cree una vista materializada que convierta los datos del motor y los coloque en una tabla creada previamente.

Cuando el `MATERIALIZED VIEW` se une al motor, comienza a recopilar datos en segundo plano. Esto le permite recibir continuamente mensajes de Kafka y convertirlos al formato requerido usando `SELECT`.
Una tabla kafka puede tener tantas vistas materializadas como desee, no leen datos de la tabla kafka directamente, sino que reciben nuevos registros (en bloques), de esta manera puede escribir en varias tablas con diferentes niveles de detalle (con agrupación - agregación y sin).

Ejemplo:

``` sql
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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Para mejorar el rendimiento, los mensajes recibidos se agrupan en bloques del tamaño de [Max\_insert\_block\_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). Si el bloque no se formó dentro de [Nombre de la red inalámbrica (SSID):](../../../operations/server-configuration-parameters/settings.md) milisegundos, los datos se vaciarán a la tabla independientemente de la integridad del bloque.

Para detener la recepción de datos de tema o cambiar la lógica de conversión, desconecte la vista materializada:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si desea cambiar la tabla de destino utilizando `ALTER`, recomendamos deshabilitar la vista de material para evitar discrepancias entre la tabla de destino y los datos de la vista.

## Configuración {#configuration}

Similar a GraphiteMergeTree, el motor Kafka admite una configuración extendida utilizando el archivo de configuración ClickHouse. Hay dos claves de configuración que puede usar: global (`kafka`) y a nivel de tema (`kafka_*`). La configuración global se aplica primero y, a continuación, se aplica la configuración de nivel de tema (si existe).

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

Para obtener una lista de posibles opciones de configuración, consulte [referencia de configuración librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Usa el guión bajo (`_`) en lugar de un punto en la configuración de ClickHouse. Por ejemplo, `check.crcs=true` será `<check_crcs>true</check_crcs>`.

## Virtual Columnas {#virtual-columns}

-   `_topic` — Kafka topic.
-   `_key` — Key of the message.
-   `_offset` — Offset of the message.
-   `_timestamp` — Timestamp of the message.
-   `_partition` — Partition of Kafka topic.

**Ver también**

-   [Virtual columnas](../index.md#table_engines-virtual_columns)

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) <!--hide-->
