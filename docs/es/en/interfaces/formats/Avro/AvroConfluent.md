---
alias: []
description: 'Documentación sobre el formato AvroConfluent'
input_format: true
keywords: ['AvroConfluent']
output_format: true
slug: /interfaces/formats/AvroConfluent
title: 'AvroConfluent'
doc_type: 'reference'
---

import DataTypesMatching from './_snippets/data-types-matching.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

[Apache Avro](https://avro.apache.org/) es un formato de serialización orientado a filas que utiliza codificación binaria para procesar datos de forma eficiente. El formato `AvroConfluent` permite leer y escribir mensajes codificados en Avro mediante [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (o servicios compatibles con su API).

Cada mensaje utiliza el formato wire de Confluent: un byte mágico (`0x00`), seguido de un schema ID de 4 bytes en formato big-endian y, a continuación, el dato binario de Avro. Durante la lectura, ClickHouse resuelve el schema ID consultando el Schema Registry. Durante la escritura, ClickHouse registra el schema derivado de las columnas de salida y antepone el ID resultante a cada fila. Los schemas se almacenan en caché para un rendimiento óptimo.

<a id="data-types-matching" />

<div id="data-type-mapping">
  ## Correspondencia de tipos de datos
</div>

<DataTypesMatching />

<div id="format-settings">
  ## Configuración del formato
</div>

[//]: # "NOTA Estas configuraciones pueden establecerse a nivel de sesión, pero no es habitual, y documentarlo de forma demasiado destacada puede resultar confuso para los usuarios."

| Configuración                                    | Descripción                                                                                                                                                                                  | Predeterminado |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `input_format_avro_allow_missing_fields`         | Si se debe usar un valor predeterminado en lugar de generar un error cuando no se encuentra un campo en el esquema.                                                                          | `0`            |
| `input_format_avro_null_as_default`              | Si se debe usar un valor predeterminado en lugar de generar un error al insertar un valor `null` en una columna no nullable.                                                                 | `0`            |
| `format_avro_schema_registry_url`                | La URL de Confluent Schema Registry. Para la autenticación básica, se pueden incluir credenciales codificadas en URL directamente en la ruta de la URL.                                      |                |
| `format_avro_schema_registry_connection_timeout` | Tiempo de espera de conexión, en segundos, para el client HTTP de Schema Registry (se usa tanto para schema fetch como para el registro). Debe ser mayor que 0 y menor que 600 (10 minutos). | `1`            |
| `format_avro_schema_registry_send_timeout`       | Tiempo de espera de envío, en segundos, para el client HTTP de Schema Registry. Debe ser mayor que 0 y menor que 600 (10 minutos).                                                           | `1`            |
| `format_avro_schema_registry_receive_timeout`    | Tiempo de espera de recepción, en segundos, para el client HTTP de Schema Registry. Debe ser mayor que 0 y menor que 600 (10 minutos).                                                       | `1`            |
| `output_format_avro_confluent_subject`           | Para la salida: el subject name con el que se registra el esquema en Schema Registry. Obligatorio para la escritura.                                                                         |                |
| `output_format_avro_string_column_pattern`       | Para la salida: regexp de las columna String que se serializarán como Avro `string` (el valor predeterminado es `bytes`).                                                                    |                |

<div id="examples">
  ## Ejemplos
</div>

<div id="reading-from-kafka">
  ### Lectura desde Kafka
</div>

Para leer un topic de Kafka codificado en Avro mediante el [motor de tabla de Kafka](/es/engines/table-engines/integrations/kafka.md), utilice la configuración `format_avro_schema_registry_url` para indicar la URL del Schema Registry.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url';

SELECT * FROM topic1_stream;
```

<div id="writing-to-kafka">
  ### Escribir en Kafka
</div>

Para escribir mensajes AvroConfluent en un topic de Kafka, configure tanto la URL del Schema Registry como el nombre del subject. El esquema se registra automáticamente en el registro la primera vez que se escribe.

```sql
CREATE TABLE topic1_sink
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'http://schema-registry-url',
output_format_avro_confluent_subject = 'topic1-value';

INSERT INTO topic1_sink VALUES ('hello', 'world');
```

<div id="using-basic-authentication">
  #### Uso de la autenticación básica
</div>

Si el Schema Registry requiere autenticación básica (p. ej., si usa Confluent Cloud), puede proporcionar credenciales codificadas en URL en la configuración `format_avro_schema_registry_url`.

```sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url = 'https://<username>:<password>@schema-registry-url';
```

<div id="troubleshooting">
  ## Solución de problemas
</div>

Para supervisar el progreso de la ingestión y depurar errores del consumidor de Kafka, puede consultar la [tabla del sistema `system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md). Si su implementación tiene varias réplicas (p. ej., ClickHouse Cloud), debe usar la [función de tabla `clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md).

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

Si tienes problemas con la resolución del esquema, puedes usar [kafkacat](https://github.com/edenhill/kafkacat) con [clickhouse-local](/es/operations/utilities/clickhouse-local.md) para diagnosticar el problema:

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```