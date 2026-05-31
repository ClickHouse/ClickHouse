/// This file is auto-generated from the Markdown documentation pages under
/// docs/en/interfaces/formats/ by tmp/gen_format_docs.py. The complete documentation
/// of every format is embedded here so that the standalone Markdown files can later be
/// dropped and regenerated from the embedded documentation.
#include <Formats/FormatFactory.h>

namespace DB
{

void registerFormatDocumentations(FormatFactory & factory)
{
    factory.setDocumentation("Arrow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format. It is designed for in-memory random access.

## Data types matching {#data-types-matching}

The table below shows the supported data types and how they correspond to ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                |
| `STRING`, `BINARY`                      | [String](/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`    |
| `INT64`                                 | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year) | `INT64`         |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) setting that allows to output [LowCardinality](/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type. Note that there might be unused values in `LowCardinality` dictionary, which can lead to unused values in Arrow `DICTIONARY` during output.

Unsupported Arrow data types: 
- `FIXED_SIZE_BINARY`
- `JSON`
- `UUID`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

You can insert Arrow data from a file into ClickHouse table using the following command:

```bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### Selecting data {#selecting-data}

You can select data from a ClickHouse table and save it into some file in the Arrow format using the following command:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

## Format settings {#format-settings}

| Setting                                                                                                                  | Description                                                                                        | Default      |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|--------------|
| `input_format_arrow_allow_missing_columns`                                                                               | Allow missing columns while reading Arrow input formats                                            | `1`          |
| `input_format_arrow_case_insensitive_column_matching`                                                                    | Ignore case when matching Arrow columns with CH columns.                                           | `0`          |
| `input_format_arrow_import_nested`                                                                                       | Obsolete setting, does nothing.                                                                    | `0`          |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`                                             | Skip columns with unsupported types while schema inference for format Arrow                        | `0`          |
| `output_format_arrow_compression_method`                                                                                 | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed) | `lz4_frame`  |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                                                                   | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                        | `1`          |
| `output_format_arrow_low_cardinality_as_dictionary`                                                                      | Enable output LowCardinality type as Dictionary Arrow type                                         | `0`          |
| `output_format_arrow_string_as_string`                                                                                   | Use Arrow String type instead of Binary for String columns                                         | `1`          |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                                                                  | Always use 64 bit integers for dictionary indexes in Arrow format                                  | `0`          |
| `output_format_arrow_use_signed_indexes_for_dictionary`                                                                  | Use signed integers for dictionary indexes in Arrow format                                         | `1`          |
)DOCS_MD"});

    factory.setDocumentation("ArrowStream", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`ArrowStream` is Apache Arrow's "stream mode" format. It is designed for in-memory stream processing.

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Avro", Documentation{
        .description = R"DOCS_MD(
import DataTypeMapping from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `Avro` format supports reading and writing [Avro data files](https://avro.apache.org/docs/++version++/specification/#object-container-files). This format expects self-describing messages with an embedded schema. If you're using Avro with a schema registry, refer to the [`AvroConfluent`](./AvroConfluent.md) format.

## Data type mapping {#data-type-mapping}

<DataTypeMapping/>

## Format settings {#format-settings}

| Setting                                     | Description                                                                                         | Default |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`    | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`         | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `output_format_avro_codec`                  | Compression algorithm for Avro output files. Possible values: `null`, `deflate`, `snappy`, `zstd`.            |         |
| `output_format_avro_sync_interval`          | Sync marker frequency in Avro files (in bytes). | `16384` |
| `output_format_avro_string_column_pattern`  | Regular expression to identify `String` columns for Avro string type mapping. By default, ClickHouse `String` columns are written as Avro `bytes` type.                                 |         |
| `output_format_avro_rows_in_file`           | Maximum number of rows per Avro output file. When this limit is reached, a new file is created (if the storage system supports file splitting).                                                         | `1`     |

## Examples {#examples}

### Reading Avro data {#reading-avro-data}

To read data from an Avro file into a ClickHouse table:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of the ingested Avro file must be of type `record`.

To find the correspondence between table columns and fields of Avro schema, ClickHouse compares their names. 
This comparison is case-sensitive and unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the corresponding column type.

While importing data, when a field is not found in the schema and setting [`input_format_avro_allow_missing_fields`](/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) is enabled, the default value will be used instead of throwing an error.

### Writing Avro data {#writing-avro-data}

To write data from a ClickHouse table into an Avro file:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

- Start with `[A-Za-z_]`
- Be followed by only `[A-Za-z0-9_]`

The output compression and sync interval for Avro files can be configured using the [`output_format_avro_codec`](/operations/settings/settings-formats.md/#output_format_avro_codec) and [`output_format_avro_sync_interval`](/operations/settings/settings-formats.md/#output_format_avro_sync_interval) settings, respectively.

### Inferring the Avro schema {#inferring-the-avro-schema}

Using the ClickHouse [`DESCRIBE`](/sql-reference/statements/describe-table) function, you can quickly view the inferred format of an Avro file like the following example. 
This example includes the URL of a publicly accessible Avro file in the ClickHouse S3 public bucket:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```
)DOCS_MD"});

    factory.setDocumentation("AvroConfluent", Documentation{
        .description = R"DOCS_MD(
import DataTypesMatching from './_snippets/data-types-matching.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Avro](https://avro.apache.org/) is a row-oriented serialization format that uses binary encoding for efficient data processing. The `AvroConfluent` format supports reading and writing Avro-encoded messages using the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) (or API-compatible services).

Each message uses the Confluent wire format: a magic byte (`0x00`) followed by a 4-byte big-endian schema ID, followed by the Avro binary datum. When reading, ClickHouse resolves the schema ID by querying the registry. When writing, ClickHouse registers the schema derived from the output columns and prepends the resulting ID to each row. Schemas are cached for optimal performance.

<a id="data-types-matching"></a>
## Data type mapping {#data-type-mapping}

<DataTypesMatching/>

## Format settings {#format-settings}

[//]: # "NOTE These settings can be set at a session-level, but this isn't common and documenting it too prominently can be confusing to users."

| Setting                                              | Description                                                                                         | Default |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------|
| `input_format_avro_allow_missing_fields`             | Whether to use a default value instead of throwing an error when a field is not found in the schema. | `0`     |
| `input_format_avro_null_as_default`                  | Whether to use a default value instead of throwing an error when inserting a `null` value into a non-nullable column. |   `0`   |
| `format_avro_schema_registry_url`                    | The Confluent Schema Registry URL. For basic authentication, URL-encoded credentials can be included directly in the URL path. |         |
| `format_avro_schema_registry_connection_timeout`     | Connection timeout in seconds for the Schema Registry HTTP client (used for both schema fetch and registration). Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `format_avro_schema_registry_send_timeout`           | Send timeout in seconds for the Schema Registry HTTP client. Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `format_avro_schema_registry_receive_timeout`        | Receive timeout in seconds for the Schema Registry HTTP client. Must be greater than 0 and less than 600 (10 minutes). | `1`     |
| `output_format_avro_confluent_subject`               | For output: the subject name under which the schema is registered in the Schema Registry. Required when writing. |         |
| `output_format_avro_string_column_pattern`           | For output: regexp of String columns to serialize as Avro `string` (default is `bytes`). |         |

## Examples {#examples}

### Reading from Kafka {#reading-from-kafka}

To read an Avro-encoded Kafka topic using the [Kafka table engine](/engines/table-engines/integrations/kafka.md), use the `format_avro_schema_registry_url` setting to provide the URL of the schema registry.

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

### Writing to Kafka {#writing-to-kafka}

To write AvroConfluent messages to a Kafka topic, set both the schema registry URL and the subject name. The schema is automatically registered with the registry on the first write.

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

#### Using basic authentication {#using-basic-authentication}

If your schema registry requires basic authentication (e.g., if you're using Confluent Cloud), you can provide URL-encoded credentials in the `format_avro_schema_registry_url` setting.

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

## Troubleshooting {#troubleshooting}

To monitor ingestion progress and debug errors with the Kafka consumer, you can query the [`system.kafka_consumers` system table](../../../operations/system-tables/kafka_consumers.md). If your deployment has multiple replicas (e.g., ClickHouse Cloud), you must use the [`clusterAllReplicas`](../../../sql-reference/table-functions/cluster.md) table function.

```sql
SELECT * FROM clusterAllReplicas('default',system.kafka_consumers)
ORDER BY assignments.partition_id ASC;
```

If you run into schema resolution issues, you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](/operations/utilities/clickhouse-local.md) to troubleshoot:

```bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```
)DOCS_MD"});

    factory.setDocumentation("BSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `BSONEachRow` format parses data as a sequence of Binary JSON (BSON) documents without any separator between them.
Each row is formatted as a single document and each column is formatted as a single BSON document field with the column name as a key.

## Data types matching {#data-types-matching}

For output it uses the following correspondence between ClickHouse types and BSON types:

| ClickHouse type                                                                                                       | BSON Type                                                                                                     |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [Bool](/sql-reference/data-types/boolean.md)                                                                  | `\x08` boolean                                                                                                |
| [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                  |
| [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md)      | `\x10` int32                                                                                                  |
| [Int32](/sql-reference/data-types/int-uint.md)                                                                | `\x10` int32                                                                                                  |
| [UInt32](/sql-reference/data-types/int-uint.md)                                                               | `\x12` int64                                                                                                  |
| [Int64/UInt64](/sql-reference/data-types/int-uint.md)                                                         | `\x12` int64                                                                                                  |
| [Float32/Float64](/sql-reference/data-types/float.md)                                                         | `\x01` double                                                                                                 |
| [Date](/sql-reference/data-types/date.md)/[Date32](/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                  |
| [DateTime](/sql-reference/data-types/datetime.md)                                                             | `\x12` int64                                                                                                  |
| [DateTime64](/sql-reference/data-types/datetime64.md)                                                         | `\x09` datetime                                                                                               |
| [Decimal32](/sql-reference/data-types/decimal.md)                                                             | `\x10` int32                                                                                                  |
| [Decimal64](/sql-reference/data-types/decimal.md)                                                             | `\x12` int64                                                                                                  |
| [Decimal128](/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Decimal256](/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [Int128/UInt128](/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Int256/UInt256](/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype or \x02 string if setting output_format_bson_string_as_string is enabled |
| [UUID](/sql-reference/data-types/uuid.md)                                                                     | `\x05` binary, `\x04` uuid subtype, size = 16                                                                 |
| [Array](/sql-reference/data-types/array.md)                                                                   | `\x04` array                                                                                                  |
| [Tuple](/sql-reference/data-types/tuple.md)                                                                   | `\x04` array                                                                                                  |
| [Named Tuple](/sql-reference/data-types/tuple.md)                                                             | `\x03` document                                                                                               |
| [Map](/sql-reference/data-types/map.md)                                                                       | `\x03` document                                                                                               |
| [IPv4](/sql-reference/data-types/ipv4.md)                                                                     | `\x10` int32                                                                                                  |
| [IPv6](/sql-reference/data-types/ipv6.md)                                                                     | `\x05` binary, `\x00` binary subtype                                                                          |

For input it uses the following correspondence between BSON types and ClickHouse types:

| BSON Type                                | ClickHouse Type                                                                                                                                                                                                                             |
|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `\x01` double                            | [Float32/Float64](/sql-reference/data-types/float.md)                                                                                                                                                                               |
| `\x02` string                            | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x03` document                          | [Map](/sql-reference/data-types/map.md)/[Named Tuple](/sql-reference/data-types/tuple.md)                                                                                                                                   |
| `\x04` array                             | [Array](/sql-reference/data-types/array.md)/[Tuple](/sql-reference/data-types/tuple.md)                                                                                                                                     |
| `\x05` binary, `\x00` binary subtype     | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)/[IPv6](/sql-reference/data-types/ipv6.md)                                                             |
| `\x05` binary, `\x02` old binary subtype | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x05` binary, `\x03` old uuid subtype   | [UUID](/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x05` binary, `\x04` uuid subtype       | [UUID](/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x07` ObjectId                          | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x08` boolean                           | [Bool](/sql-reference/data-types/boolean.md)                                                                                                                                                                                        |
| `\x09` datetime                          | [DateTime64](/sql-reference/data-types/datetime64.md)                                                                                                                                                                               |
| `\x0A` null value                        | [NULL](/sql-reference/data-types/nullable.md)                                                                                                                                                                                       |
| `\x0D` JavaScript code                   | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x0E` symbol                            | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x10` int32                             | [Int32/UInt32](/sql-reference/data-types/int-uint.md)/[Decimal32](/sql-reference/data-types/decimal.md)/[IPv4](/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/sql-reference/data-types/enum.md) |
| `\x12` int64                             | [Int64/UInt64](/sql-reference/data-types/int-uint.md)/[Decimal64](/sql-reference/data-types/decimal.md)/[DateTime64](/sql-reference/data-types/datetime64.md)                                                       |

Other BSON types are not supported. Additionally, it performs conversion between different integer types. 
For example, it is possible to insert a BSON `int32` value into ClickHouse as [`UInt8`](../../sql-reference/data-types/int-uint.md).

Big integers and decimals such as `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` can be parsed from a BSON Binary value with the `\x00` binary subtype. 
In this case, the format will validate that the size of the binary data equals the size of the expected value.

:::note
This format does not work properly on Big-Endian platforms.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a BSON file with the following data, named as `football.bson`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

### Reading data {#reading-data}

Read data using the `BSONEachRow` format:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output BSON files.
:::

## Format settings {#format-settings}

| Setting                                                                                                                                                                                               | Description                                                                                  | Default  |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|----------|
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Use BSON String type instead of Binary for String columns.                                   | `false`  |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Allow skipping columns with unsupported types while schema inference for format BSONEachRow. | `false`  |
)DOCS_MD"});

    factory.setDocumentation("Buffers", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`Buffers` is a very simple binary format for **ephemeral** data exchange, where both the consumer and producer already know the schema and column order.

Unlike [Native](./Native.md), it does **not** store column names, column types, or any extra metadata.

In this format, data is written and read by [blocks](/development/architecture#block) in a binary format. Buffers uses the same per-column binary representation as the [Native](./Native.md) format and respects the same Native format settings.

For each block, the following sequence is written:
1. Number of columns (UInt64, little-endian).
2. Number of rows (UInt64, little-endian).
3. For each column:
- Total byte size of the serialized column data (UInt64, little-endian).
- Serialized column data bytes, exactly as in the [Native](./Native.md) format.

## Example usage {#example-usage}

Write to a file:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

Read back with an explicit column types:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

If you have a table with same column types, you can populate it directly:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

Inspect the table:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("CSV", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).
When formatting, rows are enclosed in double quotes. A double quote inside a string is output as two double quotes in a row. 
There are no other rules for escaping characters. 

- Date and date-time are enclosed in double quotes. 
- Numbers are output without quotes.
- Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter). 
- Rows are separated using the Unix line feed (LF). 
- Arrays are serialized in CSV as follows: 
  - first, the array is serialized to a string as in TabSeparated format
  - The resulting string is output to CSV in double quotes.
- Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
By default, the delimiter is `,` 
See the [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter) setting for more information.
:::

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported.

Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF).
However, in violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored.
The line feed supports: Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types.

`NULL` is formatted according to setting [format_csv_null_representation](/operations/settings/settings-formats.md/#format_csv_null_representation) (the default value is `\N`).

In the input data, `ENUM` values can be represented as names or as ids. 
First, we try to match the input value to the ENUM name. 
If we fail and the input value is a number, we try to match this number to the ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) to optimize `ENUM` parsing.

## Example usage {#example-usage}

## Format settings {#format-settings}

| Setting                                                                                                                                                            | Description                                                                                                        | Default | Notes                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                     | the character to be considered as a delimiter in CSV data.                                                         | `,`     |                                                                                                                                                                                              |
| [format_csv_allow_single_quotes](/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                 | allow strings in single quotes.                                                                                    | `true`  |                                                                                                                                                                                              |
| [format_csv_allow_double_quotes](/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                 | allow strings in double quotes.                                                                                    | `true`  |                                                                                                                                                                                              | 
| [format_csv_null_representation](/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                 | custom NULL representation in CSV format.                                                                          | `\N`    |                                                                                                                                                                                              |   
| [input_format_csv_empty_as_default](/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                           | treat empty fields in CSV input as default values.                                                                 | `true`  | For complex default expressions, [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. | 
| [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                               | treat inserted enum values in CSV formats as enum indices.                                                         | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_best_effort_in_schema_inference](/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)     | use some tweaks and heuristics to infer schema in CSV format. If disabled, all fields will be inferred as Strings. | `true`  |                                                                                                                                                                                              |
| [input_format_csv_arrays_as_nested_csv](/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                   | when reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string.      | `false` |                                                                                                                                                                                              |
| [output_format_csv_crlf_end_of_line](/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                         | if it is set to true, end of line in CSV output format will be `\r\n` instead of `\n`.                             | `false` |                                                                                                                                                                                              |
| [input_format_csv_skip_first_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                           | skip the specified number of lines at the beginning of data.                                                       | `0`     |                                                                                                                                                                                              |
| [input_format_csv_detect_header](/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                 | automatically detect header with names and types in CSV format.                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_skip_trailing_empty_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                         | skip trailing empty lines at the end of data.                                                                      | `false` |                                                                                                                                                                                              |
| [input_format_csv_trim_whitespaces](/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                           | trim spaces and tabs in non-quoted CSV strings.                                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_allow_whitespace_or_tab_as_delimiter](/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter)   | Allow to use whitespace or tab as field delimiter in CSV strings.                                                  | `false` |                                                                                                                                                                                              |
| [input_format_csv_allow_variable_number_of_columns](/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)           | allow variable number of columns in CSV format, ignore extra columns and use default values on missing columns.    | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_default_on_bad_values](/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                         | Allow to set default value to column when CSV field deserialization failed on bad value.                           | `false` |                                                                                                                                                                                              |
| [input_format_csv_try_infer_numbers_from_strings](/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)               | Try to infer numbers from string fields while schema inference.                                                    | `false` |                                                                                                                                                                                              |
)DOCS_MD"});

    factory.setDocumentation("CSVWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints the header row with column names, similar to [TabSeparatedWithNames](/interfaces/formats/TabSeparatedWithNames).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

:::tip
Starting from [version](https://github.com/ClickHouse/ClickHouse/releases) 23.1, ClickHouse will automatically detect headers in CSV files when using the `CSV` format, so it is not necessary to use `CSVWithNames` or `CSVWithNamesAndTypes`.
:::

Using the following CSV file, named as `football.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

Create a table:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

Insert data using the `CSVWithNames` format:

```sql
INSERT INTO football FROM INFILE 'football.csv' FORMAT CSVWithNames;
```

### Reading data {#reading-data}

Read data using the `CSVWithNames` format:

```sql
SELECT *
FROM football
FORMAT CSVWithNames
```

The output will be a CSV with a single header row:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("CSVWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

:::tip
Starting from [version](https://github.com/ClickHouse/ClickHouse/releases) 23.1, ClickHouse will automatically detect headers in CSV files when using the `CSV` format, so it is not necessary to use `CSVWithNames` or `CSVWithNamesAndTypes`.
:::

Using the following CSV file, named as `football_types.csv`:

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
Date,Int16,LowCardinality(String),LowCardinality(String),Int8,Int8
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

Create a table:

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

Insert data using the `CSVWithNamesAndTypes` format:

```sql
INSERT INTO football FROM INFILE 'football_types.csv' FORMAT CSVWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `CSVWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT CSVWithNamesAndTypes
```

The output will be a CSV with a two header rows for column names and types:

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"Date","Int16","LowCardinality(String)","LowCardinality(String)","Int8","Int8"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

## Format settings {#format-settings}

:::note
If setting [input_format_with_names_use_header](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

:::note
If setting [input_format_with_types_use_header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("CapnProto", Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `CapnProto` format is a binary message format similar to the [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) format and [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), but not like [JSON](./JSON/JSON.md) or [MessagePack](https://msgpack.org/).
CapnProto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

See also [Format Schema](/interfaces/formats/#formatschema).

## Data types matching {#data_types-matching-capnproto}

The table below shows supported data types and how they match ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| CapnProto data type (`INSERT`)                       | ClickHouse data type                                                                                                                                                           | CapnProto data type (`SELECT`)                       |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`                                      | [UInt8](/sql-reference/data-types/int-uint.md)                                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/sql-reference/data-types/int-uint.md)                                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/sql-reference/data-types/int-uint.md), [Date](/sql-reference/data-types/date.md)                                                                     | `UINT16`                                             |
| `INT16`                                              | [Int16](/sql-reference/data-types/int-uint.md)                                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/sql-reference/data-types/int-uint.md), [DateTime](/sql-reference/data-types/datetime.md)                                                             | `UINT32`                                             |
| `INT32`                                              | [Int32](/sql-reference/data-types/int-uint.md), [Decimal32](/sql-reference/data-types/decimal.md)                                                              | `INT32`                                              |
| `UINT64`                                             | [UInt64](/sql-reference/data-types/int-uint.md)                                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/sql-reference/data-types/int-uint.md), [DateTime64](/sql-reference/data-types/datetime.md), [Decimal64](/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/sql-reference/data-types/string.md), [FixedString](/sql-reference/data-types/fixedstring.md)                                                         | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/sql-reference/data-types/date.md)                                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/sql-reference/data-types/enum.md)                                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/sql-reference/data-types/array.md)                                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/sql-reference/data-types/tuple.md)                                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/sql-reference/data-types/ipv4.md)                                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/sql-reference/data-types/ipv6.md)                                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/sql-reference/data-types/decimal.md)                                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/sql-reference/data-types/map.md)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

- Integer types can be converted into each other during input/output.
- For working with `Enum` in CapnProto format use the [format_capn_proto_enum_comparising_mode](/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) setting.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

## Example usage {#example-usage}

### Inserting and selecting data {#inserting-and-selecting-data-capnproto}

You can insert CapnProto data from a file into ClickHouse table by the following command:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Where the `schema.capnp` looks like this:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

You can select data from a ClickHouse table and save them into some file in the `CapnProto` format using the following command:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### Using autogenerated schema {#using-autogenerated-capn-proto-schema}

If you don't have an external `CapnProto` schema for your data, you can still output/input data in `CapnProto` format using autogenerated schema.

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

In this case, ClickHouse will autogenerate CapnProto schema according to the table structure using function [structureToCapnProtoSchema](/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) and will use this schema to serialize data in CapnProto format.

You can also read CapnProto file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

## Format settings {#format-settings}

The setting [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) is enabled by default and is applicable if [`format_schema`](/interfaces/formats#formatschema) is not set.

You can also save the autogenerated schema to a file during input/output using setting [`output_format_schema`](/operations/settings/formats#output_format_schema). 

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```
In this case, the autogenerated `CapnProto` schema will be saved in file `path/to/schema/schema.capnp`.
)DOCS_MD"});

    factory.setDocumentation("CustomSeparated", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to [Template](../Template/Template.md), but it prints or reads all names and types of columns and uses escaping rule from [format_custom_escaping_rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule) setting and delimiters from the following settings:

- [format_custom_field_delimiter](/operations/settings/settings-formats.md/#format_custom_field_delimiter)
- [format_custom_row_before_delimiter](/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
- [format_custom_row_after_delimiter](/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
- [format_custom_row_between_delimiter](/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
- [format_custom_result_before_delimiter](/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
- [format_custom_result_after_delimiter](/operations/settings/settings-formats.md/#format_custom_result_after_delimiter) 

:::note
It does not use escaping rules settings and delimiters from format strings.
:::

There is also the [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md) format, which is similar to [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

### Reading data {#reading-data}

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Read data using the `CustomSeparated` format:

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

The output will be in the configured custom format:

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

## Format settings {#format-settings}

Additional settings:

| Setting                                                                                                                                                        | Description                                                                                                                 | Default |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------|
| [input_format_custom_detect_header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                       | enables automatic detection of header with names and types if any.                                                          | `true`  |
| [input_format_custom_skip_trailing_empty_lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)               | skip trailing empty lines at the end of file.                                                                              | `false` |
| [input_format_custom_allow_variable_number_of_columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | allow variable number of columns in CustomSeparated format, ignore extra columns and use default values for missing columns. | `false` |
)DOCS_MD"});

    factory.setDocumentation("CustomSeparatedIgnoreSpaces", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     |        |       |

## Description {#description}

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('2022-04-30'; 2021; 'Sutton United'; 'Bradford City'; 1; 4), row( '2022-04-30'; 2021; 'Swindon Town'; 'Barrow'; 2; 1), row( '2022-04-30'; 2021; 'Tranmere Rovers'; 'Oldham Athletic'; 2; 0), row('2022-05-02'; 2021; 'Salford City'; 'Mansfield Town'; 2; 2), row('2022-05-02'; 2021; 'Port Vale'; 'Newport County'; 1; 2), row('2022-05-07'; 2021; 'Barrow'; 'Northampton Town'; 1; 3), row('2022-05-07'; 2021; 'Bradford City'; 'Carlisle United'; 2; 0), row('2022-05-07'; 2021; 'Bristol Rovers'; 'Scunthorpe United'; 7; 0), row('2022-05-07'; 2021; 'Exeter City'; 'Port Vale'; 0; 1), row('2022-05-07'; 2021; 'Harrogate Town A.F.C.'; 'Sutton United'; 0; 2), row('2022-05-07'; 2021; 'Hartlepool United'; 'Colchester United'; 0; 2), row('2022-05-07'; 2021; 'Leyton Orient'; 'Tranmere Rovers'; 0; 1), row('2022-05-07'; 2021; 'Mansfield Town'; 'Forest Green Rovers'; 2; 2), row('2022-05-07'; 2021; 'Newport County'; 'Rochdale'; 0; 2), row('2022-05-07'; 2021; 'Oldham Athletic'; 'Crawley Town'; 3; 3), row('2022-05-07'; 2021; 'Stevenage Borough'; 'Salford City'; 4; 2), row('2022-05-07'; 2021; 'Walsall'; 'Swindon Town'; 0; 3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedIgnoreSpaces;
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("CustomSeparatedIgnoreSpacesWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     |        |       |

## Description {#description}

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('date'; 'season'; 'home_team'; 'away_team'; 'home_team_goals'; 'away_team_goals'), row('2022-04-30'; 2021; 'Sutton United'; 'Bradford City'; 1; 4), row( '2022-04-30'; 2021; 'Swindon Town'; 'Barrow'; 2; 1), row( '2022-04-30'; 2021; 'Tranmere Rovers'; 'Oldham Athletic'; 2; 0), row('2022-05-02'; 2021; 'Salford City'; 'Mansfield Town'; 2; 2), row('2022-05-02'; 2021; 'Port Vale'; 'Newport County'; 1; 2), row('2022-05-07'; 2021; 'Barrow'; 'Northampton Town'; 1; 3), row('2022-05-07'; 2021; 'Bradford City'; 'Carlisle United'; 2; 0), row('2022-05-07'; 2021; 'Bristol Rovers'; 'Scunthorpe United'; 7; 0), row('2022-05-07'; 2021; 'Exeter City'; 'Port Vale'; 0; 1), row('2022-05-07'; 2021; 'Harrogate Town A.F.C.'; 'Sutton United'; 0; 2), row('2022-05-07'; 2021; 'Hartlepool United'; 'Colchester United'; 0; 2), row('2022-05-07'; 2021; 'Leyton Orient'; 'Tranmere Rovers'; 0; 1), row('2022-05-07'; 2021; 'Mansfield Town'; 'Forest Green Rovers'; 2; 2), row('2022-05-07'; 2021; 'Newport County'; 'Rochdale'; 0; 2), row('2022-05-07'; 2021; 'Oldham Athletic'; 'Crawley Town'; 3; 3), row('2022-05-07'; 2021; 'Stevenage Borough'; 'Salford City'; 4; 2), row('2022-05-07'; 2021; 'Walsall'; 'Swindon Town'; 0; 3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedIgnoreSpacesWithNames;
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("CustomSeparatedIgnoreSpacesWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     |        |       |

## Description {#description}

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('date'; 'season'; 'home_team'; 'away_team'; 'home_team_goals'; 'away_team_goals'), row('Date'; 'Int16'; 'LowCardinality(String)'; 'LowCardinality(String)'; 'Int8'; 'Int8'), row('2022-04-30'; 2021; 'Sutton United'; 'Bradford City'; 1; 4), row( '2022-04-30'; 2021; 'Swindon Town'; 'Barrow'; 2; 1), row( '2022-04-30'; 2021; 'Tranmere Rovers'; 'Oldham Athletic'; 2; 0), row('2022-05-02'; 2021; 'Salford City'; 'Mansfield Town'; 2; 2), row('2022-05-02'; 2021; 'Port Vale'; 'Newport County'; 1; 2), row('2022-05-07'; 2021; 'Barrow'; 'Northampton Town'; 1; 3), row('2022-05-07'; 2021; 'Bradford City'; 'Carlisle United'; 2; 0), row('2022-05-07'; 2021; 'Bristol Rovers'; 'Scunthorpe United'; 7; 0), row('2022-05-07'; 2021; 'Exeter City'; 'Port Vale'; 0; 1), row('2022-05-07'; 2021; 'Harrogate Town A.F.C.'; 'Sutton United'; 0; 2), row('2022-05-07'; 2021; 'Hartlepool United'; 'Colchester United'; 0; 2), row('2022-05-07'; 2021; 'Leyton Orient'; 'Tranmere Rovers'; 0; 1), row('2022-05-07'; 2021; 'Mansfield Town'; 'Forest Green Rovers'; 2; 2), row('2022-05-07'; 2021; 'Newport County'; 'Rochdale'; 0; 2), row('2022-05-07'; 2021; 'Oldham Athletic'; 'Crawley Town'; 3; 3), row('2022-05-07'; 2021; 'Stevenage Borough'; 'Salford City'; 4; 2), row('2022-05-07'; 2021; 'Walsall'; 'Swindon Town'; 0; 3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedIgnoreSpacesWithNamesAndTypes;
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("CustomSeparatedWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints the header row with column names, similar to [TabSeparatedWithNames](../TabSeparated/TabSeparatedWithNames.md).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedWithNames;
```

### Reading data {#reading-data}

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Read data using the `CustomSeparatedWithNames` format:

```sql
SELECT *
FROM football
FORMAT CustomSeparatedWithNames
```

The output will be in the configured custom format:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns from the table by their names, 
columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("CustomSeparatedWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](../TabSeparated/TabSeparatedWithNamesAndTypes.md).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('Date';'Int16';'LowCardinality(String)';'LowCardinality(String)';'Int8';'Int8'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedWithNamesAndTypes;
```

### Reading data {#reading-data}

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Read data using the `CustomSeparatedWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT CustomSeparatedWithNamesAndTypes
```

The output will be in the configured custom format:

```text
row('date';'season';'home_team';'away_team';'home_team_goals';'away_team_goals'),row('Date';'Int16';'LowCardinality(String)';'LowCardinality(String)';'Int8';'Int8'),row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

:::note
If setting [`input_format_with_types_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("DWARF", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

The `DWARF` format parses DWARF debug symbols from an ELF file (executable, library, or object file). 
It is similar to `dwarfdump`, but much faster (hundreds of MB/s) and supporting SQL. 
It produces one row for each Debug Information Entry (DIE) in the `.debug_info` section 
and includes "null"-entries that the DWARF encoding uses to terminate lists of children in the tree.

:::info
`.debug_info` consists of *units*, which correspond to compilation units: 
- Each unit is a tree of *DIE*s, with a `compile_unit` DIE as its root. 
- Each DIE has a *tag* and a list of *attributes*. 
- Each attribute has a *name* and a *value* (and also a *form*, which specifies how the value is encoded). 

The DIEs represent things from the source code, and their *tag* tells you what kind of thing it is. For example, there are:

- functions (tag = `subprogram`)
- classes/structs/enums (`class_type`/`structure_type`/`enumeration_type`)
- variables (`variable`)
- function arguments (`formal_parameter`).

The tree structure mirrors the corresponding source code. For example, a `class_type` DIE can contain `subprogram` DIEs representing methods of the class.
:::

The `DWARF` format outputs the following columns:

- `offset` - position of the DIE in the `.debug_info` section
- `size` - number of bytes in the encoded DIE (including attributes)
- `tag` - type of the DIE; the conventional "DW_TAG_" prefix is omitted
- `unit_name` - name of the compilation unit containing this DIE
- `unit_offset` - position of the compilation unit containing this DIE in the `.debug_info` section
- `ancestor_tags` - array of tags of the ancestors of the current DIE in the tree, in order from innermost to outermost
- `ancestor_offsets` - offsets of ancestors, parallel to `ancestor_tags`
- a few common attributes duplicated from the attributes array for convenience:
  - `name`
  - `linkage_name` - mangled fully qualified name; typically only functions have it (but not all functions)
  - `decl_file` - name of the source code file where this entity was declared
  - `decl_line` - line number in the source code where this entity was declared
- parallel arrays describing attributes:
  - `attr_name` - name of the attribute; the conventional "DW_AT_" prefix is omitted
  - `attr_form` - how the attribute is encoded and interpreted; the conventional DW_FORM_ prefix is omitted
  - `attr_int` - integer value of the attribute; 0 if the attribute doesn't have a numeric value
  - `attr_str` - string value of the attribute; empty if the attribute doesn't have a string value

## Example usage {#example-usage}

The `DWARF` format can be used to find compilation units that have the most function definitions (including template instantiations and functions from included header files):

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```
```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Form", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `Form` format can be used to read a single record in the application/x-www-form-urlencoded format 
in which data is formatted as `key1=value1&key2=value2`.

## Example usage {#example-usage}

Given a file `data.tmp` placed in the `user_files` path with some URL encoded data:

```text title="data.tmp"
t_page=116&c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10
```

```sql title="Query"
SELECT * FROM file(data.tmp, Form) FORMAT vertical;
```

```response title="Response"
Row 1:
──────
t_page:   116
c.e:      ls7xfkpm
c.tti.m:  raf
rt.start: navigation
rt.bmr:   390,11,10
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Hash", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `Hash` output format calculates a single hash value for all columns and rows of the result.
This is useful for calculating a "fingerprint" of the result, for example in situations where data transfer is the bottleneck.

## Example usage {#example-usage}

### Reading data {#reading-data}

Consider a table `football` with the following data:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Read data using the `Hash` format:

```sql
SELECT *
FROM football
FORMAT Hash
```

The query will process the data, but will not output anything.

```response
df2ec2f0669b000edff6adee264e7d68

1 rows in set. Elapsed: 0.154 sec.
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("HiveText", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSON", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `JSON` format reads and outputs data in the JSON format. 

The `JSON` format returns the following: 

| Parameter                    | Description                                                                                                                                                                                                                                |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `meta`                       | Column names and types.                                                                                                                                                                                                                    |
| `data`                       | Data tables                                                                                                                                                                                                                                |
| `rows`                       | The total number of output rows.                                                                                                                                                                                                           |
| `rows_before_limit_at_least` | The lower estimate of the number of rows there would have been without LIMIT. Output only if the query contains LIMIT. This estimate is calculated from the blocks of data processed in the query pipeline before the limit transform, but could then be discarded by the limit transform. If the blocks didn't even reach the limit transform in the query pipeline, they don't participate in the estimation. |
| `statistics`                 | Statistics such as `elapsed`, `rows_read`, `bytes_read`.                                                                                                                                                                                   |
| `totals`                     | Total values (when using WITH TOTALS).                                                                                                                                                                                                     |
| `extremes`                   | Extreme values (when extremes are set to 1).                                                                                                                                                                                               |

The `JSON` type is compatible with JavaScript. To ensure this, some characters are additionally escaped: 
- the slash `/` is escaped as `\/`
- alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. 
- ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t` , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. 
- Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. 

For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double quotes by default. 
To remove the quotes, you can set the configuration parameter [`output_format_json_quote_64bit_integers`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) to `0`.

ClickHouse supports [NULL](/sql-reference/syntax.md), which is displayed as `null` in the JSON output. To enable `+nan`, `-nan`, `+inf`, `-inf` values in output, set the [output_format_json_quote_denormals](/operations/settings/settings-formats.md/#output_format_json_quote_denormals) to `1`.

## Example usage {#example-usage}

Example:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## Format settings {#format-settings}

For JSON input format, if setting [`input_format_json_validate_types_from_metadata`](/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to `1`,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## See also {#see-also}

- [JSONEachRow](/interfaces/formats/JSONEachRow) format
- [output_format_json_array_of_rows](/operations/settings/settings-formats.md/#output_format_json_array_of_rows) setting
)DOCS_MD"});

    factory.setDocumentation("JSONAsObject", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

In this format, a single JSON object is interpreted as a single [JSON](/sql-reference/data-types/newjson.md) value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in `[]`, it is interpreted as an array of JSONs.

This format can only be parsed for a table with a single field of type [JSON](/sql-reference/data-types/newjson.md). The remaining columns must be set to [`DEFAULT`](/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view).

## Example usage {#example-usage}

### Basic example {#basic-example}

```sql title="Query"
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

```response title="Response"
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

### An array of JSON objects {#an-array-of-json-objects}

```sql title="Query"
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

```response title="Response"
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

### Columns with default values {#columns-with-default-values}

```sql title="Query"
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response title="Response"
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONAsString", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

In this format, a single JSON object is interpreted as a single value. 
If the input has several JSON objects (which are comma separated), they are interpreted as separate rows. 
If the input data is enclosed in `[]`, it is interpreted as an array of JSON objects.

:::note
This format can only be parsed for a table with a single field of type [String](/sql-reference/data-types/string.md). 
The remaining columns must be set to either [`DEFAULT`](/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view), 
or be omitted. 
:::

Once you serialize the entire JSON object to a String you can use the [JSON functions](/sql-reference/functions/json-functions.md) to process it.

## Example usage {#example-usage}

### Basic example {#basic-example}

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

### An array of JSON objects {#an-array-of-json-objects}

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONColumns", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

:::tip
The output of the JSONColumns* formats provides the ClickHouse field name and then the content of each row in the table for that field;
visually, the data is rotated 90 degrees to the left.
:::

In this format, all data is represented as a single JSON Object.

:::note
The `JSONColumns` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONColumns;
```

### Reading data {#reading-data}

Read data using the `JSONColumns` format:

```sql
SELECT *
FROM football
FORMAT JSONColumns
```

The output will be in JSON format:

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

## Format settings {#format-settings}

During import, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Columns that are not present in the block will be filled with default values (you can use the [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)
)DOCS_MD"});

    factory.setDocumentation("JSONColumnsWithMetadata", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONColumns`](./JSONColumns.md) format in that it also contains some metadata and statistics (similar to the [`JSON`](./JSON.md) format).

:::note
The `JSONColumnsWithMetadata` format buffers all data in memory and then outputs it as a single block, so, it can lead to high memory consumption.
:::

## Example usage {#example-usage}

Example:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

For the `JSONColumnsWithMetadata` input format, if setting [`input_format_json_validate_types_from_metadata`](/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to `1`,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompact", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [JSON](./JSON.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{
    "meta":
    [
        {
            "name": "date",
            "type": "Date"
        },
        {
            "name": "season",
            "type": "Int16"
        },
        {
            "name": "home_team",
            "type": "LowCardinality(String)"
        },
        {
            "name": "away_team",
            "type": "LowCardinality(String)"
        },
        {
            "name": "home_team_goals",
            "type": "Int8"
        },
        {
            "name": "away_team_goals",
            "type": "Int8"
        }
    ],
    "data":
    [
        ["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4],
        ["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1],
        ["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0],
        ["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2],
        ["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2],
        ["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3],
        ["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0],
        ["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0],
        ["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1],
        ["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2],
        ["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2],
        ["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1],
        ["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2],
        ["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2],
        ["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3],
        ["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2],
        ["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
    ]
}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompact;
```

### Reading data {#reading-data}

Read data using the `JSONCompact` format:

```sql
SELECT *
FROM football
FORMAT JSONCompact
```

The output will be in JSON format:

```json
{
    "meta":
    [
        {
            "name": "date",
            "type": "Date"
        },
        {
            "name": "season",
            "type": "Int16"
        },
        {
            "name": "home_team",
            "type": "LowCardinality(String)"
        },
        {
            "name": "away_team",
            "type": "LowCardinality(String)"
        },
        {
            "name": "home_team_goals",
            "type": "Int8"
        },
        {
            "name": "away_team_goals",
            "type": "Int8"
        }
    ],

    "data":
    [
        ["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4],
        ["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1],
        ["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0],
        ["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2],
        ["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2],
        ["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3],
        ["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0],
        ["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0],
        ["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1],
        ["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2],
        ["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2],
        ["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1],
        ["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2],
        ["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2],
        ["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3],
        ["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2],
        ["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
    ],

    "rows": 17,

    "statistics":
    {
        "elapsed": 0.223690876,
        "rows_read": 0,
        "bytes_read": 0
    }
}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactColumns", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

In this format, all data is represented as a single JSON Array.

:::note
The `JSONCompactColumns` output format buffers all data in memory to output it as a single block which can lead to high memory consumption.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
[
    ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactColumns;
```

### Reading data {#reading-data}

Read data using the `JSONCompactColumns` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactColumns
```

The output will be in JSON format:

```json
[
    ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
]
```

Columns that are not present in the block will be filled with default values (you can use [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONEachRow`](./JSONEachRow.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRow
```

The output will be in JSON format:

```json
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints the header row with column names, similar to the [`TabSeparatedWithNames`](../TabSeparated/TabSeparatedWithNames.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRowWithNames;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints two header rows with column names and types, similar to the [TabSeparatedWithNamesAndTypes](../TabSeparated/TabSeparatedWithNamesAndTypes.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactEachRowWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `JSONCompactEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
["2022-05-07", 2021, "Bristol Rovers", "Scunthorpe United", 7, 0]
["2022-05-07", 2021, "Exeter City", "Port Vale", 0, 1]
["2022-05-07", 2021, "Harrogate Town A.F.C.", "Sutton United", 0, 2]
["2022-05-07", 2021, "Hartlepool United", "Colchester United", 0, 2]
["2022-05-07", 2021, "Leyton Orient", "Tranmere Rovers", 0, 1]
["2022-05-07", 2021, "Mansfield Town", "Forest Green Rovers", 2, 2]
["2022-05-07", 2021, "Newport County", "Rochdale", 0, 2]
["2022-05-07", 2021, "Oldham Athletic", "Crawley Town", 3, 3]
["2022-05-07", 2021, "Stevenage Borough", "Salford City", 4, 2]
["2022-05-07", 2021, "Walsall", "Swindon Town", 0, 3]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [`input_format_with_types_use_header`](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

This format combines the compact row-by-row output of JSONCompactEachRow with streaming progress
information.
It outputs data as separate JSON objects for metadata, individual rows, progress updates,
totals, and exceptions. Values are represented in their native types.

Key features:
- Outputs metadata first with column names and types
- Each row is a separate JSON object with a "row" key containing an array of values
- Includes progress updates during query execution (as `{"progress":...}` objects)
- Supports totals and extremes
- Values keep their native types (numbers as numbers, strings as strings)

## Example usage {#example-usage}

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":[[-8], 46848.5225, ["2064-06-11 14:00:36.578","b06f4fa1-22ff-f84f-a1b7-a5807d983ae6"]]}
{"row":[[-76], -85331.598, ["2038-06-16 04:10:27.271","2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c"]]}
{"row":[[-32], -31470.8994, ["2027-07-18 16:58:34.654","1cdbae4c-ceb2-1337-b954-b175f5efbef8"]]}
{"row":[[-116], 32104.097, ["1979-04-27 21:51:53.321","66903704-3c83-8f8a-648a-da4ac1ffa9fc"]]}
{"row":[[], 2427.6614, ["1980-04-24 11:30:35.487","fee19be8-0f46-149b-ed98-43e7455ce2b2"]]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"335771"}}
{"rows_before_limit_at_least":5}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStrings", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `JSONCompactStrings` format differs from [JSONStrings](./JSONStrings.md) only in that data rows are output as arrays, not as objects.

## Example usage {#example-usage}

### Reading data {#reading-data}

Read data using the `JSONCompactStrings` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStrings
```

The output will be in JSON format:

```json
{
    "meta":
    [
            {
                    "name": "date",
                    "type": "Date"
            },
            {
                    "name": "season",
                    "type": "Int16"
            },
            {
                    "name": "home_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "away_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "home_team_goals",
                    "type": "Int8"
            },
            {
                    "name": "away_team_goals",
                    "type": "Int8"
            }
    ],

    "data":
    [
            ["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"],
            ["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"],
            ["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"],
            ["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"],
            ["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"],
            ["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"],
            ["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"],
            ["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"],
            ["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"],
            ["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"],
            ["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"],
            ["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"],
            ["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"],
            ["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"],
            ["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"],
            ["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"],
            ["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
    ],

    "rows": 17,

    "statistics":
    {
            "elapsed": 0.112012501,
            "rows_read": 0,
            "bytes_read": 0
    }
}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from [`JSONCompactEachRow`](./JSONCompactEachRow.md) only in that data fields are output as strings, not as typed JSON values.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRow
```

The output will be in JSON format:

```json
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONCompactEachRow`](./JSONCompactEachRow.md) format in that it also prints the header row with column names, similar to the [TabSeparatedWithNames](../TabSeparated/TabSeparatedWithNames.md) format.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRowWithNames;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRowWithNames` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRowWithNames
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from `JSONCompactEachRow` format in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](/interfaces/formats/TabSeparatedRawWithNamesAndTypes).

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `JSONCompactStringsEachRowWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT JSONCompactStringsEachRowWithNamesAndTypes
```

The output will be in JSON format:

```json
["date", "season", "home_team", "away_team", "home_team_goals", "away_team_goals"]
["Date", "Int16", "LowCardinality(String)", "LowCardinality(String)", "Int8", "Int8"]
["2022-04-30", "2021", "Sutton United", "Bradford City", "1", "4"]
["2022-04-30", "2021", "Swindon Town", "Barrow", "2", "1"]
["2022-04-30", "2021", "Tranmere Rovers", "Oldham Athletic", "2", "0"]
["2022-05-02", "2021", "Port Vale", "Newport County", "1", "2"]
["2022-05-02", "2021", "Salford City", "Mansfield Town", "2", "2"]
["2022-05-07", "2021", "Barrow", "Northampton Town", "1", "3"]
["2022-05-07", "2021", "Bradford City", "Carlisle United", "2", "0"]
["2022-05-07", "2021", "Bristol Rovers", "Scunthorpe United", "7", "0"]
["2022-05-07", "2021", "Exeter City", "Port Vale", "0", "1"]
["2022-05-07", "2021", "Harrogate Town A.F.C.", "Sutton United", "0", "2"]
["2022-05-07", "2021", "Hartlepool United", "Colchester United", "0", "2"]
["2022-05-07", "2021", "Leyton Orient", "Tranmere Rovers", "0", "1"]
["2022-05-07", "2021", "Mansfield Town", "Forest Green Rovers", "2", "2"]
["2022-05-07", "2021", "Newport County", "Rochdale", "0", "2"]
["2022-05-07", "2021", "Oldham Athletic", "Crawley Town", "3", "3"]
["2022-05-07", "2021", "Stevenage Borough", "Salford City", "4", "2"]
["2022-05-07", "2021", "Walsall", "Swindon Town", "0", "3"]
```

## Format settings {#format-settings}

:::note
If setting [input_format_with_names_use_header](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

:::note
If setting [input_format_with_types_use_header](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("JSONCompactStringsEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias  |
|-------|---------|--------|
| ✗     | ✔       |        |

## Description {#description}

Similar to [`JSONCompactEachRowWithProgress`](/interfaces/formats/JSONCompactEachRowWithProgress), but all values are converted to strings.
This is useful when you need consistent string representation of all data types.

Key features:
- Same structure as `JSONCompactEachRowWithProgress`
- All values are represented as strings (numbers, arrays, etc. are all quoted strings)
- Includes progress updates, totals, and exception handling
- Useful for clients that prefer or require string-based data

## Example usage {#example-usage}

### Inserting data {#inserting-data}

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactStringsEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":["[-8]", "46848.5225", "('2064-06-11 14:00:36.578','b06f4fa1-22ff-f84f-a1b7-a5807d983ae6')"]}
{"row":["[-76]", "-85331.598", "('2038-06-16 04:10:27.271','2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c')"]}
{"row":["[-32]", "-31470.8994", "('2027-07-18 16:58:34.654','1cdbae4c-ceb2-1337-b954-b175f5efbef8')"]}
{"row":["[-116]", "32104.097", "('1979-04-27 21:51:53.321','66903704-3c83-8f8a-648a-da4ac1ffa9fc')"]}
{"row":["[]", "2427.6614", "('1980-04-24 11:30:35.487','fee19be8-0f46-149b-ed98-43e7455ce2b2')"]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"191151"}}
{"rows_before_limit_at_least":5}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                 |
|-------|--------|-----------------------|
| ✔     | ✔      | `JSONLines`, `NDJSON` |

## Description {#description}

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONEachRow
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Differs from [`JSONEachRow`](./JSONEachRow.md)/[`JSONStringsEachRow`](./JSONStringsEachRow.md) in that ClickHouse will also yield progress information as JSON values.

## Example usage {#example-usage}

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONL", Documentation{
        .description = "An alias for the `JSONEachRow` format. See the `JSONEachRow` entry for the full documentation.",
        .related = {"JSONEachRow"}});

    factory.setDocumentation("JSONLines", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                        |
|-------|--------|----------------------------------------------|
| ✔     | ✔      | `JSONEachRow`, `JSONLines`, `NDJSON`, `JSONL` |

## Description {#description}

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object.

This format is also known as `JSONEachRow`, `NDJSON` (Newline Delimited JSON), or `JSONL` (`JSONLines`). All these names are aliases for the same format and can be used interchangeably.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONLines;
```

### Reading data {#reading-data}

Read data using the `JSONLines` format:

```sql
SELECT *
FROM football
FORMAT JSONLines
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONObjectEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

In this format, all data is represented as a single JSON Object, with each row represented as a separate field of this object similar to the [`JSONEachRow`](./JSONEachRow.md) format.

## Example usage {#example-usage}

### Basic example {#basic-example}

Given some JSON:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

To use an object name as a column value you can use the special setting [`format_json_object_each_row_column_for_object_name`](/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name). 
The value of this setting is set to the name of a column, that is used as JSON key for a row in the resulting object.

#### Output {#output}

Let's say we have the table `test` with two columns:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Let's output it in the `JSONObjectEachRow` format and use the `format_json_object_each_row_column_for_object_name` setting:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

#### Input {#input}

Let's say we stored the output from the previous example in a file named `data.json`:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

It also works for schema inference:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

### Inserting data {#json-inserting-data}

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse allows:

- Any order of key-value pairs in the object.
- Omitting some values.

ClickHouse ignores spaces between elements and commas after the objects. You can pass all the objects in one line. You do not have to separate them with line breaks.

#### Omitted values processing {#omitted-values-processing}

ClickHouse substitutes omitted values with the default values for the corresponding [data types](/sql-reference/data-types/index.md).

If `DEFAULT expr` is specified, ClickHouse uses different substitution rules depending on the [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting.

Consider the following table:

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

- If `input_format_defaults_for_omitted_fields = 0`, then the default value for `x` and `a` equals `0` (as the default value for the `UInt32` data type).
- If `input_format_defaults_for_omitted_fields = 1`, then the default value for `x` equals `0`, but the default value of `a` equals `x * 2`.

:::note
When inserting data with `input_format_defaults_for_omitted_fields = 1`, ClickHouse consumes more computational resources, compared to insertion with `input_format_defaults_for_omitted_fields = 0`.
:::

### Selecting data {#json-selecting-data}

Consider the `UserActivity` table as an example:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The query `SELECT * FROM UserActivity FORMAT JSONEachRow` returns:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Unlike the [JSON](/interfaces/formats/JSON) format, there is no substitution of invalid UTF-8 sequences. Values are escaped in the same way as for `JSON`.

:::info
Any set of bytes can be output in the strings. Use the [`JSONEachRow`](./JSONEachRow.md) format if you are sure that the data in the table can be formatted as JSON without losing any information.
:::

### Usage of Nested Structures {#jsoneachrow-nested}

If you have a table with the [`Nested`](/sql-reference/data-types/nested-data-structures/index.md) data type columns, you can insert JSON data with the same structure. Enable this feature with the [input_format_import_nested_json](/operations/settings/settings-formats.md/#input_format_import_nested_json) setting.

For example, consider the following table:

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

As you can see in the `Nested` data type description, ClickHouse treats each component of the nested structure as a separate column (`n.s` and `n.i` for our table). You can insert data in the following way:

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

To insert data as a hierarchical JSON object, set [`input_format_import_nested_json=1`](/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Without this setting, ClickHouse throws an exception.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Format settings {#format-settings}

| Setting                                                                                                                                                                            | Description                                                                                                                                                             | Default  | Notes                                                                                                                                                                                         |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`input_format_import_nested_json`](/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                              | map nested JSON data to nested tables (it works for JSONEachRow format).                                                                                                | `false`  |                                                                                                                                                                                               |
| [`input_format_json_read_bools_as_numbers`](/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                              | allow to parse bools as numbers in JSON input formats.                                                                                                                  | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_bools_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                              | allow to parse bools as strings in JSON input formats.                                                                                                                  | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_numbers_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                          | allow to parse numbers as strings in JSON input formats.                                                                                                                | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_arrays_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                            | allow to parse JSON arrays as strings in JSON input formats.                                                                                                            | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_objects_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                          | allow to parse JSON objects as strings in JSON input formats.                                                                                                           | `true`   |                                                                                                                                                                                               |
| [`input_format_json_named_tuples_as_objects`](/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                          | parse named tuple columns as JSON objects.                                                                                                                              | `true`   |                                                                                                                                                                                               |
| [`input_format_json_try_infer_numbers_from_strings`](/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                            | try to infer numbers from string fields while schema inference.                                                                                                         | `false`  |                                                                                                                                                                                               |
| [`input_format_json_try_infer_named_tuples_from_objects`](/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                  | try to infer named tuple from JSON objects during schema inference.                                                                                                     | `true`   |                                                                                                                                                                                               |
| [`input_format_json_infer_incomplete_types_as_strings`](/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                      | use type String for keys that contains only Nulls or empty objects/arrays during schema inference in JSON input formats.                                                | `true`   |                                                                                                                                                                                               |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | insert default values for missing elements in JSON object while parsing named tuple.                                                                                   | `true`   |                                                                                                                                                                                               |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                    | ignore unknown keys in json object for named tuples.                                                                                                                    | `false`  |                                                                                                                                                                                               |
| [`input_format_json_compact_allow_variable_number_of_columns`](/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)        | allow variable number of columns in JSONCompact/JSONCompactEachRow format, ignore extra columns and use default values on missing columns.                              | `false`  |                                                                                                                                                                                               |
| [`input_format_json_throw_on_bad_escape_sequence`](/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                | throw an exception if JSON string contains bad escape sequence. If disabled, bad escape sequences will remain as is in the data.                                        | `true`   |                                                                                                                                                                                               |
| [`input_format_json_empty_as_default`](/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                        | treat empty fields in JSON input as default values.                                                                                                                     | `false`. | For complex default expressions [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. |
| [`output_format_json_quote_64bit_integers`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                              | controls quoting of 64-bit integers in JSON output format.                                                                                                              | `true`   |                                                                                                                                                                                               |
| [`output_format_json_quote_64bit_floats`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                  | controls quoting of 64-bit floats in JSON output format.                                                                                                                | `false`  |                                                                                                                                                                                               |
| [`output_format_json_quote_denormals`](/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                        | enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.                                                                                                   | `false`  |                                                                                                                                                                                               |
| [`output_format_json_quote_decimals`](/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                          | controls quoting of decimals in JSON output format.                                                                                                                     | `false`  |                                                                                                                                                                                               |
| [`output_format_json_escape_forward_slashes`](/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                          | controls escaping forward slashes for string outputs in JSON output format.                                                                                             | `true`   |                                                                                                                                                                                               |
| [`output_format_json_named_tuples_as_objects`](/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                        | serialize named tuple columns as JSON objects.                                                                                                                          | `true`   |                                                                                                                                                                                               |
| [`output_format_json_array_of_rows`](/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                            | output a JSON array of all rows in JSONEachRow(Compact) format.                                                                                                         | `false`  |                                                                                                                                                                                               |
| [`output_format_json_validate_utf8`](/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                            | enables validation of UTF-8 sequences in JSON output formats (note that it doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8). | `false`  |                                                                                                                                                                                               |
)DOCS_MD"});

    factory.setDocumentation("JSONStrings", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [JSON](./JSON.md) format only in that data fields are output as strings, not as typed JSON values.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{
    "meta":
    [
            {
                    "name": "date",
                    "type": "Date"
            },
            {
                    "name": "season",
                    "type": "Int16"
            },
            {
                    "name": "home_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "away_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "home_team_goals",
                    "type": "Int8"
            },
            {
                    "name": "away_team_goals",
                    "type": "Int8"
            }
    ],
    "data":
    [
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Sutton United",
                    "away_team": "Bradford City",
                    "home_team_goals": "1",
                    "away_team_goals": "4"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Swindon Town",
                    "away_team": "Barrow",
                    "home_team_goals": "2",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Tranmere Rovers",
                    "away_team": "Oldham Athletic",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Port Vale",
                    "away_team": "Newport County",
                    "home_team_goals": "1",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Salford City",
                    "away_team": "Mansfield Town",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Barrow",
                    "away_team": "Northampton Town",
                    "home_team_goals": "1",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bradford City",
                    "away_team": "Carlisle United",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bristol Rovers",
                    "away_team": "Scunthorpe United",
                    "home_team_goals": "7",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Exeter City",
                    "away_team": "Port Vale",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Harrogate Town A.F.C.",
                    "away_team": "Sutton United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Hartlepool United",
                    "away_team": "Colchester United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Leyton Orient",
                    "away_team": "Tranmere Rovers",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Mansfield Town",
                    "away_team": "Forest Green Rovers",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Newport County",
                    "away_team": "Rochdale",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Oldham Athletic",
                    "away_team": "Crawley Town",
                    "home_team_goals": "3",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Stevenage Borough",
                    "away_team": "Salford City",
                    "home_team_goals": "4",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Walsall",
                    "away_team": "Swindon Town",
                    "home_team_goals": "0",
                    "away_team_goals": "3"
            }
    ]
}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONStrings;
```

### Reading data {#reading-data}

Read data using the `JSONStrings` format:

```sql
SELECT *
FROM football
FORMAT JSONStrings
```

The output will be in JSON format:

```json
{
    "meta":
    [
            {
                    "name": "date",
                    "type": "Date"
            },
            {
                    "name": "season",
                    "type": "Int16"
            },
            {
                    "name": "home_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "away_team",
                    "type": "LowCardinality(String)"
            },
            {
                    "name": "home_team_goals",
                    "type": "Int8"
            },
            {
                    "name": "away_team_goals",
                    "type": "Int8"
            }
    ],

    "data":
    [
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Sutton United",
                    "away_team": "Bradford City",
                    "home_team_goals": "1",
                    "away_team_goals": "4"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Swindon Town",
                    "away_team": "Barrow",
                    "home_team_goals": "2",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-04-30",
                    "season": "2021",
                    "home_team": "Tranmere Rovers",
                    "away_team": "Oldham Athletic",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Port Vale",
                    "away_team": "Newport County",
                    "home_team_goals": "1",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-02",
                    "season": "2021",
                    "home_team": "Salford City",
                    "away_team": "Mansfield Town",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Barrow",
                    "away_team": "Northampton Town",
                    "home_team_goals": "1",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bradford City",
                    "away_team": "Carlisle United",
                    "home_team_goals": "2",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Bristol Rovers",
                    "away_team": "Scunthorpe United",
                    "home_team_goals": "7",
                    "away_team_goals": "0"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Exeter City",
                    "away_team": "Port Vale",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Harrogate Town A.F.C.",
                    "away_team": "Sutton United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Hartlepool United",
                    "away_team": "Colchester United",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Leyton Orient",
                    "away_team": "Tranmere Rovers",
                    "home_team_goals": "0",
                    "away_team_goals": "1"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Mansfield Town",
                    "away_team": "Forest Green Rovers",
                    "home_team_goals": "2",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Newport County",
                    "away_team": "Rochdale",
                    "home_team_goals": "0",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Oldham Athletic",
                    "away_team": "Crawley Town",
                    "home_team_goals": "3",
                    "away_team_goals": "3"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Stevenage Borough",
                    "away_team": "Salford City",
                    "home_team_goals": "4",
                    "away_team_goals": "2"
            },
            {
                    "date": "2022-05-07",
                    "season": "2021",
                    "home_team": "Walsall",
                    "away_team": "Swindon Town",
                    "home_team_goals": "0",
                    "away_team_goals": "3"
            }
    ],

    "rows": 17,

    "statistics":
    {
            "elapsed": 0.173464376,
            "rows_read": 0,
            "bytes_read": 0
    }
}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Differs from the [`JSONEachRow`](./JSONEachRow.md) only in that data fields are output in strings, not in typed JSON values.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}   
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONStringsEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONStringsEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONStringsEachRow
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}   
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRowWithProgress", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse will also yield progress information as JSON values.

## Example usage {#example-usage}

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("LineAsString", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `LineAsString` format interprets every line of input data as a single string value. 
This format can only be parsed for a table with a single field of type [String](/sql-reference/data-types/string.md). 
The remaining columns must be set to [`DEFAULT`](/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view), or omitted.

## Example usage {#example-usage}

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("LineAsStringWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format but prints the header row with column names.

## Example usage {#example-usage}

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("LineAsStringWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format 
but prints two header rows: one with column names, the other with types.

## Example usage {#example-usage}

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("MD", Documentation{
        .description = "An alias for the `Markdown` format. See the `Markdown` entry for the full documentation.",
        .related = {"Markdown"}});

    factory.setDocumentation("Markdown", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      | `MD`  |

## Description {#description}

You can export results using [Markdown](https://en.wikipedia.org/wiki/Markdown) format to generate output ready to be pasted into your `.md` files:

The markdown table will be generated automatically and can be used on markdown-enabled platforms, like Github. This format is used only for output.

## Example usage {#example-usage}

```sql
SELECT
    number,
    number * 2
FROM numbers(5)
FORMAT Markdown
```
```results
| number | multiply(number, 2) |
|-:|-:|
| 0 | 0 |
| 1 | 2 |
| 2 | 4 |
| 3 | 6 |
| 4 | 8 |
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("MsgPack", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

ClickHouse supports reading and writing [MessagePack](https://msgpack.org/) data files.

## Data types matching {#data-types-matching}

| MessagePack data type (`INSERT`)                                   | ClickHouse data type                                                                                    | MessagePack data type (`SELECT`) |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|----------------------------------|
| `uint N`, `positive fixint`                                        | [`UIntN`](/sql-reference/data-types/int-uint.md)                                                  | `uint N`                         |
| `int N`, `negative fixint`                                         | [`IntN`](/sql-reference/data-types/int-uint.md)                                                   | `int N`                          |
| `bool`                                                             | [`UInt8`](/sql-reference/data-types/int-uint.md)                                                  | `uint 8`                         |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/sql-reference/data-types/string.md)                                                   | `bin 8`, `bin 16`, `bin 32`      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/sql-reference/data-types/fixedstring.md)                                         | `bin 8`, `bin 16`, `bin 32`      |
| `float 32`                                                         | [`Float32`](/sql-reference/data-types/float.md)                                                   | `float 32`                       |
| `float 64`                                                         | [`Float64`](/sql-reference/data-types/float.md)                                                   | `float 64`                       |
| `uint 16`                                                          | [`Date`](/sql-reference/data-types/date.md)                                                       | `uint 16`                        |
| `int 32`                                                           | [`Date32`](/sql-reference/data-types/date32.md)                                                   | `int 32`                         |
| `uint 32`                                                          | [`DateTime`](/sql-reference/data-types/datetime.md)                                               | `uint 32`                        |
| `uint 64`                                                          | [`DateTime64`](/sql-reference/data-types/datetime.md)                                             | `uint 64`                        |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/sql-reference/data-types/array.md)/[`Tuple`](/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/sql-reference/data-types/map.md)                                                         | `fixmap`, `map 16`, `map 32`     |
| `uint 32`                                                          | [`IPv4`](/sql-reference/data-types/ipv4.md)                                                       | `uint 32`                        |
| `bin 8`                                                            | [`String`](/sql-reference/data-types/string.md)                                                   | `bin 8`                          |
| `int 8`                                                            | [`Enum8`](/sql-reference/data-types/enum.md)                                                      | `int 8`                          |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/sql-reference/data-types/int-uint.md)                                    | `bin 8`                          |
| `int 32`                                                           | [`Decimal32`](/sql-reference/data-types/decimal.md)                                               | `int 32`                         |
| `int 64`                                                           | [`Decimal64`](/sql-reference/data-types/decimal.md)                                               | `int 64`                         |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/sql-reference/data-types/decimal.md)                                   | `bin 8 `                         |

## Example usage {#example-usage}

Writing to a file ".msgpk":

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

## Format settings {#format-settings}

| Setting                                                                                                                                    | Description                                                                                    | Default |
|--------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|---------|
| [`input_format_msgpack_number_of_columns`](/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | the number of columns in inserted MsgPack data. Used for automatic schema inference from data. | `0`     |
| [`output_format_msgpack_uuid_representation`](/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | the way how to output UUID in MsgPack format.                                                  | `EXT`   |
)DOCS_MD"});

    factory.setDocumentation("MySQLDump", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

ClickHouse supports reading MySQL [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).

It reads all the data from `INSERT` queries belonging to a single table in the dump. 
If there is more than one table, by default it reads data from the first one.

:::note
This format supports schema inference: if the dump contains a `CREATE` query for the specified table, the structure is inferred from it, otherwise the schema is inferred from the data of `INSERT` queries.
:::

## Example usage {#example-usage}

Given the following SQL dump file:

```sql title="dump.sql"
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

We can run the following queries:

```sql title="Query"
DESCRIBE TABLE file(dump.sql, MySQLDump) 
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SELECT *
FROM file(dump.sql, MySQLDump)
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## Format settings {#format-settings}

You can specify the name of the table from which to read data from using the [`input_format_mysql_dump_table_name`](/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name) setting.
If setting `input_format_mysql_dump_map_columns` is set to `1` and the dump contains a `CREATE` query for specified table or column names in the `INSERT` query, the columns from the input data will map to the columns from the table by name.
Columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
)DOCS_MD"});

    factory.setDocumentation("MySQLWire", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("NDJSON", Documentation{
        .description = "An alias for the `JSONEachRow` format. See the `JSONEachRow` entry for the full documentation.",
        .related = {"JSONEachRow"}});

    factory.setDocumentation("Native", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Native` format is ClickHouse's most efficient format because it is truly "columnar" 
in that it does not convert columns to rows.  

In this format data is written and read by [blocks](/development/architecture#block) in a binary format. 
For each block, the number of rows, number of columns, column names and types, and parts of columns in the block are recorded one after another. 

This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

:::tip
You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS.
It might not be practical to work with this format yourself.
:::

## Data types wire format {#data-types-wire-format}

Data is sent over the wire in a columnar format, which means that each column is sent separately,
and all values of a column are sent together as a single array.

Each column in a block contains a header similar to [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
When using the native TCP binary protocol (or when the HTTP endpoint receives `?client_protocol_version=<n>`),
a `BlockInfo` structure is written before the column and row counts. The examples in this section use
the plain HTTP interface without a protocol version, which omits `BlockInfo`.
:::

### Block structure {#block-structure}

The following query returns two columns, `number` and `str`, with three rows:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

The output data fits into a single ClickHouse block, and it will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

### Multiple blocks {#multiple-blocks}

However, in many cases, the data will not fit into a single block, and ClickHouse will send the data as multiple blocks.
Consider the following query that fetches two rows with reduced block size to force splitting the data as one row per block:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

The output:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

### Simple data types {#simple-data-types}

The wire format for an individual value of one of the simpler data types is similar to `RowBinary`/`RowBinaryWithNamesAndTypes`.
The full list of types that match this description includes:

- (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
- Float32, Float64
- Bool
- String
- FixedString(N)
- Date
- Date32
- DateTime
- DateTime64
- IPv4
- IPv6
- UUID

Refer to the descriptions of the types above in ["RowBinary data types wire format"](/interfaces/formats/RowBinary#data-types-wire-format) for more details.

### Complex data types {#complex-data-types}

The encoding of the following types differs from `RowBinary` and `RowBinaryWithNamesAndTypes`.

- Nullable
- LowCardinality
- Array
- Map
- Variant
- Dynamic
- JSON

#### Nullable {#nullable}

In the `Native` format, a nullable column will have a number of bytes equal to the number of rows in the block before the actual data. Each of these bytes indicates whether the value is `NULL` or not. For example, with this query, each odd number will be `NULL` instead:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

The output will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

It works similarly with `Nullable(String)`. The null indicator always comes from the nullable mask byte —
a mask value of `0x01` means the row is `NULL` regardless of the string content. For `NULL` rows,
the underlying string is stored as an empty string (LEB128 length `0`). Note that a non-`NULL` empty
string also has LEB128 length `0`, so only the mask byte distinguishes the two cases. For example, the following query:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

The output will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

#### LowCardinality {#lowcardinality}

Unlike [RowBinary](RowBinary/RowBinary.md#lowcardinality) where `LowCardinality` is transparent, the Native format uses a dictionary-based columnar encoding. A column is encoded as a version prefix, then a dictionary of unique values, and an array of integer indexes into that dictionary.

:::note
A column can be defined as `LowCardinality(Nullable(T))`, but it is not possible to define it as `Nullable(LowCardinality(T))` — it will always result in an error from the server.
:::

The version prefix is a `UInt64(LE)` with value `1`, written once per column. Then, per block, the following is written:

- `UInt64(LE)` — `IndexesSerializationType` bitfield. Bits 0–7 encode the index width (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). Bit 8 (`NeedGlobalDictionaryBit`) is never set in Native format (the server throws an exception if it is encountered). Bit 9 indicates additional dictionary keys are present. Bit 10 indicates the dictionary should be reset.
- `UInt64(LE)` — number of dictionary keys, followed by the keys bulk-serialized using the inner type encoding.
- `UInt64(LE)` — number of rows, followed by index values bulk-serialized using the appropriate UInt width.

The dictionary always contains a default value at index 0 (e.g. empty string for `String`, 0 for numeric types). For `LowCardinality(Nullable(T))`, index 0 represents `NULL`, and the keys are serialized without the `Nullable` wrapper.

For example, `LowCardinality(String)` with 5 rows `['foo', 'bar', 'baz', 'foo', 'bar']`:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

With `LowCardinality(Nullable(String))`, index 0 is `NULL`:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

#### Array {#array}

Unlike [RowBinary](RowBinary/RowBinary.md#array) where each array is prefixed with a LEB128 element count, the Native format encodes arrays as two columnar sub-streams:

- N cumulative `UInt64` offsets (little-endian, 8 bytes each). Row `i` has `offset[i] - offset[i-1]` elements, with `offset[-1]` implicitly 0.
- All nested elements across all rows, bulk-serialized contiguously.

For example, `Array(UInt32)` with 3 rows `[[0, 10], [1, 11], [2, 12]]`:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

An empty array has the same offset as the previous row. For example, `Array(String)` with 4 rows `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

#### Map {#map}

A `Map(K, V)` is encoded as `Array(Tuple(K, V))` — array offsets followed by all keys, then all values. This differs from [RowBinary](RowBinary/RowBinary.md#map) where keys and values are interleaved per entry.

For example, `Map(String, UInt64)` with 3 rows `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

#### Variant {#variant}

Unlike [RowBinary](RowBinary/RowBinary.md#variant) where each row carries its own discriminant byte followed by the value inline, the Native format separates discriminators from data.

:::warning
As with RowBinary, the types in the definition are always sorted alphabetically, and the discriminant is the index in that sorted list. `0xFF` (255) represents `NULL`.
:::

A `Variant` column is encoded as:

- `UInt64(LE)` discriminators mode prefix (`0` = BASIC, `1` = COMPACT). Native format output typically uses BASIC (`0`); COMPACT mode may appear when reading data stored with `use_compact_variant_discriminators_serialization` enabled.
- N `UInt8` discriminators, one per row.
- Each variant type's data as a separate bulk column containing only the matching rows, in discriminant order.

For example, `Variant(String, UInt32)` with 5 rows `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (sorted: `String` = 0, `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

#### Dynamic {#dynamic}

Unlike [RowBinary](RowBinary/RowBinary.md#dynamic) where each value is self-describing (type prefix + value), the Native format serializes `Dynamic` as a structure prefix followed by a [Variant](#variant) column.

The structure prefix contains a `UInt64(LE)` serialization version, then the number of dynamic types (as VarUInt), then the type names as strings. In version V1 the type count is written twice for compatibility. The data that follows is a `Variant` column whose type list is the dynamic types plus an internal `SharedVariant` type, sorted alphabetically.

For example, `Dynamic` with 5 rows `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

#### JSON {#json}

Unlike [RowBinary](RowBinary/RowBinary.md#json) where each row is self-describing with path names and values, the Native format serializes `JSON` in a columnar structure. The encoding is complex and version-dependent: it consists of a structure prefix with the serialization version, dynamic path names, and shared data layout, followed by typed paths (each as a bulk column), dynamic paths (each as a [Dynamic](#dynamic) column), and shared data for overflow paths.

For simpler interoperability, consider using the setting `output_format_native_write_json_as_string=1`, which serializes JSON columns as plain JSON text strings (one `String` per row).
)DOCS_MD"});

    factory.setDocumentation("Npy", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Npy` format is designed to load a NumPy array from a `.npy` file into ClickHouse. 
The NumPy file format is a binary format used for efficiently storing arrays of numerical data. 
During import, ClickHouse treats the top level dimension as an array of rows with a single column. 

The table below gives the supported Npy data types and their corresponding type in ClickHouse:

## Data types matching {#data_types-matching}

| Npy data type (`INSERT`) | ClickHouse data type                                            | Npy data type (`SELECT`) |
|--------------------------|-----------------------------------------------------------------|-------------------------|
| `i1`                     | [Int8](/sql-reference/data-types/int-uint.md)           | `i1`                    |
| `i2`                     | [Int16](/sql-reference/data-types/int-uint.md)          | `i2`                    |
| `i4`                     | [Int32](/sql-reference/data-types/int-uint.md)          | `i4`                    |
| `i8`                     | [Int64](/sql-reference/data-types/int-uint.md)          | `i8`                    |
| `u1`, `b1`               | [UInt8](/sql-reference/data-types/int-uint.md)          | `u1`                    |
| `u2`                     | [UInt16](/sql-reference/data-types/int-uint.md)         | `u2`                    |
| `u4`                     | [UInt32](/sql-reference/data-types/int-uint.md)         | `u4`                    |
| `u8`                     | [UInt64](/sql-reference/data-types/int-uint.md)         | `u8`                    |
| `f2`, `f4`               | [Float32](/sql-reference/data-types/float.md)           | `f4`                    |
| `f8`                     | [Float64](/sql-reference/data-types/float.md)           | `f8`                    |
| `S`, `U`                 | [String](/sql-reference/data-types/string.md)           | `S`                     |
|                          | [FixedString](/sql-reference/data-types/fixedstring.md) | `S`                     |

## Example usage {#example-usage}

### Saving an array in .npy format using Python {#saving-an-array-in-npy-format-using-python}

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

### Reading a NumPy file in ClickHouse {#reading-a-numpy-file-in-clickhouse}

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

### Selecting data {#selecting-data}

You can select data from a ClickHouse table and save it into a file in the Npy format using the following command with clickhouse-client:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Null", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

In the `Null` format - nothing is output. 
This may at first sound strange, but it's important to note that despite outputting nothing, the query is still processed, 
and when using the command-line client, data is transmitted to the client. 

:::tip
The `Null` format can be useful for performance testing.
:::

## Example usage {#example-usage}

### Reading data {#reading-data}

Consider a table `football` with the following data:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Read data using the `Null` format:

```sql
SELECT *
FROM football
FORMAT Null
```

The query will process the data, but will not output anything.

```response
0 rows in set. Elapsed: 0.154 sec.
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("ODBCDriver2", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("ORC", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widely used in the [Hadoop](https://hadoop.apache.org/) ecosystem.

## Data types matching {#data-types-matching-orc}

The table below compares supported ORC data types and their corresponding ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`)              | ClickHouse data type                                                                                              | ORC data type (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [UInt8](/sql-reference/data-types/int-uint.md)                                                            | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Char`, `Varchar`, `Binary` | [String](/sql-reference/data-types/string.md)                                                             | `Binary`                 |
| `List`                                | [Array](/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |

- Other types are not supported.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.
- The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using an ORC file with the following data, named as `football.orc`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

### Reading data {#reading-data}

Read data using the `ORC` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output ORC files.
:::

## Format settings {#format-settings}

| Setting                                                                                                                                                                                                      | Description                                                                            | Default |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------|
| [`output_format_arrow_string_as_string`](/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Use Arrow String type instead of Binary for String columns.                            | `false` |
| [`output_format_orc_compression_method`](/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Compression method used in output ORC format. Default value                            | `none`  |
| [`input_format_arrow_case_insensitive_column_matching`](/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Ignore case when matching Arrow columns with ClickHouse columns.                       | `false` |
| [`input_format_arrow_allow_missing_columns`](/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Allow missing columns while reading Arrow data.                                        | `false` |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Allow skipping columns with unsupported types while schema inference for Arrow format. | `false` |

To exchange data with Hadoop, you can use [HDFS table engine](/engines/table-engines/integrations/hdfs.md).
)DOCS_MD"});

    factory.setDocumentation("One", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `One` format is a special input format that doesn't read any data from file, and returns only one row with column of type [`UInt8`](../../sql-reference/data-types/int-uint.md), name `dummy` and value `0` (like the `system.one` table).
Can be used with virtual columns `_file/_path`  to list all files without reading actual data.

## Example usage {#example-usage}

Example:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Parquet", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

## Data types matching {#data-types-matching-parquet}

The table below shows how Parquet data types match ClickHouse [data types](/sql-reference/data-types/index.md).

| Parquet type (logical, converted, or physical) | ClickHouse data type |
|------------------------------------------------|----------------------|
| `BOOLEAN` | [Bool](/sql-reference/data-types/boolean.md) |
| `UINT_8` | [UInt8](/sql-reference/data-types/int-uint.md) |
| `INT_8` | [Int8](/sql-reference/data-types/int-uint.md) |
| `UINT_16` | [UInt16](/sql-reference/data-types/int-uint.md) |
| `INT_16` | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) |
| `UINT_32` | [UInt32](/sql-reference/data-types/int-uint.md) |
| `INT_32` | [Int32](/sql-reference/data-types/int-uint.md) |
| `UINT_64` | [UInt64](/sql-reference/data-types/int-uint.md) |
| `INT_64` | [Int64](/sql-reference/data-types/int-uint.md) |
| `DATE` | [Date32](/sql-reference/data-types/date.md) |
| `TIMESTAMP`, `TIME` | [DateTime64](/sql-reference/data-types/datetime64.md) |
| `FLOAT` | [Float32](/sql-reference/data-types/float.md) |
| `DOUBLE` | [Float64](/sql-reference/data-types/float.md) |
| `INT96` | [DateTime64(9, 'UTC')](/sql-reference/data-types/datetime64.md) |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON` | [String](/sql-reference/data-types/string.md) |
| `JSON` | [JSON](/sql-reference/data-types/newjson.md) |
| `FIXED_LEN_BYTE_ARRAY` | [FixedString](/sql-reference/data-types/fixedstring.md) |
| `DECIMAL` | [Decimal](/sql-reference/data-types/decimal.md) |
| `LIST` | [Array](/sql-reference/data-types/array.md) |
| `MAP` | [Map](/sql-reference/data-types/map.md) |
| struct | [Tuple](/sql-reference/data-types/tuple.md) |
| `FLOAT16` | [Float32](/sql-reference/data-types/float.md) |
| `UUID` | [FixedString(16)](/sql-reference/data-types/fixedstring.md) |
| `INTERVAL` | [FixedString(12)](/sql-reference/data-types/fixedstring.md) |

When writing Parquet file, data types that don't have a matching Parquet type are converted to the nearest available type:

| ClickHouse data type | Parquet type |
|----------------------|--------------|
| [IPv4](/sql-reference/data-types/ipv4.md) | `UINT_32` |
| [IPv6](/sql-reference/data-types/ipv6.md) | `FIXED_LEN_BYTE_ARRAY` (16 bytes) |
| [Date](/sql-reference/data-types/date.md) (16 bits) | `DATE` (32 bits) |
| [DateTime](/sql-reference/data-types/datetime.md) (32 bits, seconds) | `TIMESTAMP` (64 bits, milliseconds) |
| [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32 bytes, little-endian) |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to that data type which is set for the ClickHouse table column. E.g. a `UINT_32` Parquet column can be read into an [IPv4](/sql-reference/data-types/ipv4.md) ClickHouse column.

For some Parquet types there's no closely matching ClickHouse type. We read them as follows:
* `TIME` (time of day) is read as a timestamp. E.g. `10:23:13.000` becomes `1970-01-01 10:23:13.000`.
* `TIMESTAMP`/`TIME` with `isAdjustedToUTC=false` is a local wall-clock time (year, month, day, hour, minute, second and subsecond fields in a local timezone, regardless of what specific time zone is considered local), same as SQL `TIMESTAMP WITHOUT TIME ZONE`. ClickHouse reads it as if it were a UTC timestamp instead. E.g. `2025-09-29 18:42:13.000` (representing a reading of a local wall clock) becomes `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')` representing a point in time). If converted to String, it shows the correct year, month, day, hour, minute, second and subsecond, which can then be interpreted as being in some local timezone instead of UTC. Counterintuitively, changing the type from `DateTime64(3, 'UTC')` to `DateTime64(3)` would not help as both types represent a point in time rather than a clock reading, but `DateTime64(3)` would incorrectly be formatted using local timezone.
* `INTERVAL` is currently read as `FixedString(12)` with raw binary representation of the time interval, as encoded in Parquet file.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a Parquet file with the following data, named as `football.parquet`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

### Reading data {#reading-data}

Read data using the `Parquet` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquet is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output Parquet files.
:::

To exchange data with Hadoop, you can use the [`HDFS table engine`](/engines/table-engines/integrations/hdfs.md).

## Format settings {#format-settings}

| Setting                                                                        | Description                                                                                                                                                                                                                       | Default     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_parquet_case_insensitive_column_matching`                        | Ignore case when matching Parquet columns with CH columns.                                                                                                                                                                          | `0`         |
| `input_format_parquet_preserve_order`                                          | Avoid reordering rows when reading from Parquet files. Usually makes it much slower.                                                                                                                                              | `0`         |
| `input_format_parquet_filter_push_down`                                        | When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.                                                                                          | `1`         |
| `input_format_parquet_bloom_filter_push_down`                                  | When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.                                                                                                          | `0`         |
| `input_format_parquet_allow_missing_columns`                                   | Allow missing columns while reading Parquet input formats                                                                                                                                                                          | `1`         |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format                                                                                                                          | `8192`      |
| `input_format_parquet_enable_row_group_prefetch`                               | Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.                                                                                                                          | `1`         |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Parquet                                                                                                                                                      | `0`         |
| `input_format_parquet_max_block_size`                                          | Max block size for parquet reader.                                                                                                                                                                                                | `65409`     |
| `input_format_parquet_prefer_block_bytes`                                      | Average block bytes output by parquet reader                                                                                                                                                                                      | `16744704`  |
| `input_format_parquet_enable_json_parsing`                                      | When reading Parquet files, parse JSON columns as ClickHouse JSON Column.                                                                                                                                                                                      | `1`  |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LEN_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `input_format_parquet_import_nested`                                           | Obsolete setting, does nothing.                                                                                                                                                                                                   | `0`         |
| `input_format_parquet_local_time_as_utc` | true | Determines the data type used by schema inference for Parquet timestamps with isAdjustedToUTC=false. If true: DateTime64(..., 'UTC'), if false: DateTime64(...). Neither behavior is fully correct as ClickHouse doesn't have a data type for local wall-clock time. Counterintuitively, 'true' is probably the less incorrect option, because formatting the 'UTC' timestamp as String will produce representation of the correct local time. |
)DOCS_MD"});

    factory.setDocumentation("ParquetMetadata", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Special format for reading Parquet file metadata (https://parquet.apache.org/docs/file-format/metadata/). It always outputs one row with the next structure/content:
- `num_columns` - the number of columns
- ``num_rows` - the total number of rows
- `num_row_groups` - the total number of row groups
- `format_version` - parquet format version, always 1.0 or 2.6
- `total_uncompressed_size` - total uncompressed bytes size of the data, calculated as the sum of total_byte_size from all row groups
- `total_compressed_size` - total compressed bytes size of the data, calculated as the sum of total_compressed_size from all row groups
- `columns` - the list of columns metadata with the next structure:
  - `name` - column name
  - `path` - column path (differs from name for nested column)
  - `max_definition_level` - maximum definition level
  - `max_repetition_level` - maximum repetition level
  - `physical_type` - column physical type
  - `logical_type` - column logical type
  - `compression` - compression used for this column
  - `total_uncompressed_size` - total uncompressed bytes size of the column, calculated as the sum of total_uncompressed_size of the column from all row groups
  - `total_compressed_size` - total compressed bytes size of the column,  calculated as the sum of total_compressed_size of the column from all row groups
  - `space_saved` - percent of space saved by compression, calculated as (1 - total_compressed_size/total_uncompressed_size).
  - `encodings` - the list of encodings used for this column
- `row_groups` - the list of row groups metadata with the next structure:
  - `num_columns` - the number of columns in the row group
  - `num_rows` - the number of rows in the row group
  - `total_uncompressed_size` - total uncompressed bytes size of the row group
  - `total_compressed_size` - total compressed bytes size of the row group
  - `columns` - the list of column chunks metadata with the next structure:
    - `name` - column name
    - `path` - column path
    - `total_compressed_size` - total compressed bytes size of the column
    - `total_uncompressed_size` - total uncompressed bytes size of the row group
    - `have_statistics` - boolean flag that indicates if column chunk metadata contains column statistics
    - `statistics` - column chunk statistics (all fields are NULL if have_statistics = false) with the next structure:
      - `num_values` - the number of non-null values in the column chunk
      - `null_count` - the number of NULL values in the column chunk
      - `distinct_count` - the number of distinct values in the column chunk
      - `min` - the minimum value of the column chunk
      - `max` - the maximum column of the column chunk

## Example usage {#example-usage}

Example:

```sql
SELECT * 
FROM file(data.parquet, ParquetMetadata) 
FORMAT PrettyJSONEachRow
```

```json
{
    "num_columns": "2",
    "num_rows": "100000",
    "num_row_groups": "2",
    "format_version": "2.6",
    "metadata_size": "577",
    "total_uncompressed_size": "282436",
    "total_compressed_size": "26633",
    "columns": [
        {
            "name": "number",
            "path": "number",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "INT32",
            "logical_type": "Int(bitWidth=16, isSigned=false)",
            "compression": "LZ4",
            "total_uncompressed_size": "133321",
            "total_compressed_size": "13293",
            "space_saved": "90.03%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        },
        {
            "name": "concat('Hello', toString(modulo(number, 1000)))",
            "path": "concat('Hello', toString(modulo(number, 1000)))",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "BYTE_ARRAY",
            "logical_type": "None",
            "compression": "LZ4",
            "total_uncompressed_size": "149115",
            "total_compressed_size": "13340",
            "space_saved": "91.05%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        }
    ],
    "row_groups": [
        {
            "num_columns": "2",
            "num_rows": "65409",
            "total_uncompressed_size": "179809",
            "total_compressed_size": "14163",
            "columns": [
                {
                    "name": "number",
                    "path": "number",
                    "total_compressed_size": "7070",
                    "total_uncompressed_size": "85956",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "0",
                        "max": "999"
                    }
                },
                {
                    "name": "concat('Hello', toString(modulo(number, 1000)))",
                    "path": "concat('Hello', toString(modulo(number, 1000)))",
                    "total_compressed_size": "7093",
                    "total_uncompressed_size": "93853",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "Hello0",
                        "max": "Hello999"
                    }
                }
            ]
        },
        ...
    ]
}
```
)DOCS_MD"});

    factory.setDocumentation("PostgreSQLWire", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Pretty", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

The `Pretty` format outputs data as Unicode-art tables, 
using ANSI-escape sequences for displaying colors in the terminal.
A full grid of the table is drawn, and each row occupies two lines in the terminal.
Each result block is output as a separate table. 
This is necessary so that blocks can be output without buffering results (buffering would be necessary to pre-calculate the visible width of all the values).

[NULL](/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

## Example usage {#example-usage}

Example (shown for the [`PrettyCompact`](./PrettyCompact.md) format):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Rows are not escaped in any of the `Pretty` formats. The following example is shown for the [`PrettyCompact`](./PrettyCompact.md) format:

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

To avoid dumping too much data to the terminal, only the first `10,000` rows are printed. 
If the number of rows is greater than or equal to `10,000`, the message "Showed first 10 000" is printed.

:::note
This format is only appropriate for outputting a query result, but not for parsing data.
:::

The Pretty format supports outputting total values (when using `WITH TOTALS`) and extremes (when 'extremes' is set to 1). 
In these cases, total values and extreme values are output after the main data, in separate tables. 
This is shown in the following example which uses the [`PrettyCompact`](./PrettyCompact.md) format:

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompact", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`Pretty`](./Pretty.md) format in that the table is displayed with a grid drawn between rows. 
Because of this the result is more compact.

:::note
This format is used by default in the command-line client in interactive mode.
:::

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings />
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](./PrettyCompact.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](./PrettyCompact.md) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompactNoEscapes`](./PrettyCompactNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyJSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✗     | ✔      | `PrettyJSONLines`, `PrettyNDJSON` |

## Description {#description}

Differs from [JSONEachRow](./JSONEachRow.md) only in that JSON is pretty formatted with new line delimiters and 4 space indents.

## Example usage {#example-usage}
### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Sutton United",
    "away_team": "Bradford City",
    "home_team_goals": 1,
    "away_team_goals": 4
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Swindon Town",
    "away_team": "Barrow",
    "home_team_goals": 2,
    "away_team_goals": 1
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Tranmere Rovers",
    "away_team": "Oldham Athletic",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Port Vale",
    "away_team": "Newport County",
    "home_team_goals": 1,
    "away_team_goals": 2
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Salford City",
    "away_team": "Mansfield Town",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Barrow",
    "away_team": "Northampton Town",
    "home_team_goals": 1,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bradford City",
    "away_team": "Carlisle United",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bristol Rovers",
    "away_team": "Scunthorpe United",
    "home_team_goals": 7,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Exeter City",
    "away_team": "Port Vale",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Harrogate Town A.F.C.",
    "away_team": "Sutton United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Hartlepool United",
    "away_team": "Colchester United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Leyton Orient",
    "away_team": "Tranmere Rovers",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Mansfield Town",
    "away_team": "Forest Green Rovers",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Newport County",
    "away_team": "Rochdale",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Oldham Athletic",
    "away_team": "Crawley Town",
    "home_team_goals": 3,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Stevenage Borough",
    "away_team": "Salford City",
    "home_team_goals": 4,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Walsall",
    "away_team": "Swindon Town",
    "home_team_goals": 0,
    "away_team_goals": 3
} 
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT PrettyJSONEachRow;
```

### Reading data {#reading-data}

Read data using the `PrettyJSONEachRow` format:

```sql
SELECT *
FROM football
FORMAT PrettyJSONEachRow
```

The output will be in JSON format:

```json
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Sutton United",
    "away_team": "Bradford City",
    "home_team_goals": 1,
    "away_team_goals": 4
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Swindon Town",
    "away_team": "Barrow",
    "home_team_goals": 2,
    "away_team_goals": 1
}
{
    "date": "2022-04-30",
    "season": 2021,
    "home_team": "Tranmere Rovers",
    "away_team": "Oldham Athletic",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Port Vale",
    "away_team": "Newport County",
    "home_team_goals": 1,
    "away_team_goals": 2
}
{
    "date": "2022-05-02",
    "season": 2021,
    "home_team": "Salford City",
    "away_team": "Mansfield Town",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Barrow",
    "away_team": "Northampton Town",
    "home_team_goals": 1,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bradford City",
    "away_team": "Carlisle United",
    "home_team_goals": 2,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Bristol Rovers",
    "away_team": "Scunthorpe United",
    "home_team_goals": 7,
    "away_team_goals": 0
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Exeter City",
    "away_team": "Port Vale",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Harrogate Town A.F.C.",
    "away_team": "Sutton United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Hartlepool United",
    "away_team": "Colchester United",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Leyton Orient",
    "away_team": "Tranmere Rovers",
    "home_team_goals": 0,
    "away_team_goals": 1
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Mansfield Town",
    "away_team": "Forest Green Rovers",
    "home_team_goals": 2,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Newport County",
    "away_team": "Rochdale",
    "home_team_goals": 0,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Oldham Athletic",
    "away_team": "Crawley Town",
    "home_team_goals": 3,
    "away_team_goals": 3
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Stevenage Borough",
    "away_team": "Salford City",
    "home_team_goals": 4,
    "away_team_goals": 2
}
{
    "date": "2022-05-07",
    "season": 2021,
    "home_team": "Walsall",
    "away_team": "Swindon Town",
    "home_team_goals": 0,
    "away_team_goals": 3
}  
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("PrettyJSONLines", Documentation{
        .description = "An alias for the `PrettyJSONEachRow` format. See the `PrettyJSONEachRow` entry for the full documentation.",
        .related = {"PrettyJSONEachRow"}});

    factory.setDocumentation("PrettyMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`Pretty`](/interfaces/formats/Pretty) format in that up to `10,000` rows are buffered,
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyNDJSON", Documentation{
        .description = "An alias for the `PrettyJSONEachRow` format. See the `PrettyJSONEachRow` entry for the full documentation.",
        .related = {"PrettyJSONEachRow"}});

    factory.setDocumentation("PrettyNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from [Pretty](/interfaces/formats/Pretty) in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

Example:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

:::note
The [HTTP interface](/interfaces/http) can be used for displaying this format in the browser.
:::

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyNoEscapes`](./PrettyNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by blocks.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpace", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](./PrettyCompact.md) format in that whitespace 
(space characters) is used for displaying the table instead of a grid.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpace`](./PrettySpace.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpace`](./PrettySpace.md) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) are not used. 
This is necessary for displaying this format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpaceNoEscapes`](./PrettySpaceNoEscapes.md) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/development/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("Prometheus", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Exposes metrics in the [Prometheus text-based exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

For this format, it is a requirement for the output table to be structured correctly, by the following rules:

- Columns `name` ([String](/sql-reference/data-types/string.md)) and `value` (number) are required.
- Rows may optionally contain `help` ([String](/sql-reference/data-types/string.md)) and `timestamp` (number).
- Column `type` ([String](/sql-reference/data-types/string.md)) should be one of `counter`, `gauge`, `histogram`, `summary`, `untyped` or empty.
- Each metric value may also have some `labels` ([Map(String, String)](/sql-reference/data-types/map.md)).
- Several consequent rows may refer to the one metric with different labels. The table should be sorted by metric name (e.g., with `ORDER BY name`).

There are special requirements for the `histogram` and `summary` labels - see [Prometheus doc](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries) for the details. 
Special rules are applied to rows with labels `{'count':''}` and `{'sum':''}`, which are converted to `<metric_name>_count` and `<metric_name>_sum` respectively.

## Example usage {#example-usage}

```yaml
┌─name────────────────────────────────┬─type──────┬─help──────────────────────────────────────┬─labels─────────────────────────┬────value─┬─────timestamp─┐
│ http_request_duration_seconds       │ histogram │ A histogram of the request duration.      │ {'le':'0.05'}                  │    24054 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.1'}                   │    33444 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.2'}                   │   100392 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.5'}                   │   129389 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'1'}                     │   133988 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'+Inf'}                  │   144320 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'sum':''}                     │    53423 │             0 │
│ http_requests_total                 │ counter   │ Total number of HTTP requests             │ {'method':'post','code':'200'} │     1027 │ 1395066363000 │
│ http_requests_total                 │ counter   │                                           │ {'method':'post','code':'400'} │        3 │ 1395066363000 │
│ metric_without_timestamp_and_labels │           │                                           │ {}                             │    12.47 │             0 │
│ rpc_duration_seconds                │ summary   │ A summary of the RPC duration in seconds. │ {'quantile':'0.01'}            │     3102 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.05'}            │     3272 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.5'}             │     4773 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.9'}             │     9001 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.99'}            │    76656 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'count':''}                   │     2693 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'sum':''}                     │ 17560473 │             0 │
│ something_weird                     │           │                                           │ {'problem':'division by zero'} │      inf │      -3982045 │
└─────────────────────────────────────┴───────────┴───────────────────────────────────────────┴────────────────────────────────┴──────────┴───────────────┘
```

Will be formatted as:

```text
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{code="200",method="post"} 1027 1395066363000
http_requests_total{code="400",method="post"} 3 1395066363000

metric_without_timestamp_and_labels 12.47

# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 17560473
rpc_duration_seconds_count 2693

something_weird{problem="division by zero"} +Inf -3982045
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Protobuf", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Protobuf` format is the [Protocol Buffers](https://protobuf.dev/) format.

This format requires an external format schema, which is cached between queries.

ClickHouse supports:
- both `proto2` and `proto3` syntaxes.
- `Repeated`/`optional`/`required` fields.

To find the correspondence between table columns and fields of the Protocol Buffers' message type, ClickHouse compares their names.
This comparison is case-insensitive and the characters `_` (underscore) and `.` (dot) are considered as equal.
If the types of a column and a field of the Protocol Buffers' message are different, then the necessary conversion is applied.

Nested messages are supported. For example, for the field `z` in the following message type:

```capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse tries to find a column named `x.y.z` (or `x_y_z` or `X.y_Z` and so on).

Nested messages are suitable for input or output of a [nested data structures](/sql-reference/data-types/nested-data-structures/index.md).

Default values defined in a protobuf schema like the one that follows are not applied, rather the [table defaults](/sql-reference/statements/create/table#default_values) are used instead of them:

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

If a message contains [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) and `input_format_protobuf_oneof_presence` is set, ClickHouse fills column that indicates which field of oneof was found.

```capnp
syntax = "proto3";

message StringOrString {
  oneof string_oneof {
    string string1 = 1;
    string string2 = 42;
  }
}
```

```sql
CREATE TABLE string_or_string ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string
```

```text
   ┌─────────┬─────────┬──────────────┐
   │ string1 │ string2 │ string_oneof │
   ├─────────┼─────────┼──────────────┤
1. │         │ string2 │ world        │
   ├─────────┼─────────┼──────────────┤
2. │ string1 │         │ hello        │
   └─────────┴─────────┴──────────────┘
```

Name of the column that indicates presence must be the same as the name of oneof.
Nested messages are supported (see  [basic-examples](#basic-examples)). Empty messages are supported as well.
Allowed types are Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 or Enum16.
Enum (as well as Enum8 or Enum16) must contain all oneof' possible tags plus 0 to indicate absence, string representations does not matter.

The setting [`input_format_protobuf_oneof_presence`](/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) is disabled by default

ClickHouse inputs and outputs protobuf messages in the `length-delimited` format.
This means that before every message its length should be written as a [variable width integer (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

## Example usage {#example-usage}

### Reading and writing data {#basic-examples}

:::note Example files
The files used in this example are available in the [examples repository](https://github.com/ClickHouse/formats/ProtoBuf)
:::

In this example we will read some data from a file `protobuf_message.bin` into a ClickHouse table. We'll then write it
back out to a file called `protobuf_message_from_clickhouse.bin` using the `Protobuf` format.

Given the file `schemafile.proto`:

```capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

<details>
<summary>Generating the binary file</summary>

If you already know how to serialize and deserialize data in the `Protobuf` format, you can skip this step.

We'll use Python to serialize some data into `protobuf_message.bin` and read it into ClickHouse.
If there is another language you want to use, see also: ["How to read/write length-delimited Protobuf messages in popular languages"](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

Run the following command to generate a Python file named `schemafile_pb2.py` in
the same directory as `schemafile.proto`. This file contains the Python classes
that represent your `UserData` Protobuf message:

```bash
protoc --python_out=. schemafile.proto
```

Now, create a new Python file named `generate_protobuf_data.py`, in the same
directory as `schemafile_pb2.py`. Paste the following code into it:

```python
import schemafile_pb2  # Module generated by 'protoc'
from google.protobuf import text_format
from google.protobuf.internal.encoder import _VarintBytes # Import the internal varint encoder

def create_user_data_message(name, surname, birthDate, phoneNumbers):
    """
    Creates and populates a UserData Protobuf message.
    """
    message = schemafile_pb2.MessageType()
    message.name = name
    message.surname = surname
    message.birthDate = birthDate
    message.phoneNumbers.extend(phoneNumbers)
    return message

# The data for our example users
data_to_serialize = [
    {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
    {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
    {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
]

output_filename = "protobuf_messages.bin"

# Open the binary file in write-binary mode ('wb')
with open(output_filename, "wb") as f:
    for item in data_to_serialize:
        # Create a Protobuf message instance for the current user
        message = create_user_data_message(
            item["name"],
            item["surname"],
            item["birthDate"],
            item["phoneNumbers"]
        )

        # Serialize the message
        serialized_data = message.SerializeToString()

        # Get the length of the serialized data
        message_length = len(serialized_data)

        # Use the Protobuf library's internal _VarintBytes to encode the length
        length_prefix = _VarintBytes(message_length)

        # Write the length prefix
        f.write(length_prefix)
        # Write the serialized message data
        f.write(serialized_data)

print(f"Protobuf messages (length-delimited) written to {output_filename}")

# --- Optional: Verification (reading back and printing) ---
# For reading back, we'll also use the internal Protobuf decoder for varints.
from google.protobuf.internal.decoder import _DecodeVarint32

print("\n--- Verifying by reading back ---")
with open(output_filename, "rb") as f:
    buf = f.read() # Read the whole file into a buffer for easier varint decoding
    n = 0
    while n < len(buf):
        # Decode the varint length prefix
        msg_len, new_pos = _DecodeVarint32(buf, n)
        n = new_pos

        # Extract the message data
        message_data = buf[n:n+msg_len]
        n += msg_len

        # Parse the message
        decoded_message = schemafile_pb2.MessageType()
        decoded_message.ParseFromString(message_data)
        print(text_format.MessageToString(decoded_message, as_utf8=True))
```

Now run the script from the command line. It is recommended to run it from a
python virtual environment, for example using `uv`:

```bash
uv venv proto-venv
source proto-venv/bin/activate
```

You will need to install the following python libraries:

```bash
uv pip install --upgrade protobuf
```

Run the script to generate the binary file:

```bash
python generate_protobuf_data.py
```

</details>

Create a ClickHouse table matching the schema:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

Insert the data into the table from the command line:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

You can also write the data back to a binary file using the `Protobuf` format:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

With your Protobuf schema, you can now deserialize the data which was written out from ClickHouse to file `protobuf_message_from_clickhouse.bin`.

### Reading and writing data using ClickHouse Cloud {#basic-examples-cloud}

With ClickHouse Cloud you are not able to upload a Protobuf schema file. However, you can use the `format_protobuf_schema`
setting to specify the schema in the query. In this example, we show you how to read serialized data from your local
machine and insert it into a table in ClickHouse Cloud.

As in the previous example, create the table according to the schema of your Protobuf schema in ClickHouse Cloud:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

The setting `format_schema_source` defines the source of setting `format_schema`

Possible values:
- 'file' (default): unsupported in Cloud
- 'string': The `format_schema` is the literal content of the schema.
- 'query': The `format_schema` is a query to retrieve the schema.

### `format_schema_source='string'` {#format-schema-source-string}

Insert the data into ClickHouse Cloud, specifying the schema as a string, run:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Select the data inserted into the table:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

### `format_schema_source='query'` {#format-schema-source-query}

You can also store your Protobuf schema in a table.

Create a table on ClickHouse Cloud to insert data into:

```sql
CREATE TABLE testing.protobuf_schema (
  schema String
)
ENGINE = MergeTree()
ORDER BY tuple();
```

```sql
INSERT INTO testing.protobuf_schema VALUES ('syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};');
```

Insert the data into ClickHouse Cloud, specifying the schema as a query to run:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Select the data inserted into the table:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

### Using autogenerated schema {#using-autogenerated-protobuf-schema}

If you don't have an external Protobuf schema for your data, you can still output/input data in the Protobuf format
using an autogenerated schema. For this use the `format_protobuf_use_autogenerated_schema` setting.

For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

In this case, ClickHouse will autogenerate the Protobuf schema according to the table structure using function
[`structureToProtobufSchema`](/sql-reference/functions/other-functions#structureToProtobufSchema). It will then use this schema to serialize data in the Protobuf format.

You can also read a Protobuf file with the autogenerated schema. In this case it is necessary for the file to be created using the same schema:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

The setting [`format_protobuf_use_autogenerated_schema`](/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) is enabled by default and applies if [`format_schema`](/operations/settings/formats#format_schema) is not set.

You can also save autogenerated schema in the file during input/output using setting [`output_format_schema`](/operations/settings/formats#output_format_schema). For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

In this case autogenerated Protobuf schema will be saved in file `path/to/schema/schema.capnp`.

### Drop protobuf cache {#drop-protobuf-cache}

To reload the Protobuf schema loaded from [`format_schema_path`](/operations/server-configuration-parameters/settings.md/#format_schema_path) use the [`SYSTEM DROP ... FORMAT CACHE`](/sql-reference/statements/system.md/#system-drop-schema-format) statement.

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```
)DOCS_MD"});

    factory.setDocumentation("ProtobufList", Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `ProtobufList` format is similar to the [`Protobuf`](./Protobuf.md) format but rows are represented as a sequence of sub-messages contained in a message with a fixed name of "Envelope".

## Example usage {#example-usage}

For example:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

Where the file `schemafile.proto` looks like this:

```capnp title="schemafile.proto"
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

The message type specified in `format_schema` is resolved by first looking for it as a nested type inside a top-level `Envelope` message. If no match is found there — either because the schema has no `Envelope` message, or the `Envelope` does not contain a message with the requested name — the top-level message with that name is used directly.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("ProtobufSingle", Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `ProtobufSingle` format is the same as the [`Protobuf`](./Protobuf.md) format but it is intended for storing/parsing single Protobuf messages without length delimiters.

## Example usage {#example-usage}

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Raw", Documentation{
        .description = "An alias for the `TabSeparatedRaw` format. See the `TabSeparatedRaw` entry for the full documentation.",
        .related = {"TabSeparatedRaw"}});

    factory.setDocumentation("RawBLOB", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

The `RawBLOB` formats reads all input data to a single value. It is possible to parse only a table with a single field of type [`String`](/sql-reference/data-types/string.md) or similar.
The result is output as a binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

### Raw formats comparison {#raw-formats-comparison}

Below is a comparison of the formats `RawBLOB` and [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no new-line at the end of each value.

`TabSeparatedRaw`:
- data is output without escaping;
- the rows contain values separated by tabs;
- there is a line feed after the last value in every row.

The following is a comparison of the `RawBLOB` and [RowBinary](./RowBinary/RowBinary.md) formats.

`RawBLOB`:
- String fields are output without being prefixed by length.

`RowBinary`:
- String fields are represented as length in varint format (unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.

When empty data is passed to the `RawBLOB` input, ClickHouse throws an exception:

```text
Code: 108. DB::Exception: No data to insert
```

## Example usage {#example-usage}

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("RawWithNames", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNames` format. See the `TabSeparatedRawWithNames` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNames"}});

    factory.setDocumentation("RawWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNamesAndTypes` format. See the `TabSeparatedRawWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNamesAndTypes"}});

    factory.setDocumentation("Regexp", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `Regex` format parses every line of imported data according to the provided regular expression.

**Usage**

The regular expression from [format_regexp](/operations/settings/settings-formats.md/#format_regexp) setting is applied to every line of imported data. The number of subpatterns in the regular expression must be equal to the number of columns in imported dataset.

Lines of the imported data must be separated by newline character `'\n'` or DOS-style newline `"\r\n"`.

The content of every matched subpattern is parsed with the method of corresponding data type, according to [format_regexp_escaping_rule](/operations/settings/settings-formats.md/#format_regexp_escaping_rule) setting.

If the regular expression does not match the line and [format_regexp_skip_unmatched](/operations/settings/settings-formats.md/#format_regexp_escaping_rule) is set to 1, the line is silently skipped. Otherwise, exception is thrown.

## Example usage {#example-usage}

Consider the file `data.tsv`:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```
and table `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

We'll insert the data from the aforementioned file into the table above using the following query:

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

We can now `SELECT` the data from the table to see how the `Regex` format parsed the data from the file:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

## Format settings {#format-settings}

When working with the `Regexp` format, you can use the following settings:

- `format_regexp` — [String](/sql-reference/data-types/string.md). Contains regular expression in the [re2](https://github.com/google/re2/wiki/Syntax) format.
- `format_regexp_escaping_rule` — [String](/sql-reference/data-types/string.md). The following escaping rules are supported:

  - CSV (similarly to [CSV](/interfaces/formats/CSV)
  - JSON (similarly to [JSONEachRow](/interfaces/formats/JSONEachRow)
  - Escaped (similarly to [TSV](/interfaces/formats/TabSeparated)
  - Quoted (similarly to [Values](/interfaces/formats/Values)
  - Raw (extracts subpatterns as a whole, no escaping rules, similarly to [TSVRaw](/interfaces/formats/TabSeparated)

- `format_regexp_skip_unmatched` — [UInt8](/sql-reference/data-types/int-uint.md). Defines the need to throw an exception in case the `format_regexp` expression does not match the imported data. Can be set to `0` or `1`.
)DOCS_MD"});

    factory.setDocumentation("RowBinary", Documentation{
        .description = R"DOCS_MD(
import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `RowBinary` format parses data by row in binary format. 
Rows and values are listed consecutively, without separators. 
Because data is in the binary format the delimiter after `FORMAT RowBinary` is strictly specified as follows: 

- Any number of whitespaces:
  - `' '` (space - code `0x20`)
  - `'\t'` (tab - code `0x09`)
  - `'\f'` (form feed - code `0x0C`) 
- Followed by exactly one new line sequence:
  - Windows style `"\r\n"` 
  - or Unix style `'\n'`
- Immediately followed by binary data.

:::note
This format is less efficient than the [Native](../Native.md) format since it is row-based.
:::

## Data types wire format {#data-types-wire-format}

:::tip
Most of the queries provided in the examples can be executed with curl with a file output.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```
:::

Then, the data can be examined with a hex editor.

### Unsigned LEB128 (Little Endian Base 128) {#unsigned-leb128}

An **unsigned little-endian** variable-width integer encoding used to encode the length of variable-size data types such as `String`, `Array` and `Map`. A sample implementation can be found on the [LEB128 wiki page](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256 {#integer-types}

All integer types are encoded with an appropriate number of bytes as **little-endian**. Signed types (`Int8` through `Int256`) use **two's complement** representation. Most languages support extracting such integers from byte arrays, using either built-in tools, or well-known libraries. For `Int128`/`Int256` and `UInt128`/`UInt256`, which exceed most languages' native integer sizes, custom deserialization may be required.

### Bool {#bool}

Boolean values are encoded as a single byte, and can be deserialized similarly to `UInt8`.

- `0` is `false`
- `1` is `true`

### Float32, Float64 {#float32-float64}

**Little-endian** floating-point numbers encoded as 4 bytes for `Float32` and 8 bytes for `Float64`. Similarly to integers, most languages provide proper tools to deserialize these values.

### BFloat16 {#bfloat16}

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) is a 16-bit floating point format with the range of Float32 and reduced precision, making it useful for machine learning workloads. The wire format is essentially the top 16 bits of a Float32 value. If your language doesn't support it natively, the easiest way to handle it is to read and write as UInt16, converting to and from Float32:

To convert BFloat16 to Float32 (pseudocode):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

To convert Float32 to BFloat16 (pseudocode):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

Sample underlying values for `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

### Decimal32, Decimal64, Decimal128, Decimal256 {#decimal}

Decimal types are represented as **little-endian** integers with respective bit width.

- `Decimal32` - 4 bytes, or `Int32`.
- `Decimal64` - 8 bytes, or `Int64`.
- `Decimal128` - 16 bytes, or `Int128`.
- `Decimal256` - 32 bytes, or `Int256`.

When deserializing a Decimal value, the whole and fractional parts can be derived using the following pseudocode:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

Where `trunc` performs truncation toward zero (not floor division, which differs for negative values), and `scale` is the number of digits after the decimal point. For example, for `Decimal(10, 2)` (an equivalent to `Decimal32(2)`), the scale is `2`, and the value `12345` will be represented as `(123, 45)`.

Serialization requires the reverse operation:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

See more details in the [Decimal types ClickHouse docs](https://clickhouse.com/docs/sql-reference/data-types/decimal).

### String {#string}

ClickHouse strings are **arbitrary byte sequences**. They are not required to be valid UTF-8. The length prefix is the **byte length**, not the character count.

Encoded in two parts:

1. A variable-length integer (LEB128) that indicates the length of the string in bytes.
2. The raw bytes of the string.

For example, a string `foobar` will be encoded using *seven* bytes as follows:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

### FixedString {#fixedstring}

Unlike `String`, `FixedString` has a fixed length, which is defined in the schema. It is encoded as a sequence of bytes, padded with trailing zero bytes if the value is shorter than `N`.

:::note
When reading a `FixedString`, trailing zero bytes may be either padding or actual `\0` characters in the data, they are indistinguishable on the wire. ClickHouse itself preserves all `N` bytes as-is.
:::

An empty `FixedString(3)` contains only padding zeroes:

```text
0x00, 0x00, 0x00
```

Non-empty `FixedString(3)` containing the string `hi`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

Non-empty `FixedString(3)` containing the string `bar`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

No padding is required in the last example, since all *three* bytes are used.

### Date {#date}

Stored as `UInt16` (two bytes) representing the number of days ***since*** `1970-01-01`.

Supported range of values: `[1970-01-01, 2149-06-06]`.

Sample underlying values for `Date`:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

### Date32 {#date32}

Stored as `Int32` (four bytes) representing the number of days ***before or after*** `1970-01-01`.

Supported range of values: `[1900-01-01, 2299-12-31]`.

Sample underlying values for `Date32`:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

A date before the epoch:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

### DateTime {#datetime}

Stored as `UInt32` (four bytes) representing the number of seconds ***since*** `1970-01-01 00:00:00 UTC`.

Syntax:

```text
DateTime([timezone])
```

For example, `DateTime` or `DateTime('UTC')`.

:::note
The binary value is always a UTC epoch offset. The timezone does not change the encoding. However, the timezone **does** affect how string values are interpreted on insertion: inserting `'2024-01-15 10:30:00'` into a `DateTime('America/New_York')` column stores a different epoch value than inserting the same string into a `DateTime('UTC')` column, because the string is interpreted as local time in the column's timezone. On the wire, both are just `UInt32` epoch seconds.
:::

Supported range of values: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

Sample underlying values for `DateTime`:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

### DateTime64 {#datetime64}

Stored as `Int64` (eight bytes) representing the number of **ticks** ***before or after*** `1970-01-01 00:00:00 UTC`. Tick resolution is defined by the `precision` parameter, see the syntax below:

```text
DateTime64(precision, [timezone])
```

Where `precision` is an integer from `0` to `9`. Typically, only the following are used: `3` (milliseconds), `6` (microseconds),
`9` (nanoseconds).

Examples of valid DateTime64 definitions: `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')`, or `DateTime64(9, 'Europe/Amsterdam')`.

:::note
As with `DateTime`, the binary value is always a UTC epoch offset. The timezone affects how string values are interpreted on insertion (see the [DateTime](#datetime) note), but the encoding itself is always `Int64` ticks since the UTC epoch.
:::

The underlying `Int64` value of the `DateTime64` type can be interpreted as the number of the following units before or after the UNIX epoch:

- `DateTime64(0)` - seconds.
- `DateTime64(3)` - milliseconds.
- `DateTime64(6)` - microseconds.
- `DateTime64(9)` - nanoseconds.

Supported range of values: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

Sample underlying values for `DateTime64`:

- `DateTime64(3)`: value `1546300800000` represents `2019-01-01 00:00:00 UTC`.
- `DateTime64(6)`: value `1705314600123456` represents `2024-01-15 10:30:00.123456 UTC`.
- `DateTime64(9)`: value `1705314600123456789` represents `2024-01-15 10:30:00.123456789 UTC`.

:::note
The precision of the maximum value is 8. If the maximum precision of 9 digits (nanoseconds) is used, the maximum supported value is 2262-04-11 23:47:16 in UTC.
:::

### Time {#time}

Stored as `Int32` representing a time value in seconds. Negative values are valid.

Supported range of values: `[-999:59:59, 999:59:59]` (i.e., `[-3599999, 3599999]` seconds).

:::note
At the moment, the setting `enable_time_time64_type` must be set to `1` to use `Time` or `Time64`.
:::

Sample underlying values for `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

### Time64 {#time64}

Internally stored as a `Decimal64` (which is stored as `Int64`) representing a time value with fractional seconds, with configurable precision. Negative values are valid. 

Syntax:

```text
Time64(precision)
```

Where `precision` is an integer from `0` to `9`. Common values: `3` (milliseconds), `6` (microseconds), `9` (nanoseconds).

Supported range of values: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
At the moment, the setting `enable_time_time64_type` must be set to `1` to use `Time` or `Time64`.
:::

The underlying `Int64` value represents fractional seconds scaled by `10^precision`.

Sample underlying values for `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

### Interval types {#interval-types}

All interval types are stored as `Int64` (eight bytes, little-endian). The value represents the count of the respective time unit. Negative values are valid.

The interval types are: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
The interval type name (e.g., `IntervalSecond` vs `IntervalDay`) determines the unit of the stored value. The wire encoding is always the same.
:::

Sample underlying values:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

### Enum8, Enum16 {#enum8-enum16}

Stored as a single byte (`Enum8` == `Int8`) or two bytes (`Enum16` == `Int16`) representing the index of the enum value in the enum definition. Note that the storage type is **signed** — enum values can be negative (e.g., `Enum8('a' = -128, 'b' = 0)`).

An Enum can be defined in a simple way, like this:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

The Enum8 defined above will have the following values map on the client:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

Or in a more complex way, like this:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

The Enum16 defined above will have the following values map on the client:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

For the data type parser, the main challenge is tracking escaped symbols in the enum definition, such as `\'`, and special symbols like `=` that may appear within quoted strings.

### UUID {#uuid}

Represented as a sequence of 16 bytes. The UUID is stored as **two little-endian `UInt64` values**: the first 8 bytes of the standard UUID representation are byte-reversed, and the second 8 bytes are independently byte-reversed.

For example, given UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0`:
- Standard byte representation: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
- First half reversed (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
- Second half reversed (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

Sample underlying values for `UUID`:

- `61f0c404-5cb3-11e7-907b-a6006ad3dba0` is represented as:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

- The default UUID `00000000-0000-0000-0000-000000000000` is represented as 16 zero bytes:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

It can be used when a new record was inserted, but the UUID value was not specified.

### IPv4 {#ipv4}

Stored in four bytes as `UInt32` in **little-endian** byte order. Note that this differs from the traditional network byte order (big-endian) commonly used for IP addresses. Sample underlying values for `IPv4`:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

### IPv6 {#ipv6}

Stored in 16 bytes in **big-endian / network byte order** (MSB first). Sample underlying values for `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

### Nullable {#nullable}

A nullable data type is encoded as follows:

1. A single byte that indicates whether the value is `NULL` or not:
    - `0x00` means the value is not `NULL`.
    - `0x01` means the value is `NULL`.
2. If the value is not `NULL`, the underlying data type is encoded as usual. If the value is `NULL`, **no additional bytes** are written for the underlying type.

For example, a `Nullable(UInt32)` value:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

### LowCardinality {#lowcardinality}

In RowBinary format, the low-cardinality marker does not affect the wire format. For example, a `LowCardinality(String)` is encoded the same way as a regular `String`.

:::warning
This only applies to RowBinary. In the Native format, `LowCardinality` uses a different dictionary-based encoding.
:::

:::note
A column can be defined as `LowCardinality(Nullable(T))`, but it is not possible to define it as `Nullable(LowCardinality(T))` - it will always result in an error from the server.
:::

While testing, [allow_suspicious_low_cardinality_types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) can be set to `1` to allow most of the data types inside `LowCardinality` for better coverage.

### Array {#array}

An array is encoded as follows:

1. A [variable-length integer (LEB128)](#unsigned-leb128) that indicates the number of elements in the array.
2. The elements of the array, encoded in the same way as the underlying data type.

For example, an array with `UInt32` values:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

A slightly more complex example:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
An array can contain nullable values, but the array itself cannot be nullable.
:::

The following is valid:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

And it will be encoded as follows:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

An example of dealing with multidimensional arrays can be found in the [Geo section](#geo-types).

### Tuple {#tuple}

A tuple is encoded as all elements of the tuple following each other in their corresponding wire format without any additional meta-information or delimiters.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

The string encoding of the tuple data type presents similar challenges as with the [Enum type](#enum8-enum16), such as tracking the escaped symbols and special characters; now, with Tuple it is also required to track open and closing parentheses. Additionally, note that the most complex Tuples can contain other nested Tuples, Arrays, Maps, and even enums.

For example, in the following table, the tuple contains an enum with a tick and parenthesis in the name, which can cause parsing issues if not handled properly:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

### Map {#map}

A map can be viewed as an `Array(Tuple(K, V))`, where `K` is the key type and `V` is the value type. The map is encoded as follows:

1. A [variable-length integer (LEB128)](#unsigned-leb128) that indicates the number of elements in the map.
2. The elements of the map as key-value pairs, encoded as their corresponding types.

For example, a map with `String` keys and `UInt32` values:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
It is possible to have maps with deeply nested structures, such as `Map(String, Map(Int32, Array(Nullable(String))))`, which will be encoded similarly to what is described above.
:::

### Variant {#variant}

This type represents a union of other data types. Type `Variant(T1, T2, ..., TN)` means that each row of this type has a value of either type `T1` or `T2` or … or `TN` or none of them (`NULL` value).

:::warning
While for the end user `Variant(T1, T2)` means exactly the same as `Variant(T2, T1)`, the order of types in the definition matters for the wire format: the types in the definition are always sorted alphabetically, and this is important, since the exact variant is encoded by a "discriminant" - the data type index in the definition.
:::

Consider the following example:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

A `NULL` value is encoded with a discriminant byte of `0xFF`:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

The [allow_suspicious_variant_types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) setting can be used to allow more exhaustive testing of the `Variant` type.

### Dynamic {#dynamic}

The `Dynamic` type can hold values of any type, determined at runtime. In RowBinary format, each value is self-describing: the first part is the type specification in [this format](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding). The contents then follow, with the value encoding as described in this document. So to parse a value you just need to use the type index to determine the right parser and then re-use the RowBinary parsing you already have elsewhere.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

Where `BinaryTypeIndex` is a single byte identifying the type. See the reference [here](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding) for the type indices and parameters.

A `NULL` Dynamic value is encoded with `BinaryTypeIndex` `0x00` (the `Nothing` type), with no additional bytes:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**Examples:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

### JSON {#json}

The JSON type encodes data in two distinct categories:

1. **Typed Paths** - Paths declared with explicit types in the schema (e.g., `JSON(user_id UInt32, name String)`)
2. **Dynamic Paths/Overflow paths when dynamic path limit is exceeded** - Runtime-discovered paths stored as `Dynamic` type. The value encoding is preceded by the type definition.

The wire format and rules are different for these two categories.

| Path Category | Included in Serialization | Value Encoding | Variant/Nullable allowed |
| --- | --- | --- | --- |
| **Typed paths** | Always (even if NULL) | Type-specific binary format | Yes |
| **Dynamic paths** | Only if non-null | Dynamic | No |

Paths are serialized in three groups, written sequentially: typed paths, dynamic paths, then shared data (overflow) paths. Typed and dynamic paths are written in an implementation-defined order (determined by internal hash-map iteration), while shared data paths are written in alphabetical order. Readers should not rely on any specific path ordering. The deserializer dispatches each path by name, not by position.

Each JSON row in RowBinary format is serialized as:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**Examples:**

**1. Simple JSON with typed paths only:**

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true}`

Binary encoding (hex with annotations):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. Simple JSON with typed and dynamic paths:**

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true, "name": "Alice"}`

Binary encoding (hex with annotations):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. Null handling:**

With a typed nullable column you get null:

Schema: `JSON(score Nullable(Int32))`

Row: `{"score": null }`

Binary encoding (hex with annotations):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

With a typed non-nullable column, you get the default value:

Schema: `JSON(name String)`

Row: `{"name": null}`

Binary encoding:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

With a dynamic path, it is ignored:

Schema: `JSON(id UInt64)`

Row: `{"id": 100, "metadata": null}`

Binary encoding:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

Note: The `metadata` path with NULL value is **not included** because dynamic paths are only serialized when non-null. This is a key difference from typed paths.

**4. Nested JSON objects:**

Schema: `JSON()`

Row: `{"user": {"name": "Bob", "age": 30}}`

Binary encoding (hex with annotations):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

Note: Nested objects are flattened into dot-separated paths (e.g., `user.name` instead of a nested structure).

**Alternative: JSON as String Mode**

With the setting `output_format_binary_write_json_as_string=1`, JSON columns are serialized as a single JSON text string instead of the structured binary format. There is a corresponding setting for writing to JSON columns, `input_format_binary_read_json_as_string`. The choice of setting here comes down to whether you want to parse the JSON in the client or the server.

### Geo types {#geo-types}

Geo is a category of data types that represent geographical data. It includes:

- `Point` - as `Tuple(Float64, Float64)`.
- `Ring` - as `Array(Point)`, or `Array(Tuple(Float64, Float64))`.
- `Polygon` - as `Array(Ring)`, or `Array(Array(Tuple(Float64, Float64)))`.
- `MultiPolygon` - as `Array(Polygon)`, or `Array(Array(Array(Tuple(Float64, Float64))))`.
- `LineString` - as `Array(Point)`, or `Array(Tuple(Float64, Float64))`.
- `MultiLineString` - as `Array(LineString)`, or `Array(Array(Tuple(Float64, Float64)))`.

The wire format of the Geo values is exactly the same as with Tuple and Array. `RowBinaryWithNamesAndTypes` format headers will contain the aliases for these types, e.g., `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, and `MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

### Geometry {#geometry}

`Geometry` is a `Variant` type that can hold any of the Geo types listed above. On the wire, it is encoded exactly like a `Variant`, with a discriminant byte indicating which geo type follows.

The discriminant indices for Geometry are:

| Index | Type |
| --- | --- |
| 0 | LineString |
| 1 | MultiLineString |
| 2 | MultiPolygon |
| 3 | Point |
| 4 | Polygon |
| 5 | Ring |

Wire format structure:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

Sample encoding of a `Point` as `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

Sample encoding of a `Ring` as `Geometry`:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

### Nested {#nested}

The wire format for `Nested` depends on the `flatten_nested` setting.

:::warning
All component arrays in a single row **must have the same length**. This is a server-enforced constraint. Mismatched lengths will cause insertion errors.
:::

#### `flatten_nested = 1` (default) {#nested-flattened}

With the default setting, `Nested` is flattened into independent arrays. Each sub-column becomes a separate `Array` column with a dot-separated name:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` shows the flattened columns:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

Each array is serialized independently, as described in the [Array](#array) section:

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

#### `flatten_nested = 0` {#nested-unflattened}

With `flatten_nested = 0`, `Nested` is preserved as a single column of type `Array(Tuple(...))`. The column name is not dot-separated:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo` shows a single column:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

The encoding is `Array(Tuple(String, Int32))`: an array length prefix, then each element's tuple fields in order:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

Note how the fields are interleaved per element (a₁, b₁, a₂, b₂) rather than grouped by column (a₁, a₂, b₁, b₂) as in the flattened representation.

### SimpleAggregateFunction {#simpleaggregatefunction}

`SimpleAggregateFunction(func, T)` is encoded identically to its underlying data type `T`. The aggregate function name does not affect the wire format.

For example, `SimpleAggregateFunction(max, UInt32)` is encoded the same way as a plain `UInt32`:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

The RowBinaryWithNamesAndTypes header reports the type as `SimpleAggregateFunction(max, UInt32)`, but the value on the wire is just a `UInt32`:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

### AggregateFunction {#aggregatefunction}

`AggregateFunction(func, T)` stores the full intermediate state of an aggregate function. Unlike `SimpleAggregateFunction`, which also stores an intermediate state but encodes it identically to the underlying data type, `AggregateFunction` stores an opaque binary blob whose format is specific to each aggregate function.

:::warning
Aggregate states have **no length prefix** in RowBinary. A parser must understand the internal serialization format of each specific aggregate function to know how many bytes to consume. In practice, most clients treat aggregate states as opaque and use `*State` / `*Merge` combinators to let the server handle serialization.
:::

The internal format varies by function. Some simple examples:

**`countState`** — stores the count as a VarUInt (LEB128):

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — stores the accumulated sum in a fixed-size integer. The width depends on the argument type (`UInt64` for integer arguments):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — stores a flag byte followed by the value in the underlying type. The flag is `0x00` for an empty state (no values seen) or `0x01` when a value is present:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

An empty state (no rows aggregated):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
More complex functions like `uniq`, `quantile`, or `groupArray` use implementation-specific formats. If you need to read or write these states, consult the ClickHouse source code for the specific function.
:::

### QBit {#qbit}

`QBit` is a vector type for efficient lookup with different levels of precision. Internally it’s stored in a transposed format. On the wire, QBit is simply an `Array` of the underlying element type (`Float32`, `Float64`, or `BFloat16`). The bit-transpose optimization for storage happens server-side, not in the RowBinary protocol.

Syntax:

```text
QBit(element_type, dimension)
```

Where `element_type` is `Float32`, `Float64`, or `BFloat16`, and `dimension` is the fixed vector dimension.

Wire format: identical to `Array(element_type)`:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

Sample encoding of `QBit(Float32, 4)` containing `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

## Format settings {#format-settings}

<RowBinaryFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("RowBinaryWithDefaults", Documentation{
        .description = R"DOCS_MD(
import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to the [`RowBinary`](./RowBinary.md) format, but with an extra byte before each column that indicates if the default value should be used.

## Example usage {#example-usage}

Examples:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```
```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

- For column `x` there is only one byte `01` that indicates that default value should be used and no other data after this byte is provided.
- For column `y` data starts with byte `00` that indicates that column has actual value that should be read from the subsequent data `01000000`.

## Format settings {#format-settings}

<RowBinaryFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("RowBinaryWithNames", Documentation{
        .description = R"DOCS_MD(
import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to the [`RowBinary`](./RowBinary.md) format, but with added header:

- [`LEB128`](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N).
- N `String`s specifying column names.

## Example usage {#example-usage}

## Format settings {#format-settings}

<RowBinaryFormatSettings/>

:::note
- If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped. 
- If setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("RowBinaryWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to the [RowBinary](./RowBinary.md) format, but with added header:

- [`LEB128`](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N).
- N `String`s specifying column names.
- N `String`s specifying column types.

## Example usage {#example-usage}

## Format settings {#format-settings}

<RowBinaryFormatSettings/>

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [`input_format_with_types_use_header`](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::
)DOCS_MD"});

    factory.setDocumentation("SQLInsert", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Outputs data as a sequence of `INSERT INTO table (columns...) VALUES (...), (...) ...;` statements.

## Example usage {#example-usage}

Example:

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

To read data output by this format you can use [MySQLDump](../formats/MySQLDump.md) input format.

## Format settings {#format-settings}

| Setting                                                                                                                                | Description                                         | Default   |
|----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|-----------|
| [`output_format_sql_insert_max_batch_size`](../../operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)    | The maximum number of rows in one INSERT statement. | `65505`   |
| [`output_format_sql_insert_table_name`](../../operations/settings/settings-formats.md/#output_format_sql_insert_table_name)            | The name of the table in the output INSERT query.   | `'table'` |
| [`output_format_sql_insert_include_column_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) | Include column names in INSERT query.               | `true`    |
| [`output_format_sql_insert_use_replace`](../../operations/settings/settings-formats.md/#output_format_sql_insert_use_replace)          | Use REPLACE statement instead of INSERT.            | `false`   |
| [`output_format_sql_insert_quote_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_quote_names)          | Quote column names with "\`" characters.            | `true`    |
)DOCS_MD"});

    factory.setDocumentation("TSKV", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to the [`TabSeparated`](./TabSeparated.md) format, but outputs a value in `name=value` format. 
Names are escaped the same way as in the [`TabSeparated`](./TabSeparated.md) format, and the `=` symbol is also escaped.

```text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=clickhouse     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

```sql title="Query"
SELECT * FROM t_null FORMAT TSKV
```

```text title="Response"
x=1    y=\N
```

:::note
When there are a large number of small columns, this format is ineffective, and there is generally no reason to use it. 
Nevertheless, it is no worse than the [`JSONEachRow`](../JSON/JSONEachRow.md) format in terms of efficiency.
:::

For parsing, any order is supported for the values of the different columns. 
It is acceptable for some values to be omitted as they are treated as equal to their default values.
In this case, zeros and blank rows are used as default values. 
Complex values that could be specified in the table are not supported as defaults.

Parsing allows an additional field `tskv` to be added without the equal sign or a value. This field is ignored.

During import, columns with unknown names will be skipped, 
if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.

[NULL](/sql-reference/syntax.md) is formatted as `\N`.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tskv file, named as `football.tskv`:

```tsv
date=2022-04-30 season=2021     home_team=Sutton United away_team=Bradford City home_team_goals=1       away_team_goals=4
date=2022-04-30 season=2021     home_team=Swindon Town  away_team=Barrow        home_team_goals=2       away_team_goals=1
date=2022-04-30 season=2021     home_team=Tranmere Rovers       away_team=Oldham Athletic       home_team_goals=2       away_team_goals=0
date=2022-05-02 season=2021     home_team=Port Vale     away_team=Newport County        home_team_goals=1       away_team_goals=2
date=2022-05-02 season=2021     home_team=Salford City  away_team=Mansfield Town        home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Barrow        away_team=Northampton Town      home_team_goals=1       away_team_goals=3
date=2022-05-07 season=2021     home_team=Bradford City away_team=Carlisle United       home_team_goals=2       away_team_goals=0
date=2022-05-07 season=2021     home_team=Bristol Rovers        away_team=Scunthorpe United     home_team_goals=7       away_team_goals=0
date=2022-05-07 season=2021     home_team=Exeter City   away_team=Port Vale     home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Harrogate Town A.F.C. away_team=Sutton United home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Hartlepool United     away_team=Colchester United     home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Leyton Orient away_team=Tranmere Rovers       home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Mansfield Town        away_team=Forest Green Rovers   home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Newport County        away_team=Rochdale      home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Oldham Athletic       away_team=Crawley Town  home_team_goals=3       away_team_goals=3
date=2022-05-07 season=2021     home_team=Stevenage Borough     away_team=Salford City  home_team_goals=4       away_team_goals=2
date=2022-05-07 season=2021     home_team=Walsall       away_team=Swindon Town  home_team_goals=0       away_team_goals=3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tskv' FORMAT TSKV;
```

### Reading data {#reading-data}

Read data using the `TSKV` format:

```sql
SELECT *
FROM football
FORMAT TSKV
```

The output will be in tab separated format with two header rows for column names and types:

```tsv
date=2022-04-30 season=2021     home_team=Sutton United away_team=Bradford City home_team_goals=1       away_team_goals=4
date=2022-04-30 season=2021     home_team=Swindon Town  away_team=Barrow        home_team_goals=2       away_team_goals=1
date=2022-04-30 season=2021     home_team=Tranmere Rovers       away_team=Oldham Athletic       home_team_goals=2       away_team_goals=0
date=2022-05-02 season=2021     home_team=Port Vale     away_team=Newport County        home_team_goals=1       away_team_goals=2
date=2022-05-02 season=2021     home_team=Salford City  away_team=Mansfield Town        home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Barrow        away_team=Northampton Town      home_team_goals=1       away_team_goals=3
date=2022-05-07 season=2021     home_team=Bradford City away_team=Carlisle United       home_team_goals=2       away_team_goals=0
date=2022-05-07 season=2021     home_team=Bristol Rovers        away_team=Scunthorpe United     home_team_goals=7       away_team_goals=0
date=2022-05-07 season=2021     home_team=Exeter City   away_team=Port Vale     home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Harrogate Town A.F.C. away_team=Sutton United home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Hartlepool United     away_team=Colchester United     home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Leyton Orient away_team=Tranmere Rovers       home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Mansfield Town        away_team=Forest Green Rovers   home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Newport County        away_team=Rochdale      home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Oldham Athletic       away_team=Crawley Town  home_team_goals=3       away_team_goals=3
date=2022-05-07 season=2021     home_team=Stevenage Borough     away_team=Salford City  home_team_goals=4       away_team_goals=2
date=2022-05-07 season=2021     home_team=Walsall       away_team=Swindon Town  home_team_goals=0       away_team_goals=3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TSV", Documentation{
        .description = "An alias for the `TabSeparated` format. See the `TabSeparated` entry for the full documentation.",
        .related = {"TabSeparated"}});

    factory.setDocumentation("TSVRaw", Documentation{
        .description = "An alias for the `TabSeparatedRaw` format. See the `TabSeparatedRaw` entry for the full documentation.",
        .related = {"TabSeparatedRaw"}});

    factory.setDocumentation("TSVRawWithNames", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNames` format. See the `TabSeparatedRawWithNames` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNames"}});

    factory.setDocumentation("TSVRawWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedRawWithNamesAndTypes` format. See the `TabSeparatedRawWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedRawWithNamesAndTypes"}});

    factory.setDocumentation("TSVWithNames", Documentation{
        .description = "An alias for the `TabSeparatedWithNames` format. See the `TabSeparatedWithNames` entry for the full documentation.",
        .related = {"TabSeparatedWithNames"}});

    factory.setDocumentation("TSVWithNamesAndTypes", Documentation{
        .description = "An alias for the `TabSeparatedWithNamesAndTypes` format. See the `TabSeparatedWithNamesAndTypes` entry for the full documentation.",
        .related = {"TabSeparatedWithNamesAndTypes"}});

    factory.setDocumentation("TabSeparated", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias  |
|-------|--------|--------|
| ✔     | ✔      | `TSV`  |

## Description {#description}

In TabSeparated format, data is written by row. Each row contains values separated by tabs. Each value is followed by a tab, except the last value in the row, which is followed by a line feed. Strictly Unix line feeds are assumed everywhere. The last row also must contain a line feed at the end. Values are written in text format, without enclosing quotation marks, and with special characters escaped.

This format is also available under the name `TSV`.

The `TabSeparated` format is convenient for processing data using custom programs and scripts. It is used by default in the HTTP interface, and in the command-line client's batch mode. This format also allows transferring data between different DBMSs. For example, you can get a dump from MySQL and upload it to ClickHouse, or vice versa.

The `TabSeparated` format supports outputting total values (when using WITH TOTALS) and extreme values (when 'extremes' is set to 1). In these cases, the total values and extremes are output after the main data. The main result, total values, and extremes are separated from each other by an empty line. Example:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

## Data formatting {#tabseparated-data-formatting}

Integer numbers are written in decimal form. Numbers can contain an extra "+" character at the beginning (ignored when parsing, and not recorded when formatting). Non-negative numbers can't contain the negative sign. When reading, it is allowed to parse an empty string as a zero, or (for signed types) a string consisting of just a minus sign as a zero. Numbers that do not fit into the corresponding data type may be parsed as a different number, without an error message.

Floating-point numbers are written in decimal form. The dot is used as the decimal separator. Exponential entries are supported, as are 'inf', '+inf', '-inf', and 'nan'. An entry of floating-point numbers may begin or end with a decimal point.
During formatting, accuracy may be lost on floating-point numbers.
During parsing, it is not strictly required to read the nearest machine-representable number.

Dates are written in YYYY-MM-DD format and parsed in the same format, but with any characters as separators.
Dates with times are written in the format `YYYY-MM-DD hh:mm:ss` and parsed in the same format, but with any characters as separators.
This all occurs in the system time zone at the time the client or server starts (depending on which of them formats data). For dates with times, daylight saving time is not specified. So if a dump has times during daylight saving time, the dump does not unequivocally match the data, and parsing will select one of the two times.
During a read operation, incorrect dates and dates with times can be parsed with natural overflow or as null dates and times, without an error message.

As an exception, parsing dates with times is also supported in Unix timestamp format, if it consists of exactly 10 decimal digits. The result is not time zone-dependent. The formats `YYYY-MM-DD hh:mm:ss` and `NNNNNNNNNN` are differentiated automatically.

Strings are output with backslash-escaped special characters. The following escape sequences are used for output: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Parsing also supports the sequences `\a`, `\v`, and `\xHH` (hex escape sequences) and any `\c` sequences, where `c` is any character (these sequences are converted to `c`). Thus, reading data supports formats where a line feed can be written as `\n` or `\`, or as a line feed. For example, the string `Hello world` with a line feed between the words instead of space can be parsed in any of the following variations:

```text
Hello\nworld

Hello\
world
```

The second variant is supported because MySQL uses it when writing tab-separated dumps.

The minimum set of characters that you need to escape when passing data in TabSeparated format: tab, line feed (LF) and backslash.

Only a small set of symbols are escaped. You can easily stumble onto a string value that your terminal will ruin in output.

Arrays are written as a list of comma-separated values in `[]`. Number items in the array are formatted as normally. `Date` and `DateTime` types are written in single quotes. Strings are written in single quotes with the same escaping rules as above.

[NULL](/sql-reference/syntax.md) is formatted according to setting [format_tsv_null_representation](/operations/settings/settings-formats.md/#format_tsv_null_representation) (default value is `\N`).

In input data, ENUM values can be represented as names or as ids. First, we try to match the input value to the ENUM name. If we fail and the input value is a number, we try to match this number to ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_tsv_enum_as_number](/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) to optimize ENUM parsing.

Each element of [Nested](/sql-reference/data-types/nested-data-structures/index.md) structures is represented as an array.

For example:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```
```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```
```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

### Reading data {#reading-data}

Read data using the `TabSeparated` format:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

The output will be in tab separated format:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}

| Setting                                                                                                                                                          | Description                                                                                                                                                                                                                                    | Default |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`format_tsv_null_representation`](/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Custom NULL representation in TSV format.                                                                                                                                                                                                      | `\N`    |
| [`input_format_tsv_empty_as_default`](/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | treat empty fields in TSV input as default values. For complex default expressions [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. | `false` |
| [`input_format_tsv_enum_as_number`](/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | treat inserted enum values in TSV formats as enum indices.                                                                                                                                                                                     | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | use some tweaks and heuristics to infer schema in TSV format. If disabled, all fields will be inferred as Strings.                                                                                                                             | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | if it is set true, end of line in TSV output format will be `\r\n` instead of `\n`.                                                                                                                                                            | `false` |
| [`input_format_tsv_crlf_end_of_line`](/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | if it is set true, end of line in TSV input format will be `\r\n` instead of `\n`.                                                                                                                                                             | `false` |
| [`input_format_tsv_skip_first_lines`](/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | skip specified number of lines at the beginning of data.                                                                                                                                                                                       | `0`     |
| [`input_format_tsv_detect_header`](/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | automatically detect header with names and types in TSV format.                                                                                                                                                                                | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | skip trailing empty lines at the end of data.                                                                                                                                                                                                  | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | allow variable number of columns in TSV format, ignore extra columns and use default values on missing columns.                                                                                                                                | `false` |
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRaw", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias           |
|-------|--------|-----------------|
| ✔     | ✔      | `TSVRaw`, `Raw` |

## Description {#description}

Differs from the [`TabSeparated`](/interfaces/formats/TabSeparated) format in that rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

For a comparison of the `TabSeparatedRaw` format and the `RawBlob` format see: [Raw Formats Comparison](../RawBLOB.md/#raw-formats-comparison)

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRaw;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRaw` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRaw
```

The output will be in tab separated format:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRawWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                             |
|-------|--------|-----------------------------------|
| ✔     | ✔      | `TSVRawWithNames`, `RawWithNames` |

## Description {#description}

Differs from the [`TabSeparatedWithNames`](./TabSeparatedWithNames.md) format, 
in that the rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRawWithNames;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRawWithNames` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRawWithNames
```

The output will be in tab separated format with a single line header:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedRawWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                             |
|-------|--------|---------------------------------------------------|
| ✔     | ✔      | `TSVRawWithNamesAndNames`, `RawWithNamesAndNames` |

## Description {#description}

Differs from the [`TabSeparatedWithNamesAndTypes`](./TabSeparatedWithNamesAndTypes.md) format,
in that the rows are written without escaping.

:::note
When parsing with this format, tabs or line-feeds are not allowed in each field.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRawWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedRawWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRawWithNamesAndTypes
```

The output will be in tab separated format with two header rows for column names and types:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                          |
|-------|--------|--------------------------------|
|     ✔    |     ✔     | `TSVWithNames`, `RawWithNames` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written in the first row.

During parsing, the first row is expected to contain the column names. You can use column names to determine their position and to check their correctness.

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns of the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNames;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedWithNames` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNames
```

The output will be in tab separated format:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("TabSeparatedWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                          |
|-------|--------|------------------------------------------------|
|     ✔    |     ✔     | `TSVWithNamesAndTypes`, `RawWithNamesAndTypes` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written to the first row, while the column types are in the second row.

:::note
- If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns in the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
- If setting [`input_format_with_types_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNamesAndTypes;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedWithNamesAndTypes` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNamesAndTypes
```

The output will be in tab separated format with two header rows for column names and types:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
Date    Int16   LowCardinality(String)  LowCardinality(String)  Int8    Int8
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Template", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

For cases where you need more customization than other standard formats offer, 
the `Template` format allows the user to specify their own custom format string with placeholders for values,
and specifying escaping rules for the data.

It uses the following settings:

| Setting                                                                                                  | Description                                                                                                                |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| [`format_template_row`](#format_template_row)                                                            | Specifies the path to the file which contains format strings for rows.                                                     |
| [`format_template_resultset`](#format_template_resultset)                                                | Specifies the path to the file which contains format strings for rows                                                      |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                      | Specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default) |
| `format_template_row_format`                                                                             | Specifies the format string for rows [in-line](#inline_specification).                                                     |                                                                           
| `format_template_resultset_format`                                                                       | Specifies the result set format string [in-line](#inline_specification).                                                   |
| Some settings of other formats (e.g.`output_format_json_quote_64bit_integers` when using `JSON` escaping |                                                                                                                            |

## Settings and escaping rules {#settings-and-escaping-rules}

### format_template_row {#format_template_row}

The setting `format_template_row` specifies the path to the file which contains format strings for rows with the following syntax:

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

Where:

| Part of syntax | Description                                                                                                       |
|----------------|-------------------------------------------------------------------------------------------------------------------|
| `delimiter_i`  | A delimiter between values (`$` symbol can be escaped as `$$`)                                                    |
| `column_i`     | The name or index of a column whose values are to be selected or inserted (if empty, then the column will be skipped) |
|`serializeAs_i` | An escaping rule for the column values.                                                                           |

The following escaping rules are supported:

| Escaping Rule        | Description                              |
|----------------------|------------------------------------------|
| `CSV`, `JSON`, `XML` | Similar to the formats of the same names |
| `Escaped`            | Similar to `TSV`                         |
| `Quoted`             | Similar to `Values`                      |
| `Raw`                | Without escaping, similar to `TSVRaw`    |   
| `None`               | No escaping rule - see note below        |

:::note
If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.
:::

Let's look at an example. Given the following format string:

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

The following values will be printed (if using `SELECT`) or expected (if using `INPUT`), 
between columns `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively:

- `s` (with escape rule `Quoted`)
- `c` (with escape rule `Escaped`)
- `p` (with escape rule `JSON`)

For example:

- If `INSERT`ing, the line below matches the expected template and would read values `bathroom interior design`, `2166`, `$3` into columns `Search phrase`, `count`, `ad price`.
- If `SELECT`ing the line below is the output, assuming that values `bathroom interior design`, `2166`, `$3` are already stored in a table under columns `Search phrase`, `count`, `ad price`.  

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

### format_template_rows_between_delimiter {#format_template_rows_between_delimiter}

The setting `format_template_rows_between_delimiter` setting specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

### format_template_resultset {#format_template_resultset}

The setting `format_template_resultset` specifies the path to the file, which contains a format string for the result set. 

The format string for the result set has the same syntax as a format string for rows. 
It allows for specifying a prefix, a suffix and a way to print some additional information and contains the following placeholders instead of column names:

- `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
- `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS).
- `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1).
- `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1).
- `rows` is the total number of output rows.
- `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
- `time` is the request execution time in seconds.
- `rows_read` is the number of rows has been read.
- `bytes_read` is the number of bytes (uncompressed) has been read.

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.

:::note
If the `format_template_resultset` setting is an empty string, `${data}` is used as the default value.
:::

For insert queries format allows skipping some columns or fields if prefix or suffix (see example).

### In-line specification {#inline_specification}

Often times it is challenging or not possible to deploy the format configurations
(set by `format_template_row`, `format_template_resultset`) for the template format to a directory on all nodes in a cluster. 
Furthermore, the format may be so trivial that it does not require being placed in a file.

For these cases, `format_template_row_format` (for `format_template_row`) and `format_template_resultset_format` (for `format_template_resultset`) can be used to set the template string directly in the query, 
rather than as a path to the file which contains it.

:::note
The rules for format strings and escape sequences are the same as those for:
- [`format_template_row`](#format_template_row) when using `format_template_row_format`.
- [`format_template_resultset`](#format_template_resultset) when using `format_template_resultset_format`.
:::

## Example usage {#example-usage}

Let's look at two examples of how we can use the `Template` format, first for selecting data and then for inserting data.

### Selecting data {#selecting-data}

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

### Inserting data {#inserting-data}

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

### In-line specification {#in-line-specification}

Tired of manually formatting markdown tables? In this example we'll look at how we can use the `Template` format and in-line specification settings to achieve a simple task - `SELECT`ing the names of some ClickHouse formats from the `system.formats` table and formatting them as a markdown table. This can be easily achieved using the `Template` format and settings `format_template_row_format` and `format_template_resultset_format`.

In previous examples we specified the result-set and row format strings in separate files, with the paths to those files specified using the `format_template_resultset` and `format_template_row` settings respectively. Here we'll do it in-line because our template is trivial, consisting only of a few `|` and `-` to make the markdown table. We'll specify our result-set template string using the setting `format_template_resultset_format`. To make the table header we've added `|ClickHouse Formats|\n|---|\n` before `${data}`. We use setting `format_template_row_format` to specify the template string `` |`{0:XML}`| `` for our rows. The `Template` format will insert our rows with the given format into placeholder `${data}`. In this example we have only one column, but if you wanted to add more you could do so by adding `{1:XML}`, `{2:XML}`... etc to your row template string, choosing the escaping rule as appropriate. In this example we've gone with escaping rule `XML`. 

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

Look at that! We've saved ourselves the trouble of having to manually add all those `|`s and `-`s to make that markdown table:

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```
)DOCS_MD"});

    factory.setDocumentation("TemplateIgnoreSpaces", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to [`Template`], but skips whitespace characters between delimiters and values in the input stream. 
However, if format strings contain whitespace characters, these characters will be expected in the input stream. 
Also allows specifying empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. 
Such placeholders are used only for skipping whitespace characters.
It's possible to read `JSON` using this format if the values of columns have the same order in all rows.

:::note
This format is suitable only for input.
:::

## Example usage {#example-usage}

The following request can be used for inserting data from its output example of format [JSON](/interfaces/formats/JSON):

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("Values", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Values` format prints every row in brackets. 

- Rows are separated by commas without a comma after the last row. 
- The values inside the brackets are also comma-separated. 
- Numbers are output in a decimal format without quotes. 
- Arrays are output in `[]`.
- Strings, dates, and dates with times are output in quotes. 
- Escaping rules and parsing are similar to the [TabSeparated](TabSeparated/TabSeparated.md) format.

During formatting, extra spaces aren't inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). 
[`NULL`](/sql-reference/syntax.md) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in the `Values` format: 
- single quotes
- backslashes

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

## Example usage {#example-usage}

## Format settings {#format-settings}

| Setting                                                                                                                                                     | Description                                                                                                                                                                                   | Default |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.                                                                               | `true`  |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows. | `true`  |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.                                                       | `true`  |
)DOCS_MD"});

    factory.setDocumentation("Vertical", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.

Note that [`NULL`](/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ` to make it easier to distinguish between the string value `NULL` and no value. JSON columns will be pretty printed, and `NULL` is output as `null`, because it is a valid JSON value and easily distinguishable from `"null"`.

## Example usage {#example-usage}

Example:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Rows are not escaped in Vertical format:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("XML", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `XML` format is suitable only for output, and not for parsing. 

If the column name does not have an acceptable format, just 'field' is used as the element name. In general, the XML structure follows the JSON structure.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character `�` so the output text will consist of valid UTF-8 sequences.

In string values, the characters `<` and `&` are escaped as `<` and `&`.

Arrays are output as `<array><elem>Hello</elem><elem>World</elem>...</array>`,and tuples as `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## Example usage {#example-usage}

Example:

```xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>clickhouse</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

## Format settings {#format-settings}

## XML {#xml}
)DOCS_MD"});

}

}
