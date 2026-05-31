#include <Storages/Kafka/StorageKafkaUtils.h>


#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicatedHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/config_version.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <cppkafka/cppkafka.h>
#include <librdkafka/rdkafka.h>

#if USE_KRB5
#    include <Access/KerberosInit.h>
#endif // USE_KRB5

namespace ProfileEvents
{
extern const Event KafkaConsumerErrors;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_kafka_offsets_storage_in_keeper;
    extern const SettingsBool kafka_disable_num_consumers_limit;
}

namespace KafkaSetting
{
    extern const KafkaSettingsUInt64 input_format_allow_errors_num;
    extern const KafkaSettingsFloat input_format_allow_errors_ratio;
    extern const KafkaSettingsBool input_format_skip_unknown_fields;
    extern const KafkaSettingsString kafka_broker_list;
    extern const KafkaSettingsString kafka_client_id;
    extern const KafkaSettingsBool kafka_commit_every_batch;
    extern const KafkaSettingsBool kafka_commit_on_select;
    extern const KafkaSettingsMilliseconds kafka_flush_interval_ms;
    extern const KafkaSettingsString kafka_format;
    extern const KafkaSettingsString kafka_group_name;
    extern const KafkaSettingsStreamingHandleErrorMode kafka_handle_error_mode;
    extern const KafkaSettingsString kafka_keeper_path;
    extern const KafkaSettingsUInt64 kafka_max_block_size;
    extern const KafkaSettingsUInt64 kafka_max_rows_per_message;
    extern const KafkaSettingsUInt64 kafka_num_consumers;
    extern const KafkaSettingsUInt64 kafka_poll_max_batch_size;
    extern const KafkaSettingsMilliseconds kafka_poll_timeout_ms;
    extern const KafkaSettingsString kafka_replica_name;
    extern const KafkaSettingsString kafka_schema;
    extern const KafkaSettingsUInt64 kafka_schema_registry_skip_bytes;
    extern const KafkaSettingsUInt64 kafka_skip_broken_messages;
    extern const KafkaSettingsBool kafka_thread_per_consumer;
    extern const KafkaSettingsString kafka_topic_list;
}

using namespace std::chrono_literals;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}


void registerStorageKafka(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args) -> std::shared_ptr<IStorage>
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        const bool has_settings = args.storage_def->settings;

        auto kafka_settings = std::make_unique<KafkaSettings>();
        String collection_name;
        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
        {
            kafka_settings->loadFromNamedCollection(named_collection);
            collection_name = assert_cast<const ASTIdentifier *>(args.engine_args[0].get())->name();
        }

        if (has_settings)
        {
            kafka_settings->loadFromQuery(*args.storage_def);
        }

// Check arguments and settings
#define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME, EVAL, TYPE) \
    /* One of the four required arguments is not specified */ \
    if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 && !(*kafka_settings)[KafkaSetting::PAR_NAME].changed) \
    { \
        throw Exception( \
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, \
            "Required parameter '{}' " \
            "for storage Kafka not specified", \
            #PAR_NAME); \
    } \
    if (args_count >= (ARG_NUM)) \
    { \
        /* The same argument is given in two places */ \
        if (has_settings && (*kafka_settings)[KafkaSetting::PAR_NAME].changed) \
            throw Exception( \
                ErrorCodes::BAD_ARGUMENTS, \
                "The argument №{} of storage Kafka " \
                "and the parameter '{}' " \
                "in SETTINGS cannot be specified at the same time", \
                #ARG_NUM, \
                #PAR_NAME); \
        /* move engine args to settings */ \
        if constexpr ((EVAL) == 1) \
        { \
            engine_args[(ARG_NUM)-1] = evaluateConstantExpressionAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
        } \
        if constexpr ((EVAL) == 2) \
        { \
            engine_args[(ARG_NUM)-1] \
                = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
        } \
        (*kafka_settings)[KafkaSetting::PAR_NAME] = checkAndGetLiteralArgument<TYPE>(engine_args[(ARG_NUM)-1], #PAR_NAME); \
    }

        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constant expression with a string result)
          * - Message format (string)
          * - Row delimiter
          * - Schema (optional, if the format supports it)
          * - Number of consumers
          * - Max block size for background consumption
          * - Skip (at least) unreadable messages number
          * - Do intermediate commits when the batch consumed and handled
          */

        /* 0 = raw, 1 = evaluateConstantExpressionAsLiteral, 2=evaluateConstantExpressionOrIdentifierAsLiteral */
        /// In case of named collection we already validated the arguments.
        if (collection_name.empty())
        {
            CHECK_KAFKA_STORAGE_ARGUMENT(1, kafka_broker_list, 0, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(2, kafka_topic_list, 1, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(3, kafka_group_name, 2, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(4, kafka_format, 2, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(6, kafka_schema, 2, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(7, kafka_num_consumers, 0, UInt64)
            CHECK_KAFKA_STORAGE_ARGUMENT(8, kafka_max_block_size, 0, UInt64)
            CHECK_KAFKA_STORAGE_ARGUMENT(9, kafka_skip_broken_messages, 0, UInt64)
            CHECK_KAFKA_STORAGE_ARGUMENT(10, kafka_commit_every_batch, 0, bool)
            CHECK_KAFKA_STORAGE_ARGUMENT(11, kafka_client_id, 2, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(12, kafka_poll_timeout_ms, 0, UInt64)
            CHECK_KAFKA_STORAGE_ARGUMENT(13, kafka_flush_interval_ms, 0, UInt64)
            CHECK_KAFKA_STORAGE_ARGUMENT(14, kafka_thread_per_consumer, 0, bool)
            CHECK_KAFKA_STORAGE_ARGUMENT(15, kafka_handle_error_mode, 0, String)
            CHECK_KAFKA_STORAGE_ARGUMENT(16, kafka_commit_on_select, 0, bool)
            CHECK_KAFKA_STORAGE_ARGUMENT(17, kafka_max_rows_per_message, 0, UInt64)
        }

#undef CHECK_KAFKA_STORAGE_ARGUMENT

        auto num_consumers = (*kafka_settings)[KafkaSetting::kafka_num_consumers].value;
        auto max_consumers = std::max<uint32_t>(getNumberOfCPUCoresToUse(), 16);

        if (!args.getLocalContext()->getSettingsRef()[Setting::kafka_disable_num_consumers_limit] && num_consumers > max_consumers)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The number of consumers can not be bigger than {}. "
                "A single consumer can read any number of partitions. "
                "Extra consumers are relatively expensive, "
                "and using a lot of them can lead to high memory and CPU usage. "
                "To achieve better performance "
                "of getting data from Kafka, consider using a setting kafka_thread_per_consumer=1, "
                "and ensure you have enough threads "
                "in MessageBrokerSchedulePool (background_message_broker_schedule_pool_size). "
                "See also https://clickhouse.com/docs/integrations/kafka/kafka-table-engine#tuning-performance",
                max_consumers);
        }
        if (num_consumers < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of consumers can not be lower than 1");
        }

        if ((*kafka_settings)[KafkaSetting::kafka_max_block_size].changed && (*kafka_settings)[KafkaSetting::kafka_max_block_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_max_block_size can not be lower than 1");
        }

        if ((*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].changed && (*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_poll_max_batch_size can not be lower than 1");
        }

        constexpr size_t MAX_SKIP_BYTES = 255;
        if ((*kafka_settings)[KafkaSetting::kafka_schema_registry_skip_bytes].value > MAX_SKIP_BYTES)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                           "kafka_schema_registry_skip_bytes value {} must be between 0 and {}",
                           (*kafka_settings)[KafkaSetting::kafka_schema_registry_skip_bytes].value, MAX_SKIP_BYTES);
        }
        NamesAndTypesList supported_columns;
        for (const auto & column : args.columns)
        {
            if (column.default_desc.kind == ColumnDefaultKind::Alias)
                supported_columns.emplace_back(column.name, column.type);
            if (column.default_desc.kind == ColumnDefaultKind::Default && !column.default_desc.expression)
                supported_columns.emplace_back(column.name, column.type);
        }
        // Kafka engine allows only ordinary columns without default expression or alias columns.
        if (args.columns.getAll() != supported_columns)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "KafkaEngine doesn't support DEFAULT/MATERIALIZED/EPHEMERAL expressions for columns. "
                "See https://clickhouse.com/docs/engines/table-engines/integrations/kafka/#configuration");
        }

        const auto has_keeper_path = (*kafka_settings)[KafkaSetting::kafka_keeper_path].changed && !(*kafka_settings)[KafkaSetting::kafka_keeper_path].value.empty();
        const auto has_replica_name = (*kafka_settings)[KafkaSetting::kafka_replica_name].changed && !(*kafka_settings)[KafkaSetting::kafka_replica_name].value.empty();

        if (!has_keeper_path && !has_replica_name)
            return std::make_shared<StorageKafka>(
                args.table_id, args.getContext(), args.columns, args.comment, std::move(kafka_settings), collection_name);

        if (!args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_kafka_offsets_storage_in_keeper] && !args.query.attach)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Storing the Kafka offsets in Keeper is experimental. Set `allow_experimental_kafka_offsets_storage_in_keeper` setting "
                "to enable it");

        if (!has_keeper_path || !has_replica_name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "To store committed offsets in Keeper both kafka_keeper_path and kafka_replica_name must be specified");

        const auto is_on_cluster = args.getLocalContext()->isDDLOrOnClusterInternal();
        const auto is_replicated_database = args.getLocalContext()->isDDLOrOnClusterInternal()
            && DatabaseCatalog::instance().getDatabase(args.table_id.database_name)->getEngineName() == "Replicated";

        // UUID macro is only allowed:
        // - with Atomic database only with ON CLUSTER queries, otherwise it is easy to misuse: each replica would have separate uuid generated.
        // - with Replicated database
        // - with attach queries, as those are used on server startup
        const auto allow_uuid_macro = is_on_cluster || is_replicated_database || args.query.attach;

        auto context = args.getContext();
        // Unfold {database} and {table} macro on table creation, so table can be renamed.
        if (args.mode < LoadingStrictnessLevel::ATTACH)
        {
            Macros::MacroExpansionInfo info;
            /// NOTE: it's not recursive
            info.expand_special_macros_only = true;
            info.table_id = args.table_id;
            // We could probably unfold UUID here too, but let's keep it similar to ReplicatedMergeTree, which doesn't do the unfolding.
            info.table_id.uuid = UUIDHelpers::Nil;
            (*kafka_settings)[KafkaSetting::kafka_keeper_path].value = context->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_keeper_path].value, info);

            info.level = 0;
            (*kafka_settings)[KafkaSetting::kafka_replica_name].value = context->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_replica_name].value, info);
        }


        auto * settings_query = args.storage_def->settings;
        chassert(has_settings && "Unexpected settings query in StorageKafka");

        settings_query->changes.setSetting("kafka_keeper_path", (*kafka_settings)[KafkaSetting::kafka_keeper_path].value);
        settings_query->changes.setSetting("kafka_replica_name", (*kafka_settings)[KafkaSetting::kafka_replica_name].value);

        // Expand other macros (such as {replica}). We do not expand them on previous step to make possible copying metadata files between replicas.
        // Disable expanding {shard} macro, because it can lead to incorrect behavior and it doesn't make sense to shard Kafka tables.
        Macros::MacroExpansionInfo info;
        info.table_id = args.table_id;
        if (is_replicated_database)
        {
            auto database = DatabaseCatalog::instance().getDatabase(args.table_id.database_name);
            info.shard.reset();
            info.replica = getReplicatedDatabaseReplicaName(database);
        }
        if (!allow_uuid_macro)
            info.table_id.uuid = UUIDHelpers::Nil;
        (*kafka_settings)[KafkaSetting::kafka_keeper_path].value = context->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_keeper_path].value, info);

        info.level = 0;
        info.table_id.uuid = UUIDHelpers::Nil;
        (*kafka_settings)[KafkaSetting::kafka_replica_name].value = context->getMacros()->expand((*kafka_settings)[KafkaSetting::kafka_replica_name].value, info);

        return std::make_shared<StorageKafka2>(
            args.table_id, args.getContext(), args.columns, args.comment, std::move(kafka_settings), collection_name);
    };

    factory.registerStorage(
        "Kafka",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .source_access_type = AccessTypeObjects::Source::KAFKA,
            .has_builtin_setting_fn = KafkaSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# Kafka table engine

:::tip
If you're on ClickHouse Cloud, we recommend using [ClickPipes](/integrations/clickpipes) instead. ClickPipes natively supports private network connections, scaling ingestion and cluster resources independently, and comprehensive monitoring for streaming Kafka data into ClickHouse.
:::

- Publish or subscribe to data flows.
- Organize fault-tolerant storage.
- Process streams as they become available.

## Creating a table {#creating-a-table}

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

Required parameters:

- `kafka_broker_list` — A comma-separated list of brokers (for example, `localhost:9092`).
- `kafka_topic_list` — A list of Kafka topics.
- `kafka_group_name` — A group of Kafka consumers. Reading margins are tracked for each group separately. If you do not want messages to be duplicated in the cluster, use the same group name everywhere.
- `kafka_format` — Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `kafka_security_protocol` - Protocol used to communicate with brokers. Possible values: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
- `kafka_sasl_mechanism` - SASL mechanism to use for authentication. Possible values: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`.
- `kafka_sasl_username` - SASL username for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
- `kafka_sasl_password` - SASL password for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
- `kafka_schema` — Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `kafka_schema_registry_skip_bytes` — The number of bytes to skip from the beginning of each message when using schema registry with envelope headers (e.g., AWS Glue Schema Registry which includes a 19-byte envelope). Range: `[0, 255]`. Default: `0`.
- `kafka_num_consumers` — The number of consumers per table. Specify more consumers if the throughput of one consumer is insufficient. The total number of consumers should not exceed the number of partitions in the topic, since only one consumer can be assigned per partition, and must not be greater than the number of physical cores on the server where ClickHouse is deployed. Default: `1`.
- `kafka_max_block_size` — The maximum batch size (in messages) for poll. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `kafka_skip_broken_messages` — Kafka message parser tolerance to schema-incompatible messages per block. If `kafka_skip_broken_messages = N` then the engine skips *N* Kafka messages that cannot be parsed (a message equals a row of data). Default: `0`.
- `kafka_commit_every_batch` — Commit every consumed and handled batch instead of a single commit after writing a whole block. Default: `0`.
- `kafka_client_id` — Client identifier. Empty by default.
- `kafka_poll_timeout_ms` — Timeout for single poll from Kafka. Default: [stream_poll_timeout_ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
- `kafka_poll_max_batch_size` — Maximum amount of messages to be polled in a single Kafka poll. Default: [max_block_size](/operations/settings/settings#max_block_size).
- `kafka_flush_interval_ms` — Timeout for flushing data from Kafka. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `kafka_consumer_reschedule_ms` — Reschedule interval when Kafka stream processing is stalled (e.g., when no messages are available to consume). This setting controls the delay before the consumer retries polling. Must not exceed `kafka_consumers_pool_ttl_ms`. Default: `500` milliseconds.
- `kafka_thread_per_consumer` — Provide independent thread for each consumer. When enabled, every consumer flush the data independently, in parallel (otherwise — rows from several consumers squashed to form one block). Default: `0`.
- `kafka_handle_error_mode` — How to handle errors for Kafka engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`), dead_letter_queue (error related data will be saved in system.dead_letter_queue).
- `kafka_commit_on_select` —  Commit messages when select query is made. Default: `false`.
- `kafka_consumer_acquire_timeout_ms` — Timeout in milliseconds for acquiring a Kafka consumer during direct `SELECT` queries on a `Kafka2` table (with Keeper-based offset storage). When multiple concurrent direct `SELECT` queries run on the same table, each must wait for consumers to become available. The timeout prevents deadlocks when queries hold different subsets of consumers. Default: `30000`.
- `kafka_max_rows_per_message` — The maximum number of rows written in one kafka message for row-based formats. Default : `1`.
- `kafka_autodetect_client_rack` — Automatically sets the `client.rack` parameter for `librdkafka` to prefer the nearest Kafka replicas.
  Supported sources:
  `AWS_ZONE_ID` for the AWS IMDSv2 availability zone ID, for example `euc1-az1`;
  `AWS_ZONE_NAME` for the AWS IMDSv2 availability zone name, for example `eu-central-1a`;
  `GCP_ZONE` for the GCP metadata service zone, for example `europe-central2-a`;
  `CLICKHOUSE` to use ClickHouse internal detection, which may rely on cloud metadata or configuration;
  `AWS_ZONE_NAME_THEN_GCP_ZONE` to try `AWS_ZONE_NAME` and then `GCP_ZONE`.
  Default: empty string, disabled.
  Tip: different environments use different availability zone formats. Amazon MSK typically uses zone IDs, so prefer `AWS_ZONE_ID`. Confluent Cloud typically uses zone names, so prefer `AWS_ZONE_NAME`. If unsure, use `AWS_ZONE_NAME_THEN_GCP_ZONE` or check the `broker.rack` value on your cluster.
  Note: Kafka brokers must be configured with `broker.rack` and `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
- `kafka_compression_codec` — Compression codec used for producing messages. Supported: empty string, `none`, `gzip`, `snappy`, `lz4`, `zstd`. In case of empty string the compression codec is not set by the table, thus values from the config files or default value from `librdkafka` will be used. Default: empty string.
- `kafka_compression_level` — Compression level parameter for algorithm selected by kafka_compression_codec. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: `[0-9]` for `gzip`; `[0-12]` for `lz4`; only `0` for `snappy`; `[0-12]` for `zstd`; `-1` = codec-dependent default compression level. Default: `-1`.
- `kafka_map_virtual_columns_on_write` — If enabled, columns with special names `_key`, `_timestamp`, `_headers.name` and `_headers.value` in the table schema are mapped to the corresponding Kafka message metadata on `INSERT` and are excluded from the message payload. See [Mapping columns to Kafka message metadata](#mapping-columns-to-kafka-message-metadata). Default: `false`.

Examples:

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

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

```sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
```

</details>

:::info
The Kafka table engine doesn't support columns with [default value](/sql-reference/statements/create/table#default_values). If you need columns with default value, you can add them at materialized view level (see below).
:::

## Description {#description}

The delivered messages are tracked automatically, so each message in a group is only counted once. If you want to get the data twice, then create a copy of the table with another group name.

Groups are flexible and synced on the cluster. For instance, if you have 10 topics and 5 copies of a table in a cluster, then each copy gets 2 topics. If the number of copies changes, the topics are redistributed across the copies automatically. Read more about this at http://kafka.apache.org/intro.

It is recommended that each Kafka topic have its own dedicated consumer group, ensuring exclusive pairing between the topic and the group, especially in environments where topics may be created and deleted dynamically (e.g., in testing or staging).

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using materialized views. To do this:

1.  Use the engine to create a Kafka consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from Kafka and convert them to the required format using `SELECT`.
One kafka table can have as many materialized views as you like, they do not read data from the kafka table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

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
To improve performance, received messages are grouped into blocks the size of [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size). If the block wasn't formed within [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

To stop receiving topic data or to change the conversion logic, detach the materialized view:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Configuration {#configuration}

Similar to GraphiteMergeTree, the Kafka engine supports extended configuration using the ClickHouse config file. There are two configuration keys that you can use: global (below `<kafka>`) and topic-level (below `<kafka><kafka_topic>`). The global configuration is applied first, and then the topic-level configuration is applied (if it exists).

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

For a list of possible configuration options, see the [librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Use the underscore (`_`) instead of a dot in the ClickHouse configuration. For example, `check.crcs=true` will be `<check_crcs>true</check_crcs>`.

### Kerberos support {#kafka-kerberos-support}

To deal with Kerberos-aware Kafka, add `security_protocol` child element with `sasl_plaintext` value. It is enough if Kerberos ticket-granting ticket is obtained and cached by OS facilities.
ClickHouse is able to maintain Kerberos credentials using a keytab file. Consider `sasl_kerberos_service_name`, `sasl_kerberos_keytab` and `sasl_kerberos_principal` child elements.

Example:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

## Virtual columns {#virtual-columns}

- `_topic` — Kafka topic. Data type: `LowCardinality(String)`.
- `_key` — Key of the message. Data type: `String`.
- `_offset` — Offset of the message. Data type: `UInt64`.
- `_timestamp` — Timestamp of the message Data type: `Nullable(DateTime)`.
- `_timestamp_ms` — Timestamp in milliseconds of the message. Data type: `Nullable(DateTime64(3))`.
- `_partition` — Partition of Kafka topic. Data type: `UInt64`.
- `_headers.name` — Array of message's headers keys. Data type: `Array(String)`.
- `_headers.value` — Array of message's headers values. Data type: `Array(String)`.

Additional virtual columns when `kafka_handle_error_mode='stream'`:

- `_raw_message` - Raw message that couldn't be parsed successfully. Data type: `String`.
- `_error` - Exception message happened during failed parsing. Data type: `String`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of exception during parsing, they are always empty when message was parsed successfully.

## Mapping columns to Kafka message metadata {#mapping-columns-to-kafka-message-metadata}

When producing messages with `INSERT INTO`, the Kafka engine always uses a column named `_key` (of type `String`) as the Kafka message key and a column named `_timestamp` (of type `DateTime`) as the Kafka message timestamp — if those columns exist in the table. By default, these columns also appear in the produced message payload alongside the other columns.

With `kafka_map_virtual_columns_on_write = 1`, the behaviour changes:

- `_key` (type `String`) — mapped to the Kafka message key.
- `_timestamp` (type `DateTime`) — mapped to the Kafka message timestamp.
- `_headers.name` (type `Array(String)`) and `_headers.value` (type `Array(String)`) — mapped to Kafka message headers. Each pair `(_headers.name[i], _headers.value[i])` becomes one Kafka header. Because `_headers.name` and `_headers.value` share the `_headers` Nested prefix, ClickHouse requires both arrays to have the same size for every row.

Columns with these names are **excluded from the message payload** only if their types match those listed above; otherwise they stay in the payload, so schemas that happen to reuse these names for unrelated data keep working.

Example:

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

The produced Kafka message has payload `{"event_json":"{\"a\":1}"}`, key `session-42`, the current timestamp, and two headers `source=api` and `trace_id=abc-123`.

## Data formats support {#data-formats-support}

Kafka engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one Kafka message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one Kafka message can be controlled by setting `kafka_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](/operations/settings/settings#max_block_size).

## Engine to store committed offsets in ClickHouse Keeper {#engine-to-store-committed-offsets-in-clickhouse-keeper}

<ExperimentalBadge/>

If `allow_experimental_kafka_offsets_storage_in_keeper` is enabled, then two more settings can be specified to the Kafka table engine:
- `kafka_keeper_path` specifies the path to the table in ClickHouse Keeper
- `kafka_replica_name` specifies the replica name in ClickHouse Keeper

Either both of the settings must be specified or neither of them. When both of them are specified, then a new, experimental Kafka engine will be used. The new engine doesn't depend on storing the committed offsets in Kafka, but stores them in ClickHouse Keeper. It still tries to commit the offsets to Kafka, but it only depends on those offsets when the table is created. In any other circumstances (table is restarted, or recovered after some error) the offsets stored in ClickHouse Keeper will be used as an offset to continue consuming messages from. Apart from the committed offset, it also stores how many messages were consumed in the last batch, so if the insert fails, the same amount of messages will be consumed, thus enabling deduplication if necessary.

Example:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

### Known limitations {#known-limitations}

As the new engine is experimental, it is not production ready yet. There are few known limitations of the implementation:
- Rapidly dropping and recreating the table or specifying the same ClickHouse Keeper path to different engines might cause issues. As best practice you can use the `{uuid}` in `kafka_keeper_path` to avoid clashing paths.
- To make repeatable reads, messages cannot be consumed from multiple partitions on a single thread. On the other hand, the Kafka consumers have to be polled regularly to keep them alive. As a result of these two objectives, we decided to only allow creating multiple consumers if `kafka_thread_per_consumer` is enabled, otherwise it is too complicated to avoid issues regarding polling consumers regularly.

**See Also**

- [Virtual columns](../../../engines/table-engines/index.md#table_engines-virtual_columns)
- [background_message_broker_schedule_pool_size](/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
- [system.kafka_consumers](../../../operations/system-tables/kafka_consumers.md)
)DOCS_MD",
            .syntax = "ENGINE = Kafka() SETTINGS kafka_broker_list = 'host:port', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'format', ...",
            .related = {"RabbitMQ", "NATS", "FileLog"}});
}

template <typename RevocationCb, typename AssignmentCb>
void stopConsumerImpl(
    cppkafka::Consumer& consumer,
    RevocationCb revocation_cb,
    AssignmentCb assignment_cb,
    const std::chrono::milliseconds drain_timeout,
    const LoggerPtr& log,
    StorageKafkaUtils::ErrorHandler error_handler)
{
    consumer.set_revocation_callback(revocation_cb);

    consumer.set_assignment_callback(assignment_cb);

    try
    {
        auto assignment = consumer.get_assignment();

        if (!assignment.empty())
        {
            consumer.pause_partitions(assignment);

            for (const auto& partition : assignment)
            {
                // that call disables the forwarding of the messages to the customer queue
                consumer.get_partition_queue(partition);
            }
        }
    }
    catch (const cppkafka::HandleException & e)
    {
        LOG_ERROR(log, "Error during pause (stopConsumerImpl): {}", e.what());
    }

    StorageKafkaUtils::drainConsumer(consumer, drain_timeout, log, std::move(error_handler));
}

namespace StorageKafkaUtils
{
Names parseTopics(String topic_list)
{
    Names result;
    boost::split(result, topic_list, [](char c) { return c == ','; });
    for (String & topic : result)
        boost::trim(topic);
    return result;
}

String getDefaultClientId(const StorageID & table_id)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id.database_name, table_id.table_name);
}

void consumerGracefulStop(
    cppkafka::Consumer & consumer, const std::chrono::milliseconds drain_timeout, const LoggerPtr & log, ErrorHandler error_handler)
{
    // Note: librdkafka is very sensitive to the proper termination sequence and have some race conditions there.
    // Before destruction, our objectives are:
    //   (1) Process all outstanding callbacks by polling the event queue.
    //   (2) Ensure that only special events (e.g. callbacks, rebalances) are polled (we don't want to poll regular messages).
    //
    // Previously, we performed an unsubscribe to stop message consumption and clear 'read' messages.
    // However, unsubscribe triggers a rebalance that schedules additional background tasks, such as locking
    // and removal of internal toppar queues. Meanwhile, polling to release callbacks may concurrently
    // cause those same queues to be destroyed.
    // This can lead to a situation where the background thread doing rebalance and the current thread doing polling access
    // the toppar queues simultaneously, potentially locking them in a different order, which risks a deadlock.
    //
    // To mitigate this, we now:
    //   (1) Avoid calling unsubscribe (letting rebalance occur naturally via consumer group timeout).
    //   (2) Set up different rebalance callbacks to repeat (3) if a rebalance will occur before consumer destruction.
    //   (3) Pause the consumer to stop processing new messages.
    //   (4) Disconnect the toppar queues to reduce the risk of lock inversion (less cascading locks).
    //   (5) Poll the event queue to process any remaining callbacks.

    stopConsumerImpl(
        consumer,
        /*revocation*/ [](const cppkafka::TopicPartitionList &)
        {
            // we don't care during the destruction
        },
        /*assignment*/ [&consumer](const cppkafka::TopicPartitionList & topic_partitions)
        {
            if (!topic_partitions.empty())
            {
                consumer.pause_partitions(topic_partitions);
            }

            // it's not clear if get_partition_queue will work in that context
            // as just after processing the callback cppkafka will call run assign
            // and that can reset the queues
        },
        drain_timeout, log, std::move(error_handler));
}

void consumerStopWithoutRebalance(
    cppkafka::Consumer & consumer, const std::chrono::milliseconds drain_timeout, const LoggerPtr & log, ErrorHandler error_handler)
{
    stopConsumerImpl(
        consumer,
        /*revocation*/ [](const cppkafka::TopicPartitionList &)
        {
            // we don't care during the destruction
        },
        /*assignment*/ [](const cppkafka::TopicPartitionList &)
        {
            // we don't care during the destruction
        },
        drain_timeout, log, std::move(error_handler));
}

// Needed to drain rest of the messages / queued callback calls from the consumer after unsubscribe, otherwise consumer
// will hang on destruction. Partition queues doesn't have to be attached as events are not handled by those queues.
// see https://github.com/edenhill/librdkafka/issues/2077
//     https://github.com/confluentinc/confluent-kafka-go/issues/189 etc.
void drainConsumer(
    cppkafka::Consumer & consumer, const std::chrono::milliseconds drain_timeout, const LoggerPtr & log, ErrorHandler error_handler)
{
    auto start_time = std::chrono::steady_clock::now();
    cppkafka::Error last_error(RD_KAFKA_RESP_ERR_NO_ERROR);

    while (true)
    {
        auto msg = consumer.poll(100ms);
        if (!msg)
            break;

        auto error = msg.get_error();

        if (error)
        {
            if (msg.is_eof() || error == last_error)
            {
                break;
            }

            LOG_ERROR(log, "Error during draining: {}", error);
            error_handler(error);
        }

        // i don't stop draining on first error,
        // only if it repeats once again sequentially
        last_error = error;

        auto ts = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time) > drain_timeout)
        {
            LOG_ERROR(log, "Timeout during draining.");
            break;
        }
    }
}

size_t eraseMessageErrors(Messages & messages, const LoggerPtr & log, ErrorHandler error_handler)
{
    size_t skipped = std::erase_if(
        messages,
        [&](auto & message)
        {
            if (auto error = message.get_error())
            {
                ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
                LOG_ERROR(log, "Consumer error: {}", error);
                error_handler(error);
                return true;
            }
            return false;
        });

    if (skipped)
        LOG_ERROR(log, "There were {} messages with an error", skipped);

    return skipped;
}

SettingsChanges createSettingsAdjustments(KafkaSettings & kafka_settings, const String & schema_name)
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (!kafka_settings[KafkaSetting::input_format_skip_unknown_fields].changed)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        kafka_settings[KafkaSetting::input_format_skip_unknown_fields] = true;
    }

    if (!kafka_settings[KafkaSetting::input_format_allow_errors_ratio].changed)
    {
        kafka_settings[KafkaSetting::input_format_allow_errors_ratio] = 0.;
    }

    if (!kafka_settings[KafkaSetting::input_format_allow_errors_num].changed)
    {
        kafka_settings[KafkaSetting::input_format_allow_errors_num] = kafka_settings[KafkaSetting::kafka_skip_broken_messages].value;
    }

    if (!schema_name.empty())
        result.emplace_back("format_schema", schema_name);

    auto kafka_format_settings = kafka_settings.getFormatSettings();
    result.insert(result.end(), kafka_format_settings.begin(), kafka_format_settings.end());

    /// It does not make sense to use auto detection here, since the format
    /// will be reset for each message, plus, auto detection takes CPU
    /// time.
    result.setSetting("input_format_csv_detect_header", false);
    result.setSetting("input_format_tsv_detect_header", false);
    result.setSetting("input_format_custom_detect_header", false);

    return result;
}

bool checkDependencies(const StorageID & table_id, const ContextPtr & context)
{
    return !DatabaseCatalog::instance().getReadyDependentViews(table_id, context).empty();
}

VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_topic", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_key", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_offset", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_partition", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3)), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_headers.name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_headers.value", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
        desc.addEphemeral("_error", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}

PayloadSplit splitPayloadColumns(const Block & header, bool map_virtual_columns_on_write)
{
    PayloadSplit split;
    split.format_column_indices.reserve(header.columns());

    auto is_string_array = [](const DataTypePtr & type)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        return array_type && isString(array_type->getNestedType());
    };

    /// Headers are mapped only when both `_headers.name` and `_headers.value` are present
    /// with `Array(String)` type — see `KafkaProducer::KafkaProducer`. Excluding them
    /// independently would silently drop one-sided header columns from the payload.
    bool map_headers = false;
    if (map_virtual_columns_on_write
        && header.has("_headers.name") && header.has("_headers.value"))
    {
        map_headers = is_string_array(header.getByName("_headers.name").type)
            && is_string_array(header.getByName("_headers.value").type);
    }

    auto is_mapped_to_metadata = [&](const ColumnWithTypeAndName & column)
    {
        /// Keep type checks in sync with `KafkaProducer::KafkaProducer`. Columns whose type
        /// does not match are left in the payload rather than being silently dropped.
        if (column.name == "_key")
            return isString(column.type);
        if (column.name == "_timestamp")
            return isDateTime(column.type);
        if (column.name == "_headers.name" || column.name == "_headers.value")
            return map_headers;
        return false;
    };

    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i);

        if (map_virtual_columns_on_write && is_mapped_to_metadata(column))
            continue;

        split.format_header.insert(column);
        split.format_column_indices.push_back(i);
    }

    return split;
}
}
}
