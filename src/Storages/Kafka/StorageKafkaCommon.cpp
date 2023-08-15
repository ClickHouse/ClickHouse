#include <Storages/Kafka/StorageKafkaCommon.h>


#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/replace.hpp>
#include <cppkafka/cppkafka.h>
#include <librdkafka/rdkafka.h>


namespace CurrentMetrics
{
extern const Metric KafkaLibrdkafkaThreads;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t
StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    CurrentMetrics::add(CurrentMetrics::KafkaLibrdkafkaThreads, 1);

    const auto & storage_id = self->getStorageID();
    const auto & table = storage_id.getTableName();

    switch (thread_type)
    {
        case RD_KAFKA_THREAD_MAIN:
            setThreadName(("rdk:m/" + table.substr(0, 9)).c_str());
            break;
        case RD_KAFKA_THREAD_BACKGROUND:
            setThreadName(("rdk:bg/" + table.substr(0, 8)).c_str());
            break;
        case RD_KAFKA_THREAD_BROKER:
            setThreadName(("rdk:b/" + table.substr(0, 9)).c_str());
            break;
    }

    /// Create ThreadStatus to track memory allocations from librdkafka threads.
    //
    /// And store them in a separate list (thread_statuses) to make sure that they will be destroyed,
    /// regardless how librdkafka calls the hooks.
    /// But this can trigger use-after-free if librdkafka will not destroy threads after rd_kafka_wait_destroyed()
    auto thread_status = std::make_shared<ThreadStatus>();
    std::lock_guard lock(self->thread_statuses_mutex);
    self->thread_statuses.emplace_back(std::move(thread_status));

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t
StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    CurrentMetrics::sub(CurrentMetrics::KafkaLibrdkafkaThreads, 1);

    std::lock_guard lock(self->thread_statuses_mutex);
    const auto it = std::find_if(
        self->thread_statuses.begin(),
        self->thread_statuses.end(),
        [](const auto & thread_status_ptr) { return thread_status_ptr.get() == current_thread; });
    if (it == self->thread_statuses.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No thread status for this librdkafka thread.");

    self->thread_statuses.erase(it);

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnNew(
    rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    rd_kafka_resp_err_t status;

    status = rd_kafka_interceptor_add_on_thread_start(rk, "init-thread", rdKafkaOnThreadStart, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(self->log, "Cannot set on thread start interceptor due to {} error", status);
        return status;
    }

    status = rd_kafka_interceptor_add_on_thread_exit(rk, "exit-thread", rdKafkaOnThreadExit, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(self->log, "Cannot set on thread exit interceptor due to {} error", status);

    return status;
}

template <typename TStorageKafka>
rd_kafka_resp_err_t StorageKafkaInterceptors<TStorageKafka>::rdKafkaOnConfDup(
    rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx)
{
    TStorageKafka * self = reinterpret_cast<TStorageKafka *>(ctx);
    rd_kafka_resp_err_t status;

    // cppkafka copies configuration multiple times
    status = rd_kafka_conf_interceptor_add_on_conf_dup(new_conf, "init", rdKafkaOnConfDup, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(self->log, "Cannot set on conf dup interceptor due to {} error", status);
        return status;
    }

    status = rd_kafka_conf_interceptor_add_on_new(new_conf, "init", rdKafkaOnNew, ctx);
    if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(self->log, "Cannot set on conf new interceptor due to {} error", status);

    return status;
}

void KafkaConfigLoader::loadConfig(
    cppkafka::Configuration & kafka_config, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    /// Read all tags one level below <kafka>
    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        if (tag.starts_with(CONFIG_KAFKA_TOPIC_TAG)) /// multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
            continue; /// used by new per-topic configuration, ignore

        const String setting_path = config_prefix + "." + tag;
        const String setting_value = config.getString(setting_path);

        /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
        /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        const String setting_name_in_kafka_config = (tag == "log_level") ? tag : boost::replace_all_copy(tag, "_", ".");
        kafka_config.set(setting_name_in_kafka_config, setting_value);
    }
}

void KafkaConfigLoader::loadTopicConfig(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & topic)
{
    /// Read all tags one level below <kafka>
    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        /// Only consider tag <kafka_topic>. Multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
        if (!tag.starts_with(CONFIG_KAFKA_TOPIC_TAG))
            continue;

        /// Read topic name between <name>...</name>
        const String kafka_topic_path = config_prefix + "." + tag;
        const String kafpa_topic_name_path = kafka_topic_path + "." + String{CONFIG_NAME_TAG};

        const String topic_name = config.getString(kafpa_topic_name_path);
        if (topic_name == topic)
        {
            /// Found it! Now read the per-topic configuration into cppkafka.
            Poco::Util::AbstractConfiguration::Keys inner_tags;
            config.keys(kafka_topic_path, inner_tags);
            for (const auto & inner_tag : inner_tags)
            {
                if (inner_tag == CONFIG_NAME_TAG)
                    continue; // ignore <name>

                /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
                /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                const String setting_path = kafka_topic_path + "." + inner_tag;
                const String setting_value = config.getString(setting_path);

                const String setting_name_in_kafka_config
                    = (inner_tag == "log_level") ? inner_tag : boost::replace_all_copy(inner_tag, "_", ".");
                kafka_config.set(setting_name_in_kafka_config, setting_value);
            }
        }
    }
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
        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext()))
        {
            for (const auto & setting : kafka_settings->all())
            {
                const auto & setting_name = setting.getName();
                if (named_collection->has(setting_name))
                    kafka_settings->set(setting_name, named_collection->get<String>(setting_name));
            }
            collection_name = assert_cast<const ASTIdentifier *>(args.engine_args[0].get())->name();
        }

        if (has_settings)
        {
            kafka_settings->loadFromQuery(*args.storage_def);
        }

// Check arguments and settings
#define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME, EVAL) \
    /* One of the four required arguments is not specified */ \
    if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 && !kafka_settings->PAR_NAME.changed) \
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
        if (has_settings && kafka_settings->PAR_NAME.changed) \
        { \
            throw Exception( \
                ErrorCodes::BAD_ARGUMENTS, \
                "The argument №{} of storage Kafka " \
                "and the parameter '{}' " \
                "in SETTINGS cannot be specified at the same time", \
                #ARG_NUM, \
                #PAR_NAME); \
        } \
        /* move engine args to settings */ \
        else \
        { \
            if constexpr ((EVAL) == 1) \
            { \
                engine_args[(ARG_NUM)-1] = evaluateConstantExpressionAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
            } \
            if constexpr ((EVAL) == 2) \
            { \
                engine_args[(ARG_NUM)-1] \
                    = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[(ARG_NUM)-1], args.getLocalContext()); \
            } \
            kafka_settings->PAR_NAME = engine_args[(ARG_NUM)-1]->as<ASTLiteral &>().value; \
        } \
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
            CHECK_KAFKA_STORAGE_ARGUMENT(1, kafka_broker_list, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(2, kafka_topic_list, 1)
            CHECK_KAFKA_STORAGE_ARGUMENT(3, kafka_group_name, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(4, kafka_format, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(5, kafka_row_delimiter, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(6, kafka_schema, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(7, kafka_num_consumers, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(8, kafka_max_block_size, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(9, kafka_skip_broken_messages, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(10, kafka_commit_every_batch, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(11, kafka_client_id, 2)
            CHECK_KAFKA_STORAGE_ARGUMENT(12, kafka_poll_timeout_ms, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(13, kafka_flush_interval_ms, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(14, kafka_thread_per_consumer, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(15, kafka_handle_error_mode, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(16, kafka_commit_on_select, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(17, kafka_max_rows_per_message, 0)
            CHECK_KAFKA_STORAGE_ARGUMENT(18, kafka_keeper_path, 0)
        }

#undef CHECK_KAFKA_STORAGE_ARGUMENT

        auto num_consumers = kafka_settings->kafka_num_consumers.value;
        auto max_consumers = std::max<uint32_t>(getNumberOfPhysicalCPUCores(), 16);

        if (!args.getLocalContext()->getSettingsRef().kafka_disable_num_consumers_limit && num_consumers > max_consumers)
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
        else if (num_consumers < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of consumers can not be lower than 1");
        }

        if (kafka_settings->kafka_max_block_size.changed && kafka_settings->kafka_max_block_size.value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_max_block_size can not be lower than 1");
        }

        if (kafka_settings->kafka_poll_max_batch_size.changed && kafka_settings->kafka_poll_max_batch_size.value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "kafka_poll_max_batch_size can not be lower than 1");
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
                "See https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka/#configuration");
        }

        if (kafka_settings->kafka_keeper_path.changed)
        {
            if (!args.getLocalContext()->getSettingsRef().allow_experimental_kafka_store_offsets_in_keeper)

                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Storing the Kafka offsets in Keeper is experimental. "
                    "Set `allow_experimental_kafka_store_offsets_in_keeper` setting to enable it");

            return std::make_shared<StorageKafka2>(
                args.table_id, args.getContext(), args.columns, std::move(kafka_settings), collection_name);
        }

        return std::make_shared<StorageKafka>(args.table_id, args.getContext(), args.columns, std::move(kafka_settings), collection_name);
    };

    factory.registerStorage(
        "Kafka",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

template struct StorageKafkaInterceptors<StorageKafka>;
template struct StorageKafkaInterceptors<StorageKafka2>;

}
