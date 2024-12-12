#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
}

#define KAFKA_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, kafka_broker_list, "", "A comma-separated list of brokers for Kafka engine.", 0) \
    DECLARE(String, kafka_topic_list, "", "A list of Kafka topics.", 0) \
    DECLARE(String, kafka_group_name, "", "Client group id string. All Kafka consumers sharing the same group.id belong to the same group.", 0) \
    /* those are mapped to format factory settings */ \
    DECLARE(String, kafka_format, "", "The message format for Kafka engine.", 0) \
    DECLARE(String, kafka_schema, "", "Schema identifier (used by schema-based formats) for Kafka engine", 0) \
    DECLARE(UInt64, kafka_num_consumers, 1, "The number of consumers per table for Kafka engine.", 0) \
    /* default is = max_insert_block_size / kafka_num_consumers  */ \
    DECLARE(UInt64, kafka_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Kafka.", 0) \
    DECLARE(UInt64, kafka_skip_broken_messages, 0, "Skip at least this number of broken messages from Kafka topic per block", 0) \
    DECLARE(Bool, kafka_commit_every_batch, false, "Commit every consumed and handled batch instead of a single commit after writing a whole block", 0) \
    DECLARE(String, kafka_client_id, "", "Client identifier.", 0) \
    /* default is stream_poll_timeout_ms */ \
    DECLARE(Milliseconds, kafka_poll_timeout_ms, 0, "Timeout for single poll from Kafka.", 0) \
    DECLARE(UInt64, kafka_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Kafka poll.", 0) \
    DECLARE(UInt64, kafka_consumers_pool_ttl_ms, 60'000, "TTL for Kafka consumers (in milliseconds)", 0) \
    /* default is stream_flush_interval_ms */ \
    DECLARE(Milliseconds, kafka_flush_interval_ms, 0, "Timeout for flushing data from Kafka.", 0) \
    DECLARE(Bool, kafka_thread_per_consumer, false, "Provide independent thread for each consumer", 0) \
    DECLARE(StreamingHandleErrorMode, kafka_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for Kafka engine. Possible values: default (throw an exception after kafka_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \
    DECLARE(Bool, kafka_commit_on_select, false, "Commit messages when select query is made", 0) \
    DECLARE(UInt64, kafka_max_rows_per_message, 1, "The maximum number of rows produced in one kafka message for row-based formats.", 0) \
    DECLARE(String, kafka_keeper_path, "", "The path to the table in ClickHouse Keeper", 0) \
    DECLARE(String, kafka_replica_name, "", "The replica name in ClickHouse Keeper", 0) \

#define OBSOLETE_KAFKA_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, kafka_row_delimiter, '\0') \

    /** TODO: */
    /* https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md */
    /* https://github.com/edenhill/librdkafka/blob/v1.4.2/src/rdkafka_conf.c */

#define LIST_OF_KAFKA_SETTINGS(M, ALIAS)  \
    KAFKA_RELATED_SETTINGS(M, ALIAS)      \
    OBSOLETE_KAFKA_SETTINGS(M, ALIAS)     \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(KafkaSettingsTraits, LIST_OF_KAFKA_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(KafkaSettingsTraits, LIST_OF_KAFKA_SETTINGS)

struct KafkaSettingsImpl : public BaseSettings<KafkaSettingsTraits>
{
};


#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) KafkaSettings##TYPE NAME = &KafkaSettingsImpl ::NAME;

namespace KafkaSetting
{
LIST_OF_KAFKA_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

KafkaSettings::KafkaSettings() : impl(std::make_unique<KafkaSettingsImpl>())
{
}

KafkaSettings::KafkaSettings(const KafkaSettings & settings) : impl(std::make_unique<KafkaSettingsImpl>(*settings.impl))
{
}

KafkaSettings::KafkaSettings(KafkaSettings && settings) noexcept : impl(std::make_unique<KafkaSettingsImpl>(std::move(*settings.impl)))
{
}

KafkaSettings::~KafkaSettings() = default;

KAFKA_SETTINGS_SUPPORTED_TYPES(KafkaSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void KafkaSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void KafkaSettings::loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection->has(setting_name))
            impl->set(setting_name, named_collection->get<String>(setting_name));
    }
}

void KafkaSettings::sanityCheck() const
{
    if (impl->kafka_consumers_pool_ttl_ms < KAFKA_RESCHEDULE_MS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of 'kafka_consumers_pool_ttl_ms' ({}) cannot be less then rescheduled interval ({})",
            impl->kafka_consumers_pool_ttl_ms,
            KAFKA_RESCHEDULE_MS);

    if (impl->kafka_consumers_pool_ttl_ms > KAFKA_CONSUMERS_POOL_TTL_MS_MAX)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of 'kafka_consumers_pool_ttl_ms' ({}) cannot be too big (greater then {}), since this may cause live memory leaks",
            impl->kafka_consumers_pool_ttl_ms,
            KAFKA_CONSUMERS_POOL_TTL_MS_MAX);
}

SettingsChanges KafkaSettings::getFormatSettings() const
{
    SettingsChanges values;

    for (const auto & setting : *impl)
    {
        const auto & setting_name = setting.getName();

        /// check for non-kafka-related settings
        if (!setting_name.starts_with("kafka_"))
            values.emplace_back(setting_name, setting.getValue());
    }

    return values;
}
}
