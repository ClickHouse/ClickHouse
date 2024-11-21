#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/RabbitMQ/RabbitMQSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define RABBITMQ_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, rabbitmq_host_port, "", "A host-port to connect to RabbitMQ server.", 0) \
    DECLARE(String, rabbitmq_exchange_name, "clickhouse-exchange", "The exchange name, to which messages are sent.", 0) \
    DECLARE(String, rabbitmq_format, "", "The message format.", 0) \
    DECLARE(String, rabbitmq_exchange_type, "default", "The exchange type.", 0) \
    DECLARE(String, rabbitmq_routing_key_list, "5672", "A string of routing keys, separated by dots.", 0) \
    DECLARE(String, rabbitmq_schema, "", "Schema identifier (used by schema-based formats) for RabbitMQ engine", 0) \
    DECLARE(UInt64, rabbitmq_num_consumers, 1, "The number of consumer channels per table.", 0) \
    DECLARE(UInt64, rabbitmq_num_queues, 1, "The number of queues per consumer.", 0) \
    DECLARE(String, rabbitmq_queue_base, "", "Base for queue names to be able to reopen non-empty queues in case of failure.", 0) \
    DECLARE(Bool, rabbitmq_persistent, false, "For insert query messages will be made 'persistent', durable.", 0) \
    DECLARE(Bool, rabbitmq_secure, false, "Use SSL connection", 0) \
    DECLARE(String, rabbitmq_address, "", "Address for connection", 0) \
    DECLARE(UInt64, rabbitmq_skip_broken_messages, 0, "Skip at least this number of broken messages from RabbitMQ per block", 0) \
    DECLARE(UInt64, rabbitmq_max_block_size, 0, "Number of row collected before flushing data from RabbitMQ.", 0) \
    DECLARE(UInt64, rabbitmq_flush_interval_ms, 0, "Timeout for flushing data from RabbitMQ.", 0) \
    DECLARE(String, rabbitmq_vhost, "/", "RabbitMQ vhost.", 0) \
    DECLARE(String, rabbitmq_queue_settings_list, "", "A list of rabbitmq queue settings", 0) \
    DECLARE(UInt64, rabbitmq_empty_queue_backoff_start_ms, 10, "A minimum backoff point to reschedule read if the rabbitmq queue is empty", 0) \
    DECLARE(UInt64, rabbitmq_empty_queue_backoff_end_ms, 10000, "A maximum backoff point to reschedule read if the rabbitmq queue is empty", 0) \
    DECLARE(UInt64, rabbitmq_empty_queue_backoff_step_ms, 100, "A backoff step to reschedule read if the rabbitmq queue is empty", 0) \
    DECLARE(Bool, rabbitmq_queue_consume, false, "Use user-defined queues and do not make any RabbitMQ setup: declaring exchanges, queues, bindings", 0) \
    DECLARE(String, rabbitmq_username, "", "RabbitMQ username", 0) \
    DECLARE(String, rabbitmq_password, "", "RabbitMQ password", 0) \
    DECLARE(Bool, reject_unhandled_messages, false, "Allow messages to be rejected in case they cannot be processed. This also automatically implies if there is a x-deadletter-exchange queue setting added", 0) \
    DECLARE(Bool, rabbitmq_commit_on_select, false, "Commit messages when select query is made", 0) \
    DECLARE(UInt64, rabbitmq_max_rows_per_message, 1, "The maximum number of rows produced in one message for row-based formats.", 0) \
    DECLARE(StreamingHandleErrorMode, rabbitmq_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for RabbitMQ engine. Possible values: default (throw an exception after rabbitmq_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \

#define OBSOLETE_RABBITMQ_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, rabbitmq_row_delimiter, '\0') \

#define LIST_OF_RABBITMQ_SETTINGS(M, ALIAS) \
    RABBITMQ_RELATED_SETTINGS(M, ALIAS)     \
    OBSOLETE_RABBITMQ_SETTINGS(M, ALIAS)    \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)   \

DECLARE_SETTINGS_TRAITS(RabbitMQSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(RabbitMQSettingsTraits, LIST_OF_RABBITMQ_SETTINGS)

struct RabbitMQSettingsImpl : public BaseSettings<RabbitMQSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) RabbitMQSettings##TYPE NAME = &RabbitMQSettingsImpl ::NAME;

namespace RabbitMQSetting
{
LIST_OF_RABBITMQ_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

RabbitMQSettings::RabbitMQSettings() : impl(std::make_unique<RabbitMQSettingsImpl>())
{
}

RabbitMQSettings::RabbitMQSettings(const RabbitMQSettings & settings) : impl(std::make_unique<RabbitMQSettingsImpl>(*settings.impl))
{
}

RabbitMQSettings::RabbitMQSettings(RabbitMQSettings && settings) noexcept
    : impl(std::make_unique<RabbitMQSettingsImpl>(std::move(*settings.impl)))
{
}

RabbitMQSettings::~RabbitMQSettings() = default;

RABBITMQ_SETTINGS_SUPPORTED_TYPES(RabbitMQSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void RabbitMQSettings::loadFromQuery(ASTStorage & storage_def)
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

void RabbitMQSettings::loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection->has(setting_name))
            impl->set(setting_name, named_collection->get<String>(setting_name));
    }
}

SettingsChanges RabbitMQSettings::getFormatSettings() const
{
    SettingsChanges values;

    for (const auto & setting : *impl)
    {
        const auto & setting_name = setting.getName();

        /// check for non-rabbitmq-related settings
        if (!setting_name.starts_with("rabbitmq_"))
            values.emplace_back(setting_name, setting.getValue());
    }

    return values;
}
}
