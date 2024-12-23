#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/NATS/NATSSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define NATS_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, nats_url, "", "A host-port to connect to NATS server.", 0) \
    DECLARE(String, nats_subjects, "", "List of subject for NATS table to subscribe/publish to.", 0) \
    DECLARE(String, nats_format, "", "The message format.", 0) \
    DECLARE(String, nats_schema, "", "Schema identifier (used by schema-based formats) for NATS engine", 0) \
    DECLARE(UInt64, nats_num_consumers, 1, "The number of consumer channels per table.", 0) \
    DECLARE(String, nats_queue_group, "", "Name for queue group of NATS subscribers.", 0) \
    DECLARE(Bool, nats_secure, false, "Use SSL connection", 0) \
    DECLARE(UInt64, nats_max_reconnect, 5, "Maximum amount of reconnection attempts.", 0) \
    DECLARE(UInt64, nats_reconnect_wait, 2000, "Amount of time in milliseconds to sleep between each reconnect attempt.", 0) \
    DECLARE(String, nats_server_list, "", "Server list for connection", 0) \
    DECLARE(UInt64, nats_skip_broken_messages, 0, "Skip at least this number of broken messages from NATS per block", 0) \
    DECLARE(UInt64, nats_max_block_size, 0, "Number of row collected before flushing data from NATS.", 0) \
    DECLARE(Milliseconds, nats_flush_interval_ms, 0, "Timeout for flushing data from NATS.", 0) \
    DECLARE(String, nats_username, "", "NATS username", 0) \
    DECLARE(String, nats_password, "", "NATS password", 0) \
    DECLARE(String, nats_token, "", "NATS token", 0) \
    DECLARE(String, nats_credential_file, "", "Path to a NATS credentials file", 0) \
    DECLARE(UInt64, nats_startup_connect_tries, 5, "Number of connect tries at startup", 0) \
    DECLARE(UInt64, nats_max_rows_per_message, 1, "The maximum number of rows produced in one message for row-based formats.", 0) \
    DECLARE(StreamingHandleErrorMode, nats_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for NATS engine. Possible values: default (throw an exception after nats_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \

#define OBSOLETE_NATS_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, nats_row_delimiter, '\0') \

#define LIST_OF_NATS_SETTINGS(M, ALIAS)   \
    NATS_RELATED_SETTINGS(M, ALIAS)       \
    OBSOLETE_NATS_SETTINGS(M, ALIAS)      \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(NATSSettingsTraits, LIST_OF_NATS_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(NATSSettingsTraits, LIST_OF_NATS_SETTINGS)

struct NATSSettingsImpl : public BaseSettings<NATSSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) NATSSettings##TYPE NAME = &NATSSettingsImpl ::NAME;

namespace NATSSetting
{
LIST_OF_NATS_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

NATSSettings::NATSSettings() : impl(std::make_unique<NATSSettingsImpl>())
{
}

NATSSettings::NATSSettings(const NATSSettings & settings) : impl(std::make_unique<NATSSettingsImpl>(*settings.impl))
{
}

NATSSettings::NATSSettings(NATSSettings && settings) noexcept : impl(std::make_unique<NATSSettingsImpl>(std::move(*settings.impl)))
{
}

NATSSettings::~NATSSettings() = default;

NATS_SETTINGS_SUPPORTED_TYPES(NATSSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void NATSSettings::loadFromQuery(ASTStorage & storage_def)
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

void NATSSettings::loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection->has(setting_name))
            impl->set(setting_name, named_collection->get<String>(setting_name));
    }
}

SettingsChanges NATSSettings::getFormatSettings() const
{
    SettingsChanges values;

    for (const auto & setting : *impl)
    {
        const auto & setting_name = setting.getName();

        /// check for non-nats-related settings
        if (!setting_name.starts_with("nats_"))
            values.emplace_back(setting_name, setting.getValue());
    }

    return values;
}
}
