#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Pulsar/PulsarSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define PULSAR_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, pulsar_service_url, "", "A broker url for Pulsar engine.", 0) \
    DECLARE(String, pulsar_topic_list, "", "A list of Pulsar topics.", 0) \
    DECLARE(String, pulsar_group_name, "", "Client group id string. All Pulsar consumers sharing the same group.id belong to the same group.", 0) \
    DECLARE(String, pulsar_format, "", "The message format for Pulsar engine.", 0) \
    DECLARE(String, pulsar_schema, "", "Schema identifier (used by schema-based formats) for Pulsar engine", 0) \
    DECLARE(UInt64, pulsar_num_consumers, 1, "The number of consumers per table for Pulsar engine.", 0) \
    DECLARE(UInt64, pulsar_max_block_size, 0, "Number of row collected by poll(s) for flushing data from Pulsar.", 0) \
    DECLARE(UInt64, pulsar_skip_broken_messages, 0, "Skip at least this number of broken messages from Pulsar topic per block", 0) \
    DECLARE(Milliseconds, pulsar_poll_timeout_ms, 0, "Timeout for single poll from Pulsar.", 0) \
    DECLARE(UInt64, pulsar_poll_max_batch_size, 0, "Maximum amount of messages to be polled in a single Pulsar poll.", 0) \
    DECLARE(Milliseconds, pulsar_flush_interval_ms, 0, "Timeout for flushing data from Pulsar.", 0) \
    DECLARE(StreamingHandleErrorMode, pulsar_handle_error_mode, StreamingHandleErrorMode::DEFAULT, "How to handle errors for Pulsar engine. Possible values: default (throw an exception after pulsar_skip_broken_messages broken messages), stream (save broken messages and errors in virtual columns _raw_message, _error).", 0) \
    DECLARE(UInt64, pulsar_max_rows_per_message, 1, "The maximum number of rows produced in one Pulsar message for row-based formats.", 0) \
    DECLARE(Bool, pulsar_commit_on_select, false, "Acknowledge polled messages when a direct SELECT query is made from the table.", 0) \

#define OBSOLETE_PULSAR_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, pulsar_row_delimiter, '\0') \

#define LIST_OF_PULSAR_SETTINGS(M, ALIAS) \
    PULSAR_RELATED_SETTINGS(M, ALIAS) \
    OBSOLETE_PULSAR_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(PulsarSettingsTraits, LIST_OF_PULSAR_SETTINGS, PULSAR_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(PulsarSettingsTraits, LIST_OF_PULSAR_SETTINGS, PulsarSettings, PulsarSetting)

PulsarSettings::PulsarSettings() : impl(std::make_unique<PulsarSettingsImpl>())
{
}

PulsarSettings::PulsarSettings(const PulsarSettings & settings) : impl(std::make_unique<PulsarSettingsImpl>(*settings.impl))
{
}

PulsarSettings::PulsarSettings(PulsarSettings && settings) noexcept = default;

PulsarSettings::~PulsarSettings() = default;

PULSAR_SETTINGS_SUPPORTED_TYPES(PulsarSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void PulsarSettings::loadFromQuery(ASTStorage & storage_def)
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
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void PulsarSettings::loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection->has(setting_name))
            impl->set(setting_name, named_collection->get<String>(setting_name));
    }
}

SettingsChanges PulsarSettings::getFormatSettings() const
{
    SettingsChanges values;

    for (const auto & setting : *impl)
    {
        const auto & setting_name = setting.getName();

        /// check for non-pulsar-related settings
        if (!setting_name.starts_with("pulsar_"))
            values.emplace_back(setting_name, setting.getValue());
    }

    return values;
}

bool PulsarSettings::hasBuiltin(std::string_view name)
{
    return PulsarSettingsImpl::hasBuiltin(name);
}
}
