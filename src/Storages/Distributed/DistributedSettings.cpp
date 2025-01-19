#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_DISTRIBUTED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, fsync_after_insert, false, "Do fsync for every inserted. Will decreases performance of inserts (only for background INSERT, i.e. distributed_foreground_insert=false)", 0) \
    DECLARE(Bool, fsync_directories, false, "Do fsync for temporary directory (that is used for background INSERT only) after all part operations (writes, renames, etc.).", 0) \
    /** This is the distributed version of the skip_unavailable_shards setting available in src/Core/Settings.cpp */ \
    DECLARE(Bool, skip_unavailable_shards, false, "If true, ClickHouse silently skips unavailable shards. Shard is marked as unavailable when: 1) The shard cannot be reached due to a connection failure. 2) Shard is unresolvable through DNS. 3) Table does not exist on the shard.", 0) \
    /** Inserts settings. */ \
    DECLARE(UInt64, bytes_to_throw_insert, 0, "If more than this number of compressed bytes will be pending for background INSERT, an exception will be thrown. 0 - do not throw.", 0) \
    DECLARE(UInt64, bytes_to_delay_insert, 0, "If more than this number of compressed bytes will be pending for background INSERT, the query will be delayed. 0 - do not delay.", 0) \
    DECLARE(UInt64, max_delay_to_insert, 60, "Max delay of inserting data into Distributed table in seconds, if there are a lot of pending bytes for background send.", 0) \
    /** Async INSERT settings */ \
    DECLARE(UInt64, background_insert_batch, 0, "Default - distributed_background_insert_batch", 0) ALIAS(monitor_batch_inserts) \
    DECLARE(UInt64, background_insert_split_batch_on_failure, 0, "Default - distributed_background_insert_split_batch_on_failure", 0) ALIAS(monitor_split_batch_on_failure) \
    DECLARE(Milliseconds, background_insert_sleep_time_ms, 0, "Default - distributed_background_insert_sleep_time_ms", 0) ALIAS(monitor_sleep_time_ms) \
    DECLARE(Milliseconds, background_insert_max_sleep_time_ms, 0, "Default - distributed_background_insert_max_sleep_time_ms", 0) ALIAS(monitor_max_sleep_time_ms) \
    DECLARE(Bool, flush_on_detach, true, "Flush data to remote nodes on DETACH/DROP/server shutdown", 0) \

DECLARE_SETTINGS_TRAITS(DistributedSettingsTraits, LIST_OF_DISTRIBUTED_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DistributedSettingsTraits, LIST_OF_DISTRIBUTED_SETTINGS)

struct DistributedSettingsImpl : public BaseSettings<DistributedSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) DistributedSettings##TYPE NAME = &DistributedSettingsImpl ::NAME;

namespace DistributedSetting
{
LIST_OF_DISTRIBUTED_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

DistributedSettings::DistributedSettings() : impl(std::make_unique<DistributedSettingsImpl>())
{
}

DistributedSettings::DistributedSettings(const DistributedSettings & settings)
    : impl(std::make_unique<DistributedSettingsImpl>(*settings.impl))
{
}

DistributedSettings::DistributedSettings(DistributedSettings && settings) noexcept
    : impl(std::make_unique<DistributedSettingsImpl>(std::move(*settings.impl)))
{
}

DistributedSettings::~DistributedSettings() = default;

DISTRIBUTED_SETTINGS_SUPPORTED_TYPES(DistributedSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void DistributedSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            impl->set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Distributed config");
        throw;
    }
}

void DistributedSettings::loadFromQuery(ASTStorage & storage_def)
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

}

