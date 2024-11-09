#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(ObjectStorageQueueMode, mode, ObjectStorageQueueMode::ORDERED, \
      "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKeepeer." \
      "With ordered mode, only the max name of the successfully consumed file stored.", \
      0) \
    DECLARE(ObjectStorageQueueAction, after_processing, ObjectStorageQueueAction::KEEP, "Delete or keep file in after successful processing", 0) \
    DECLARE(String, keeper_path, "", "Zookeeper node path", 0) \
    DECLARE(UInt64, loading_retries, 10, "Retry loading up to specified number of times", 0) \
    DECLARE(UInt64, processing_threads_num, 1, "Number of processing threads", 0) \
    DECLARE(UInt32, enable_logging_to_queue_log, 1, "Enable logging to system table system.(s3/azure_)queue_log", 0) \
    DECLARE(String, last_processed_path, "", "For Ordered mode. Files that have lexicographically smaller file name are considered already processed", 0) \
    DECLARE(UInt64, tracked_files_limit, 1000, "For unordered mode. Max set size for tracking processed files in ZooKeeper", 0) \
    DECLARE(UInt64, tracked_file_ttl_sec, 0, "Maximum number of seconds to store processed files in ZooKeeper node (store forever by default)", 0) \
    DECLARE(UInt64, polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    DECLARE(UInt64, polling_max_timeout_ms, 10000, "Maximum timeout before next polling", 0) \
    DECLARE(UInt64, polling_backoff_ms, 1000, "Polling backoff", 0) \
    DECLARE(UInt32, cleanup_interval_min_ms, 60000, "For unordered mode. Polling backoff min for cleanup", 0) \
    DECLARE(UInt32, cleanup_interval_max_ms, 60000, "For unordered mode. Polling backoff max for cleanup", 0) \
    DECLARE(UInt32, buckets, 0, "Number of buckets for Ordered mode parallel processing", 0) \
    DECLARE(UInt64, max_processed_files_before_commit, 100, "Number of files which can be processed before being committed to keeper", 0) \
    DECLARE(UInt64, max_processed_rows_before_commit, 0, "Number of rows which can be processed before being committed to keeper", 0) \
    DECLARE(UInt64, max_processed_bytes_before_commit, 0, "Number of bytes which can be processed before being committed to keeper", 0) \
    DECLARE(UInt64, max_processing_time_sec_before_commit, 0, "Timeout in seconds after which to commit files committed to keeper", 0) \

#define LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS(M, ALIAS) \
    OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)

struct ObjectStorageQueueSettingsImpl : public BaseSettings<ObjectStorageQueueSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    ObjectStorageQueueSettings##TYPE NAME = &ObjectStorageQueueSettingsImpl ::NAME;

namespace ObjectStorageQueueSetting
{
LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

ObjectStorageQueueSettings::ObjectStorageQueueSettings() : impl(std::make_unique<ObjectStorageQueueSettingsImpl>())
{
}

ObjectStorageQueueSettings::ObjectStorageQueueSettings(const ObjectStorageQueueSettings & settings)
    : impl(std::make_unique<ObjectStorageQueueSettingsImpl>(*settings.impl))
{
}

ObjectStorageQueueSettings::ObjectStorageQueueSettings(ObjectStorageQueueSettings && settings) noexcept
    : impl(std::make_unique<ObjectStorageQueueSettingsImpl>(std::move(*settings.impl)))
{
}

void ObjectStorageQueueSettings::dumpToSystemEngineSettingsColumns(
    MutableColumnsAndConstraints & params,
    const std::string & table_name,
    const std::string & database_name,
    const StorageObjectStorageQueue & storage) const
{
    MutableColumns & res_columns = params.res_columns;

    /// We cannot use setting.isValueChanged(), because we do not store initial settings in storage.
    /// Therefore check if the setting was changed via table metadata.
    const auto & settings_changes = storage.getInMemoryMetadataPtr()->settings_changes->as<ASTSetQuery>()->changes;
    auto is_changed = [&](const std::string & setting_name) -> bool
    {
        return settings_changes.end() != std::find_if(
            settings_changes.begin(), settings_changes.end(),
            [&](const SettingChange & change){ return change.name == setting_name; });
    };

    for (const auto & change : impl->all())
    {
        size_t i = 0;
        res_columns[i++]->insert(database_name);
        res_columns[i++]->insert(table_name);
        res_columns[i++]->insert(change.getName());
        res_columns[i++]->insert(convertFieldToString(change.getValue()));
        res_columns[i++]->insert(change.getTypeName());
        res_columns[i++]->insert(is_changed(change.getName()));
        res_columns[i++]->insert(change.getDescription());
        res_columns[i++]->insert(false);
    }
}

ObjectStorageQueueSettings::~ObjectStorageQueueSettings() = default;

OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(ObjectStorageQueueSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void ObjectStorageQueueSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void ObjectStorageQueueSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            std::vector<std::string> ignore_settings;
            auto settings_changes = storage_def.settings->changes;

            /// We support settings starting with s3_ for compatibility.
            for (auto & change : settings_changes)
            {
                if (change.name.starts_with("s3queue_"))
                    change.name = change.name.substr(std::strlen("s3queue_"));

                if (change.name == "enable_logging_to_s3queue_log")
                    change.name = "enable_logging_to_queue_log";

                if (change.name == "current_shard_num")
                    ignore_settings.push_back(change.name);
                if (change.name == "total_shards_num")
                    ignore_settings.push_back(change.name);
            }

            for (const auto & setting : ignore_settings)
                settings_changes.removeSetting(setting);

            impl->applyChanges(settings_changes);
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

Field ObjectStorageQueueSettings::get(const std::string & name)
{
    return impl->get(name);
}

}
