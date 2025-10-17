#include <optional>
#include <Columns/IColumn.h>
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
    extern const int BAD_ARGUMENTS;
}

#define OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(ObjectStorageQueueMode, mode, ObjectStorageQueueMode::ORDERED, \
      "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKeepeer." \
      "With ordered mode, only the max name of the successfully consumed file stored.", \
      0) \
    DECLARE(ObjectStorageQueueAction, after_processing, ObjectStorageQueueAction::KEEP, "Delete or keep file in after successful processing", 0) \
    DECLARE(String, keeper_path, "", "Zookeeper node path", 0) \
    DECLARE(UInt64, loading_retries, 10, "Retry loading up to specified number of times", 0) \
    DECLARE(UInt64, processing_threads_num, 1, "Number of processing threads (default number of available CPUs or 16)", 0) \
    DECLARE(Bool, parallel_inserts, false, "By default processing_threads_num will produce one INSERT, but this limits the parallel execution, so better scalability enable this setting (note this will create not one INSERT per file, but one per max_process*_before_commit)", 0) \
    DECLARE(UInt32, enable_logging_to_queue_log, 1, "Enable logging to system table system.(s3/azure_)queue_log", 0) \
    DECLARE(String, last_processed_path, "", "For Ordered mode. Files that have lexicographically smaller file name are considered already processed", 0) \
    DECLARE(UInt64, tracked_files_limit, 1000, "For unordered mode. Max set size for tracking processed files in ZooKeeper", 0) \
    DECLARE(UInt64, tracked_file_ttl_sec, 0, "Maximum number of seconds to store processed files in ZooKeeper node (store forever by default)", 0) \
    DECLARE(UInt64, polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    DECLARE(UInt64, polling_max_timeout_ms, 10 * 60 * 1000, "Maximum timeout before next polling", 0) \
    DECLARE(UInt64, polling_backoff_ms, 30 * 1000, "Polling backoff", 0) \
    DECLARE(UInt32, cleanup_interval_min_ms, 60000, "For unordered mode. Polling backoff min for cleanup", 0) \
    DECLARE(UInt32, cleanup_interval_max_ms, 60000, "For unordered mode. Polling backoff max for cleanup", 0) \
    DECLARE(Bool, use_persistent_processing_nodes, true, "This setting is deprecated", 0) \
    DECLARE(UInt32, persistent_processing_node_ttl_seconds, 60 * 60, "Cleanup period for abandoned processing nodes", 0) \
    DECLARE(UInt64, buckets, 0, "Number of buckets for Ordered mode parallel processing", 0) \
    DECLARE(UInt64, list_objects_batch_size, 1000, "Size of a list batch in object storage", 0) \
    DECLARE(UInt64, min_insert_block_size_rows_for_materialized_views, 0, "Override for profile setting min_insert_block_size_rows_for_materialized_views", 0) \
    DECLARE(UInt64, min_insert_block_size_bytes_for_materialized_views, 0, "Override for profile setting min_insert_block_size_bytes_for_materialized_views", 0) \
    DECLARE(Bool, enable_hash_ring_filtering, 0, "Enable filtering files among replicas according to hash ring for Unordered mode", 0) \
    DECLARE(UInt64, max_processed_files_before_commit, 100, "Number of files which can be processed before being committed to keeper (in case of parallel_inserts=true, works on a per-thread basis)", 0) \
    DECLARE(UInt64, max_processed_rows_before_commit, 0, "Number of rows which can be processed before being committed to keeper (in case of parallel_inserts=true, works on a per-thread basis)", 0) \
    DECLARE(UInt64, max_processed_bytes_before_commit, 0, "Number of bytes which can be processed before being committed to keeper (in case of parallel_inserts=true, works on a per-thread basis)", 0) \
    DECLARE(UInt64, max_processing_time_sec_before_commit, 0, "Timeout in seconds after which to commit files committed to keeper (in case of parallel_inserts=true, works on a per-thread basis)", 0) \

#define LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS(M, ALIAS) \
    OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)

struct ObjectStorageQueueSettingsImpl : public BaseSettings<ObjectStorageQueueSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    ObjectStorageQueueSettings##TYPE NAME = &ObjectStorageQueueSettingsImpl ::NAME;

namespace ObjectStorageQueueSetting
{
LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
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
    auto settings_changes_ast = storage.getInMemoryMetadataPtr()->settings_changes;
    if (!settings_changes_ast)
        return;

    /// We cannot use setting.isValueChanged(), because we do not store initial settings in storage.
    /// Therefore check if the setting was changed via table metadata.
    const auto & settings_changes = settings_changes_ast->as<ASTSetQuery>()->changes;
    auto is_changed = [&](const std::string & setting_name) -> bool
    {
        return settings_changes.end() != std::find_if(
            settings_changes.begin(), settings_changes.end(),
            [&](const SettingChange & change){ return change.name == setting_name; });
    };
    auto is_changeable = [&](const std::string & setting_name) -> bool
    {
        return StorageObjectStorageQueue::isSettingChangeable(setting_name, (*this)[ObjectStorageQueueSetting::mode]);
    };

    for (const auto & change : impl->all())
    {
        size_t i = 0;
        const auto & name = change.getName();
        res_columns[i++]->insert(database_name);
        res_columns[i++]->insert(table_name);
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(convertFieldToString(change.getValue()));
        res_columns[i++]->insert(change.getTypeName());
        res_columns[i++]->insert(is_changed(name));
        res_columns[i++]->insert(change.getDescription());
        res_columns[i++]->insert(is_changeable(name));
    }
}

ObjectStorageQueueSettings::~ObjectStorageQueueSettings() = default;

OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(ObjectStorageQueueSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void ObjectStorageQueueSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

namespace
{

std::optional<std::string_view> adjustSettingName(std::string_view name)
{
    static constexpr std::string_view s3queue_prefix = "s3queue_";

    bool modified = false;
    if (name.starts_with(s3queue_prefix))
    {
        modified = true;
        name = name.substr(s3queue_prefix.size());
    }

    if (name == "enable_logging_to_s3queue_log")
    {
        modified = true;
        name = "enable_logging_to_queue_log";
    }

    if (modified)
        return name;

    return std::nullopt;
}

}

void ObjectStorageQueueSettings::loadFromQuery(ASTStorage & storage_def, bool is_attach, const StorageID & storage_id)
{
    if (storage_def.settings)
    {
        try
        {
            std::vector<std::string> ignore_settings;
            auto settings_changes = storage_def.settings->changes;

            std::set<std::string> names;

            /// We support settings starting with s3_ for compatibility.
            for (auto & change : settings_changes)
            {
                if (auto maybe_new_name = adjustSettingName(change.name); maybe_new_name.has_value())
                    change.name = std::string{*maybe_new_name};

                if (change.name == "current_shard_num")
                    ignore_settings.push_back(change.name);
                else if (change.name == "total_shards_num")
                    ignore_settings.push_back(change.name);
                else
                {
                    bool inserted = names.insert(change.name).second;
                    if (!inserted)
                    {
                        if (is_attach)
                        {
                            LOG_WARNING(
                                getLogger("StorageObjectStorageQueue"),
                                "Storage {} has a duplicated setting {}. "
                                "Will use the first declared setting's value",
                                storage_id.getNameForLogs(), change.name);
                        }
                        else
                        {
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Setting {} is defined multiple times", change.name);
                        }
                    }
                }
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

bool ObjectStorageQueueSettings::hasBuiltin(std::string_view name)
{
    if (auto maybe_new_name = adjustSettingName(name); maybe_new_name.has_value())
        name = *maybe_new_name;
    return ObjectStorageQueueSettingsImpl::hasBuiltin(name);
}
}
