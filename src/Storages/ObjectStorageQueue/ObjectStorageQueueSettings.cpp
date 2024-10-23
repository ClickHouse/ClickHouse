#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(M, ALIAS) \
    M(ObjectStorageQueueMode, mode, ObjectStorageQueueMode::ORDERED, \
      "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKepeer." \
      "With ordered mode, only the max name of the successfully consumed file stored.", \
      0) \
    M(ObjectStorageQueueAction, after_processing, ObjectStorageQueueAction::KEEP, "Delete or keep file in after successful processing", 0) \
    M(String, keeper_path, "", "Zookeeper node path", 0) \
    M(UInt32, loading_retries, 10, "Retry loading up to specified number of times", 0) \
    M(UInt32, processing_threads_num, 1, "Number of processing threads", 0) \
    M(UInt32, enable_logging_to_queue_log, 1, "Enable logging to system table system.(s3/azure_)queue_log", 0) \
    M(String, last_processed_path, "", "For Ordered mode. Files that have lexicographically smaller file name are considered already processed", 0) \
    M(UInt32, tracked_file_ttl_sec, 0, "Maximum number of seconds to store processed files in ZooKeeper node (store forever by default)", 0) \
    M(UInt32, polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    M(UInt32, polling_max_timeout_ms, 10000, "Maximum timeout before next polling", 0) \
    M(UInt32, polling_backoff_ms, 1000, "Polling backoff", 0) \
    M(UInt32, tracked_files_limit, 1000, "For unordered mode. Max set size for tracking processed files in ZooKeeper", 0) \
    M(UInt32, cleanup_interval_min_ms, 60000, "For unordered mode. Polling backoff min for cleanup", 0) \
    M(UInt32, cleanup_interval_max_ms, 60000, "For unordered mode. Polling backoff max for cleanup", 0) \
    M(UInt32, buckets, 0, "Number of buckets for Ordered mode parallel processing", 0) \
    M(UInt64, max_processed_files_before_commit, 100, "Number of files which can be processed before being committed to keeper", 0) \
    M(UInt64, max_processed_rows_before_commit, 0, "Number of rows which can be processed before being committed to keeper", 0) \
    M(UInt64, max_processed_bytes_before_commit, 0, "Number of bytes which can be processed before being committed to keeper", 0) \
    M(UInt64, max_processing_time_sec_before_commit, 0, "Timeout in seconds after which to commit files committed to keeper", 0) \

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

ObjectStorageQueueSettings::~ObjectStorageQueueSettings() = default;

OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(ObjectStorageQueueSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


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

}
