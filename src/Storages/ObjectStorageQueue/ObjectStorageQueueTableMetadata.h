#pragma once

#include <Core/SettingsEnums.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/types.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>

namespace DB
{

struct ObjectStorageQueueSettings;
class WriteBuffer;
class ReadBuffer;

/** The basic parameters of ObjectStorageQueue table engine for saving in ZooKeeper.
 * Lets you verify that they match local ones.
 */
struct ObjectStorageQueueTableMetadata
{
    /// Non-changeable settings.
    const String format_name;
    const String columns;
    const String mode;
    const UInt32 buckets;
    const String last_processed_path;
    /// Changeable settings.
    std::atomic<ObjectStorageQueueAction> after_processing;
    std::atomic<UInt64> loading_retries;
    std::atomic<UInt64> processing_threads_num;
    std::atomic<UInt64> tracked_files_limit;
    std::atomic<UInt64> tracked_files_ttl_sec;

    bool processing_threads_num_changed = false;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_);

    ObjectStorageQueueTableMetadata(const ObjectStorageQueueTableMetadata & other)
        : format_name(other.format_name)
        , columns(other.columns)
        , mode(other.mode)
        , buckets(other.buckets)
        , last_processed_path(other.last_processed_path)
        , after_processing(other.after_processing.load())
        , loading_retries(other.loading_retries.load())
        , processing_threads_num(other.processing_threads_num.load())
        , tracked_files_limit(other.tracked_files_limit.load())
        , tracked_files_ttl_sec(other.tracked_files_ttl_sec.load())
    {
    }

    void syncChangeableSettings(const ObjectStorageQueueTableMetadata & other)
    {
        after_processing = other.after_processing.load();
        loading_retries = other.loading_retries.load();
        processing_threads_num = other.processing_threads_num.load();
        tracked_files_limit = other.tracked_files_limit.load();
        tracked_files_ttl_sec = other.tracked_files_ttl_sec.load();
    }

    explicit ObjectStorageQueueTableMetadata(const Poco::JSON::Object::Ptr & json);

    static ObjectStorageQueueTableMetadata parse(const String & metadata_str);

    static ObjectStorageQueueAction actionFromString(const std::string & action);
    static std::string actionToString(ObjectStorageQueueAction action);

    String toString() const;

    ObjectStorageQueueMode getMode() const;

    void adjustFromKeeper(const ObjectStorageQueueTableMetadata & from_zk);

    void checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const;

    static bool isStoredInKeeper(const std::string & name)
    {
        static const std::unordered_set<std::string_view> settings_names
        {
            "format_name",
            "columns",
            "mode",
            "buckets",
            "last_processed_path",
            "after_processing",
            "loading_retries",
            "processing_threads_num",
            "tracked_files_limit",
            "tracked_file_ttl_sec",
            "tracked_files_ttl_sec",
        };
        return settings_names.contains(name);
    }

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
