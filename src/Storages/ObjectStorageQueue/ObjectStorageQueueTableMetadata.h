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
    const String last_processed_path;
    const String bucketing_mode;
    const String partitioning_mode;
    const String partition_regex;
    const String partition_component;
    /// Changeable settings.
    std::atomic<ObjectStorageQueueAction> after_processing;
    std::atomic<UInt64> loading_retries;
    std::atomic<UInt64> processing_threads_num;
    /// Construction-time only; not live-changeable. Persisted in Keeper for new records.
    std::atomic<bool> parallel_inserts;
    std::atomic<UInt64> tracked_files_limit;
    std::atomic<UInt64> tracked_files_ttl_sec;
    std::atomic<UInt64> buckets;

    bool processing_threads_num_changed = false;
    /// Live objects always know the operational value. Parsed Keeper JSON is known
    /// only when the key is present. `checkEquals` compares iff both sides are known.
    bool parallel_inserts_is_known = false;
    /// Whether `toString` may write the key. Copied from parsed Keeper JSON on attach
    /// so unrelated ALTER rewrites do not fabricate a value for legacy records.
    bool parallel_inserts_present_in_keeper = false;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_);

    ObjectStorageQueueTableMetadata(const ObjectStorageQueueTableMetadata & other)
        : format_name(other.format_name)
        , columns(other.columns)
        , mode(other.mode)
        , last_processed_path(other.last_processed_path)
        , bucketing_mode(other.bucketing_mode)
        , partitioning_mode(other.partitioning_mode)
        , partition_regex(other.partition_regex)
        , partition_component(other.partition_component)
        , after_processing(other.after_processing.load())
        , loading_retries(other.loading_retries.load())
        , processing_threads_num(other.processing_threads_num.load())
        , parallel_inserts(other.parallel_inserts.load())
        , tracked_files_limit(other.tracked_files_limit.load())
        , tracked_files_ttl_sec(other.tracked_files_ttl_sec.load())
        , buckets(other.buckets.load())
        , parallel_inserts_is_known(other.parallel_inserts_is_known)
        , parallel_inserts_present_in_keeper(other.parallel_inserts_present_in_keeper)
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
    ObjectStorageQueueBucketingMode getBucketingMode() const;
    ObjectStorageQueuePartitioningMode getPartitioningMode() const;

    void adjustFromKeeper(const ObjectStorageQueueTableMetadata & from_zk);

    /// After a successful checkEquals against parsed Keeper metadata, copy whether
    /// the key is stored. Does not overwrite the local operational value when the
    /// parsed record does not have a known boolean.
    void applyParallelInsertsKeeperPresence(const ObjectStorageQueueTableMetadata & from_zk);

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
            "bucketing_mode",
            "partitioning_mode",
            "partition_regex",
            "partition_component",
            "after_processing",
            "loading_retries",
            "processing_threads_num",
            "parallel_inserts",
            "tracked_files_limit",
            "tracked_file_ttl_sec",
            "tracked_files_ttl_sec",
        };
        return settings_names.contains(name);
    }

    size_t getBucketsNum() const
    {
        if (buckets)
            return buckets;
        return processing_threads_num;
    }

    bool hasTrackedFilesLimit() const { return tracked_files_limit || tracked_files_ttl_sec; }

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
