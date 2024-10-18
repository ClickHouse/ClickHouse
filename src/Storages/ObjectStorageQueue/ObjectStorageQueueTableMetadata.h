#pragma once

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <base/types.h>

namespace DB
{

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
    const String after_processing;
    const String mode;
    const UInt64 tracked_files_limit;
    const UInt64 tracked_files_ttl_sec;
    const UInt64 buckets;
    const String last_processed_path;
    /// Changeable settings.
    std::atomic<UInt64> loading_retries;
    std::atomic<UInt64> processing_threads_num;

    bool processing_threads_num_changed = false;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_);

    ObjectStorageQueueTableMetadata(const ObjectStorageQueueTableMetadata & other)
        : format_name(other.format_name)
        , columns(other.columns)
        , after_processing(other.after_processing)
        , mode(other.mode)
        , tracked_files_limit(other.tracked_files_limit)
        , tracked_files_ttl_sec(other.tracked_files_ttl_sec)
        , buckets(other.buckets)
        , last_processed_path(other.last_processed_path)
        , loading_retries(other.loading_retries.load())
        , processing_threads_num(other.processing_threads_num.load())
    {
    }

    explicit ObjectStorageQueueTableMetadata(const Poco::JSON::Object::Ptr & json);

    static ObjectStorageQueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    ObjectStorageQueueMode getMode() const;

    void adjustFromKeeper(const ObjectStorageQueueTableMetadata & from_zk);

    void checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const;

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
