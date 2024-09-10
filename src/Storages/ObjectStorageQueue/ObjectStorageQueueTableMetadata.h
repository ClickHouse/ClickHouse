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
    String format_name;
    String columns;
    String after_processing;
    String mode;
    UInt64 tracked_files_limit;
    UInt64 tracked_file_ttl_sec;
    UInt64 buckets;
    UInt64 processing_threads_num;
    String last_processed_path;

    bool processing_threads_num_from_cpu_cores;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_,
        bool processing_threads_num_from_cpu_cores_);

    explicit ObjectStorageQueueTableMetadata(const Poco::JSON::Object::Ptr & json);

    static ObjectStorageQueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    void adjustFromKeeper(const ObjectStorageQueueTableMetadata & from_zk, ObjectStorageQueueSettings & settings);

    void checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const;

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
