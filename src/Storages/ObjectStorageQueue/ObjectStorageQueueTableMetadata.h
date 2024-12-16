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
    const String format_name;
    const String columns;
    const String after_processing;
    const String mode;
    const UInt64 tracked_files_limit;
    const UInt64 tracked_files_ttl_sec;
    const UInt64 buckets;
    const UInt64 processing_threads_num;
    const String last_processed_path;
    const UInt64 loading_retries;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_);

    explicit ObjectStorageQueueTableMetadata(const Poco::JSON::Object::Ptr & json);

    static ObjectStorageQueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    ObjectStorageQueueMode getMode() const;

    void checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const;

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
