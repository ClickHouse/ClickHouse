#pragma once

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
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
    UInt64 tracked_files_limit = 0;
    UInt64 tracked_file_ttl_sec = 0;
    UInt64 buckets = 0;
    UInt64 processing_threads_num = 1;
    String last_processed_path;

    ObjectStorageQueueTableMetadata() = default;
    ObjectStorageQueueTableMetadata(
        const StorageObjectStorage::Configuration & configuration,
        const ObjectStorageQueueSettings & engine_settings,
        const StorageInMemoryMetadata & storage_metadata);

    void read(const String & metadata_str);
    static ObjectStorageQueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    void checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
    static void checkEquals(const ObjectStorageQueueSettings & current, const ObjectStorageQueueSettings & expected);

private:
    void checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const;
};


}
