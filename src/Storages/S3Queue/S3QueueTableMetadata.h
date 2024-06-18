#pragma once

#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <base/types.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/** The basic parameters of S3Queue table engine for saving in ZooKeeper.
 * Lets you verify that they match local ones.
 */
struct S3QueueTableMetadata
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

    S3QueueTableMetadata() = default;
    S3QueueTableMetadata(
        const StorageObjectStorage::Configuration & configuration,
        const S3QueueSettings & engine_settings,
        const StorageInMemoryMetadata & storage_metadata);

    void read(const String & metadata_str);
    static S3QueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    void checkEquals(const S3QueueTableMetadata & from_zk) const;
    static void checkEquals(const S3QueueSettings & current, const S3QueueSettings & expected);

private:
    void checkImmutableFieldsEquals(const S3QueueTableMetadata & from_zk) const;
};


}
