#pragma once

#if USE_AWS_S3

#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/StorageS3.h>
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
    UInt64 s3queue_tracked_files_limit = 0;
    UInt64 s3queue_tracked_file_ttl_sec = 0;
    UInt64 s3queue_total_shards_num = 1;
    UInt64 s3queue_processing_threads_num = 1;

    S3QueueTableMetadata() = default;
    S3QueueTableMetadata(const StorageS3::Configuration & configuration, const S3QueueSettings & engine_settings, const StorageInMemoryMetadata & storage_metadata);

    void read(const String & metadata_str);
    static S3QueueTableMetadata parse(const String & metadata_str);

    String toString() const;

    void checkEquals(const S3QueueTableMetadata & from_zk) const;

private:
    void checkImmutableFieldsEquals(const S3QueueTableMetadata & from_zk) const;
};


}

#endif
