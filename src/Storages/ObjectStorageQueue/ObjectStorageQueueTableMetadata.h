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
    const String format_name;
    const String columns;
    const String after_processing;
    const String mode;
    const UInt32 tracked_files_limit;
    const UInt32 tracked_files_ttl_sec;
    const UInt32 buckets;
    const String last_processed_path;
    const UInt32 loading_retries;

    UInt32 processing_threads_num; /// Can be changed from keeper.
    bool processing_threads_num_changed = false;

    ObjectStorageQueueTableMetadata(
        const ObjectStorageQueueSettings & engine_settings,
        const ColumnsDescription & columns_,
        const std::string & format_);

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
