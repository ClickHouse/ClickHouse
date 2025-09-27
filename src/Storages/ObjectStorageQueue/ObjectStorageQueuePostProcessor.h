#pragma once

#include <Common/logger_useful.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>

namespace DB
{

struct ObjectStorageQueueTableMetadata;

class ObjectStorageQueuePostProcessor: public WithContext
{
public:
    ObjectStorageQueuePostProcessor(
        ContextPtr context_,
        ObjectStorageType type_,
        ObjectStoragePtr object_storage_,
        String engine_name_,
        const ObjectStorageQueueTableMetadata & table_metadata_);

    void process(const StoredObjects & objects) const;

private:
    String getName() const { return engine_name; }

    /// Move processed objects to another prefix
    void moveWithinBucket(const StoredObjects & objects, const String & move_prefix) const;
    /// Move processed S3 objects, possibly to another S3 storage
    void moveS3Objects(const StoredObjects & objects) const;
    /// Move processed Azure blobs, possibly to another Azure storage
    void moveAzureBlobs(const StoredObjects & objects) const;

    ObjectStorageType type;
    const ObjectStoragePtr object_storage;
    const String engine_name;
    const ObjectStorageQueueTableMetadata & table_metadata;

    LoggerPtr log;
};

}
