#pragma once

#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>

namespace DB
{

struct ObjectStorageQueueTableMetadata;

class ObjectStorageQueuePostProcessor : public WithContext
{
public:
    struct AfterProcessingSettings
    {
        UInt32 after_processing_retries{};
        String after_processing_move_uri;
        String after_processing_move_prefix;
        bool after_processing_move_preserve_path = false;
        bool after_processing_move_preserve_tags = false;
        String after_processing_move_access_key_id;
        String after_processing_move_secret_access_key;
        String after_processing_move_connection_string;
        String after_processing_move_container;
        String after_processing_tag_key;
        String after_processing_tag_value;
    };

    ObjectStorageQueuePostProcessor(
        ContextPtr context_,
        ObjectStorageType type_,
        ObjectStoragePtr object_storage_,
        const ObjectStorageQueueTableMetadata & table_metadata_,
        AfterProcessingSettings settings_);

    /// Apply post-processing to the objects. Can throw exceptions in case of misconfiguration.
    /// The method intercepts exceptions caused by remote storage interaction and reports them to the log.
    void process(const StoredObjects & objects) const;

private:
    void doWithRetries(std::function<void()> action) const;
    bool copyAndRemoveObject(const StoredObject & object, const std::function<bool()> & copy_object) const;
    void reportMoveCollision(const StoredObject & source, const StoredObject & destination) const;

    /// Move processed objects to another prefix
    void moveWithinBucket(const StoredObjects & objects, const String & move_prefix, bool preserve_path) const;
    /// Move processed S3 objects, possibly to another S3 storage
    void moveS3Objects(const StoredObjects & objects) const;
    /// Move processed Azure blobs, possibly to another Azure storage
    void moveAzureBlobs(const StoredObjects & objects) const;

    ObjectStorageType type;
    const ObjectStoragePtr object_storage;
    const ObjectStorageQueueTableMetadata & table_metadata;
    const AfterProcessingSettings settings;

    LoggerPtr log;
};

}
