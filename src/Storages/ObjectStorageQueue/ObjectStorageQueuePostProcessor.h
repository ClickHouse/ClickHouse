#pragma once

#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>

#include <exception>
#include <mutex>

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

    /// `keeper_identity_` identifies the queue (shared by its replicas) that stamps and recognizes its
    /// own moves: the Keeper path, qualified by the Keeper name when it is not the default one.
    ObjectStorageQueuePostProcessor(
        ContextPtr context_,
        ObjectStorageType type_,
        ObjectStoragePtr object_storage_,
        const ObjectStorageQueueTableMetadata & table_metadata_,
        AfterProcessingSettings settings_,
        String keeper_identity_);

    /// Apply post-processing to the objects. Can throw exceptions in case of misconfiguration.
    /// The method intercepts exceptions caused by remote storage interaction and reports them to the log,
    /// with one exception. `FILE_CHANGED_DURING_READ` (Azure) or `S3_OBJECT_CHANGED_DURING_READ` (S3)
    /// means that an object is no longer the generation
    /// that was ingested: it was overwritten after it was read, and the newer generation has never been
    /// ingested. The object is left in place, every other object of the batch is still handled, and
    /// the error is then rethrown, so that the caller does not commit the file as processed - which
    /// would leave the newer generation in the bucket and never ingest it. A post-processing that
    /// merely failed (a network error, say) is reported to the log only, as before: the generation
    /// that was ingested is then still the one in the bucket, and committing the file is right.
    void process(
        const StoredObjects & objects,
        UnorderedSetWithMemoryTracking<String> & failed_object_paths) const;

private:
    /// The source generation a copy step consumed. `version_id` is set only when the copy was pinned
    /// to that generation, so pinning the delete to it cannot remove contents nothing copied.
    struct SourceGeneration
    {
        String version_id;
        String etag;
    };

    struct CopyResult
    {
        /// False when the destination is occupied by an object this queue did not put there.
        bool destination_is_ours = true;
        /// Defaulted so a designated initializer may name only the fields it cares about.
        SourceGeneration consumed = {};
    };

    enum class MoveResult : uint8_t
    {
        Moved,
        DestinationCollision,
    };

    /// The first object of a batch found to be no longer the generation that was ingested
    /// (`FILE_CHANGED_DURING_READ` or `S3_OBJECT_CHANGED_DURING_READ`), remembered while the rest of
    /// the batch is handled, so that
    /// `process` can rethrow it afterwards. Safe to share between the threads that handle a batch.
    class ChangedGeneration
    {
    public:
        /// To be called from a `catch` block: remembers the exception being handled if it is one.
        void rememberIfCurrentExceptionIsOne();
        void rethrowIfAny() const;

    private:
        mutable std::mutex mutex;
        std::exception_ptr exception;
    };


    void doWithRetries(std::function<void()> action) const;
    MoveResult copyAndRemoveObject(const StoredObject & object, const std::function<CopyResult()> & copy_object) const;
    /// Removes the source only while it still holds the generation the copy consumed.
    void removeCopiedSource(const StoredObject & object, const SourceGeneration & consumed) const;
    void reportMoveCollision(const StoredObject & source, const StoredObject & destination) const;

    /// Move processed objects to another prefix. Each of the three rethrows the first
    /// `FILE_CHANGED_DURING_READ` / `S3_OBJECT_CHANGED_DURING_READ` once the whole batch has been
    /// handled (see `process`).
    void moveWithinBucket(const StoredObjects & objects, const String & move_prefix, bool preserve_path, StoredObjects & successful_objects) const;
    /// Move processed S3 objects, possibly to another S3 storage
    /// Deletes each object by the version a `HEAD` reports for it, after checking that version is still
    /// the generation that was ingested. Returns false when no version comes back, leaving an unversioned
    /// bucket to the batched delete that matches on `ETag` alone.
    bool deleteVersionedS3Objects(const StoredObjects & objects, StoredObjects & successful_objects) const;

    void moveS3Objects(const StoredObjects & objects, StoredObjects & successful_objects) const;
    /// Move processed Azure blobs, possibly to another Azure storage
    void moveAzureBlobs(const StoredObjects & objects, StoredObjects & successful_objects) const;

    ObjectStorageType type;
    const ObjectStoragePtr object_storage;
    const ObjectStorageQueueTableMetadata & table_metadata;
    const AfterProcessingSettings settings;
    const String keeper_identity;

    LoggerPtr log;
};

}
