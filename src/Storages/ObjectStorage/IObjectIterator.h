#pragma once
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/StorageID.h>
#include <Formats/FormatFilterInfo.h>
#include <IO/Progress.h>
#include <Common/Logger.h>
#include <Common/Macros.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Formats/FormatSettings.h>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct FileBucketInfo;
using FileBucketInfoPtr = std::shared_ptr<FileBucketInfo>;
class RuntimeFilterPruningPredicate;

struct ObjectInfo
{
    RelativePathWithMetadata relative_path_with_metadata;
    std::optional<DataLakeObjectMetadata> data_lake_metadata;

    ObjectInfo() = default;

    explicit ObjectInfo(const String & relative_path_)
        : relative_path_with_metadata(RelativePathWithMetadata(relative_path_))
    {
    }
    explicit ObjectInfo(RelativePathWithMetadata relative_path_with_metadata_)
        : relative_path_with_metadata(relative_path_with_metadata_)
    {
    }

    ObjectInfo(const ObjectInfo & other) = default;

    virtual ~ObjectInfo() = default;

    [[nodiscard]] virtual std::shared_ptr<ObjectInfo> clone() const { return std::make_shared<ObjectInfo>(*this); }

    [[nodiscard]] virtual std::string getFileName() const { return relative_path_with_metadata.getFileName(); }
    [[nodiscard]] virtual std::string getPath() const { return relative_path_with_metadata.relative_path; }
    [[nodiscard]] virtual bool isArchive() const { return false; }
    [[nodiscard]] virtual std::string getPathToArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    [[nodiscard]] virtual size_t fileSizeInArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    [[nodiscard]] virtual std::string getPathOrPathToArchiveIfArchive() const;
    [[nodiscard]] virtual std::optional<std::string> getFileFormat() const { return std::nullopt; }

    /// The exact size of the object as the table metadata records it, if it records a positive one.
    [[nodiscard]] virtual std::optional<size_t> getFileSizeHint() const { return std::nullopt; }

    [[nodiscard]] virtual VectorWithMemoryTracking<UInt64> getSplitOffsetsHint() const { return {}; }

    /// The number of rows of the object as the table metadata records it, before applying deletes, if known.
    /// It is only an estimate used to report the progress of a query, not to compute any result.
    [[nodiscard]] virtual std::optional<size_t> getNumRowsHint() const
    {
        if (data_lake_metadata)
            return data_lake_metadata->num_rows;
        return std::nullopt;
    }

    [[nodiscard]] std::optional<ObjectMetadata> getObjectMetadata() const { return relative_path_with_metadata.metadata; }
    void setObjectMetadata(const ObjectMetadata & metadata) { relative_path_with_metadata.metadata = metadata; }

    FileBucketInfoPtr file_bucket_info;
    /// Whether `file_bucket_info` only leaves out row groups the query condition cache proved to have no
    /// match, rather than splitting the file between readers: only then does reading the buckets tell
    /// which row groups of the whole file match.
    bool file_bucket_info_from_query_condition_cache = false;

    /// Lazy materialization: if set, read only these rows of the file.
    /// Sorted absolute row indexes within the file, see FormatFilterInfo::rows_to_read.
    std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read;

    /// Set for an archive entry whose name an earlier entry of the archive has, see `ArchiveEntryPositions`.
    std::optional<size_t> archive_entry_index;

    [[nodiscard]] String getIdentifier(bool include_file_bucket_info = true) const;
    [[nodiscard]] String getIdentifierForPath(const String & path, bool include_file_bucket_info = true) const;
};

using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectInfos = std::vector<ObjectInfoPtr>;
class ExpressionActions;

struct IObjectIterator
{
    virtual ~IObjectIterator() = default;
    virtual ObjectInfoPtr next(size_t) = 0;
    virtual size_t estimatedKeysCount() = 0;
    [[nodiscard]] virtual std::optional<UInt64> getSnapshotVersion() const { return std::nullopt; }

    /// When false, the iterator should not emit ProfileEvents.
    /// Used when the iterator is created for metadata purposes (e.g. `getPathSample`)
    /// rather than for actual data reading.
    bool emit_profile_events = true;

    /// Set `emit_profile_events` flag, propagating to nested iterators if any.
    virtual void setEmitProfileEvents(bool value) { emit_profile_events = value; }

    /// The predicate of the JOIN runtime filters every row of the read passes, set before the first `next`.
    /// An iterator may skip the objects that cannot contain any of their keys once it is built.
    virtual void setJoinRuntimeFilters(std::shared_ptr<const RuntimeFilterPruningPredicate> /*predicate*/) {}
};

using ObjectIterator = std::shared_ptr<IObjectIterator>;

class ObjectIteratorWithPathAndFileFilter : public IObjectIterator, private WithContext
{
public:
    ObjectIteratorWithPathAndFileFilter(
        ObjectIterator iterator_,
        const DB::ActionsDAG & filter_,
        const NamesAndTypesList & virtual_columns_,
        const NamesAndTypesList & hive_partition_columns_,
        StorageObjectStorageConfigurationPtr configuration_,
        const ContextPtr & context_,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    [[nodiscard]] std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        iterator->setEmitProfileEvents(value);
    }

    void setJoinRuntimeFilters(std::shared_ptr<const RuntimeFilterPruningPredicate> predicate) override
    {
        iterator->setJoinRuntimeFilters(std::move(predicate));
    }

private:
    const ObjectIterator iterator;
    const StorageObjectStorageConfigurationPtr configuration;
    const NamesAndTypesList virtual_columns;
    const NamesAndTypesList hive_partition_columns;
    const std::shared_ptr<ExpressionActions> filter_actions;
    const std::function<void(FileProgress)> file_progress_callback;
    LoggerPtr log = getLogger("ObjectIteratorWithPathAndFileFilter");
};

/// Hands out objects already taken from `iterator` before continuing with it, so a caller can look ahead
/// without losing what it took.
class ObjectIteratorWithPeekedObjects : public IObjectIterator
{
public:
    ObjectIteratorWithPeekedObjects(ObjectInfos peeked_objects_, ObjectIterator iterator_)
        : peeked_objects(std::move(peeked_objects_)), iterator(std::move(iterator_))
    {
    }

    ObjectInfoPtr next(size_t id) override
    {
        {
            std::lock_guard lock(mutex);
            if (next_peeked_object < peeked_objects.size())
                return peeked_objects[next_peeked_object++];
        }
        return iterator->next(id);
    }

    /// The peeked objects not handed out yet count too: a drained `iterator` can report 0, and a consumer
    /// seeing <= 1 key collapses to a single stream.
    size_t estimatedKeysCount() override
    {
        const size_t delegate_count = iterator->estimatedKeysCount();
        if (delegate_count == std::numeric_limits<size_t>::max())
            return delegate_count;

        std::lock_guard lock(mutex);
        return (peeked_objects.size() - next_peeked_object) + delegate_count;
    }

    [[nodiscard]] std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        iterator->setEmitProfileEvents(value);
    }

private:
    std::mutex mutex;
    const ObjectInfos peeked_objects;
    size_t next_peeked_object = 0;
    const ObjectIterator iterator;
};

/// Hands out exactly one object, then reports exhaustion. Gives a source a private file so
/// file-to-stream assignment is deterministic rather than a pull race.
class SingleObjectIterator : public IObjectIterator
{
public:
    SingleObjectIterator(ObjectInfoPtr object_, ObjectIterator snapshot_source_)
        : object(std::move(object_)), snapshot_source(std::move(snapshot_source_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        if (consumed.exchange(true))
            return nullptr;
        return object;
    }

    size_t estimatedKeysCount() override { return 1; }
    [[nodiscard]] std::optional<UInt64> getSnapshotVersion() const override { return snapshot_source->getSnapshotVersion(); }

private:
    const ObjectInfoPtr object;
    const ObjectIterator snapshot_source;
    std::atomic_bool consumed = false;
};

class ObjectIteratorSplitByBuckets : public IObjectIterator, private WithContext
{
public:
    ObjectIteratorSplitByBuckets(
        ObjectIterator iterator_,
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const ContextPtr & context_,
        const StorageID & storage_id_ = StorageID::createEmpty(),
        FormatFilterInfoPtr format_filter_info_ = nullptr);

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    [[nodiscard]] std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

private:
    const ObjectIterator iterator;
    StorageObjectStorageConfigurationPtr configuration;
    ObjectStoragePtr object_storage;
    FormatSettings format_settings;
    StorageID storage_id;
    FormatFilterInfoPtr format_filter_info;
    QueryConditionCachePtr query_condition_cache;

    std::queue<ObjectInfoPtr> pending_objects_info;
    const LoggerPtr log = getLogger("GlobIterator");
};


}
