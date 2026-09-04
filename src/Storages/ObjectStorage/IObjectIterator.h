#pragma once
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/StorageID.h>
#include <Formats/FormatFilterInfo.h>
#include <IO/Progress.h>
#include <Common/Logger.h>
#include <Common/Macros.h>
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

    virtual std::string getFileName() const { return relative_path_with_metadata.getFileName(); }
    virtual std::string getPath() const { return relative_path_with_metadata.relative_path; }
    virtual bool isArchive() const { return false; }
    virtual std::string getPathToArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual size_t fileSizeInArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual std::string getPathOrPathToArchiveIfArchive() const;
    virtual std::optional<std::string> getFileFormat() const { return std::nullopt; }

    virtual std::optional<size_t> getFileSizeHint() const { return std::nullopt; }

    std::optional<ObjectMetadata> getObjectMetadata() const { return relative_path_with_metadata.metadata; }
    void setObjectMetadata(const ObjectMetadata & metadata) { relative_path_with_metadata.metadata = metadata; }

    FileBucketInfoPtr file_bucket_info;

    /// Lazy materialization: if set, read only these rows of the file.
    /// Sorted absolute row indexes within the file, see FormatFilterInfo::rows_to_read.
    std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read;

    String getIdentifier(bool include_file_bucket_info = true) const;
    String getIdentifierForPath(const String & path, bool include_file_bucket_info = true) const;
};

using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectInfos = std::vector<ObjectInfoPtr>;
class ExpressionActions;

struct IObjectIterator
{
    virtual ~IObjectIterator() = default;
    virtual ObjectInfoPtr next(size_t) = 0;
    virtual size_t estimatedKeysCount() = 0;
    virtual std::optional<UInt64> getSnapshotVersion() const { return std::nullopt; }

    /// When false, the iterator should not emit ProfileEvents.
    /// Used when the iterator is created for metadata purposes (e.g. `getPathSample`)
    /// rather than for actual data reading.
    bool emit_profile_events = true;

    /// Set `emit_profile_events` flag, propagating to nested iterators if any.
    virtual void setEmitProfileEvents(bool value) { emit_profile_events = value; }
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
        const std::string & object_namespace_,
        const ContextPtr & context_,
        std::function<void(FileProgress)> file_progress_callback_ = {});

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        iterator->setEmitProfileEvents(value);
    }

private:
    const ObjectIterator iterator;
    const std::string object_namespace;
    const NamesAndTypesList virtual_columns;
    const NamesAndTypesList hive_partition_columns;
    const std::shared_ptr<ExpressionActions> filter_actions;
    const std::function<void(FileProgress)> file_progress_callback;
    LoggerPtr log = getLogger("ObjectIteratorWithPathAndFileFilter");
};

/// Replays an already-enumerated prefix, then continues delegating to the underlying iterator.
/// Enumerating a prefix consumes it destructively; this restores what the consumer would
/// otherwise never see.
class ObjectIteratorReplayThenDelegate : public IObjectIterator
{
public:
    ObjectIteratorReplayThenDelegate(ObjectInfos replay_, ObjectIterator iterator_)
        : replay(std::move(replay_)), iterator(std::move(iterator_))
    {
    }

    ObjectInfoPtr next(size_t id) override
    {
        {
            std::lock_guard lock(mutex);
            if (replay_pos < replay.size())
                return replay[replay_pos++];
        }
        return iterator->next(id);
    }

    /// The un-replayed prefix counts too: a drained delegate can report 0, and a consumer seeing
    /// <= 1 key collapses to a single stream.
    size_t estimatedKeysCount() override
    {
        const size_t delegate_count = iterator->estimatedKeysCount();
        if (delegate_count == std::numeric_limits<size_t>::max())
            return delegate_count;

        std::lock_guard lock(mutex);
        return (replay.size() - replay_pos) + delegate_count;
    }

    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        iterator->setEmitProfileEvents(value);
    }

private:
    std::mutex mutex;
    const ObjectInfos replay;
    size_t replay_pos = 0;
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
    std::optional<UInt64> getSnapshotVersion() const override { return snapshot_source->getSnapshotVersion(); }

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
        const String & format_,
        ObjectStoragePtr object_storage_,
        const ContextPtr & context_,
        const StorageID & storage_id_ = StorageID::createEmpty(),
        FormatFilterInfoPtr format_filter_info_ = nullptr);

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

private:
    const ObjectIterator iterator;
    String format;
    ObjectStoragePtr object_storage;
    FormatSettings format_settings;
    StorageID storage_id;
    FormatFilterInfoPtr format_filter_info;
    QueryConditionCachePtr query_condition_cache;

    std::queue<ObjectInfoPtr> pending_objects_info;
    const LoggerPtr log = getLogger("GlobIterator");
};


}
