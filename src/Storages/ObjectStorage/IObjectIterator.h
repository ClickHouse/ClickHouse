#pragma once
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Logger.h>
#include <Common/Macros.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct ObjectInfo
{
    PathWithMetadata path_with_metadata;
    std::optional<DataLakeObjectMetadata> data_lake_metadata;

    ObjectInfo() = default;

    explicit ObjectInfo(const String & relative_path_)
        : path_with_metadata(PathWithMetadata(relative_path_))
    {
    }
    explicit ObjectInfo(PathWithMetadata path_with_metadata_)
        : path_with_metadata(path_with_metadata_)
    {
    }

    ObjectInfo(const ObjectInfo & other) = default;

    virtual ~ObjectInfo() = default;

    virtual std::string getFileName() const { return path_with_metadata.getFileName(); }
    virtual std::string getPath() const { return path_with_metadata.relative_path; }
    virtual std::optional<std::string> getAbsolutePath() const { return path_with_metadata.absolute_path; }
    virtual bool isArchive() const { return false; }
    virtual std::string getPathToArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual size_t fileSizeInArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual std::string getPathOrPathToArchiveIfArchive() const;
    virtual std::optional<std::string> getFileFormat() const { return std::nullopt; }

    std::optional<ObjectMetadata> getObjectMetadata() const { return path_with_metadata.metadata; }
    void setObjectMetadata(const ObjectMetadata & metadata) { path_with_metadata.metadata = metadata; }

    ObjectStoragePtr getObjectStorage() const { return path_with_metadata.object_storage_to_use; }

    FileBucketInfoPtr file_bucket_info;

    String getIdentifier() const;
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
        const ContextPtr & context_);

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

private:
    const ObjectIterator iterator;
    const std::string object_namespace;
    const NamesAndTypesList virtual_columns;
    const NamesAndTypesList hive_partition_columns;
    const std::shared_ptr<ExpressionActions> filter_actions;
};

class ObjectIteratorSplitByBuckets : public IObjectIterator, private WithContext
{
public:
    ObjectIteratorSplitByBuckets(
        ObjectIterator iterator_,
        const String & format_,
        ObjectStoragePtr object_storage_,
        const ContextPtr & context_);

    ObjectInfoPtr next(size_t) override;
    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

private:
    const ObjectIterator iterator;
    String format;
    ObjectStoragePtr object_storage;
    FormatSettings format_settings;

    std::queue<ObjectInfoPtr> pending_objects_info;
    const LoggerPtr log = getLogger("GlobIterator");
};


}
