#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

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

    std::optional<ObjectMetadata> getObjectMetadata() const { return relative_path_with_metadata.metadata; }
    void setObjectMetadata(const ObjectMetadata & metadata) { relative_path_with_metadata.metadata = metadata; }
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
}
