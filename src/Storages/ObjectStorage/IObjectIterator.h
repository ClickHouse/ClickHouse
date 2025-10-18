#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Logger.h>
#include <Common/Macros.h>

namespace DB
{

using ObjectInfo = RelativePathWithMetadata;
using ObjectInfoPtr = std::shared_ptr<RelativePathWithMetadata>;
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

    std::vector<std::vector<size_t>> splitObjectToBuckets(const std::vector<size_t> bucket_sizes);
    std::queue<ObjectInfoPtr> pending_objects_info;
    const LoggerPtr log = getLogger("GlobIterator");
};


}
