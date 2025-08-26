#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/ISimpleTransform.h>

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
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ObjectInfoPtr) const { return nullptr; }
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer(ObjectInfoPtr) const { return nullptr; }
    virtual ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr) const { return nullptr; }
    virtual ColumnMapperPtr getColumnMapperForCurrentSchema() const { return nullptr; }
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
    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ObjectInfoPtr object_info) const override
    {
        return iterator->getInitialSchemaByPath(object_info);
    }
    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ObjectInfoPtr object_info) const override
    {
        return iterator->getSchemaTransformer(object_info);
    }

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override
    {
        return iterator->getColumnMapperForObject(object_info);
    }
    ColumnMapperPtr getColumnMapperForCurrentSchema() const override { return iterator->getColumnMapperForCurrentSchema(); }

private:
    const ObjectIterator iterator;
    const std::string object_namespace;
    const NamesAndTypesList virtual_columns;
    const NamesAndTypesList hive_partition_columns;
    const std::shared_ptr<ExpressionActions> filter_actions;
};
}
