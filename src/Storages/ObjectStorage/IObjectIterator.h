#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/ObjectInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
};

class ExpressionActions;

struct IObjectIterator
{
    virtual ~IObjectIterator() = default;
    virtual ObjectInfoPtr next(size_t) = 0;
    virtual size_t estimatedKeysCount() = 0;
    virtual std::optional<UInt64> getSnapshotVersion() const { return std::nullopt; }
    virtual std::shared_ptr<ISimpleTransform>
    getPositionDeleteTransformer(const ObjectInfoPtr &, const SharedHeader &, const std::optional<FormatSettings> &, ContextPtr) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getPositionDeleteTransformer is not implemented for this iterator type");
    }
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
    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const ObjectInfoPtr & object_info,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_) const override
    {
        return iterator->getPositionDeleteTransformer(object_info, header, format_settings, context_);
    }

private:
    const ObjectIterator iterator;
    const std::string object_namespace;
    const NamesAndTypesList virtual_columns;
    const NamesAndTypesList hive_partition_columns;
    const std::shared_ptr<ExpressionActions> filter_actions;
};
}
