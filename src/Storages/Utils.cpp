#include <algorithm>
#include <optional>
#include <vector>
#include <Storages/Utils.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <DataTypes/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageSnapshot.h>


namespace CurrentMetrics
{
    extern const Metric AttachedTable;
    extern const Metric AttachedReplicatedTable;
    extern const Metric AttachedView;
    extern const Metric AttachedDictionary;
}


namespace DB
{
    std::vector<CurrentMetrics::Metric> getAttachedCountersForStorage(const StoragePtr & storage)
    {
        if (storage->isView())
        {
            return {CurrentMetrics::AttachedView};
        }
        if (storage->isDictionary())
        {
            return {CurrentMetrics::AttachedDictionary};
        }
        /// NOLINT(storage-cast): runs under the database lock, and attach and detach must count a proxy alike.
        if (typeid_cast<StorageReplicatedMergeTree *>(storage.get()) != nullptr)
        {
            return {CurrentMetrics::AttachedTable, CurrentMetrics::AttachedReplicatedTable};
        }
        return {CurrentMetrics::AttachedTable};
    }

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace
{

struct OrderBreakingConversion
{
    String name;
    DataTypePtr source_type;
    DataTypePtr declared_type;
};

std::optional<OrderBreakingConversion> findOrderBreakingConversion(const StorageSnapshot & source, const StorageSnapshot & declared)
{
    /// `getColumns` does not enumerate dynamic subcolumns, so each side is also resolved from the other.
    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysicalAndAliases)
                             .withSubcolumns()
                             .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);
    for (const auto & source_column : source.getColumns(options))
    {
        auto declared_column = declared.tryGetColumn(options, source_column.name);
        if (declared_column && !conversionPreservesOrder(*source_column.type, *declared_column->type))
            return OrderBreakingConversion{source_column.name, source_column.type, declared_column->type};
    }
    for (const auto & declared_column : declared.getColumns(options))
    {
        auto source_column = source.tryGetColumn(options, declared_column.name);
        if (source_column && !conversionPreservesOrder(*source_column->type, *declared_column.type))
            return OrderBreakingConversion{declared_column.name, source_column->type, declared_column.type};
    }
    return {};
}

/// Whether a read of `storage` lands on a `Merge`, through wrappers that forward both the stage and the read.
bool readLandsOnMerge(StoragePtr storage)
{
    std::vector<StoragePtr> passed;
    while (storage && std::find(passed.begin(), passed.end(), storage) == passed.end())
    {
        if (dynamic_cast<const StorageMerge *>(storage.get()))
            return true;
        passed.push_back(storage);
        if (const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get()))
            storage = proxy->getNested();
        else if (const auto * alias = dynamic_cast<const StorageAlias *>(storage.get()))
            storage = alias->tryGetTargetTable();
        else if (const auto * buffer = dynamic_cast<const StorageBuffer *>(storage.get()))
            storage = buffer->getDestinationTable();
        else if (const auto * view = dynamic_cast<const StorageMaterializedView *>(storage.get()))
            storage = view->tryGetTargetTable();
        else
            return false;
    }
    return false;
}

}

QueryProcessingStage::Enum getStageForConvertingRead(
    QueryProcessingStage::Enum source_stage,
    const StoragePtr & source,
    const StorageSnapshotPtr & source_snapshot,
    const StorageSnapshotPtr & declared_snapshot)
{
    if (source_stage > QueryProcessingStage::FetchColumns && readLandsOnMerge(source)
        && findOrderBreakingConversion(*source_snapshot, *declared_snapshot))
        return QueryProcessingStage::FetchColumns;
    return source_stage;
}

void checkStageForConvertingRead(
    QueryProcessingStage::Enum stage,
    const StoragePtr & source,
    const StorageSnapshotPtr & source_snapshot,
    const StorageSnapshotPtr & declared_snapshot,
    const StorageID & reader)
{
    if (stage <= QueryProcessingStage::FetchColumns || !readLandsOnMerge(source))
        return;

    if (auto conversion = findOrderBreakingConversion(*source_snapshot, *declared_snapshot))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_COLUMNS,
            "Column {} of table {} has type {}, which does not preserve the order when converted to the type {} "
            "declared by {}. The query processing stage was chosen before this type was visible, most likely "
            "because the table was altered while the query was being planned. Retry the query",
            backQuoteIfNeed(conversion->name), source->getStorageID().getNameForLogs(), conversion->source_type->getName(),
            conversion->declared_type->getName(), reader.getNameForLogs());
}

}
