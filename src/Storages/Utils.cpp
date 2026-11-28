#include <vector>
#include <Storages/Utils.h>
#include <DataTypes/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
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

    std::optional<OrderBreakingConversion> findOrderBreakingConversion(
        const StorageSnapshot & source, const StorageSnapshot & declared, const NameSet * names)
    {
        const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysicalAndAliases)
                                 .withSubcolumns()
                                 .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);

        if (names)
        {
            for (const auto & name : *names)
            {
                auto source_column = source.tryGetColumn(options, name);
                auto declared_column = declared.tryGetColumn(options, name);
                if (source_column && declared_column && !conversionPreservesOrder(*source_column->type, *declared_column->type))
                    return OrderBreakingConversion{name, source_column->type, declared_column->type};
            }
            return {};
        }

        /// `getColumns` does not enumerate dynamic subcolumns, so each side is also resolved from the other.
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
}
