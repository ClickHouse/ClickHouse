#pragma once

#include <optional>
#include <Common/CurrentMetrics.h>
#include <Core/Names.h>
#include <DataTypes/IDataType_fwd.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{
    struct StorageSnapshot;

    std::vector<CurrentMetrics::Metric> getAttachedCountersForStorage(const StoragePtr & storage);

    struct OrderBreakingConversion
    {
        String name;
        DataTypePtr source_type;
        DataTypePtr declared_type;
    };

    /// The first name whose conversion from `source` to `declared` does not preserve the order (see `conversionPreservesOrder`),
    /// with each name resolved on both sides as a column, a subcolumn or a virtual column. With `names`, only those names are checked.
    std::optional<OrderBreakingConversion> findOrderBreakingConversion(
        const StorageSnapshot & source, const StorageSnapshot & declared, const NameSet * names = nullptr);
}
