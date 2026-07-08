#include <Storages/System/StorageSystemPartStatistics.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

StorageSystemPartStatistics::StorageSystemPartStatistics(const StorageID & table_id_)
    : StorageSystemPartsBase(table_id_,
    ColumnsDescription{
        {"database",       std::make_shared<DataTypeString>(), "Name of the database."},
        {"table",          std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine",         std::make_shared<DataTypeString>(), "Name of the table engine without parameters."},
        {"partition_id",   std::make_shared<DataTypeString>(), "ID of the partition the data part belongs to."},
        {"name",           std::make_shared<DataTypeString>(), "Name of the data part."},
        {"active",         std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging."},
        {"column",         std::make_shared<DataTypeString>(), "Name of the column."},
        {"type",           std::make_shared<DataTypeString>(), "Column type."},
        {"statistics",     std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Types of statistics present for the column in this data part, e.g. ['MinMax','Uniq']."},
        {"rows",           std::make_shared<DataTypeUInt64>(), "The number of rows the statistics were built over. May exceed the current number of rows in the data part if rows were removed by lightweight deletes after the statistics had been built."},
        {"min",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The minimum value of the column recorded in the statistics (from 'basic' or 'minmax' statistics), rendered as a string. NULL if not available."},
        {"max",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The maximum value of the column recorded in the statistics (from 'basic' or 'minmax' statistics), rendered as a string. NULL if not available."},
        {"cardinality",    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Estimated number of distinct values of the column (from 'uniq' statistics). NULL if not available."},
        {"null_count",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The number of NULL values of the column recorded in the statistics (from 'basic' statistics on a Nullable column). NULL if not available."},
    }
    )
{
}

void StorageSystemPartStatistics::processNextStorage(
    ContextPtr, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column)
{
    using State = MergeTreeDataPartState;

    MergeTreeData::DataPartStateVector all_parts_state;
    MergeTreeData::DataPartsVector all_parts = info.getParts(all_parts_state, has_state_column);

    for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
    {
        const auto & part = all_parts[part_number];
        auto part_state = all_parts_state[part_number];

        /// Rows exist only for columns present in the estimates map, so the map is needed
        /// unconditionally (even a bare `SELECT count()` depends on it).
        Estimates estimates = part->getEstimates();
        if (estimates.empty())
            continue;

        /// Iterate columns of the part (not the unordered estimates map) for deterministic row order.
        for (const auto & column : part->getColumns())
        {
            auto estimate_it = estimates.find(column.name);
            if (estimate_it == estimates.end())
                continue;

            const Estimate & estimate = estimate_it->second;

            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.database);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.table);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(info.engine);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->info.getPartitionId());
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part->name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(part_state == State::Active);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column.name);
            if (columns_mask[src_index++])
                columns[res_index++]->insert(column.type->getName());

            if (columns_mask[src_index++])
            {
                Array types;
                for (const auto & stat_type : estimate.types)
                    types.push_back(toString(stat_type));

                columns[res_index++]->insert(types);
            }

            if (columns_mask[src_index++])
                columns[res_index++]->insert(estimate.rows_count);

            if (columns_mask[src_index++])
            {
                if (estimate.estimated_min.has_value())
                    columns[res_index++]->insert(applyVisitor(FieldVisitorToString(), *estimate.estimated_min));
                else
                    columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                if (estimate.estimated_max.has_value())
                    columns[res_index++]->insert(applyVisitor(FieldVisitorToString(), *estimate.estimated_max));
                else
                    columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                if (estimate.estimated_cardinality.has_value())
                    columns[res_index++]->insert(*estimate.estimated_cardinality);
                else
                    columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                if (estimate.estimated_null_count.has_value())
                    columns[res_index++]->insert(*estimate.estimated_null_count);
                else
                    columns[res_index++]->insertDefault();
            }

            if (has_state_column)
                columns[res_index++]->insert(part->stateString());
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemPartStatistics) }
