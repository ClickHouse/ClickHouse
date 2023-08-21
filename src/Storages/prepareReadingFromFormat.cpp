#include <Storages/prepareReadingFromFormat.h>
#include <Formats/FormatFactory.h>

namespace DB
{

ReadFromFormatInfo prepareReadingFromFormat(const Strings & requested_columns, const StorageSnapshotPtr & storage_snapshot, bool supports_subset_of_columns, const NamesAndTypesList & virtuals)
{
    ReadFromFormatInfo info;
    /// Collect requested virtual columns and remove them from requested columns.
    Strings columns_to_read;
    for (const auto & column_name : requested_columns)
    {
        bool is_virtual = false;
        for (const auto & virtual_column : virtuals)
        {
            if (column_name == virtual_column.name)
            {
                info.requested_virtual_columns.push_back(virtual_column);
                is_virtual = true;
                break;
            }
        }

        if (!is_virtual)
            columns_to_read.push_back(column_name);
    }

    /// Create header for Source that will contain all requested columns including virtual columns at the end
    /// (because they will be added to the chunk after reading regular columns).
    info.source_header = storage_snapshot->getSampleBlockForColumns(columns_to_read);
    for (const auto & requested_virtual_column : info.requested_virtual_columns)
        info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    /// Set requested columns that should be read from data.
    info.requested_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), columns_to_read);

    if (supports_subset_of_columns)
    {
        /// If only virtual columns were requested, just read the smallest column.
        if (columns_to_read.empty())
        {
            columns_to_read.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);
        }
        /// We need to replace all subcolumns with their nested columns (e.g `a.b`, `a.b.c`, `x.y` -> `a`, `x`),
        /// because most formats cannot extract subcolumns on their own.
        /// All requested subcolumns will be extracted after reading.
        else
        {
            std::unordered_set<String> columns_to_read_set;
            /// Save original order of columns.
            std::vector<String> new_columns_to_read;
            for (const auto & column_to_read : info.requested_columns)
            {
                auto name = column_to_read.getNameInStorage();
                if (!columns_to_read_set.contains(name))
                {
                    columns_to_read_set.insert(name);
                    new_columns_to_read.push_back(name);
                }
            }
            columns_to_read = std::move(new_columns_to_read);
        }
        info.columns_description = storage_snapshot->getDescriptionForColumns(columns_to_read);
    }
    /// If format doesn't support reading subset of columns, read all columns.
    /// Requested columns/subcolumns will be extracted after reading.
    else
    {
        info.columns_description = storage_snapshot->metadata->getColumns();
    }

    /// Create header for InputFormat with columns that will be read from the data.
    info.format_header = storage_snapshot->getSampleBlockForColumns(info.columns_description.getNamesOfPhysical());
    return info;
}

}
