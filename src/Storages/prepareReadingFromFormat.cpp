#include <Storages/prepareReadingFromFormat.h>
#include <Formats/FormatFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool enable_parsing_to_custom_serialization;
}

ReadFromFormatInfo prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool supports_subset_of_subcolumns)
{
    ReadFromFormatInfo info;
    /// Collect requested virtual columns and remove them from requested columns.
    Strings columns_to_read;
    for (const auto & column_name : requested_columns)
    {
        if (auto virtual_column = storage_snapshot->virtual_columns->tryGet(column_name))
            info.requested_virtual_columns.emplace_back(std::move(*virtual_column));
        else
            columns_to_read.push_back(column_name);
    }

    /// Create header for Source that will contain all requested columns including virtual columns at the end
    /// (because they will be added to the chunk after reading regular columns).
    info.source_header = storage_snapshot->getSampleBlockForColumns(columns_to_read);

    /// Set requested columns that should be read from data.
    info.requested_columns = info.source_header.getNamesAndTypesList();

    for (const auto & requested_virtual_column : info.requested_virtual_columns)
        info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    if (supports_subset_of_columns)
    {
        if (supports_subset_of_subcolumns)
        {
            /// Format can handle anything, no need to modify columns_to_read.
        }
        else if (columns_to_read.empty())
        {
            /// If only virtual columns were requested, just read the smallest column.
            columns_to_read.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);
        }
        else
        {
            /// We need to replace all subcolumns with their nested columns (e.g `a.b`, `a.b.c`, `x.y` -> `a`, `x`),
            /// because most formats cannot extract subcolumns on their own.
            /// All requested subcolumns will be extracted after reading.
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
    else
    {
        /// If format doesn't support reading subset of columns, read all columns.
        /// Requested columns/subcolumns will be extracted after reading.
        info.columns_description = storage_snapshot->metadata->getColumns();
    }

    /// Create header for InputFormat with columns that will be read from the data.
    info.format_header = storage_snapshot->getSampleBlockForColumns(info.columns_description.getNamesOfPhysical());

    info.serialization_hints = getSerializationHintsForFileLikeStorage(storage_snapshot->metadata, context);

    return info;
}

ReadFromFormatInfo updateFormatPrewhereInfo(const ReadFromFormatInfo & info, const PrewhereInfoPtr & prewhere_info)
{
    chassert(prewhere_info);

    if (info.prewhere_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "updateFormatPrewhereInfo called more than once");

    ReadFromFormatInfo new_info;
    new_info.prewhere_info = prewhere_info;

    /// Removes columns that are only used as prewhere input.
    /// Adds prewhere result column if !remove_prewhere_column.
    new_info.format_header = SourceStepWithFilter::applyPrewhereActions(info.format_header, prewhere_info);

    /// We assume that any format that supports prewhere also supports subset of subcolumns, so we
    /// don't need to replace subcolumns with their nested columns etc.
    new_info.source_header = new_info.format_header;

    new_info.requested_virtual_columns = info.requested_virtual_columns;
    for (const auto & requested_virtual_column : new_info.requested_virtual_columns)
        new_info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    for (const auto & col : new_info.format_header)
    {
        new_info.requested_columns.emplace_back(col.name, col.type);
        if (info.format_header.has(col.name))
        {
            new_info.columns_description.add(info.columns_description.get(col.name));
        }
        else
        {
            chassert(col.name == prewhere_info->prewhere_column_name);
            chassert(!prewhere_info->remove_prewhere_column);
            new_info.columns_description.add(ColumnDescription(col.name, col.type));
        }
    }

    return new_info;
}

SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::enable_parsing_to_custom_serialization])
        return {};

    auto insertion_table = context->getInsertionTable();
    if (!insertion_table)
        return {};

    auto storage_ptr = DatabaseCatalog::instance().tryGetTable(insertion_table, context);
    if (!storage_ptr)
        return {};

    const auto & our_columns = metadata_snapshot->getColumns();
    const auto & storage_columns = storage_ptr->getInMemoryMetadataPtr()->getColumns();
    auto storage_hints = storage_ptr->getSerializationHints();
    SerializationInfoByName res;

    for (const auto & hint : storage_hints)
    {
        if (our_columns.tryGetPhysical(hint.first) == storage_columns.tryGetPhysical(hint.first))
            res.insert(hint);
    }

    return res;
}

}
