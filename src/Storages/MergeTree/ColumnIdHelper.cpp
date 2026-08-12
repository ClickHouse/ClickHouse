#include <Storages/MergeTree/ColumnIdHelper.h>

#include <Storages/MergeTree/ColumnIdMappingStore.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

/// Runs on the load path BEFORE the mapping is published, so a torn or hand-edited `column_ids.json`
/// is refused with a file-naming error rather than tripping the schema-stamp desync assertion (a
/// debug abort).
void checkColumnIdMappingCoversMetadata(const MergeTreeData & data, const ColumnIdMapping & mapping)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr(nullptr, false);

    Names missing_from_mapping;
    for (const auto & col : metadata_snapshot->getColumns().getAllPhysical())
    {
        if (!mapping.hasColumnName(col.name))
            missing_from_mapping.push_back(col.name);
    }
    if (!missing_from_mapping.empty())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Column ID mapping for table {} is missing entries for column(s): {}. "
            "This indicates a torn write of `column_ids.json` (mapping not "
            "updated while `metadata.sql` already committed the schema change). "
            "The mapping cannot be rebuilt safely because DROP + re-ADD of the "
            "same column name makes on-disk files indistinguishable from their "
            "column ID alone. Restore the table from backup or fix "
            "`column_ids.json` manually.",
            data.getStorageID().getNameForLogs(),
            fmt::join(missing_from_mapping, ", "));
}

/// A two-phase rename transiently maps two column names to one column ID, so this runs only after
/// the reconciliation trim has dropped the old name: a duplicate surviving that is real corruption
/// that would silently let two SQL columns share one on-disk stream.
void checkNoColumnNamesShareColumnId(const MergeTreeData & data, const ColumnIdMapping & mapping)
{
    std::unordered_map<String, Names> names_by_id;
    for (const auto & [column_name, column_id] : mapping.getNameToId())
        names_by_id[column_id].push_back(column_name);

    for (const auto & [column_id, names] : names_by_id)
    {
        if (names.size() > 1)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Column ID mapping for table {} has multiple columns "
                "mapped to the same column ID '{}': {}.  All of them are "
                "still present in the schema, which indicates a corrupted "
                "`column_ids.json` (not a transient two-phase-rename state). "
                "Restore from backup or repair the file manually.",
                data.getStorageID().getNameForLogs(),
                column_id,
                fmt::join(names, ", "));
    }
}

}

void loadColumnIdMapping(MergeTreeData & data, bool attach)
{
    auto loaded_mapping = data.getColumnIdMappingStore().load(attach);
    if (!loaded_mapping)
        return;

    if (loaded_mapping->isActive())
        checkColumnIdMappingCoversMetadata(data, *loaded_mapping);
    data.setColumnIdMapping(std::move(*loaded_mapping));
}

void reconcileColumnIdMappingWithMetadata(MergeTreeData & data)
{
    if (!data.hasActiveColumnIdMapping())
        return;

    auto mapping = data.getColumnIdMapping();
    auto metadata_snapshot = data.getInMemoryMetadataPtr(nullptr, false);
    auto metadata_columns = metadata_snapshot->getColumns().getAllPhysical();

    checkColumnIdMappingCoversMetadata(data, *mapping);

    /// Trim mapping entries whose column name is no longer in metadata (mapping-ahead-of-schema
    /// window from a crash between the mapping write and the `metadata.sql` truncation). Safe to
    /// recover from: the dropped or renamed column lost its on-disk authority at the schema commit,
    /// so the entry is dead weight.
    ColumnIdMapping reconciled = *mapping;
    bool changed = false;
    for (const auto & col_name : mapping->columnNames())
    {
        if (!metadata_columns.tryGetByName(col_name))
        {
            reconciled.removeColumn(col_name);
            changed = true;
        }
    }

    if (changed)
        data.persistMapping(std::move(reconciled));

    if (auto final_mapping = data.getColumnIdMapping())
        checkNoColumnNamesShareColumnId(data, *final_mapping);
}

/// `getColumnId()` of the part's column of that name, so a metadata-only RENAME cannot orphan the
/// artifacts. A name the part does not hold passes through as an id -- the same fallback
/// `getColumnId()` itself makes. For the subcolumn-aware form that keys `getSerialization`, see
/// `NameAndTypePair::getStorageKey`.
ColumnId getColumnIdInPart(const NamesAndTypesList & part_columns, const String & column_name)
{
    auto column = part_columns.tryGetByName(column_name);
    return column ? column->getColumnId() : ColumnId{column_name};
}

String getColumnNameByIdInPart(const NamesAndTypesList & part_columns, const ColumnId & column_id)
{
    for (const auto & column : part_columns)
        if (column.getColumnId() == column_id)
            return column.name;
    return column_id.value();
}

}
