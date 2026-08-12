#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/ColumnIdMapping.h>

namespace DB
{

class MergeTreeData;

/// Publishes @data's stored mapping into its metadata. A table without one does not use column IDs.
void loadColumnIdMapping(MergeTreeData & data);

/// Republishes the mapping trimmed to what @data's metadata still names. Throws `CORRUPTED_DATA`
/// when metadata names a column the mapping does not cover, or when two live columns share one ID.
void reconcileColumnIdMappingWithMetadata(MergeTreeData & data);

/// The id a part's per-column artifacts carry for @column_name -- `minmax_<id>.idx`,
/// `statistics_<id>.stats`, `ttl.txt` entries. Whole columns only.
ColumnId getColumnIdInPart(const NamesAndTypesList & part_columns, const String & column_name);

/// Inverse of getColumnIdInPart.
String getColumnNameByIdInPart(const NamesAndTypesList & part_columns, const ColumnId & column_id);

}
