#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/ColumnIdMapping.h>

namespace DB
{

class MergeTreeData;

/// Publishes @data's stored mapping into its metadata. `attach` distinguishes a pre-existing table
/// (which must have a stored mapping once it opted into column IDs) from CREATE, which persists the
/// mapping only after the storage is constructed.
void loadColumnIdMapping(MergeTreeData & data, bool attach);

/// Republishes the mapping trimmed to what @data's metadata still names. Throws `CORRUPTED_DATA`
/// when metadata names a column the mapping does not cover, or when two live columns share one ID.
void reconcileColumnIdMappingWithMetadata(MergeTreeData & data);

/// The id a part's per-column artifacts carry for a column -- `minmax_<id>.idx`,
/// `statistics_<id>.stats`, `ttl.txt` entries: `getColumnId()` of the part's column of that name, so a
/// metadata-only RENAME cannot orphan them. A name the part does not hold passes through as an id, the
/// same fallback `getColumnId()` itself makes. Whole columns only -- for the subcolumn-aware form that
/// keys `getSerialization`, see `NameAndTypePair::getStorageKey`.
ColumnId getColumnIdInPart(const NamesAndTypesList & part_columns, const String & column_name);

/// Inverse of getColumnIdInPart.
String getColumnNameByIdInPart(const NamesAndTypesList & part_columns, const ColumnId & column_id);

}
