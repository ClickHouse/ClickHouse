#pragma once

#include <Core/NamesAndTypes.h>

#include <optional>

namespace DB
{

class ColumnIdMapping;

/** Resolving a column across the domains that name it.
  *
  * A column ID is assigned once and never changes; a column NAME belongs to a domain and means nothing
  * outside it. Four domains meet in MergeTree:
  *
  * - A DATA PART is immutable and, once the feature is active, ID-based throughout: `columns.txt` stores
  *   IDs in the name slot and every stream file is named after one
  *   (`ISerialization::getFileNameForStreamByColumnId`). No name is written down at all. The names a part
  *   reports in memory are put there at load, by resolving those IDs through the mapping
  *   (`IMergeTreeDataPart::remapColumnIdsToColumnNames`), so they are a cache of the schema as it stood
  *   then -- right when loaded, stale as soon as a metadata-only RENAME lands, and never an identity.
  *   Parts therefore match each other by ID: a merge or mutation pairs source columns with target columns
  *   through IDs, never by comparing names.
  * - The TABLE SCHEMA is the current, mutable truth. Names here change under DDL; IDs do not. The
  *   metadata snapshot carries the `ColumnIdMapping`, which is the *only* translation between an ID and
  *   a current name.
  * - A STORAGE SNAPSHOT is what one query sees: schema columns plus virtuals. Pairs resolved from it
  *   carry the ID, but also the schema's TYPE -- which is not the part's type when a MODIFY COLUMN has
  *   not been materialised, so a snapshot pair must not be used to enumerate a part's streams.
  * - A USER COMMAND (ALTER, mutation) names columns as typed, and one that targets parts outlives the
  *   schema it was written against: a queued RENAME or DROP names its column as it stood BEFORE the
  *   mutation, so the current snapshot may not have that name at all while the part still answers to it.
  *   Translating a command to a part is therefore the ID path with a name fallback, which is what
  *   `MutateTask::tryGetPartColumnForCommand` is; on the read path the same shape is
  *   `getColumnIdOrPartName`, where the old name comes from the alter conversions instead.
  *
  * Hence the translation rules:
  *
  * - Entering a part from a request or the schema: resolve to the part's own pair by ID
  *   (`IMergeTreeDataPart::tryGetColumnBySnapshotName` / `tryGetColumnByRequest`), then use THAT pair
  *   for anything naming, sizing or deserialising the part's files.
  * - Leaving a part for the schema -- a background operation writing metadata, per-column sizes, a
  *   codec or TTL lookup: translate the part's ID through the snapshot's mapping
  *   (`tryGetCurrentColumnName`) rather than trusting the name the part reports, which was resolved
  *   against an older schema. No current name means the schema dropped the column and this part's copy
  *   is an orphan.
  * - Virtual columns (`_row_exists`, `_part_offset`) have no ID and appear in no mapping; they stay
  *   name-keyed in every domain.
  * - Before activation nothing is stamped, `NameAndTypePair::getColumnId` falls back to the name, and
  *   every rule above degrades to the name lookup it replaced. That is why the feature is inert until a
  *   table gets a mapping -- and why a name-keyed leftover on any of these paths goes unnoticed until it
  *   is not.
  */

/// The key a part holds @requested_column under: its id, else @name_in_part. Not `getColumnId`, whose
/// own fallback is the CURRENT name -- which a part left on the old name by a rename never answers to.
ColumnId getColumnIdOrPartName(const NameAndTypePair & requested_column, const String & name_in_part);

/// The name @part_column goes by in the CURRENT schema, through @mapping -- its own name when the part
/// carries no ids. `nullopt` for an id the mapping no longer knows.
std::optional<String> tryGetCurrentColumnName(const NameAndTypePair & part_column, const ColumnIdMapping * mapping);

}
