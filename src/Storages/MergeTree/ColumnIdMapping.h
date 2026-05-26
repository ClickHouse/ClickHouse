#pragma once

#include <Core/NamesAndTypes.h>

#include <memory>
#include <unordered_map>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

class ColumnIdMapping;
using ColumnIdMappingPtr = std::shared_ptr<const ColumnIdMapping>;

/// Bidirectional map between logical SQL column names and stable on-disk column IDs.
///
/// Existing columns get column_id == column_name at activation time.
/// New columns added after activation get monotonically increasing numeric
/// column IDs ("1", "2", ...) from a counter that never decreases.
/// RENAME only updates the mapping; the column ID (and therefore all
/// on-disk filenames) stays unchanged.  DROP removes the entry but the
/// counter is never recycled, so a subsequent ADD of the same logical name
/// gets a fresh column ID — this is the key invariant that makes
/// DROP + re-ADD safe (old data files become orphans, not wrong-type reads).
///
/// Names not present in the mapping (virtual columns, `_row_exists`, etc.)
/// pass through unchanged via `getColumnIdOrDefault`.
///
/// Thread safety: instances are immutable once published through
/// `MultiVersion<ColumnIdMapping>` in `MergeTreeData`.  Mutation methods
/// (`addColumn`, `renameColumn`, ...) are only called on local copies
/// inside engine `alter()` before atomic publication.
class ColumnIdMapping
{
public:
    bool isActive() const
    {
        return active;
    }

    const std::unordered_map<String, String> & getLogicalToId() const
    {
        return logical_to_id;
    }

    UInt64 getNextColumnIdCounter() const
    {
        return next_column_id;
    }

    /// Throws if `logical_name` is not in the mapping.
    String getColumnId(const String & logical_name) const;

    /// Returns `logical_name` itself when not in the mapping — safe default
    /// for virtual columns, helper columns, and legacy (pre-activation) code paths.
    String getColumnIdOrDefault(const String & logical_name) const;

    /// Reverse lookup: column ID -> logical name. Throws if not found.
    String getLogicalName(const String & column_id) const;

    bool hasLogicalName(const String & logical_name) const;
    bool hasColumnId(const String & column_id) const;

    /// Allocates the next numeric column ID and advances the counter.
    /// Also sets `active = true` as a side effect (first allocation activates the mapping).
    String allocateColumnId();

    void addColumn(const String & logical_name, const String & column_id);
    void removeColumn(const String & logical_name);
    void renameColumn(const String & old_logical_name, const String & new_logical_name);

    /// Two-phase rename for crash safety.
    ///
    /// RENAME COLUMN b TO name must update two persisted artifacts that cannot
    /// be written atomically: `column_ids.json` (the mapping) and the table
    /// metadata in the database catalog.  A naive single-step rename that
    /// changes the mapping key from "b" to "name" in one write would lose the
    /// column ID "b" if the server crashes before metadata commits — on
    /// restart, metadata still has column "b", but the mapping no longer has
    /// an entry for it, and `reconcileColumnIdMappingWithMetadata` would
    /// remove the dangling "name" entry.
    ///
    /// Phase 1 (`beginRename`): add the NEW logical name while keeping the OLD
    /// one — both point to the same column ID.  Persist the mapping.
    ///   mapping on disk:  { "b":"b", "name":"b" }    (both present)
    ///   metadata:         column "b"                  (not yet committed)
    ///
    /// Then commit the metadata (column "b" becomes "name").
    ///
    /// Phase 2 (`finishRename`): remove the OLD logical name and persist.
    ///   mapping on disk:  { "name":"b" }
    ///   metadata:         column "name"
    ///
    /// Crash scenarios (reconciliation removes mapping entries absent from
    /// metadata):
    ///  - Crash before metadata commit: metadata has "b", reconciliation
    ///    keeps "b"->"b" and removes "name"->"b".  Correct original state.
    ///  - Crash after metadata commit but before phase 2: metadata has "name",
    ///    reconciliation keeps "name"->"b" and removes "b"->"b".  Correct
    ///    renamed state.
    void beginRename(const String & old_logical_name, const String & new_logical_name);
    void finishRename(const String & old_logical_name);

    Names logicalNames() const;

    /// For existing tables: every column gets column_id == column_name.
    /// The counter is initialized past the highest numeric column name to
    /// avoid collisions (e.g. a table with column "2" starts the counter at 3).
    static ColumnIdMapping createForExistingTable(const NamesAndTypesList & columns);

    /// Currently identical to `createForExistingTable` — new tables also
    /// start with identity mapping because the initial column set has no
    /// reason to use counter-allocated names.  Kept as a separate entry
    /// point for future divergence (e.g. new tables could start all columns
    /// with numeric names).
    static ColumnIdMapping createForNewTable(const NamesAndTypesList & columns);

    void serialize(WriteBuffer & buf) const;
    static ColumnIdMapping deserialize(ReadBuffer & buf);

    String toString() const;
    static ColumnIdMapping fromString(const String & str);

private:
    bool active = false;
    UInt64 next_column_id = 1;
    std::unordered_map<String, String> logical_to_id;
    std::unordered_map<String, String> id_to_logical;
};

/// Sets `column_id` on each `NameAndTypePair` by looking it up in the mapping.
/// No-op when the mapping is inactive.  Columns not found in the mapping
/// get their logical name as the column ID (passthrough).
void populateColumnIds(NamesAndTypesList & columns, const ColumnIdMapping & mapping);

}
