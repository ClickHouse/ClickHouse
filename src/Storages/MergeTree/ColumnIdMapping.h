#pragma once

#include <Core/NamesAndTypes.h>

#include <memory>
#include <optional>
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
/// Thread safety: instances are immutable once published. The mapping is folded into
/// the versioned `StorageInMemoryMetadata` (a `shared_ptr`-to-const field), so it is
/// published atomically with the schema through the metadata `MultiVersion`.  Mutation
/// methods (`addColumn`, `renameColumn`, ...) are only called on local copies inside
/// engine `alter()` before that atomic publication.
///
/// Concurrency contract (no lock stops an ALTER from publishing a new mapping in the
/// middle of a merge, mutation, SELECT, or part load):
///  1. Parts speak only IDs. Any question about a specific part — stream names,
///     columns.txt, serialization.json, minmax files — is answered from the part's
///     stamped column IDs (`part->getColumns()`, each pair carrying `column_id`),
///     never from the live mapping. In-memory structures loaded from ID-keyed disk
///     files stay keyed by ID.
///  2. An operation captures ONE mapping snapshot at entry and threads it (SELECTs
///     via `StorageSnapshot`, merges/mutations via their context) — the same
///     discipline as `metadata_snapshot`.
///  3. The live mapping is read only at operation entry and in table-level actions
///     under the alter lock; any other call is a defect.
/// One stale snapshot is safe: IDs are stable under RENAME and never recycled, so a
/// snapshot can lag only in names; every artifact of a part produced from one
/// snapshot agrees by construction.
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

    static UInt64 extractNumericCounter(const String & s);

    /// An ID at or above the counter is unreadable here: a later `ADD COLUMN` gets handed the same
    /// one.  A pre-activation ID (a bare column name) is never one.
    bool isColumnIdAtOrAboveCounter(const String & column_id) const
    {
        return extractNumericCounter(column_id) >= next_column_id;
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

    /// Guard + lookup folded together for the pattern call sites repeat: translate a logical
    /// name to its (typed) id, or an id back to its logical name. `nullopt` when unmapped.
    std::optional<ColumnId> tryGetColumnId(const String & logical_name) const;
    std::optional<String> tryGetLogicalName(const ColumnId & column_id) const;

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

    /// Ingress stamp for both write (new part) and read (query resolution): set `column_id`
    /// on each `NameAndTypePair` by name→id lookup. No-op when the mapping is inactive.
    ///  - An already-stamped column (a part-local id) is preserved — re-stamping from the
    ///    live mapping would clobber it after a DROP + re-ADD that reuses the name.
    ///  - A mapped column gets its id.
    ///  - A virtual column (`isVirtualColumn`, covering persistent virtuals stored in the
    ///    part and ephemeral ones like `_part_offset`) is left UNSTAMPED (empty id). An empty
    ///    id is equivalent to name-keyed on disk for every consumer, so this is the same as
    ///    the old write behavior of stamping a virtual to its own name.
    ///  - Any other unmapped column is a real physical column: a schema/mapping desync, which
    ///    throws `LOGICAL_ERROR` rather than silently mis-resolving.
    /// Called before `convertToSubcolumns`, so a flattened-Nested field is still a flat `n.x`
    /// pair whose full dotted name is directly in the mapping.
    void stampColumnIds(NamesAndTypesList & columns) const;

    /// Lenient counterpart to `stampColumnIds`: a mapped column gets its id, everything else is
    /// left UNSTAMPED (empty id ≡ name-keyed on disk). Never throws. Used where an unmapped
    /// non-virtual name is legitimate rather than a desync:
    ///  - projection-part write — synthetic aggregate columns (`sum(c)`) are outside the base
    ///    table's mapping;
    ///  - the ALTER collision pre-check (`checkColumnFilenamesForCollision`) — validates a
    ///    not-yet-applied schema whose new/renamed columns aren't in the live mapping yet;
    ///  - `loadColumns`' no-`columns.txt` fallback — a projection part rebuilds its columns from
    ///    projection metadata (synthetic aggregates, `_parent_part_offset`) and stamps them with
    ///    the PARENT table's mapping, so those names are legitimately absent.
    void stampColumnIdsLenient(NamesAndTypesList & columns) const;

    /// Identity mapping: every column gets column_id == column_name — the
    /// initial state of both a new table and an existing one at activation.
    /// The counter is initialized past the highest numeric column name to
    /// avoid collisions (e.g. a table with column "2" starts the counter at 3).
    static ColumnIdMapping createIdentity(const NamesAndTypesList & columns);

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

/// The id a part's per-column artifacts carry for a column -- `minmax_<id>.idx`,
/// `statistics_<id>.stats`, `ttl.txt` entries: `getColumnId()` of the part's column of that name, so a
/// metadata-only RENAME cannot orphan them. A name the part does not hold passes through as an id, the
/// same fallback `getColumnId()` itself makes. Whole columns only -- for the subcolumn-aware form that
/// keys `getSerialization`, see `NameAndTypePair::getStorageKey`.
ColumnId getColumnIdInPart(const NamesAndTypesList & part_columns, const String & column_name);

/// Inverse of getColumnIdInPart.
String getColumnNameByIdInPart(const NamesAndTypesList & part_columns, const ColumnId & column_id);

}
