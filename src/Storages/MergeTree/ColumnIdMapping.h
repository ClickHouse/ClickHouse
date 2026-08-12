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

/// Bidirectional map between SQL column names and stable on-disk column IDs.
///
/// Existing columns get column_id == column_name at activation time.
/// New columns added after activation get monotonically increasing numeric
/// column IDs ("1", "2", ...) from a counter that never decreases; a flattened
/// Nested child takes two of them, as "<group prefix>.<child counter>" ("5.7"),
/// so that siblings share the group prefix their offsets stream is named from.
/// RENAME only updates the mapping; the column ID (and therefore all
/// on-disk filenames) stays unchanged.  DROP removes the entry but the
/// counter is never recycled, so a subsequent ADD of the same column name
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

    const std::unordered_map<String, String> & getNameToId() const
    {
        return name_to_id;
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

    /// Throws if `column_name` is not in the mapping.
    String getColumnId(const String & column_name) const;

    /// Returns `column_name` itself when not in the mapping — safe default
    /// for virtual columns, helper columns, and legacy (pre-activation) code paths.
    String getColumnIdOrDefault(const String & column_name) const;

    /// Reverse lookup: column ID -> column name. Throws if not found.
    String getColumnName(const String & column_id) const;

    bool hasColumnName(const String & column_name) const;
    bool hasColumnId(const String & column_id) const;

    /// Guard + lookup folded together for the pattern call sites repeat: translate a column
    /// name to its (typed) id, or an id back to its column name. `nullopt` when unmapped.
    std::optional<ColumnId> tryGetColumnId(const String & column_name) const;
    std::optional<String> tryGetColumnName(const ColumnId & column_id) const;

    /// Allocates the next numeric column ID and advances the counter.
    /// Also sets `active = true` as a side effect (first allocation activates the mapping).
    String allocateColumnId();

    void addColumn(const String & column_name, const String & column_id);
    void removeColumn(const String & column_name);

    /// Two-phase rename for crash safety: `beginRename` before the metadata commit, `finishRename`
    /// after it.  The mechanism and its crash windows are at `beginRename`'s definition.
    void beginRename(const String & old_column_name, const String & new_column_name);
    void finishRename(const String & old_column_name);

    Names columnNames() const;

    /// Ingress stamp for both write (new part) and read (query resolution): set `column_id` on each
    /// `NameAndTypePair` that carries none yet (an existing one is a part-local id and is kept).
    /// Throws `LOGICAL_ERROR` for a physical column the mapping does not hold.
    /// Must be called before `convertToSubcolumns`, while a flattened-Nested field is still `n.x`.
    void stampColumnIds(NamesAndTypesList & columns) const;

    /// Lenient counterpart to `stampColumnIds` for callers whose columns are legitimately outside
    /// the mapping (projection aggregates, a not-yet-applied schema): leaves those unstamped
    /// instead of throwing.
    void stampColumnIdsLenient(NamesAndTypesList & columns) const;

    /// Identity mapping: every column gets column_id == column_name — the initial state of both a
    /// new table and an existing one at activation.
    static ColumnIdMapping createIdentity(const NamesAndTypesList & columns);

    void serialize(WriteBuffer & buf) const;
    static ColumnIdMapping deserialize(ReadBuffer & buf);

    String toString() const;
    static ColumnIdMapping fromString(const String & str);

private:
    /// Drop `it`'s forward entry and repair the reverse map.
    void detachColumnName(std::unordered_map<String, String>::iterator it);

    bool active = false;
    UInt64 next_column_id = 1;
    std::unordered_map<String, String> name_to_id;
    std::unordered_map<String, String> id_to_name;
};

}
