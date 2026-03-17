#pragma once

#include <Core/NamesAndTypes.h>

#include <memory>
#include <unordered_map>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

class PhysicalNameMapping;
using PhysicalNameMappingPtr = std::shared_ptr<const PhysicalNameMapping>;

/// Bidirectional map between logical SQL column names and stable on-disk physical names.
///
/// Existing columns get physical_name == column_name at activation time.
/// New columns added after activation get monotonically increasing numeric
/// physical names ("1", "2", ...) from a counter that never decreases.
/// RENAME only updates the mapping; the physical name (and therefore all
/// on-disk filenames) stays unchanged.  DROP removes the entry but the
/// counter is never recycled, so a subsequent ADD of the same logical name
/// gets a fresh physical name — this is the key invariant that makes
/// DROP + re-ADD safe (old data files become orphans, not wrong-type reads).
///
/// Names not present in the mapping (virtual columns, `_row_exists`, etc.)
/// pass through unchanged via `getPhysicalNameOrDefault`.
///
/// Thread safety: instances are immutable once published through
/// `MultiVersion<PhysicalNameMapping>` in `MergeTreeData`.  Mutation methods
/// (`addColumn`, `renameColumn`, ...) are only called on local copies
/// inside engine `alter()` before atomic publication.
class PhysicalNameMapping
{
public:
    bool isActive() const
    {
        return active;
    }

    const std::unordered_map<String, String> & getLogicalToPhysical() const
    {
        return logical_to_physical;
    }

    /// Throws if `logical_name` is not in the mapping.
    String getPhysicalName(const String & logical_name) const;

    /// Returns `logical_name` itself when not in the mapping — safe default
    /// for virtual columns, helper columns, and legacy (pre-activation) code paths.
    String getPhysicalNameOrDefault(const String & logical_name) const;

    /// Reverse lookup: physical -> logical.  Throws if not found.
    String getLogicalName(const String & physical_name) const;

    bool hasLogicalName(const String & logical_name) const;
    bool hasPhysicalName(const String & physical_name) const;

    /// Allocates the next numeric physical name and advances the counter.
    /// Also sets `active = true` as a side effect (first allocation activates the mapping).
    String allocatePhysicalName();

    void addColumn(const String & logical_name, const String & physical_name);
    void removeColumn(const String & logical_name);
    void renameColumn(const String & old_logical_name, const String & new_logical_name);

    Names logicalNames() const;

    /// For existing tables: every column gets physical_name == column_name.
    /// The counter is initialized past the highest numeric column name to
    /// avoid collisions (e.g. a table with column "2" starts the counter at 3).
    static PhysicalNameMapping createForExistingTable(const NamesAndTypesList & columns);

    /// Currently identical to `createForExistingTable` — new tables also
    /// start with identity mapping because the initial column set has no
    /// reason to use counter-allocated names.  Kept as a separate entry
    /// point for future divergence (e.g. new tables could start all columns
    /// with numeric names).
    static PhysicalNameMapping createForNewTable(const NamesAndTypesList & columns);

    void serialize(WriteBuffer & buf) const;
    static PhysicalNameMapping deserialize(ReadBuffer & buf);

    String toString() const;
    static PhysicalNameMapping fromString(const String & str);

private:
    bool active = false;
    UInt64 next_physical_column_id = 1;
    std::unordered_map<String, String> logical_to_physical;
    std::unordered_map<String, String> physical_to_logical;
};

/// Sets `physical_name` on each `NameAndTypePair` by looking it up in the mapping.
/// No-op when the mapping is inactive.  Columns not found in the mapping
/// get their logical name as the physical name (passthrough).
void populatePhysicalNames(NamesAndTypesList & columns, const PhysicalNameMapping & mapping);

}
