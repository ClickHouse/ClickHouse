#pragma once

#include <base/UUID.h>
#include <Core/Types.h>

namespace DB
{

/// Identifies the data view that a QueryPlan was built against.
/// The value is captured from the exact storage snapshot the plan holds, so it describes
/// "what this plan actually saw" rather than the current state of the table.
struct DataVersion
{
    /// Fingerprint of the set of active data parts, including patch parts.
    UInt64 parts_generation = 0;

    /// Fingerprint of mutation state: mutation parameters, on-fly commands, patch parts,
    /// unique-key delete-bitmap snapshot and unfinished-mutation counters.
    UInt64 mutation_version = 0;

    /// Table metadata version, used as a fallback for schema changes.
    UInt64 metadata_version = 0;

    bool operator==(const DataVersion & other) const = default;
};

/// Data version of one table referenced by a cached QueryPlan.
struct TableDataVersion
{
    UUID table_uuid;
    DataVersion version;

    bool operator==(const TableDataVersion & other) const = default;
};

}
