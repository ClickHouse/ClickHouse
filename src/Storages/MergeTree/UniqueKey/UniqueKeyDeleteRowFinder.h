#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class StorageMergeTree;

}

namespace DB::UniqueKeyTxn
{

/// Per-part dead-row set discovered by the row finder.
struct UniqueKeyDeleteRowsForPart
{
    PartName         part_name;
    RoaringBitmapPtr rows;   /// row numbers within `part_name`'s row-id space
};

/// Map: partition_id -> ordered list of (part, rows) hits. The map keeps
/// partitions independent so the caller can drive the per-partition critical
/// section one partition at a time without re-grouping. The inner vector is
/// sorted by `part_name` for deterministic logs.
using UniqueKeyDeleteRowsByPartition =
    std::unordered_map<String, std::vector<UniqueKeyDeleteRowsForPart>>;

/// Stats returned alongside the row map; mirror what the previous
/// `executeUniqueKeyDelete` body used to log inline. The caller decides
/// whether to surface them as ProfileEvents / log lines.
struct UniqueKeyDeleteRowFinderStats
{
    size_t total_matched_rows = 0;
    size_t parts_with_hits = 0;
};

/// Resolve `(part_name, row_number)` pairs to mark dead, by running an
/// internal
///   `SELECT _part, _part_offset FROM <db>.<tbl>[IN PARTITION ...] WHERE <pred>`
/// and grouping the result rows by part name + partition id.
///
/// This is the row-identification step of the UNIQUE KEY synchronous DELETE
/// path (canonical §3 Writer probe), extracted into a pure helper so the
/// `PartitionTxnController::commit` driver can plug into the same input shape
/// without re-running the SELECT.
///
/// Pure-function seam: `group()` takes already-extracted `(part_name,
/// row_number)` pairs and produces the grouped result. `find()` is the
/// production wrapper that runs the internal SELECT, decodes the pairs, and
/// delegates to `group()`. Tests drive `group()` directly so they don't need
/// a full server context.
///
/// Behaviour:
///   - Reuses MergeTree's read-path predicate plumbing (primary-key
///     skipping, PREWHERE, skip indexes, delete-bitmap row-level filtering).
///   - `_part` is decoded as a plain String; `_part_offset` is decoded as
///     UInt64 with a range check against UInt32 (UNIQUE KEY row-id space).
///   - A malformed `_part` (cannot parse partition id) from the read pipeline
///     is a should-never-happen; `find()` throws (fail-closed) rather than
///     silently under-deleting that part's rows.
///   - The row finder does NOT take the per-partition UK mutex; the caller
///     is responsible for acquiring it before resolving part_name →
///     DataPartPtr and writing bitmaps.
struct UniqueKeyDeleteRowFinder
{
    struct Result
    {
        UniqueKeyDeleteRowsByPartition by_partition;
        UniqueKeyDeleteRowFinderStats  stats;
    };

    /// One `(part_name, row_number)` pair from the internal SELECT. Row
    /// numbers are UInt32 (UNIQUE KEY row-id space); the SELECT pulls them
    /// as UInt64 and `find()` range-checks before narrowing.
    struct PartRowEntry
    {
        PartName part_name;
        UInt32   row_number;
    };

    /// Pure-function seam: groups already-decoded `(part_name, row_number)`
    /// pairs into the result shape. Malformed `_part` (cannot parse partition
    /// id) entries are dropped here — this lenient path exists only for the
    /// direct `group()` unit tests; the production caller `find()` fail-closes
    /// (throws) on any unparseable `_part` before delegating, so no production
    /// path can silently under-delete.
    static Result group(
        const std::vector<PartRowEntry> & pairs,
        MergeTreeDataFormatVersion format_version);

    /// Production driver: runs the internal SELECT against `storage`,
    /// decodes `(_part, _part_offset)` per row, and forwards to `group()`.
    static Result find(
        StorageMergeTree & storage,
        const ASTPtr & query_ptr,
        ContextPtr query_context);
};

}
