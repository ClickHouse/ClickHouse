#pragma once

#include <Columns/IColumn.h>

#include <span>

namespace DB::ColumnsScatter
{

/// Count rows routed to each shard from a batch of pids spans.
///
/// `rows_per_shard` must be pre-zeroed with size == num_shards. Compute it once and
/// pass the result to every `scatter` call for that batch (one per column-position) to
/// eliminate the K − 1 redundant pids re-scans that the internal counting path would do.
void countRowsPerShard(std::span<const std::span<const UInt32>> pids_per_source, std::span<UInt32> rows_per_shard);

/// Batched, type-dispatched physical scatter.
///
/// `source_columns[b]` is the source column extracted from chunk b for one
/// column-position; every element has the same concrete column type. For
/// each b, row j of `source_columns[b]` is routed to shard
/// `pids_per_source[b][j]`. Destinations are allocated and exact-sized
/// inside; the caller does not pre-reserve.
///
/// `rows_per_shard` (optional): pre-computed row counts produced by
/// `countRowsPerShard`.  When non-empty (size must equal `num_shards`) the
/// counting step inside each typed kernel is skipped, saving K−1 full pids
/// re-scans per flush.  When empty the row counts are computed internally
/// (backward-compatible convenience path used by callers that process only
/// one column at a time).
///
/// PRECONDITION (asserted at dispatch entry): every element of `source_columns`
/// has the same concrete column type.  The caller groups columns by position
/// across the pending chunk queue (one `scatter` call per column-position per
/// flush).
///
/// Returns: `MutableColumns` of length `num_shards`, each of the same
/// concrete type as the source columns.  The k-th destination holds, in
/// source-column order, every row routed to shard k.
///
/// Dispatch is O(1): a static function-pointer table indexed by
/// `IColumn::getDataType()` (one virtual call + one indexed indirect call,
/// independent of the number of supported types). Transparent wrappers
/// (`ColumnConst` / `ColumnSparse` / `ColumnReplicated`) are normalized away
/// recursively before dispatch (preserving `ColumnLowCardinality`).
///
/// Fast paths:
///   ColumnVector<T>   for the full integer / float / UUID / IPv4 / IPv6 set
///   ColumnDecimal<T>  (reinterpret as NativeType storage; incl. DateTime64 / Time64)
///   ColumnFixedString (runtime element size = getN())
///   ColumnString      (fused chars + offsets + per-partition byte-cursor)
///   ColumnNullable(X) (null-map scatter via UInt8 path + recursive scatter on nested)
///   ColumnTuple(...)  (recursive scatter per element, wrapped with ColumnTuple::create)
///
/// Fallback: any concrete type not in the dispatch table delegates
/// per-source-column to the legacy `IColumn::scatter()` virtual and appends
/// via `insertRangeFrom` into pre-cloneEmpty'd destinations.
[[nodiscard]] MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard = {});

}
