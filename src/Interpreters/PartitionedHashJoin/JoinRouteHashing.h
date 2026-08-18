#pragma once

#include <Columns/IColumn.h>
#include <base/types.h>

namespace DB
{

struct DenseHyperLogLog;

/** One 32-bit route word per row over the join keys, from the `ColumnsScatter` route-word family and
  * so independent of the CRC32C the leaf tables bucket by. A plan of `bits` partitions sends a row to
  * leaf `word >> (32 - bits)`; the build saves the top 16 bits per row, the probe recomputes.
  *
  * Build and probe must agree bit-for-bit, which is why every entry point below funnels into one
  * fold implementation. `key_columns` are the prepared keys, already stripped of null maps; a live
  * `ColumnLowCardinality` is routed by its value bytes so it matches the plain column of the same
  * values on the other side.
  */
void computeJoinRouteWords(const ColumnRawPtrs & key_columns, size_t rows, UInt32 * words);

/// Stores routes for every row, skipped ones included - the scatter's bucket derivation reads them -
/// and feeds the sketch only for rows `skip` (nullable, 1 = skip) lets through.
void computeJoinRoutesForFill(const ColumnRawPtrs & key_columns, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog & hll);

/// For when a cached distinct-key count from an earlier run of the same query replaces the sketch.
/// No `skip`: it only ever filtered the sketch feed, never the route store.
void computeJoinRoutesForFill(const ColumnRawPtrs & key_columns, size_t rows, UInt16 * routes);

/// Stores the leaf id directly. Agrees with the build's `route >> (16 - bits)` for every plan,
/// since both slice the same top bits and `0 < bits <= 16`.
void computeJoinLeafIds(const ColumnRawPtrs & key_columns, size_t rows, size_t bits, UInt16 * leaf_ids);

}
