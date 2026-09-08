#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <base/types.h>

namespace DB
{

struct DenseHyperLogLog;

/** The build fill's per-row routing, from the same hash the shared table buckets by.
  *
  * For every row the map hash of the build's `HashJoin::Type` is computed through that type's key
  * getter (so the fill and the table agree byte for byte on what the key is), mixed with
  * `sharedJoinMix`, and its top 16 bits are saved as the row's route. A plan of `bits` partitions sends
  * the row to partition `route >> (16 - bits)`, and because the table places the key at the top
  * `size_degree` bits of the same mixed value, that partition's cell range contains the key's home cell
  * for any `size_degree >= bits`. The probe never routes: it hashes and walks the one table.
  *
  * The sketch is fed the top 32 bits of the mixed value for every insertable row (`skip`, 1 = skip, is
  * the merged null map and ON mask). Skipped rows still get a route, since the scatter's bucket
  * derivation reads every row; they land in the drop bucket by the skip byte, not by the route.
  *
  * The fixed-size map types (`key8`, `key16`) always build a single partition and hash nothing; their
  * routes are zero and the sketch sees the key values themselves.
  */
void computeJoinRoutesForFill(
    HashJoin::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes,
    DenseHyperLogLog & hll);

}
