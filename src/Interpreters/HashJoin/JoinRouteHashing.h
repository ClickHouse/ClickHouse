#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/HashJoin/HashJoinTypes.h>
#include <base/types.h>

namespace DB
{

struct DenseHyperLogLog;

/** The build fill's per-row routing, from the same hash `HashJoinTable` buckets by.
  *
  * For every row, the map hash of the build's `HashJoinTypes::Type` is computed through that type's key
  * getter. The fill and the table agree byte for byte on what the key is. `hashJoinTablePlacement`
  * turns the hash into the table's placement word, and the word's top 16 bits are saved as the row's
  * route. A plan of `bits` partitions sends the row to partition `route >> (16 - bits)`. The table
  * places the key at the top `size_degree` bits of the same word. For any `size_degree >= bits`
  * that partition's cell range contains the key's home cell. The probe never routes: it hashes and
  * walks the one table.
  *
  * When supplied, the sketch is fed the top 32 bits of the multiplicatively mixed hash
  * (`hashJoinTableMix`) for every insertable row. `skip` (1 = skip) is the merged null map and ON mask.
  * Skipped rows still get a route because scatter reads every route. The skip byte sends them to the
  * drop bucket.
  *
  * The fixed-size map types (`key8`, `key16`) always build a single partition and hash nothing. Their
  * routes are zero and the sketch sees the key values themselves.
  */
void computeJoinRoutesForFill(
    HashJoinTypes::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes,
    DenseHyperLogLog & hll);

/// Write the same routes when a cached distinct count makes the sketch unnecessary.
void computeJoinRoutesForFill(
    HashJoinTypes::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes);

}
