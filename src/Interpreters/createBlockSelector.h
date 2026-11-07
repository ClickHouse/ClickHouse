#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray_fwd.h>


namespace DB
{

/** Create a 'selector' to be used in IColumn::scatter method
  *  according to sharding scheme and values of column with sharding key.
  *
  * Each of num_shards has its weight. Weight must be small.
  * 'slots' contains weight elements for each shard, in total - sum of all weight elements.
  *
  * Values of column get divided to sum_weight, and modulo of division
  *  will map to corresponding shard through 'slots' array.
  *
  * Column must have integer type.
  * T is type of column elements.
  * `Id` is the type of the selector values, `UInt64` or `UInt16`. With `UInt16`, every value of `slots` must be below 65536.
  */
template <typename T, typename Id = UInt64>
PaddedPODArray<Id> createBlockSelector(
    const IColumn & column,
    const std::vector<UInt64> & slots);

}
