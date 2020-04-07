#pragma once

#include <Columns/IColumn.h>


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
  */
template <typename T>
IColumn::Selector createBlockSelector(
    const IColumn & column,
    const std::vector<UInt64> & slots);

}
