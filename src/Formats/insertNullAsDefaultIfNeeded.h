#pragma once

#include <Core/BlockMissingValues.h>
#include <Functions/CastOverloadResolver.h>

namespace DB
{
struct ColumnWithTypeAndName;

/// Unless `tuple_elements_matching` is `Default`, the elements of two named tuples are paired as the cast with it pairs
/// them (`DataTypeTuple::matchElementsByName`) instead of by position.
bool insertNullAsDefaultIfNeeded(ColumnWithTypeAndName & input_column, const ColumnWithTypeAndName & header_column, size_t column_i,
    BlockMissingValues * block_missing_values, TupleElementsMatching tuple_elements_matching = TupleElementsMatching::Default);

}
