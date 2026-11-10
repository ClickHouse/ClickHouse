#pragma once

#include <Core/BlockMissingValues.h>

namespace DB
{
struct ColumnWithTypeAndName;

/// `match_tuple_elements_by_name`: the elements of two named tuples are paired by name instead of by position.
bool insertNullAsDefaultIfNeeded(
    ColumnWithTypeAndName & input_column,
    const ColumnWithTypeAndName & header_column,
    size_t column_i,
    BlockMissingValues * block_missing_values,
    bool match_tuple_elements_by_name = false);

}
