#pragma once

#include <Core/Block.h>

namespace DB
{

void insertNullAsDefaultIfNeeded(ColumnWithTypeAndName & input_column, const ColumnWithTypeAndName & header_column, size_t column_i, BlockMissingValues * block_missing_values);

}
