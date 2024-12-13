#pragma once
#include <Core/Block.h>

namespace DB
{

/// Helps in-memory storages to extract columns from block.
/// Properly handles cases, when column is a subcolumn and when it is compressed.
ColumnPtr getColumnFromBlock(const Block & block, const NameAndTypePair & requested_column);

ColumnPtr tryGetColumnFromBlock(const Block & block, const NameAndTypePair & requested_column);
ColumnPtr tryGetSubcolumnFromBlock(const Block & block, const DataTypePtr & requested_column_type, const NameAndTypePair & requested_subcolumn);

}
