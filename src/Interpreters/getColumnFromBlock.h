#pragma once

namespace DB
{

class Block;
class IColumn;
class IDataType;
class ColumnsDescription;
struct ColumnWithTypeAndName;

/// Helps in-memory storages to extract columns from block.
/// Properly handles cases, when column is a subcolumn and when it is compressed.
ColumnPtr getColumnFromBlock(const Block & block, const NameAndTypePair & requested_column);

ColumnPtr tryGetColumnFromBlock(const Block & block, const NameAndTypePair & requested_column);
ColumnPtr tryGetSubcolumnFromBlock(const Block & block, const DataTypePtr & requested_column_type, const NameAndTypePair & requested_subcolumn);

/// The column `name` of the block, else the subcolumn `name` of the column `columns` registers it under (the block
/// must have that column), else the subcolumn `name` of the first block column that has it.
ColumnWithTypeAndName getColumnOrSubcolumnFromBlock(const Block & block, const String & name, const ColumnsDescription & columns);

/// The offsets of an array column stored compressed, decompressed without its elements,
/// or nullptr if they cannot be decompressed separately.
ColumnPtr tryDecompressArrayOffsets(const IDataType & type, const IColumn & column);

}
