#pragma once
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IMergeTreeDataPart;

struct RowExistsColumn
{
    static const String name;
    static const DataTypePtr type;
};

struct BlockNumberColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct BlockOffsetColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct PartDataVersionColumn
{
    static const String name;
    static const DataTypePtr type;
};

/// Whether a column is a persistent virtual column that is physically stored
/// inside data parts (as opposed to computed on the fly).  These columns are
/// NOT managed by the physical name mapping and should be passed through
/// unchanged during column remapping.
/// Keep in sync when adding new persistent virtual columns.
inline bool isPersistentVirtualColumn(const String & column_name)
{
    return column_name == RowExistsColumn::name
        || column_name == BlockNumberColumn::name
        || column_name == BlockOffsetColumn::name;
}

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection);

}
