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

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part);

}
