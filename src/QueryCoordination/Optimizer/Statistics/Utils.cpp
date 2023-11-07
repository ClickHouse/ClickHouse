#include "Utils.h"
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

bool isNumeric(const DataTypePtr & type)
{
    /// isColumnedAsNumber
    switch (type->getTypeId())
    {
        /// number
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128: /// TODO check
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        /// datetime
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        /// enum
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        /// interval
        case TypeIndex::Interval:
        /// ip
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
            return true;
        case TypeIndex::Nullable:
            return isNumeric(dynamic_cast<const DataTypeNullable *>(type.get())->getNestedType());
        default:
            return false;
    }
}

bool isConstColumn(const ActionsDAG::Node * node)
{
    return node->column && isColumnConst(*node->column);
}

bool isAlwaysFalse(const ASTPtr & ast)
{
    if (auto literal = ast->as<ASTLiteral>())
    {
        if (isInt64OrUInt64orBoolFieldType(literal->value.getType()))
            return literal->value.safeGet<UInt64>() == 0;
        if (literal->value.getType() == Field::Types::Bool)
            return !literal->value.safeGet<bool>();
    }
    return false;
}

void adjustStatisticsByColumns(Statistics & statistics, const Names & final_columns)
{
    /// remove additional
    for (const auto & column : statistics.getColumnNames())
        if (std::find(final_columns.begin(), final_columns.end(), column) == final_columns.end())
            statistics.removeColumnStatistics(column);
    /// add missing
    for (const auto & column : final_columns)
        if (!statistics.containsColumnStatistics(column))
            statistics.addColumnStatistics(column, ColumnStatistics::unknown());
}

}
