#include <Core/Field.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeNullable.h>
#include <Optimizer/Statistics/Utils.h>

namespace DB
{

bool isNumeric(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        /// number
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
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
        /// interval
        case TypeIndex::Interval:
        /// enum
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
            return true;
        case TypeIndex::Nullable:
            return isNumeric(dynamic_cast<const DataTypeNullable *>(type.get())->getNestedType());
        default:
            return false;
    }
}

bool canConvertToFloat64(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Array:
        case TypeIndex::AggregateFunction:
        case TypeIndex::UUID:
        case TypeIndex::Tuple:
        case TypeIndex::Set:
        case TypeIndex::Nothing:
        case TypeIndex::Interval:
        case TypeIndex::JSONPaths:
        case TypeIndex::Map:
        case TypeIndex::Function:
        case TypeIndex::Object:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
            return false;
        default:
            return true;
    }
}

bool isConstColumn(const ActionsDAG::Node * node)
{
    return node->column && isColumnConst(*node->column);
}

bool isAlwaysFalse(const ASTPtr & ast)
{
    if (auto * literal = ast->as<ASTLiteral>())
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
