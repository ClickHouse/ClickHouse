#include "Utils.h"
#include <Core/Field.h>

namespace DB
{

[[maybe_unused]] bool isNumeric(DataTypePtr)
{
    /// TODO
    return true;
}

bool isConstColumn(const ActionsDAG::Node * node_)
{
    return node_->column && isColumnConst(*node_->column);
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


void adjustStatisticsByColumns(Statistics & statistics, const Names & output_columns)
{
    /// remove additional
    for (const auto & column : statistics.getColumnNames())
    {
        if (std::find(output_columns.begin(), output_columns.end(), column) == output_columns.end())
            statistics.removeColumnStatistics(column);
    }
    /// add missing
    for (const auto & column : output_columns)
    {
        if (!statistics.containsColumnStatistics(column))
            statistics.addColumnStatistics(column, ColumnStatistics::unknown());
    }
}

}
