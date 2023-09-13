#include "Utils.h"

namespace DB
{

[[maybe_unused]]bool isNumeric(DataTypePtr )
{
    /// TODO
    return true;
}

bool isConstColumn(const ActionsDAG::Node * node_)
{
    return node_->column && isColumnConst(*node_->column);
}

void adjustActionNodeStats(Float64 /*row_count*/, ColumnStatisticsPtr /*column_stats*/)
{
    /// TODO
}

}
