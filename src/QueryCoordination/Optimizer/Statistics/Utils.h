#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

bool isNumeric(DataTypePtr data_type);
bool isConstColumn(const ActionsDAG::Node * node_);
void adjustActionNodeStats(Float64 row_count, ColumnStatisticsPtr column_stats);

}
