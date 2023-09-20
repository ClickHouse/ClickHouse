#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

bool isNumeric(DataTypePtr data_type);
bool isConstColumn(const ActionsDAG::Node * node_);
bool isAlwaysFalse(const ASTPtr & ast);

/// Adjust 'statistics' to match output_columns.
void adjustStatisticsByColumns(Statistics & statistics, const Names & output_columns);

}
