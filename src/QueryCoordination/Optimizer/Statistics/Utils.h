#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

/// Whether type is numeric compatible type. Used to distinguish data types
/// who can be cast to Float64. The casted value will be used as min or max
/// value of a column statistics.
///
/// For these who (such as string) can not be cast, use 0 which means that we
/// can not calculate statistics by range for these data types.
bool isNumeric(const DataTypePtr & type);

/// Whether a node is a const column.
bool isConstColumn(const ActionsDAG::Node * node);

/// Whether a ast represent false.
bool isAlwaysFalse(const ASTPtr & ast);

/// Adjust 'statistics' to match output_columns by adding the missing and removing the additional.
void adjustStatisticsByColumns(Statistics & statistics, const Names & final_columns);

}
