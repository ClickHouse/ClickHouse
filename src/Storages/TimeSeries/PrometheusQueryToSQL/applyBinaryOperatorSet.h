#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Check argument types for set binary operator (i.e. one of "and", "or", "unless").
void checkArgumentTypesForSetBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    const SQLQueryPiece & left_argument,
    const SQLQueryPiece & right_argument,
    const ConverterContext & context);

/// Build a compact per-step presence mask for a vector grid.
ASTPtr makePresenceMask(ASTPtr values);

/// A JOIN ON expression needs a column from each side. When the join key is the constant group 0 (empty on()),
/// the side is wrapped into a subquery that exposes the constant as the `join_group` column.
ASTPtr makeJoinKeyColumn(String & side, ASTPtr && join_group, ConverterContext & context);

}
