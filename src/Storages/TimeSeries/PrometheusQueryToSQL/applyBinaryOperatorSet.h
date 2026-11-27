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

}
