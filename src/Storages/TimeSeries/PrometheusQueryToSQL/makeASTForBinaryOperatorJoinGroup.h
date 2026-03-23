#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Makes an expression to evaluate the `join_group` column to join the sides of a binary operator on instant vectors.
ASTPtr makeASTForBinaryOperatorJoinGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && group,
    bool metric_name_dropped_from_group,
    bool * metric_name_dropped_from_join_group = nullptr);

}
