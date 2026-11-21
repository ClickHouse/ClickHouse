#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks whether the specified AST is the empty set of tags made by timeSeriesNoTagsToAST().
bool isNoTagsAST(const ASTPtr & tags);

/// Applies the `on(tags)` or `ignoring(tags)` matching rules to a `tags` expression,
/// produces a reduced set of tags that can be used to match time series from both sides of a binary operator,
/// and also to calculate the result group if there is no grouping modifier used.
ASTPtr transformGroupASTForBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped);

}
