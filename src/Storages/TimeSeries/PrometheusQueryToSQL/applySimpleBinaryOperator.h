#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies a simple binary operator (arithmetic or comparison) to two instant vectors or scalars.
/// The actual operation is provided via `apply_function_to_ast`, which receives two AST nodes
/// (left and right values) and returns the combined AST.
/// If at least one operand is scalar, the operation is applied element-wise without joining.
/// If both operands are instant vectors, they are joined on their label sets (respecting
/// `on()`/`ignoring()` and `group_left`/`group_right` modifiers from `operator_node`).
/// If `drop_metric_name` is true, the `__name__` tag is removed from the result.
/// If `allow_grouping_modifier_copy_metric_name` is true, `group_left(__name__)` or `group_right(__name__)`
/// can re-introduce `__name__` into the result from the "one" side even when `drop_metric_name` is true.
/// This corresponds to the default Prometheus behavior for arithmetic operators.
/// For comparison operators with the `bool` modifier, `allow_grouping_modifier_copy_metric_name` should be
/// false, because `bool` always drops `__name__` unconditionally.
SQLQueryPiece applySimpleBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    bool allow_grouping_modifier_copy_metric_name);

}
