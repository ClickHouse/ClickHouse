#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// The histogram arm of a simple binary operator, mirroring the `hlhs`/`hrhs` cases of `vectorElemBinop`
/// in Prometheus promql/engine.go; engaged only when at least one operand is StoreMethod::HISTOGRAM_GRID.
struct SimpleBinaryOperatorHistogramArm
{
    /// The per-step float value, histogram payload and kind (0 = float, 1 = histogram, NULL = no sample)
    /// of each side; a scalar side has kind statically 0 and a typed-NULL histogram (never used).
    struct Input
    {
        ASTPtr left_value;
        ASTPtr left_histogram;
        ASTPtr left_kind;
        ASTPtr right_value;
        ASTPtr right_histogram;
        ASTPtr right_kind;
        bool left_is_scalar = false;
        bool right_is_scalar = false;
    };

    /// Builds the `histogram_values` arm expression from the per-step values/kinds of both sides:
    /// the histogram sample produced by the operation, or NULL where it is not allowed (the sample is dropped).
    std::function<ASTPtr(const Input &)> build_histogram_values_arm;
};

/// Applies a simple binary operator (via `apply_function_to_ast`) to two scalars or instant vectors, joining vectors
/// on label sets per `operator_node`; `drop_metric_name` drops `__name__`, `allow_grouping_modifier_copy_metric_name` lets grouping modifiers re-add it.
SQLQueryPiece applySimpleBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    bool allow_grouping_modifier_copy_metric_name,
    const SimpleBinaryOperatorHistogramArm * histogram_arm = nullptr);

}
