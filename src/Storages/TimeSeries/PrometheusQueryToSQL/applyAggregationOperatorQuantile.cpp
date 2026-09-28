#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <cmath>
#include <limits>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the `quantile` aggregation operator.
    void checkArgumentTypes(
        const PrometheusQueryTree::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects 2 arguments, but was called with {} arguments",
                            operator_name, arguments.size());
        }

        const auto & phi_arg = arguments[0];

        if (phi_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR,
                            getPromQLText(phi_arg, context), phi_arg.type);
        }

        const auto & vector_arg = arguments[1];

        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }

    /// Converts the quantile parameter phi to an AST expression usable in SQL.
    ASTPtr getPhi(SQLQueryPiece && phi_arg, ConverterContext & context)
    {
        switch (phi_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return timeSeriesScalarToAST(phi_arg.scalar_value, context.scalar_data_type);
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(phi_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                return makeASTFunction("assumeNotNull", std::move(subquery_id));
            }
            case StoreMethod::SCALAR_GRID:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Aggregation operator 'quantile' with a non-constant scalar parameter is not supported");
            }
            default:
            {
                throwUnexpectedStoreMethod(phi_arg, context);
            }
        }
    }
}


SQLQueryPiece applyAggregationOperatorQuantile(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto & phi_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (phi_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_arg = toVectorGrid(std::move(vector_arg), context);

    /// PromQL evaluates `quantile` with an out-of-range or NaN parameter to a constant
    /// (with a warning) instead of failing the query:
    ///   phi < 0  -> -Inf for every group
    ///   phi > 1  -> +Inf for every group
    ///   phi NaN  -> NaN for every group
    /// quantileExactInclusive() would throw an exception for such phi, so if a constant phi
    /// is out of range we emit a constant-valued array aligned to the time grid instead
    /// (the same way `histogram_quantile` handles an out-of-range phi). A runtime scalar phi
    /// (e.g. `scalar(vector(-0.5))`, StoreMethod::SINGLE_SCALAR) gets the same treatment at
    /// runtime, see below.
    bool const_phi_out_of_range = (phi_arg.store_method == StoreMethod::CONST_SCALAR)
        && (std::isnan(phi_arg.scalar_value) || (phi_arg.scalar_value < 0) || (phi_arg.scalar_value > 1));

    auto res = vector_arg;
    res.node = operator_node;

    /// Step 1: aggregate over series, using `new_group` as an intermediate alias to avoid
    /// ambiguity with the input `group` column when the alias and the source column share the same name.
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr new_group = transformGroupASTForAggregationOperator(
            operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        ASTPtr quantile_expr;
        if (const_phi_out_of_range)
        {
            Float64 out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            if (std::isnan(phi_arg.scalar_value))
                out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            else if (phi_arg.scalar_value < 0)
                out_of_range_value = -std::numeric_limits<Float64>::infinity();
            else
                out_of_range_value = std::numeric_limits<Float64>::infinity();

            /// arrayMap(x -> if(isNotNull(x), <constant>, NULL), anyForEach(values))
            /// anyForEach produces one array per group aligned to the time grid, with NULL at
            /// positions where no input series had data. arrayMap then replaces every non-NULL
            /// position with the constant and keeps NULLs as-is.
            quantile_expr = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                        make_intrusive<ASTLiteral>(out_of_range_value),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        }
        else if (phi_arg.store_method == StoreMethod::SINGLE_SCALAR)
        {
            /// The value of a runtime scalar phi (e.g. `scalar(vector(-0.5))`) is not known here,
            /// so the out-of-range check has to happen at runtime: the phi parameter passed to
            /// the aggregate function is clamped to [0, 1] to keep the aggregate function from
            /// throwing (a scalar subquery is still a constant at analysis time, so it can be
            /// used as a parameter), and then every non-NULL element of the result is replaced
            /// with the Prometheus constant if phi turns out to be out of range or NaN:
            ///
            /// arrayMap(x -> if(isNotNull(x),
            ///                  multiIf(isNaN(phi), nan, phi < 0, -inf, phi > 1, inf, x),
            ///                  NULL),
            ///          quantileExactInclusiveForEach(least(greatest(phi, 0.), 1.))(values))
            ///
            /// least() and greatest() clamp a NaN phi to a valid value too, which one doesn't
            /// matter because the result is replaced with NaN anyway.
            ASTPtr phi = getPhi(std::move(phi_arg), context);

            ASTPtr clamped_phi = makeASTFunction("least",
                makeASTFunction("greatest", phi->clone(), make_intrusive<ASTLiteral>(0.)),
                make_intrusive<ASTLiteral>(1.));

            ASTPtr substituted_element = makeASTFunction("multiIf",
                makeASTFunction("isNaN", phi->clone()),
                make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::quiet_NaN()),
                makeASTFunction("less", phi->clone(), make_intrusive<ASTLiteral>(0.)),
                make_intrusive<ASTLiteral>(-std::numeric_limits<Float64>::infinity()),
                makeASTFunction("greater", phi->clone(), make_intrusive<ASTLiteral>(1.)),
                make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::infinity()),
                make_intrusive<ASTIdentifier>("x"));

            quantile_expr = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                        std::move(substituted_element),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                addParametersToAggregateFunction(
                    makeASTFunction("quantileExactInclusiveForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                    std::move(clamped_phi)));
        }
        else
        {
            /// quantileExactInclusiveForEach(phi)(values)
            quantile_expr = addParametersToAggregateFunction(
                makeASTFunction("quantileExactInclusiveForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                getPhi(std::move(phi_arg), context));
        }
        builder.select_list.push_back(std::move(quantile_expr));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        /// Drop empty-values rows.
        /// If the input has no rows then quantileExactInclusiveForEach(...)([]) returns [], but the number of values
        /// in array must always match the number of steps in SQLQueryPiece (see StoreMethod::VECTOR_GRID),
        /// so we just drop such rows.
        builder.having = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group`.
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
