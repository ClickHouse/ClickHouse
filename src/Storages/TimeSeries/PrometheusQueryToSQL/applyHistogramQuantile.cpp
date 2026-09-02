#include <Storages/TimeSeries/PrometheusQueryToSQL/applyHistogramQuantile.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <cmath>
#include <limits>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the `histogram_quantile` function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 2 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        const auto & phi_arg = arguments[0];

        if (phi_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::SCALAR,
                            getPromQLText(phi_arg, context), phi_arg.type);
        }

        const auto & vector_arg = arguments[1];

        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects second argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }

    template <typename BuildValuesExpression>
    SQLQueryPiece applyHistogramFunction(
        const PrometheusQueryTree::Function * function_node,
        SQLQueryPiece expression,
        ConverterContext & context,
        BuildValuesExpression && build_values_expression)
    {
        ASTPtr aggregation_query;
        {
            SelectQueryBuilder builder;

            /// Keep `__name__` in the group key so distinct histograms remain separate. It is
            /// removed from the result below after the aggregate has been evaluated.
            auto new_group_expr = makeASTFunction(
                "timeSeriesRemoveTag",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTLiteral>("le"));
            new_group_expr->setAlias(ColumnNames::NewGroup);
            builder.select_list.push_back(std::move(new_group_expr));

            auto le_array_expr = makeASTFunction(
                "arrayResize",
                makeASTFunction("CAST",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>("Array(Float64)")),
                makeASTFunction("length", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                makeASTFunction("ifNull",
                    makeASTFunction("toFloat64OrNull",
                        makeASTFunction("timeSeriesExtractTag",
                            make_intrusive<ASTIdentifier>(ColumnNames::Group),
                            make_intrusive<ASTLiteral>("le"))),
                    make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::quiet_NaN())));

            auto values_expr = build_values_expression(std::move(le_array_expr));
            values_expr->setAlias(ColumnNames::Values);
            builder.select_list.push_back(std::move(values_expr));

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            /// Prometheus silently drops input series whose `le` label is missing or cannot be
            /// parsed as a float, so a pure non-histogram input produces an empty result.
            builder.where = makeASTFunction("isNotNull",
                makeASTFunction("toFloat64OrNull",
                    makeASTFunction("timeSeriesExtractTag",
                        make_intrusive<ASTIdentifier>(ColumnNames::Group),
                        make_intrusive<ASTLiteral>("le"))));

            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            aggregation_query = builder.getSelectQuery();
        }

        ASTPtr column_renaming_query;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            builder.select_list.back()->setAlias(ColumnNames::Group);
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;
            column_renaming_query = builder.getSelectQuery();
        }

        SQLQueryPiece res{function_node, function_node->result_type, StoreMethod::VECTOR_GRID};
        res.select_query = std::move(column_renaming_query);
        res.start_time = expression.start_time;
        res.end_time = expression.end_time;
        res.step = expression.step;
        res.metric_name_dropped = false;

        /// PromQL histogram functions do not preserve the input metric name.
        return dropMetricName(std::move(res), context);
    }
}


bool isHistogramQuantile(std::string_view function_name)
{
    return function_name == "histogram_quantile";
}

bool isHistogramFraction(std::string_view function_name)
{
    return function_name == "histogram_fraction";
}

SQLQueryPiece applyHistogramQuantile(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    auto & phi_arg = arguments[0];
    auto & expression = arguments[1];

    if (phi_arg.store_method != StoreMethod::CONST_SCALAR)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Function 'histogram_quantile' currently requires a constant phi parameter");
    }

    expression = toVectorGrid(std::move(expression), context);

    if (expression.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    Float64 phi = phi_arg.scalar_value;

    auto build_quantile_expression = [phi](ASTPtr le_array_expr)
    {
        /// PromQL `histogram_quantile` has constant out-of-range semantics:
        ///   phi < 0  -> -Inf for every time step
        ///   phi > 1  -> +Inf for every time step
        ///   phi NaN  -> NaN for every time step
        /// These short-circuits happen before looking at the histogram, so instead of
        /// calling quantilePrometheusHistogramForEach we emit a constant-valued array
        /// aligned to the time grid (preserving NULL at positions where no input existed).
        if (std::isnan(phi) || phi < 0.0 || phi > 1.0)
        {
            Float64 out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            if (phi < 0.0)
                out_of_range_value = -std::numeric_limits<Float64>::infinity();
            else if (phi > 1.0)
                out_of_range_value = std::numeric_limits<Float64>::infinity();

            /// arrayMap(x -> if(isNotNull(x), <constant>, NULL), anyForEach(values))
            /// anyForEach produces one array per group aligned to the time grid, with NULL at
            /// positions where no input series had data. arrayMap then replaces every non-NULL
            /// position with the constant and keeps NULLs as-is.
            return makeASTFunction(
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

        /// quantilePrometheusHistogramForEach(phi)(le_array, values)
        ///
        /// `le_array` is constructed for each row as an array of the same length as values,
        /// filled with the extracted `le` tag value. Series without a parsable `le` are
        /// excluded by the shared WHERE clause, so the NaN fallback here is just a safety net.
        return addParametersToAggregateFunction(
            makeASTFunction("quantilePrometheusHistogramForEach",
                std::move(le_array_expr),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            make_intrusive<ASTLiteral>(phi));
    };

    return applyHistogramFunction(function_node, std::move(expression), context, std::move(build_quantile_expression));
}

namespace
{
    void checkHistogramFractionArgumentTypes(
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 3)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects 3 arguments, but was called with {} arguments",
                            function_name, arguments.size());
        }

        for (size_t i = 0; i < 2; ++i)
        {
            const auto & argument = arguments[i];
            if (argument.type != ResultType::SCALAR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument {} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::SCALAR,
                                getPromQLText(argument, context), argument.type);
            }
        }

        const auto & vector_arg = arguments[2];
        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects third argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }
}

SQLQueryPiece applyHistogramFraction(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkHistogramFractionArgumentTypes(function_node, arguments, context);

    auto & lower_arg = arguments[0];
    auto & upper_arg = arguments[1];
    auto & expression = arguments[2];

    if (lower_arg.store_method != StoreMethod::CONST_SCALAR || upper_arg.store_method != StoreMethod::CONST_SCALAR)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Function 'histogram_fraction' currently requires constant lower and upper parameters");
    }

    expression = toVectorGrid(std::move(expression), context);

    if (expression.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    const Float64 lower = lower_arg.scalar_value;
    const Float64 upper = upper_arg.scalar_value;
    auto build_fraction_expression = [lower, upper](ASTPtr le_array_expr)
    {
        return addParametersToAggregateFunction(
            makeASTFunction("fractionPrometheusHistogramForEach",
                std::move(le_array_expr),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            make_intrusive<ASTLiteral>(lower),
            make_intrusive<ASTLiteral>(upper));
    };

    return applyHistogramFunction(function_node, std::move(expression), context, std::move(build_fraction_expression));
}

}
