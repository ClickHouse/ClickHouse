#include <Storages/TimeSeries/PrometheusQueryToSQL/applyHistogramQuantile.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
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
        const PQT::Function * function_node,
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
}


bool isHistogramQuantile(std::string_view function_name)
{
    return function_name == "histogram_quantile";
}

SQLQueryPiece applyHistogramQuantile(
    const PQT::Function * function_node,
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

    /// Step 1: Extract le tags, group by non-le labels, and compute quantile.
    /// SELECT timeSeriesRemoveTags(group, ['le', '__name__']) AS new_group,
    ///        quantilePrometheusHistogramForEach(phi)(
    ///            arrayResize(CAST([] AS Array(Float64)), length(values),
    ///                toFloat64(ifNull(timeSeriesExtractTag(group, 'le'), 'NaN'))),
    ///            values) AS values
    /// FROM <subquery>
    /// GROUP BY new_group
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        /// new_group expression: remove 'le' and '__name__' tags
        auto new_group_expr = makeASTFunction(
            "timeSeriesRemoveTags",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            make_intrusive<ASTLiteral>(Array{"le", kMetricName}));
        new_group_expr->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(std::move(new_group_expr));

        /// PromQL `histogram_quantile` has constant out-of-range semantics:
        ///   phi < 0  -> -Inf for every time step
        ///   phi > 1  -> +Inf for every time step
        ///   phi NaN  -> NaN for every time step
        /// These short-circuits happen before looking at the histogram, so instead of
        /// calling quantilePrometheusHistogramForEach we emit a constant-valued array
        /// aligned to the time grid (preserving NULL at positions where no input existed).
        ASTPtr quantile_expr;
        if (std::isnan(phi) || phi < 0.0 || phi > 1.0)
        {
            Float64 out_of_range_value;
            if (std::isnan(phi))
                out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            else if (phi < 0.0)
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
        else
        {
            /// quantilePrometheusHistogramForEach(phi)(le_array, values)
            ///
            /// le_array is constructed for each row as an array of the same length as values,
            /// filled with the extracted `le` tag value (or NaN if the tag is missing or cannot
            /// be parsed as a float). Prometheus silently drops bucket series with unparseable
            /// `le` from the histogram rather than failing the query, so we match that by mapping
            /// any non-numeric `le` to NaN — `quantilePrometheusHistogramForEach` already treats
            /// NaN `le` as "ignore this bucket".
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

            quantile_expr = addParametersToAggregateFunction(
                makeASTFunction("quantilePrometheusHistogramForEach",
                    std::move(le_array_expr),
                    make_intrusive<ASTIdentifier>(ColumnNames::Values)),
                make_intrusive<ASTLiteral>(phi));
        }

        quantile_expr->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(quantile_expr));

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: Rename new_group -> group.
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
    res.metric_name_dropped = true;

    return res;
}

}
