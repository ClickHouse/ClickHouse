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
}


bool isHistogramQuantile(std::string_view function_name)
{
    return function_name == "histogram_quantile";
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

    Float64 phi = phi_arg.scalar_value;

    ASTPtr native_query;

    if (expression.store_method == StoreMethod::HISTOGRAM_GRID)
    {
        /// One grid feeds both branches: the native one reads `histogram_values`/`sample_kinds`, the classic one only the float `values`.
        /// Both read the same subquery, so it must be materialized (see SQLSubqueryType::MATERIALIZED_TABLE).
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::MATERIALIZED_TABLE});
        const String & grid_name = context.subqueries.back().name;

        /// The native branch: per-step `timeSeriesHistogramQuantile` over the grid's histogram samples. Steps whose newest sample
        /// is a float stay NULL (Prometheus skips floats), and groups with no histogram sample in range are dropped.
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.back()->setAlias(ColumnNames::NewGroup);

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTLambda({"x", "k"}, makeASTFunction(
                    "if",
                    makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), make_intrusive<ASTLiteral>(UInt64{1})),
                    makeASTFunction("timeSeriesHistogramQuantile", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(phi)),
                    make_intrusive<ASTLiteral>(Field{}))),
                make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues),
                make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds)));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            builder.from_table = grid_name;

            builder.where = makeASTFunction("has",
                make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds),
                make_intrusive<ASTLiteral>(UInt64{1}));

            native_query = builder.getSelectQuery();
        }

        /// The float view of the same grid for the classic branch (same as dropHistogramValues):
        /// SELECT group, values FROM <histogram_grid>
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            builder.from_table = grid_name;

            expression.select_query = builder.getSelectQuery();
            expression.store_method = StoreMethod::VECTOR_GRID;
        }
    }
    else
    {
        expression = toVectorGrid(std::move(expression), context);
    }

    if (expression.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

    /// Step 1: Extract `le` tags, group by non-`le` labels (keeping `__name__` so distinct histograms stay separate),
    /// and compute the quantile per group over the grid.
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        /// Remove only the `le` tag; `__name__` stays in the group key so multiple `*_bucket` metrics keep separate quantiles.
        /// `dropMetricName` strips it afterwards, enforcing the no-duplicate-labelset rule via `timeSeriesThrowDuplicateSeriesIf`.
        auto new_group_expr = makeASTFunction(
            "timeSeriesRemoveTag",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            make_intrusive<ASTLiteral>("le"));
        new_group_expr->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(std::move(new_group_expr));

        /// Out-of-range phi short-circuits before looking at the histogram: phi < 0 -> -Inf, phi > 1 -> +Inf, NaN -> NaN, for every time step.
        /// Emit a constant-valued array aligned to the time grid (NULL where no input existed) instead of calling `quantilePrometheusHistogramForEach`.
        ASTPtr quantile_expr;
        if (std::isnan(phi) || phi < 0.0 || phi > 1.0)
        {
            Float64 out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            if (std::isnan(phi))
                out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            else if (phi < 0.0)
                out_of_range_value = -std::numeric_limits<Float64>::infinity();
            else
                out_of_range_value = std::numeric_limits<Float64>::infinity();

            /// `anyForEach` yields one grid-aligned array per group (NULL where no series had data);
            /// `arrayMap` replaces every non-NULL position with the constant.
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
            /// le_array repeats the extracted `le` tag value for each time step; the WHERE below excludes series with unparsable `le`,
            /// so the NaN fallback is only a safety net (`quantilePrometheusHistogramForEach` treats NaN `le` as "ignore this bucket").
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

        /// Prometheus drops series whose `le` is missing or unparsable, so a non-histogram input yields an empty result.
        /// This filter applies before the out-of-range phi short-circuit above.
        builder.where = makeASTFunction("isNotNull",
            makeASTFunction("toFloat64OrNull",
                makeASTFunction("timeSeriesExtractTag",
                    make_intrusive<ASTIdentifier>(ColumnNames::Group),
                    make_intrusive<ASTLiteral>("le"))));

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: Rename new_group -> group. With a native branch, UNION ALL its rows with the
    /// classic branch's rows (both arms select `new_group AS group, values`).
    ASTPtr column_renaming_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        if (native_query)
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(native_query), SQLSubqueryType::TABLE});
            builder.union_table = context.subqueries.back().name;
        }

        column_renaming_query = builder.getSelectQuery();
    }

    SQLQueryPiece res{function_node, function_node->result_type, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(column_renaming_query);
    res.start_time = expression.start_time;
    res.end_time = expression.end_time;
    res.step = expression.step;
    res.metric_name_dropped = false;

    /// Drop `__name__` from the result (PromQL: function outputs have no metric name);
    /// `dropMetricName` also enforces uniqueness via `timeSeriesThrowDuplicateSeriesIf`.
    return dropMetricName(std::move(res), context);
}

}
