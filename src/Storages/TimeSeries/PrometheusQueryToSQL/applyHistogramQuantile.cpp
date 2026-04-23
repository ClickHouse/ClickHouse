#include <Storages/TimeSeries/PrometheusQueryToSQL/applyHistogramQuantile.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

bool isHistogramQuantile(std::string_view function_name)
{
    return function_name == "histogram_quantile";
}

SQLQueryPiece applyHistogramQuantile(
    const PQT::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    if (arguments.size() != 2)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Function 'histogram_quantile' expects 2 arguments, but got {}",
            arguments.size());
    }

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

        /// quantilePrometheusHistogramForEach(phi)(le_array, values)
        ///
        /// le_array is constructed for each row as an array of the same length as values,
        /// filled with the extracted 'le' tag value (or NaN if missing).
        auto le_array_expr = makeASTFunction(
            "arrayResize",
            makeASTFunction("CAST",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>("Array(Float64)")),
            makeASTFunction("length", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            makeASTFunction("toFloat64",
                makeASTFunction("ifNull",
                    makeASTFunction("timeSeriesExtractTag",
                        make_intrusive<ASTIdentifier>(ColumnNames::Group),
                        make_intrusive<ASTLiteral>("le")),
                    make_intrusive<ASTLiteral>("NaN"))));

        auto quantile_expr = addParametersToAggregateFunction(
            makeASTFunction("quantilePrometheusHistogramForEach",
                std::move(le_array_expr),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            make_intrusive<ASTLiteral>(phi));

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
