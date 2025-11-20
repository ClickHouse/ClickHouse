#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void addParameterToAggregateFunction(ASTFunction & function, const String & parameter)
    {
        if (!function.parameters)
        {
            function.parameters = std::make_shared<ASTExpressionList>();
            function.children.push_back(function.parameters);
        }
        function.parameters->children.push_back(std::make_shared<ASTLiteral>(parameter));
    }
}

SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.metric_name_dropped)
        return std::move(query_piece);

    switch (query_piece.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SCALAR_GRID:
        {
            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT timeSeriesRemoveTag(group, '__name__') AS group,
            ///        timeSeriesCoalesceGridValues('throw_if_conflict')(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY group
            SelectQueryParams params;

            params.select_list.push_back(makeASTFunction(
                "timeSeriesRemoveTag",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                std::make_shared<ASTLiteral>("__name__")));
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);

            auto coalesce_function = makeASTFunction("timeSeriesCoalesceGridValues",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values));
            addParameterToAggregateFunction(*coalesce_function, "throw_if_conflict");

            params.select_list.push_back(std::move(coalesce_function));
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            params.from_subquery = context.subqueries.back().name;

            params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

            query_piece.select_query = buildSelectQuery(std::move(params));

            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::RAW_DATA:
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Cannot drop metric name for expression {} because it's a range vector (store_method: {})",
                            getPromQLQuery(query_piece, context), query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
