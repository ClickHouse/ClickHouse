#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

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
                std::make_shared<ASTIdentifier>(ColumnNames::Group),
                std::make_shared<ASTLiteral>(kMetricName)));
            params.select_list.back()->setAlias(ColumnNames::Group);

            auto coalesce_function = addParametersToAggregateFunction(
                makeASTFunction(
                    "timeSeriesCoalesceGridValues",
                    std::make_shared<ASTIdentifier>(ColumnNames::Values),
                    std::make_shared<ASTIdentifier>(ColumnNames::Group)),
                std::make_shared<ASTLiteral>("throw"));

            params.select_list.push_back(std::move(coalesce_function));
            params.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            params.from_table = context.subqueries.back().name;

            params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            query_piece.select_query = buildSelectQuery(std::move(params));

            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::RAW_DATA:
        {
            /// dropMetricName() must not be called with StoreMethod::RAW_DATA.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot drop the metric name from the result of expression {} because of its store method {}",
                            getPromQLQuery(query_piece, context), query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
