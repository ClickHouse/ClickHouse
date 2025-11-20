#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

std::string_view getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    return context.promql_tree.getQuery(query_piece.node);
}


void checkStartTimeEqualsToEndTime(const SQLQueryPiece & result, const ConverterContext & context)
{
    if (result.start_time != result.end_time)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Expression {} has type {} and cannot be evaluated on range {}..{}",
            getPromQLQuery(result, context), Field{result.type}, Field{result.start_time}, Field{result.end_time});
    }
}

}
