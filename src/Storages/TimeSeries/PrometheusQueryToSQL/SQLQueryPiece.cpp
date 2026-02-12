#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::PrometheusQueryToSQL
{

std::string_view getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    return context.promql_tree->getQuery(query_piece.node);
}

}
