#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::PrometheusQueryToSQL
{

String getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    return query_piece.node->toString(*context.promql_tree);
}

}
