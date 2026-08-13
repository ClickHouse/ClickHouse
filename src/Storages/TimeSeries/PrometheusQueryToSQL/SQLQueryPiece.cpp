#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

String getPromQLText(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    chassert(query_piece.node);
    return query_piece.node->toString(*context.promql_tree);
}

[[noreturn]] void throwUnexpectedStoreMethod(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expression '{}' (type {}) has unexpected store method {}",
                    getPromQLText(query_piece, context), query_piece.type, query_piece.store_method);
}

}
