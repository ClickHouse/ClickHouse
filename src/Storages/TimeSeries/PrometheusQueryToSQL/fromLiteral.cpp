#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromLiteral(const PQT::Scalar * scalar_node, ConverterContext & context)
{
    SQLQueryPiece res{scalar_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
    res.scalar_value = scalar_node->scalar;

    auto node_range = context.node_range_getter.get(scalar_node);
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    return res;
}


SQLQueryPiece fromLiteral(const PQT::StringLiteral * string_node, ConverterContext & context)
{
    SQLQueryPiece res{string_node, ResultType::STRING, StoreMethod::CONST_STRING};
    res.string_value = string_node->string;

    auto node_range = context.node_range_getter.get(string_node);
    res.start_time = node_range.start_time;
    res.end_time = node_range.end_time;
    res.step = node_range.step;

    return res;
}

}
