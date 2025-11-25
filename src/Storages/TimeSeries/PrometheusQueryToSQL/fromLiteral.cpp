#include <Storages/TimeSeries/PrometheusQueryToSQL/fromLiteral.h>

#include <Core/DecimalFunctions.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece fromLiteral(const PQT::StringLiteral * string_node, ConverterContext & context)
{
    SQLQueryPiece res{string_node, ResultType::STRING, StoreMethod::CONST_STRING};
    res.string_value = string_node->string;

    auto evaluation_range = context.node_evaluation_range_getter.get(string_node);
    res.start_time = evaluation_range.start_time;
    res.end_time = evaluation_range.end_time;
    res.step = evaluation_range.step;

    return res;
}


SQLQueryPiece fromLiteral(const PQT::ScalarLiteral * scalar_node, ConverterContext & context)
{
    SQLQueryPiece res{scalar_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
    res.scalar_value = scalar_node->scalar;

    auto evaluation_range = context.node_evaluation_range_getter.get(scalar_node);
    res.start_time = evaluation_range.start_time;
    res.end_time = evaluation_range.end_time;
    res.step = evaluation_range.step;

    return res;
}


SQLQueryPiece fromLiteral(const PQT::Duration * duration_node, ConverterContext & context)
{
    SQLQueryPiece res{duration_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
    const auto & duration = duration_node->duration;
    res.scalar_value = DecimalUtils::convertTo<Float64>(duration.getValue(), duration.getScale());

    auto evaluation_range = context.node_evaluation_range_getter.get(duration_node);
    res.start_time = evaluation_range.start_time;
    res.end_time = evaluation_range.end_time;
    res.step = evaluation_range.step;

    return res;
}

}
