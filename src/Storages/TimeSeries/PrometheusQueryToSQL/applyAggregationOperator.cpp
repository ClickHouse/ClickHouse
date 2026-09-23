#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperator.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentAggregationOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLimitAggregationOperator.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;

    if (isOneArgumentAggregationOperator(operator_name))
        return applyOneArgumentAggregationOperator(operator_node, std::move(arguments), context);

    if (isAggregationOperatorQuantile(operator_name))
        return applyAggregationOperatorQuantile(operator_node, std::move(arguments), context);

    if (isLimitAggregationOperator(operator_name))
        return applyLimitAggregationOperator(operator_node, std::move(arguments), context);

    throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                    "Aggregation operator '{}' is not implemented", operator_name);
}

}
