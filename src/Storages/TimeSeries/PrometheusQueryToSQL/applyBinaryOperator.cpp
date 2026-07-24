#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperator.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyBinaryOperator(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context)
{
    std::string_view operator_name = operator_node->operator_name;

    if (isMathBinaryOperator(operator_name))
        return applyMathBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context);

    if (isComparisonOperator(operator_name))
        return applyComparisonOperator(operator_node, std::move(left_argument), std::move(right_argument), context);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", operator_name);
}

}
