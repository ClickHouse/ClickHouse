#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyDateTimeFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathSimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionScalar.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionVector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionPi.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    std::string_view function_name = function_node->function_name;

    if (isFunctionVector(function_name))
        return applyFunctionVector(function_node, std::move(arguments), context);

    if (isFunctionScalar(function_name))
        return applyFunctionScalar(function_node, std::move(arguments), context);

    if (isFunctionTime(function_name))
        return fromFunctionTime(function_node, std::move(arguments), context);

    if (isDateTimeFunction(function_name))
        return applyDateTimeFunction(function_node, std::move(arguments), context);

    if (isMathSimpleFunction(function_name))
        return applyMathSimpleFunction(function_node, std::move(arguments), context);

    if (isFunctionPi(function_name))
        return fromFunctionPi(function_node, std::move(arguments), context);

    if (isFunctionOverRange(function_name))
        return applyFunctionOverRange(function_node, std::move(arguments), context);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function_name);
}

}
