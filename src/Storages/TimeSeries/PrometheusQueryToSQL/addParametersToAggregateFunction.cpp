#include <Storages/TimeSeries/PrometheusQueryToSQL/addParametersToAggregateFunction.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

std::shared_ptr<ASTFunction> addParametersToAggregateFunction(
    std::shared_ptr<ASTFunction> && function,
    DecimalField<DateTime64> start_time,
    DecimalField<DateTime64> end_time,
    DecimalField<Decimal64> step,
    DecimalField<Decimal64> window)
{
    if (!function->parameters)
    {
        function->parameters = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->parameters);
    }
    function->parameters->children.push_back(timeseriesTimeToAST(start_time));
    function->parameters->children.push_back(timeseriesTimeToAST(end_time));
    function->parameters->children.push_back(timeseriesDurationToAST(step));
    function->parameters->children.push_back(timeseriesDurationToAST(window));
    return std::move(function);
}


std::shared_ptr<ASTFunction> addParameterToAggregateFunction(std::shared_ptr<ASTFunction> && function, const String & parameter)
{
    if (!function->parameters)
    {
        function->parameters = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->parameters);
    }
    function->parameters->children.push_back(std::make_shared<ASTLiteral>(parameter));
    return std::move(function);
}

}
