#pragma once

#include <Core/Field.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB
{
    class ASTFunction;
}


namespace DB::PrometheusQueryToSQL
{

/// Adds four parameters to an aggregate function like timeSeriesRateToGrid().
std::shared_ptr<ASTFunction> addParametersToAggregateFunction(
    std::shared_ptr<ASTFunction> && function,
    DecimalField<DateTime64> start_time,
    DecimalField<DateTime64> end_time,
    DecimalField<Decimal64> step,
    DecimalField<Decimal64> window);

/// Adds a string parameter to aggregate function timeSeriesCoalesceGridValues().
std::shared_ptr<ASTFunction> addParameterToAggregateFunction(std::shared_ptr<ASTFunction> && function, const String & parameter);

}
