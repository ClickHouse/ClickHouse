#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

void registerAggregateFunctionTimeseriesExtrapolatedValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesInstantValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesLinearRegression(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesChanges(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesLastToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesFirstToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesCompensatedSum(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMax(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMin(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesPresentToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesQuantileToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionLast2Samples(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesTopKMasks(AggregateFunctionFactory & factory);

void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory)
{
    registerAggregateFunctionTimeseriesExtrapolatedValue(factory);
    registerAggregateFunctionTimeseriesInstantValue(factory);
    registerAggregateFunctionTimeseriesLinearRegression(factory);
    registerAggregateFunctionTimeseriesChanges(factory);
    registerAggregateFunctionTimeseriesLastToGrid(factory);
    registerAggregateFunctionTimeseriesFirstToGrid(factory);
    registerAggregateFunctionTimeseriesCompensatedSum(factory);
    registerAggregateFunctionTimeseriesCount(factory);
    registerAggregateFunctionTimeseriesMax(factory);
    registerAggregateFunctionTimeseriesMin(factory);
    registerAggregateFunctionTimeseriesPresentToGrid(factory);
    registerAggregateFunctionTimeseriesQuantileToGrid(factory);
    registerAggregateFunctionLast2Samples(factory);
    registerAggregateFunctionTimeseriesGroupArray(factory);
    registerAggregateFunctionTimeSeriesTopKMasks(factory);
}

}
