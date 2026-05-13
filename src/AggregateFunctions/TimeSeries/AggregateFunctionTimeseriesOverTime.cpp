#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCreation.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTime.h>


namespace DB
{

void registerAggregateFunctionTimeseriesOverTimeGrid(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesAvgOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesAvgOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesSumOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesSumOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesCountOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesCountOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesStddevOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesStddevPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesStdvarOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesStdvarPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesPresentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesPresentOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesAbsentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesAbsentOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfLastOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfLastOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    /// phi is dimensionless [0, 1] and must NOT be scaled by the timestamp's unit.
    factory.registerFunction("timeSeriesQuantileOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, true, AggregateFunctionTimeseriesQuantileOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMadOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMadOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});
}

}
