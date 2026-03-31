#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCreation.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTime.h>


namespace DB
{

void registerAggregateFunctionTimeseriesOverTimeGrid(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesAvgOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesAvgOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesSumOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesSumOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesCountOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesCountOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesStddevOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesStddevPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesStdvarOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesStdvarPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesPresentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesPresentOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesAbsentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesAbsentOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfLastOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesTsOfLastOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesTsOfFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesTsOfMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesTsOfMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesTsOfMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesQuantileOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, true, AggregateFunctionTimeseriesQuantileOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});

    factory.registerFunction("timeSeriesMadOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesMadOverTimeTraits, AggregateFunctionTimeseriesOverTime>, {}});
}

}
