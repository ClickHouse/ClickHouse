#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultSortingByDefault.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ResultSorting.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ResultSorting getResultSortingByDefault(const PrometheusQueryTree & promql_tree,
                                        const PrometheusQueryEvaluationSettings & settings)
{
    ResultSorting result_sorting;

    auto result_type = getResultType(promql_tree, settings);
    if (result_type == ResultType::RANGE_VECTOR)
    {
        /// Data from range queries comes sorted alphabetically by tags.
        result_sorting.mode = ResultSorting::Mode::ORDERED_BY_TAGS;
        result_sorting.direction = 1;
    }

    /// Data from instant queries comes in an arbitrary order (unless a function like sort() or sort_by_labels() is used).

    return result_sorting;
}

}
