#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if a prometheus query allows evaluating over a range and throws an exception if not.
    /// If a query normally returns a scalar or an instant vector then it can be evaluated over a range
    /// and produce a range vector as a result.
    void checkPrometheusQueryAllowsEvaluationRange(const PQT & promql_tree)
    {
        if ((promql_tree.getResultType() == ResultType::SCALAR)
            || (promql_tree.getResultType() == ResultType::INSTANT_VECTOR))
            return;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid expression type {} for range query, must be {} or {}",
            promql_tree.getResultType(),
            ResultType::SCALAR,
            ResultType::INSTANT_VECTOR);
    }
}


ResultType getResultType(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    if (settings.mode == PrometheusQueryEvaluationMode::QUERY_RANGE)
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        return ResultType::RANGE_VECTOR;
    }
    else
    {
        return promql_tree.getResultType();
    }
}

}
