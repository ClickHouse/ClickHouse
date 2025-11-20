#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if a prometheus query allows evaluating over a range and throws an exception if not.
    void checkPrometheusQueryAllowsEvaluationRange(const PrometheusQueryTree & promql_tree)
    {
        if ((promql_tree.getResultType() == PrometheusQueryResultType::SCALAR)
            || (promql_tree.getResultType() == PrometheusQueryResultType::INSTANT_VECTOR))
            return;

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression type {} for range query, must be {} or {}",
                        promql_tree.getResultType(),
                        PrometheusQueryResultType::SCALAR,
                        PrometheusQueryResultType::INSTANT_VECTOR);
    }
}


PrometheusQueryResultType getResultType(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    if (settings.evaluation_time)
    {
        return promql_tree.getResultType();
    }
    else if (settings.evaluation_range)
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        return PrometheusQueryResultType::RANGE_VECTOR;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");
    }
}


ColumnsDescription getResultColumns(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    ColumnsDescription columns;

    auto result_type = getResultType(promql_tree, settings);
    switch (result_type)
    {
        case PrometheusQueryResultType::SCALAR:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Value, std::make_shared<DataTypeFloat64>()});
            break;
        }
        case PrometheusQueryResultType::STRING:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Value, std::make_shared<DataTypeString>()});
            break;
        }
        case PrometheusQueryResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Value, std::make_shared<DataTypeFloat64>()});
            break;
        }
        case PrometheusQueryResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(
                        std::make_shared<DataTypeTuple>(DataTypes{settings.result_timestamp_type, std::make_shared<DataTypeFloat64>()}))});
            break;
        }
    }
    return columns;
}

}
