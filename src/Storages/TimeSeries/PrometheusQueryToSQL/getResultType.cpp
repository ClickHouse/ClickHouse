#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


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
    void checkPrometheusQueryAllowsEvaluationRange(const PQT & promql_tree)
    {
        if ((promql_tree.getResultType() == ResultType::SCALAR)
            || (promql_tree.getResultType() == ResultType::INSTANT_VECTOR))
            return;

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression type {} for range query, must be {} or {}",
                        promql_tree.getResultType(),
                        ResultType::SCALAR,
                        ResultType::INSTANT_VECTOR);
    }
}


ResultType getResultType(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    if (settings.evaluation_time)
    {
        return promql_tree.getResultType();
    }
    else if (settings.evaluation_range)
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        return ResultType::RANGE_VECTOR;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");
    }
}


ColumnsDescription getResultColumns(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    ColumnsDescription columns;

    auto result_type = getResultType(promql_tree, settings);
    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, std::make_shared<DataTypeFloat64>()});
            break;
        }
        case ResultType::STRING:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, std::make_shared<DataTypeString>()});
            break;
        }
        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    ColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(ColumnDescription{ColumnNames::Timestamp, settings.result_timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, std::make_shared<DataTypeFloat64>()});
            break;
        }
        case ResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    ColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    ColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(
                        std::make_shared<DataTypeTuple>(DataTypes{settings.result_timestamp_type, std::make_shared<DataTypeFloat64>()}))});
            break;
        }
    }
    return columns;
}

}
