#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <Storages/TimeSeries/getPromQLResultTypes.h>


namespace DB::PrometheusQueryToSQL
{

ColumnsDescription getResultColumns(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    auto result_type = getResultType(promql_tree, settings);
    auto timestamp_type = getPromQLResultTimestampType(settings.table_timestamp_type);
    auto value_type = getPromQLResultValueType();

    ColumnsDescription columns;

    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, value_type});
            return columns;
        }

        case ResultType::STRING:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, std::make_shared<DataTypeString>()});
            return columns;
        }

        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    ColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, value_type});
            return columns;
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
                    ColumnNames::getOuterSamples(settings.time_series_version),
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}))});
            return columns;
        }
    }

    UNREACHABLE();
}

}
