#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ColumnsDescription getResultColumns(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    auto result_type = getResultType(promql_tree, settings);
    const auto & timestamp_data_type = settings.timestamp_data_type;
    const auto & scalar_data_type = settings.scalar_data_type;

    ColumnsDescription columns;

    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_data_type});
            columns.add(ColumnDescription{ColumnNames::Value, scalar_data_type});
            return columns;
        }

        case ResultType::STRING:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_data_type});
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
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_data_type});
            columns.add(ColumnDescription{ColumnNames::Value, scalar_data_type});
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
                    ColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_data_type, scalar_data_type}))});
            return columns;
        }
    }

    UNREACHABLE();
}

}
