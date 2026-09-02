#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

DataTypePtr getTimeSeriesHistogramSpansType()
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeUInt32>()},
        Names{"offset", "length"}));
}

NamesAndTypes getTimeSeriesHistogramPayloadColumns()
{
    auto float64 = std::make_shared<DataTypeFloat64>();
    auto float64_array = std::make_shared<DataTypeArray>(float64);
    auto uint64 = std::make_shared<DataTypeUInt64>();
    auto uint64_array = std::make_shared<DataTypeArray>(uint64);
    auto spans = getTimeSeriesHistogramSpansType();

    return NamesAndTypes{
        {TimeSeriesColumnNames::Flags, std::make_shared<DataTypeUInt8>()},
        {TimeSeriesColumnNames::Schema, std::make_shared<DataTypeInt8>()},
        {TimeSeriesColumnNames::ZeroThreshold, float64},
        {TimeSeriesColumnNames::Count, float64},
        {TimeSeriesColumnNames::Sum, float64},
        {TimeSeriesColumnNames::ZeroCount, float64},
        {TimeSeriesColumnNames::PositiveSpans, spans},
        {TimeSeriesColumnNames::PositiveValues, float64_array},
        {TimeSeriesColumnNames::NegativeSpans, spans},
        {TimeSeriesColumnNames::NegativeValues, float64_array},
        {TimeSeriesColumnNames::CustomValues, float64_array},
        {TimeSeriesColumnNames::CountInt, uint64},
        {TimeSeriesColumnNames::ZeroCountInt, uint64},
        {TimeSeriesColumnNames::PositiveValuesInt, uint64_array},
        {TimeSeriesColumnNames::NegativeValuesInt, uint64_array},
    };
}

DataTypePtr getTimeSeriesHistogramsOuterColumnType(const DataTypePtr & timestamp_type)
{
    DataTypes element_types;
    Names element_names;
    element_types.push_back(timestamp_type);
    element_names.push_back(TimeSeriesColumnNames::Timestamp);
    for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
    {
        element_types.push_back(type);
        element_names.push_back(name);
    }
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names)));
}

}
