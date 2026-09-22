#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

const DataTypePtr & TimeSeriesHistogramsColumns::getDataType(TimeSeriesHistogramsColumn column)
{
    static const auto data_types = []
    {
        std::array<DataTypePtr, getAll().size()> result;
        for (auto payload_column : getAll())
            result[static_cast<size_t>(payload_column)] = DataTypeFactory::instance().get(String{getType(payload_column)});
        return result;
    }();
    return data_types[static_cast<size_t>(column)];
}

String TimeSeriesHistogramsColumns::getOuterColumnName(std::string_view inner_column_name)
{
    return fmt::format("{}.{}", TimeSeriesColumnNames::Histograms, inner_column_name);
}

std::string_view TimeSeriesHistogramsColumns::getInnerColumnName(std::string_view outer_column_name)
{
    std::string_view prefix = TimeSeriesColumnNames::Histograms;
    if ((outer_column_name.size() <= prefix.size() + 1) || !outer_column_name.starts_with(prefix) || (outer_column_name[prefix.size()] != '.'))
        return {};
    return outer_column_name.substr(prefix.size() + 1);
}

NameAndTypePair TimeSeriesHistogramsColumns::getOuterTimestampColumn(const DataTypePtr & timestamp_type)
{
    return {getOuterColumnName(TimeSeriesColumnNames::Timestamp), std::make_shared<DataTypeArray>(timestamp_type)};
}

const NamesAndTypesList & TimeSeriesHistogramsColumns::getOuterPayloadColumns()
{
    static const NamesAndTypesList columns = []
    {
        NamesAndTypesList result;
        for (auto column : getAll())
            result.emplace_back(getOuterColumnName(getName(column)), std::make_shared<DataTypeArray>(getDataType(column)));
        return result;
    }();
    return columns;
}

}
