#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

#define MY_LOG(...) LOG_DEBUG(getLogger("Alex Statistics MinMax"), __VA_ARGS__)

StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
{
}

void StatisticsMinMax::build(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        auto value = column->getFloat64(row);
        min = std::min(value, min);
        max = std::max(value, max);
    }
    row_count += column->size();
}

void StatisticsMinMax::serialize(WriteBuffer & buf)
{
    writeIntBinary(row_count, buf);
    writeFloatBinary(min, buf);
    writeFloatBinary(max, buf);
}

void StatisticsMinMax::deserialize(ReadBuffer & buf)
{
    readIntBinary(row_count, buf);
    readFloatBinary(min, buf);
    readFloatBinary(max, buf);
}

Float64 StatisticsMinMax::estimateLess(const Field & val, std::optional<Float64> * calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    Float64 used_min = custom_min.has_value() ? *custom_min : min;
    Float64 used_max = custom_max.has_value() ? *custom_max : max;
    MY_LOG("estimateLess started, min - {}, max - {}, used_min - {}, used_max - {}, row_count - {}", min, max, used_min, used_max, row_count);
    if (row_count == 0)
        return 0;

    MY_LOG("estimateLess 1");
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    MY_LOG("estimateLess 2, val as float - {}", val_as_float.has_value() ? *val_as_float : -1.234);
    if (calculated_val)
        *calculated_val = val_as_float.has_value() ? *val_as_float : 0.0;
    if (!val_as_float.has_value())
        return 0;

    MY_LOG("estimateLess 3");
    if (val_as_float < used_min)
        return 0;

    MY_LOG("estimateLess 4");
    if (val_as_float > used_max)
        return row_count;

    MY_LOG("estimateLess 5");
    if (used_min == used_max)
        return (val_as_float != used_max) ? 0 : row_count;

    MY_LOG("estimateLess 6");
    auto to_return = ((*val_as_float - used_min) / (used_max - used_min)) * row_count;
    MY_LOG("estimateLess 7, final result - {}", to_return);
    return to_return;
}

void minMaxStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    auto inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    if (!inner_data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'minmax' do not support type {}", data_type->getName());
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsMinMax>(description, data_type);
}

}
