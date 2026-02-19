#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <algorithm>


namespace DB
{

StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
{
}

void StatisticsMinMax::build(const ColumnPtr & column)
{
    Field min_field;
    Field max_field;

    column->getExtremes(min_field, max_field);

    if (!min_field.isNull())
    {
        Float64 current_min = applyVisitor(FieldVisitorConvertToNumber<Float64>(), min_field);
        min = std::min(min, current_min);
    }

    if (!max_field.isNull())
    {
        Float64 current_max = applyVisitor(FieldVisitorConvertToNumber<Float64>(), max_field);
        max = std::max(max, current_max);
    }

    row_count += column->size();
}

void StatisticsMinMax::merge(const StatisticsPtr & other_stats)
{
    const StatisticsMinMax * other = typeid_cast<const StatisticsMinMax *>(other_stats.get());
    min = std::min(min, other->min);
    max = std::max(max, other->max);
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

Float64 StatisticsMinMax::estimateLess(const Field & val) const
{
    if (row_count == 0)
        return 0;

    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!val_as_float.has_value())
        return 0;

    if (val_as_float < min)
        return 0;

    if (val_as_float > max)
        return row_count;

    if (min == max)
        return (val_as_float != max) ? 0 : row_count;

    return ((*val_as_float - min) / (max - min)) * row_count;
}

bool minMaxStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    auto inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber();
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsMinMax>(description, data_type);
}

}
