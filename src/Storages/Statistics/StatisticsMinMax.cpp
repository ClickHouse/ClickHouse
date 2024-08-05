#include <Storages/Statistics/StatisticsMinMax.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & statistics_description, const DataTypePtr & data_type_)
    : IStatistics(statistics_description)
    , data_type(data_type_)
{
}

void StatisticsMinMax::update(const ColumnPtr & column)
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

Float64 StatisticsMinMax::estimateLess(const Field & val) const
{
    Field val_converted = convertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    auto val_as_float = applyVisitor(FieldVisitorConvertToNumber<Float64>(), val_converted);

    if (val_as_float < min)
        return 0;

    if (val_as_float > max)
        return row_count;

    if (min == max)
        return (val_as_float != max) ? 0 : row_count;

    return ((val_as_float - min) / (max - min)) * row_count;
}

void minMaxStatisticsValidator(const SingleStatisticsDescription & /*statistics_description*/, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'minmax' do not support type {}", data_type->getName());
}

StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & statistics_description, DataTypePtr data_type)
{
    return std::make_shared<StatisticsMinMax>(statistics_description, data_type);
}

}
