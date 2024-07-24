#include <Storages/Statistics/StatisticsMinMax.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

StatisticsMinMax::StatisticsMinMax(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type_)
    : IStatistics(stat_)
    , min(std::numeric_limits<Float64>::max())
    , max(std::numeric_limits<Float64>::min())
    , row_count(0)
    , data_type(data_type_)
{
}

Float64 StatisticsMinMax::estimateLess(const Field & val) const
{
    Field val_converted = convertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    auto val_float = applyVisitor(FieldVisitorConvertToNumber<Float64>(), val_converted);

    if (val_float < min)
        return 0;

    if (val_float > max)
        return row_count;

    if (max == min)
        return row_count;

    return ((val_float - min) / (max - min)) * row_count;
}

void StatisticsMinMax::update(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        auto data = column->getFloat64(row);
        min = std::min(data, min);
        max = std::max(data, max);
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


void minMaxValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'min_max' do not support type {}", data_type->getName());
}

StatisticsPtr minMaxCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<StatisticsMinMax>(stat, data_type);
}

}
