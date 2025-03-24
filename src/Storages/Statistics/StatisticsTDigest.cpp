#include <Storages/Statistics/StatisticsTDigest.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

StatisticsTDigest::StatisticsTDigest(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(data_type_))
{
}

void StatisticsTDigest::build(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        auto data = column->getFloat64(row);
        t_digest.add(data, 1);
    }
}

void StatisticsTDigest::serialize(WriteBuffer & buf)
{
    t_digest.serialize(buf);
}

void StatisticsTDigest::deserialize(ReadBuffer & buf)
{
    t_digest.deserialize(buf);
}

Float64 StatisticsTDigest::estimateLess(const Field & val, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    val_as_float_to_return = val_as_float;
    if (!val_as_float.has_value())
        return 0;
    Float64 count_less_than_val = t_digest.getCountLessThan((right_bound.has_value() && right_bound.value() < val_as_float.value()) ? *right_bound : *val_as_float);
    Float64 count_less_than_left_bound = left_bound.has_value() ? t_digest.getCountLessThan(*left_bound) : 0;
    return std::max(count_less_than_val - count_less_than_left_bound, 0.0);
}

Float64 StatisticsTDigest::estimateEqual(const Field & val, std::optional<Float64> & val_as_float_to_return) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    val_as_float_to_return = val_as_float;
    if (!val_as_float.has_value())
        return 0;
    return t_digest.getCountEqual(*val_as_float);
}

void tdigestStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    if (!inner_data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'tdigest' do not support type {}", data_type->getName());
}

StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsTDigest>(description, data_type);
}

}
