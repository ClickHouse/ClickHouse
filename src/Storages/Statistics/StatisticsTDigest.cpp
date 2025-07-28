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

Float64 StatisticsTDigest::estimateLess(const Field & val) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!val_as_float.has_value())
        return 0;
    return t_digest.getCountLessThan(*val_as_float);
}

Float64 StatisticsTDigest::estimateEqual(const Field & val) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
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
