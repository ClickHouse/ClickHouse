#include <Storages/Statistics/StatisticsTDigest.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

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

void StatisticsTDigest::merge(const StatisticsPtr & other_stats)
{
    const StatisticsTDigest * other = typeid_cast<const StatisticsTDigest*>(other_stats.get());
    t_digest.merge(other->t_digest);
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

bool tdigestStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber() && !isIPv4(inner_data_type);
}

StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsTDigest>(description, data_type);
}

}
