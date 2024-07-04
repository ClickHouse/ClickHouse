#include <Storages/Statistics/StatisticsTDigest.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_STATISTICS;
}

StatisticsTDigest::StatisticsTDigest(const SingleStatisticsDescription & stat_)
    : IStatistics(stat_)
{
}

void StatisticsTDigest::update(const ColumnPtr & column)
{
    size_t rows = column->size();
    for (size_t row = 0; row < rows; ++row)
    {
        Field f;
        column->get(row, f);

        if (f.isNull())
            continue;

        if (auto float_val = IStatistics::getFloat64(f))
            t_digest.add(*float_val, 1);
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

Float64 StatisticsTDigest::estimateLess(Float64 val) const
{
    return t_digest.getCountLessThan(val);
}

Float64 StatisticsTDigest::estimateEqual(const Field & val) const
{
    return t_digest.getCountEqual(IStatistics::getFloat64(val).value());
}

void TDigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'tdigest' do not support type {}", data_type->getName());
}

StatisticsPtr TDigestCreator(const SingleStatisticsDescription & stat, DataTypePtr)
{
    return std::make_shared<StatisticsTDigest>(stat);
}

}
