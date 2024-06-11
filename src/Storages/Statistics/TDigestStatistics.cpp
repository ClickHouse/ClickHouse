#include <Storages/Statistics/TDigestStatistics.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_STATISTICS;
}

TDigestStatistics::TDigestStatistics(const SingleStatisticsDescription & stat_):
    IStatistics(stat_)
{
}

Float64 TDigestStatistics::estimateLess(Float64 val) const
{
    return data.getCountLessThan(val);
}

Float64 TDigestStatistics::estimateEqual(Float64 val) const
{
    return data.getCountEqual(val);
}

void TDigestStatistics::serialize(WriteBuffer & buf)
{
    data.serialize(buf);
}

void TDigestStatistics::deserialize(ReadBuffer & buf)
{
    data.deserialize(buf);
}

void TDigestStatistics::update(const ColumnPtr & column)
{
    size_t size = column->size();

    for (size_t i = 0; i < size; ++i)
    {
        /// TODO: support more types.
        Float64 value = column->getFloat64(i);
        data.add(value, 1);
    }
}

StatisticsPtr TDigestCreator(const SingleStatisticsDescription & stat, DataTypePtr)
{
    return std::make_shared<TDigestStatistics>(stat);
}

void TDigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'tdigest' does not support type {}", data_type->getName());
}

}
