#pragma once

#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/QuantileTDigest.h>

namespace DB
{

class StatisticsTDigest : public IStatistics
{
public:
    explicit StatisticsTDigest(const SingleStatisticsDescription & stat_);

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    Float64 estimateLess(Float64 val) const override;
    Float64 estimateEqual(Float64 val) const override;

private:
    QuantileTDigest<Float64> t_digest;
};

void TDigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr TDigestCreator(const SingleStatisticsDescription & stat, DataTypePtr);

}
