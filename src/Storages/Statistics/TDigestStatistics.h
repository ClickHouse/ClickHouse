#pragma once

#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/QuantileTDigest.h>

namespace DB
{


/// TDigestStatistic is a kind of histogram.
class TDigestStatistics : public IStatistics
{
public:
    explicit TDigestStatistics(const SingleStatisticsDescription & stat_);

    Float64 estimateLess(Float64 val) const;

    Float64 estimateEqual(Float64 val) const;

    void serialize(WriteBuffer & buf) override;

    void deserialize(ReadBuffer & buf) override;

    void update(const ColumnPtr & column) override;
private:
    QuantileTDigest<Float64> data;
};

StatisticsPtr TDigestCreator(const SingleStatisticsDescription & stat, DataTypePtr);
void TDigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type);

}
