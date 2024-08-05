#pragma once

#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/QuantileTDigest.h>

namespace DB
{

class StatisticsTDigest : public IStatistics
{
public:
    explicit StatisticsTDigest(const SingleStatisticsDescription & stat_, DataTypePtr data_type_);

    Float64 estimateLess(const Field & val) const override;
    Float64 estimateEqual(const Field & val) const override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

private:
    QuantileTDigest<Float64> t_digest;
    DataTypePtr data_type;
};

void tdigestStatisticsValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & stat, DataTypePtr);

}
