#pragma once

#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/QuantileTDigest.h>

namespace DB
{

class StatisticsTDigest : public IStatistics
{
public:
    explicit StatisticsTDigest(const SingleStatisticsDescription & description, const DataTypePtr & data_type_);

    void build(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    Float64 estimateLess(const Field & val) const override;
    Float64 estimateEqual(const Field & val) const override;

private:
    QuantileTDigest<Float64> t_digest;
    DataTypePtr data_type;
};

void tdigestStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
