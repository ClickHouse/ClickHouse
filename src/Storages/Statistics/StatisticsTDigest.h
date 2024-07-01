#pragma once

#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/QuantileTDigest.h>

namespace DB
{

class StatisticsTDigest : public IStatistics
{
public:
    explicit StatisticsTDigest(const SingleStatisticsDescription & stat_);

    StatisticsType getType() const override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    std::optional<Float64> estimateEqual(Float64 val) const override; /// (*)
    std::optional<Float64> estimateLess(Float64 val) const override;

    /// (*) TDigests are supposed to estimate quantiles. Equality estimations are also available but they are implemented in a
    ///     surprising way. They return an estimation only if the provided value matches a centroid's mean exactly. Use with care!

private:
    QuantileTDigest<Float64> t_digest;
};

void TDigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr TDigestCreator(const SingleStatisticsDescription & stat, DataTypePtr);

}
