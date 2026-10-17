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
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    std::optional<Float64> estimateLess(const Field & val) const override;
    std::optional<Float64> estimateEqual(const Field & val) const override;
    /// Whether every one of `distinct_values` values has its own centroid, so that `estimateEqual` counts its rows
    /// exactly. Centroids of neighbor values merge even when there are fewer values than centroids allowed.
    bool hasCentroidPerValue(UInt64 distinct_values) const;

    String getNameForLogs() const override { return "TDigest"; }
    size_t allocatedBytes() const override;

private:
    QuantileTDigest<Float64> t_digest;
    DataTypePtr data_type;
};

bool tdigestStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
