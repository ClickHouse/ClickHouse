#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include <base/types.h>

namespace DB
{

/*
Sketch for finding fraction of granules that can contain values between.
Must be more efficient for PREWHERE optimization than simple tdigest (MergeTreeColumnDistributionStatisticTDigest)
due to the fact that granule is the smallest thing that can be read from disk by ClickHouse.
It stores separately minimums and maximums per block.
Minimums are used for lowerbound estimation and maximums for upperbound.
This estimates are approximate, so they can not be used for data skipping.
*/
class MergeTreeGranuleDistributionStatisticTDigest : public IDistributionStatistic
{
public:
    explicit MergeTreeGranuleDistributionStatisticTDigest(
        const String & name_,
        const String & column_name_);
    MergeTreeGranuleDistributionStatisticTDigest(
        QuantileTDigest<Float32>&& min_sketch_,
        QuantileTDigest<Float32>&& max_sketch_,
        const String & column_name_);

    const String& name() const override;
    const String& type() const override;
    bool empty() const override;
    void merge(const IStatisticPtr & other) override;

    const String& getColumnsRequiredForStatisticCalculation() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    double estimateQuantileLower(const Field& value) const override;
    double estimateQuantileUpper(const Field& value) const override;
    double estimateProbability(const Field& lower, const Field& upper) const override;

private:
    const String stat_name;
    const String column_name;
    mutable QuantileTDigest<Float32> min_sketch;
    mutable QuantileTDigest<Float32> max_sketch;
    bool is_empty;
};

class MergeTreeGranuleDistributionStatisticCollectorTDigest : public IMergeTreeDistributionStatisticCollector
{
public:
    explicit MergeTreeGranuleDistributionStatisticCollectorTDigest(
        const String & name_,
        const String & column_name_);

    const String& name() const override;
    const String & type() const override;
    const String & column() const override;
    bool empty() const override;
    IDistributionStatisticPtr getStatisticAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;
    void granuleFinished() override;

private:
    const String stat_name;
    const String column_name;
    std::optional<QuantileTDigest<Float32>> min_sketch;
    std::optional<QuantileTDigest<Float32>> max_sketch;
    std::optional<Float32> min_current;
    std::optional<Float32> max_current;
};

IDistributionStatisticPtr creatorGranuleDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
IMergeTreeDistributionStatisticCollectorPtr creatorGranuleDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
void validatorGranuleDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
}
