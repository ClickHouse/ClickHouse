#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include "base/types.h"

namespace DB
{

class MergeTreeColumnDistributionStatisticTDigest : public IMergeTreeColumnDistributionStatistic
{
public:
    explicit MergeTreeColumnDistributionStatisticTDigest(const String & column_name_);
    MergeTreeColumnDistributionStatisticTDigest(QuantileTDigest<Float32>&& sketch_, const String & column_name_);

    const String& name() const override;

    bool empty() const override;
    void merge(const std::shared_ptr<IMergeTreeStatistic> & other) override;

    const String& getColumnsRequiredForStatisticCalculation() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    double estimateQuantileLower(const Field& value) const override;
    double estimateQuantileUpper(const Field& value) const override;
    double estimateProbability(const Field& lower, const Field& upper) const override;

private:
    const String column_name;
    mutable QuantileTDigest<Float32> sketch;
    bool is_empty;
};

class MergeTreeColumnDistributionStatisticCollectorTDigest : public IMergeTreeColumnDistributionStatisticCollector
{
public:
    explicit MergeTreeColumnDistributionStatisticCollectorTDigest(const String & column_name_);

    const String & name() const override;
    const String & column() const override;
    bool empty() const override;
    IMergeTreeColumnDistributionStatisticPtr getStatisticAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;
    void granuleFinished() override;

private:
    const String column_name;
    std::optional<QuantileTDigest<Float32>> sketch;
};

IMergeTreeColumnDistributionStatisticPtr creatorColumnDistributionStatisticTDigest(const StatisticDescription & stat);
IMergeTreeColumnDistributionStatisticCollectorPtr creatorColumnDistributionStatisticCollectorTDigest(const StatisticDescription & stat);

}
