#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include "base/types.h"

namespace DB
{

class MergeTreeColumnDistributionStatisticTDigest : public IColumnDistributionStatistic
{
public:
    explicit MergeTreeColumnDistributionStatisticTDigest(const String & column_name_);
    MergeTreeColumnDistributionStatisticTDigest(QuantileTDigest<Float32>&& sketch_, const String & column_name_);

    const String& name() const override;

    bool empty() const override;
    void merge(const IStatisticPtr & other) override;

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
    IColumnDistributionStatisticPtr getStatisticAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;
    void granuleFinished() override;

private:
    const String column_name;
    std::optional<QuantileTDigest<Float32>> sketch;
};

IColumnDistributionStatisticPtr creatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
IMergeTreeColumnDistributionStatisticCollectorPtr creatorColumnDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
void validatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
}
