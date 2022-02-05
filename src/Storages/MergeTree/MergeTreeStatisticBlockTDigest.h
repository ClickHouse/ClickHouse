#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include <base/types.h>

namespace DB
{

/*
*/
class MergeTreeColumnDistributionStatisticBlockTDigest : public IColumnDistributionStatistic
{
public:
    explicit MergeTreeColumnDistributionStatisticBlockTDigest(const String & column_name_);
    MergeTreeColumnDistributionStatisticBlockTDigest(QuantileTDigest<Float32>&& sketch_, const String & column_name_);

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
    mutable QuantileTDigest<Float32> min_sketch;
    mutable QuantileTDigest<Float32> max_sketch;
    bool is_empty;
};

class MergeTreeColumnDistributionStatisticCollectorBlockTDigest : public IMergeTreeColumnDistributionStatisticCollector
{
public:
    explicit MergeTreeColumnDistributionStatisticCollectorBlockTDigest(const String & column_name_);

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

IColumnDistributionStatisticPtr creatorColumnDistributionStatisticBlockTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
IMergeTreeColumnDistributionStatisticCollectorPtr creatorColumnDistributionStatisticCollectorBlockTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
void validatorColumnDistributionStatisticBlockTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
}
