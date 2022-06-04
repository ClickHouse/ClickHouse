#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include <base/types.h>

namespace DB
{

/*
Simple TDigest sketch for column.
Includes each value.
*/
class MergeTreeColumnDistributionStatisticTDigest : public IDistributionStatistic
{
public:
    explicit MergeTreeColumnDistributionStatisticTDigest(
        const String & name_,
        const String & column_name_);
    MergeTreeColumnDistributionStatisticTDigest(
        QuantileTDigest<Float32>&& sketch_,
        const String & name_,
        const String & column_name_);

    const String& name() const override;
    const String& type() const override;
    bool empty() const override;
    void merge(const IStatisticPtr & other) override;

    const String& getColumnsRequiredForStatisticCalculation() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    bool validateTypeBinary(ReadBuffer & istr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    double estimateQuantileLower(double value) const override;
    double estimateQuantileUpper(double value) const override;
    double estimateProbability(const Field& lower, const Field& upper) const override;

    size_t getSizeInMemory() const override;

private:
    const String stat_name;
    const String column_name;
    mutable QuantileTDigest<Float32> sketch;
    bool is_empty;
};

class MergeTreeColumnDistributionStatisticCollectorTDigest : public IMergeTreeStatisticCollector
{
public:
    explicit MergeTreeColumnDistributionStatisticCollectorTDigest(
        const String & name_,
        const String & column_name_);

    const String& name() const override;
    const String & type() const override;
    const String & column() const override;
    bool empty() const override;

    StatisticType statisticType() const override;
    IDistributionStatisticPtr getDistributionStatisticAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;
    void granuleFinished() override;

private:
    const String stat_name;
    const String column_name;
    std::optional<QuantileTDigest<Float32>> sketch;
};

IStatisticPtr creatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
IMergeTreeStatisticCollectorPtr creatorColumnDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
void validatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
}
