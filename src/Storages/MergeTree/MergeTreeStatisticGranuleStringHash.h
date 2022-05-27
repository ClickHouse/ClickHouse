#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <base/types.h>

namespace DB
{

class CountMinSketch {

};

/*
Sketch for finding fraction of granules that can contain strings or substrings.
*/
template<bool fullstring>
class MergeTreeGranuleDistributionStatisticTDigest : public IDistributionStatistic
{
public:
    explicit MergeTreeGranuleDistributionStatisticTDigest(
        const String & name_,
        const String & column_name_);
    MergeTreeGranuleDistributionStatisticTDigest(
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

    double estimateQuantileLower(const Field& value) const override;
    double estimateQuantileUpper(const Field& value) const override;
    double estimateProbability(const Field& lower, const Field& upper) const override;

    size_t getSizeInMemory() const override;

private:
    const String stat_name;
    const String column_name;
    mutable QuantileTDigest<Float32> min_sketch;
    mutable QuantileTDigest<Float32> max_sketch;
    bool is_empty;
};

template<bool fullstring>
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
IMergeTreeStatisticCollectorPtr creatorGranuleDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
void validatorGranuleDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column);
}
