#pragma once

#include <config.h>

#if USE_DATASKETCHES

#include <Storages/Statistics/Statistics.h>

#include <kll_sketch.hpp>

#include <mutex>

namespace DB
{

class StatisticsHistogram final : public IStatistics
{
public:
    static constexpr UInt64 MIN_BUCKETS = 2;
    static constexpr UInt64 MAX_BUCKETS = 1024;
    static constexpr UInt64 DEFAULT_BUCKETS_FOR_DESERIALIZATION = 128;

    explicit StatisticsHistogram(const SingleStatisticsDescription & description, const DataTypePtr & data_type_, UInt64 random_seed = 0);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    /// Returns equality mass only when KLL retains it exactly or it exceeds the sketch rank error.
    std::optional<Float64> estimateEqual(const Field & val) const override;
    std::optional<Float64> estimateLess(const Field & val) const override;
    std::optional<Float64> estimateLessOrEqual(const Field & val) const override;
    std::optional<Float64> estimateGreater(const Field & val) const override;
    std::optional<Float64> estimateGreaterOrEqual(const Field & val) const override;

    String getNameForLogs() const override;
    bool isCompatibleWith(const IStatistics & other) const override;

    UInt64 getBucketCount() const { return bucket_count; }
    UInt64 getNonNullCount() const { return non_null_count; }
    const std::vector<Float64> & getBucketBounds() const;

    static UInt64 getBucketCountFromDescription(const SingleStatisticsDescription & description, bool require_parameter);

private:
    class RandomBitGenerator
    {
    public:
        explicit RandomBitGenerator(UInt64 state_) : state(state_) {}

        bool operator()();
        UInt64 getState() const { return state; }

    private:
        UInt64 state;
    };

    using Sketch = datasketches::kll_sketch<Float64>;

    static UInt16 getSketchK(UInt64 buckets);

    void invalidateCache();
    void buildCache() const;
    Float64 estimateFiniteLess(Float64 value, bool inclusive) const;
    Float64 comparableCount() const;

    DataTypePtr data_type;
    String data_type_name;
    UInt64 bucket_count;
    RandomBitGenerator random_bit_generator;
    Sketch sketch;
    UInt64 non_null_count = 0;
    UInt64 nan_count = 0;
    UInt64 negative_inf_count = 0;
    UInt64 positive_inf_count = 0;
    /// Exact finite bounds are tracked explicitly while building and restored from native KLL state.
    std::optional<Float64> finite_min;
    std::optional<Float64> finite_max;

    mutable std::mutex cache_mutex;
    mutable bool cache_valid = false;
    mutable std::vector<Float64> bucket_bounds;
    mutable std::vector<Float64> counts_less;
    mutable std::vector<Float64> counts_less_or_equal;
};

bool histogramStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr histogramStatisticsCreator(
    const SingleStatisticsDescription & description, const DataTypePtr & data_type, UInt64 random_seed);

}

#endif
