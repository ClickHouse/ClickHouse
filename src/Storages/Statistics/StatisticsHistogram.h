#pragma once

#include <config.h>

#if USE_DATASKETCHES

#include <Storages/Statistics/Statistics.h>

#include <frequent_items_sketch.hpp>
#include <kll_sketch.hpp>

#include <mutex>
#include <utility>
#include <vector>

namespace DB
{

/// Misra-Gries summary (DataSketches frequent_items_sketch) of the most common finite values of a
/// histogram column statistic. Keys are finite Float64 values with canonical zero.
///
/// Every unit of weight that a purge removes from the counters is reported to a spill buffer, so the
/// counters and the spilled weight together account for every update exactly. This is not an
/// `IStatistics`: it is owned by `StatisticsHistogram`, which forwards spilled weight to its KLL sketch.
class HistogramFrequentValues
{
public:
    static constexpr UInt64 MIN_VALUES = 1;
    /// With at most 768 counters a purge sees at most 769 active counters, below the purge sample size
    /// of 1024, so the purge median is exact and purges do not depend on the hash map layout.
    static constexpr UInt64 MAX_VALUES = 768;

    struct Item
    {
        Float64 value;
        UInt64 count;
    };
    using Items = std::vector<Item>;
    /// (value, weight) pairs removed from counters by purges, in the order in which they were reported.
    using Spill = std::vector<std::pair<Float64, UInt64>>;

    explicit HistogramFrequentValues(UInt64 requested_values_);

    /// log2 of the smallest map size whose capacity holds at least `values` values.
    static UInt8 getLgMapSize(UInt64 values);
    static UInt64 getMapCapacity(UInt8 lg_map_size);

    /// Counts one row with a finite value with canonical zero.
    /// Weight evicted by a purge is appended to `spill`.
    void update(Float64 finite_value, Spill & spill);
    /// Merges in item order, so the result depends only on the logical states of both summaries.
    /// Weight evicted by purges is appended to `spill`.
    void merge(const HistogramFrequentValues & other, Spill & spill);

    /// The number of values requested in `histogram(N, M)` (`M`).
    UInt64 getRequestedValues() const { return requested_values; }
    /// The number of values that are held at least, `M` rounded up.
    UInt64 getCapacity() const { return getMapCapacity(sketch.get_lg_max_map_size()); }
    /// Maximum error (delta): true count - counter <= delta for every value, and untracked values occur
    /// at most delta times.
    UInt64 getMaximumError() const { return sketch.get_maximum_error(); }
    /// Sum of all counters.
    UInt64 getRetainedWeight() const { return retained_weight; }
    /// Number of finite rows seen.
    UInt64 getTotalWeight() const { return sketch.get_total_weight(); }
    size_t size() const { return sketch.get_num_active_items(); }

    /// Tracked values with their counters, sorted by value.
    Items getSortedItems() const;

    void serialize(WriteBuffer & buf) const;
    /// Reads and validates the summary of `finite_rows` finite rows within the given finite bounds.
    void deserialize(ReadBuffer & buf, UInt64 finite_rows, Float64 finite_min, Float64 finite_max);

private:
    /// The map applies fmix64 to the hash, so the raw bit pattern is enough and does not depend on `std::hash`.
    struct CanonicalFloat64Hash
    {
        size_t operator()(Float64 value) const;
    };
    using Sketch = datasketches::frequent_items_sketch<Float64, UInt64, CanonicalFloat64Hash, std::equal_to<>>;

    void reserveSpill(Spill & spill) const;

    UInt64 requested_values;
    Sketch sketch;
    UInt64 retained_weight = 0;
};

/// `histogram(N, M)`: an equi-depth histogram with `N` buckets combined with the `M` most common values.
///
/// Every finite row is counted by a Misra-Gries summary (the MCVs) first. Weight that the summary evicts
/// while purging is spilled into a KLL quantile sketch. The counters and the KLL sketch therefore split the
/// finite rows exactly, and the histogram buckets, derived from the KLL sketch, describe only the residual
/// values. Estimates combine the exact MCV counts with the residual buckets.
class StatisticsHistogram final : public IStatistics
{
public:
    static constexpr UInt64 MIN_BUCKETS = 2;
    static constexpr UInt64 MAX_BUCKETS = 1024;
    static constexpr UInt64 MIN_MCV_VALUES = HistogramFrequentValues::MIN_VALUES;
    static constexpr UInt64 MAX_MCV_VALUES = HistogramFrequentValues::MAX_VALUES;
    static constexpr UInt64 DEFAULT_BUCKETS_FOR_DESERIALIZATION = 128;
    static constexpr UInt64 DEFAULT_MCV_VALUES_FOR_DESERIALIZATION = 64;

    struct Parameters
    {
        UInt64 bucket_count;
        UInt64 mcv_values;
        bool operator==(const Parameters &) const = default;
    };

    /// Parses `histogram(N, M)`. An identifier or a null AST (internal placeholders) yields the defaults
    /// unless `require_parameters` is set. Throws BAD_ARGUMENTS for invalid descriptions.
    static Parameters getParametersFromDescription(const SingleStatisticsDescription & description, bool require_parameters);
    /// Same as `getParametersFromDescription(description, false)`, but returns nullopt instead of throwing.
    static std::optional<Parameters> tryGetParametersFromDescription(const SingleStatisticsDescription & description);

    explicit StatisticsHistogram(const SingleStatisticsDescription & description, const DataTypePtr & data_type_, UInt64 random_seed = 0);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;
    bool isLoaded() const override { return loaded; }

    /// Tracked values: counter plus the spilled share recovered from the KLL sketch (at most the maximum error).
    /// Untracked values: 0 if nothing was evicted or the value is out of bounds, otherwise the KLL mass if it is
    /// exact or exceeds the KLL rank error (capped at the maximum error); no answer otherwise.
    std::optional<Float64> estimateEqual(const Field & val) const override;
    std::optional<Float64> estimateLess(const Field & val) const override;
    std::optional<Float64> estimateLessOrEqual(const Field & val) const override;
    std::optional<Float64> estimateGreater(const Field & val) const override;
    std::optional<Float64> estimateGreaterOrEqual(const Field & val) const override;

    String getNameForLogs() const override;
    bool isCompatibleWith(const IStatistics & other) const override;

    UInt64 getBucketCount() const { return bucket_count; }
    UInt64 getMcvValues() const { return frequent_values.getRequestedValues(); }
    UInt64 getNonNullCount() const { return non_null_count; }
    UInt64 getFiniteCount() const { return non_null_count - nan_count - negative_inf_count - positive_inf_count; }
    /// Number of finite rows held by the residual KLL sketch.
    UInt64 getResidualCount() const { return sketch.get_n(); }
    /// True if the residual KLL sketch retains every value with weight one, so its masses are exact.
    bool isResidualExact() const { return !sketch.is_estimation_mode(); }
    /// Maximum error of the MCV counters (delta).
    UInt64 getMaximumError() const { return frequent_values.getMaximumError(); }
    /// Bucket boundaries of the residual values (values other than the MCVs).
    const std::vector<Float64> & getBucketBounds() const;
    /// The MCVs with their counters, sorted by value.
    HistogramFrequentValues::Items getFrequentValues() const { return frequent_values.getSortedItems(); }

    /// Data for estimating values that are not tracked as MCVs (PostgreSQL's non-MCV formula).
    struct NonFrequentSummary
    {
        /// Number of finite rows (F).
        Float64 finite_rows = 0;
        /// Upper bound for the count of an untracked value (delta).
        UInt64 maximum_error = 0;
        /// Estimated rows of the MCVs whose counter exceeds delta, i.e. that are certainly frequent.
        Float64 frequent_rows = 0;
        /// Number of MCVs whose counter exceeds delta.
        UInt64 frequent_values = 0;
    };
    NonFrequentSummary getNonFrequentSummary() const;

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

    /// Estimation model derived from the KLL sketch and the MCVs, built lazily under `cache_mutex`.
    struct Model
    {
        /// MCVs sorted by value with their counters and the KLL weight at exactly that value.
        std::vector<Float64> mcv_values;
        std::vector<UInt64> mcv_counts;
        std::vector<UInt64> mcv_kll_weights;
        /// mcv_prefix[i] = sum_{j < i} (mcv_counts[j] + mcv_kll_weights[j]).
        std::vector<UInt64> mcv_prefix;

        /// Residual values: retained KLL values other than MCVs, unique and sorted, with cumulative weights.
        std::vector<Float64> residual_values;
        std::vector<UInt64> residual_cumulative;
        UInt64 residual_total = 0;

        /// Equi-depth buckets over the residual values.
        std::vector<Float64> bucket_bounds;
        std::vector<Float64> counts_less;
        std::vector<Float64> counts_less_or_equal;

        /// Sum of the KLL weights of the residual values at exactly `value`.
        UInt64 residualWeightAt(Float64 value) const;
        /// Residual weight of values < `value` (or <= if `inclusive`) among retained values.
        UInt64 residualCountBelow(Float64 value, bool inclusive) const;
        /// Sum of (counter + KLL weight) of MCVs < `value` (or <= if `inclusive`).
        UInt64 frequentCountBelow(Float64 value, bool inclusive) const;
        /// Interpolated residual count of values < `value` (or <= if `inclusive`).
        Float64 interpolateResidualBelow(Float64 value, bool inclusive) const;
    };

    static UInt16 getSketchK(UInt64 buckets);

    Float64 projectToFloat64(const IColumn & column, size_t row) const;
    void flushSpill();
    void assertInvariants() const;
    void invalidateCache();
    const Model & getModel() const;
    /// Estimated finite rows with values < `value` (or <= if `inclusive`).
    Float64 estimateFiniteBelow(Float64 value, bool inclusive) const;
    Float64 comparableCount() const;

    DataTypePtr data_type;
    String data_type_name;
    UInt64 bucket_count;
    RandomBitGenerator random_bit_generator;
    HistogramFrequentValues frequent_values;
    /// Residual: the finite rows evicted from `frequent_values`.
    Sketch sketch;
    UInt64 non_null_count = 0;
    UInt64 nan_count = 0;
    UInt64 negative_inf_count = 0;
    UInt64 positive_inf_count = 0;
    /// Exact bounds of all finite values, including the MCVs.
    std::optional<Float64> finite_min;
    std::optional<Float64> finite_max;
    /// False if `deserialize` skipped a payload of an unsupported version.
    bool loaded = true;

    HistogramFrequentValues::Spill spill_buffer;

    /// Also guards the lazily built sorted view of the KLL sketch.
    mutable std::mutex cache_mutex;
    mutable bool cache_valid = false;
    mutable Model model;
};

bool histogramStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr histogramStatisticsCreator(
    const SingleStatisticsDescription & description, const DataTypePtr & data_type, UInt64 random_seed);

}

#endif
