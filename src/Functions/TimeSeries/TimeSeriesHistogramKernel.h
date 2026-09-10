#pragma once

/// Native-histogram arithmetic kernel: a verbatim port of FloatHistogram (pinned upstream tmp/upstream_slice4_float_histogram.go),
/// bit-for-bit including Kahan compensation; upstream panics map to INCORRECT_DATA (fail-close), bucket indices accumulate in Int64.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Counter reset hints, mirroring `CounterResetHint` in Prometheus model/histogram/histogram.go.
/// The values match the payload `flags` bits: the hint is (flags & CounterResetHintMask) >> CounterResetHintShift.
namespace TimeSeriesHistogramCounterResetHint
{
    constexpr UInt8 UnknownCounterReset = 0;
    constexpr UInt8 CounterReset = 1;
    constexpr UInt8 NotCounterReset = 2;
    constexpr UInt8 GaugeType = 3;
}

/// Out-params of `add`/`sub`, mirroring FloatHistogram.Add/Sub: `counter_reset_collision` marks a
/// CounterReset/NotCounterReset collision; `nhcb_bounds_reconciled` marks reconciled mismatched custom bounds.
struct TimeSeriesFloatHistogramAddSubOutcome
{
    bool counter_reset_collision = false;
    bool nhcb_bounds_reconciled = false;
};

/// Defined after TimeSeriesFloatHistogram (it contains one by value).
struct TimeSeriesFloatHistogramKahanAddOutcome;

/// A decoded native histogram mirroring FloatHistogram: bucket counts are absolute (not deltas), spans
/// keep the stored layout; `custom_values` is used only with HISTOGRAM_CUSTOM_BUCKETS_SCHEMA (negative side then unused).
struct TimeSeriesFloatHistogram
{
    UInt8 counter_reset_hint = TimeSeriesHistogramCounterResetHint::UnknownCounterReset;
    Int32 schema = 0;
    Float64 zero_threshold = 0;
    Float64 zero_count = 0;
    Float64 count = 0;
    Float64 sum = 0;
    std::vector<TimeSeriesHistogramSpan> positive_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> negative_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> custom_values; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Mirrors `IsCustomBucketsSchema` in Prometheus model/histogram/generic.go.
    bool usesCustomBuckets() const { return schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA; }

    /// Extracts the counter reset hint bits from the payload `flags` column value.
    static UInt8 counterResetHintFromFlags(UInt8 flags)
    {
        return (flags & TimeSeriesHistogramFlags::CounterResetHintMask) >> TimeSeriesHistogramFlags::CounterResetHintShift;
    }

    /// Validates a decoded payload row: valid exponential or custom-buckets schema (like
    /// `walkTimeSeriesHistogramBuckets`) and spans covering exactly the stored bucket values. Throws INCORRECT_DATA otherwise.
    void validateDecodedLayout() const;

    /// Decodes one histogram from a row of the payload tuple columns (see getTimeSeriesHistogramPayloadTupleType);
    /// the arrays are copied verbatim (no span expansion) and validated with `validateDecodedLayout`.
    static TimeSeriesFloatHistogram fromPayloadTupleRow(
        const ColumnTuple & tuple_column, const TimeSeriesHistogramPayloadPositions & positions, size_t row);

    /// Mirrors FloatHistogram.CopyToSchema (exponential resolution reduction only): the equal-schema fast path
    /// copies the counter reset hint, the slow path does not; throws INCORRECT_DATA where upstream panics.
    TimeSeriesFloatHistogram copyToSchema(Int32 target_schema) const;

    /// Mirrors FloatHistogram.Add: reconciles the zero threshold and the schema (via `reduceResolution`) and
    /// intersects mismatched custom bucket bounds; modifies this histogram, returns the outcome.
    TimeSeriesFloatHistogramAddSubOutcome add(const TimeSeriesFloatHistogram & other) { return addOrSub(other, false); }

    /// Mirrors FloatHistogram.Sub: like `add` but subtracts; the counter reset hint is adjusted as in `add`
    /// (setting GaugeType for the PromQL "-" operator is left to the caller upstream).
    TimeSeriesFloatHistogramAddSubOutcome sub(const TimeSeriesFloatHistogram & other) { return addOrSub(other, true); }

    /// Mirrors FloatHistogram.KahanAdd: like `add` with Kahan (Neumaier) summation for every scalar and bucket
    /// count; `compensation` is nullopt on the first call, then the previous outcome's `updated_compensation`.
    TimeSeriesFloatHistogramKahanAddOutcome kahanAdd(
        const TimeSeriesFloatHistogram & other, std::optional<TimeSeriesFloatHistogram> compensation);

    /// Mirrors FloatHistogram.Mul: scales the zero count, count, sum and all bucket counts by `factor`
    /// (layout unchanged); a negative `factor` sets the counter reset hint to GaugeType.
    void mul(Float64 factor);

    /// Mirrors FloatHistogram.Div: divides the zero count, count, sum and all bucket counts; division by zero
    /// removes all buckets, a negative `scalar` sets GaugeType. `irate` uses Div (a `mul` by the reciprocal is not bit-identical).
    void div(Float64 scalar);

    /// Mirrors FloatHistogram.DetectReset: true on buckets populated in `previous` but missing here or any decreased
    /// count (never the sum), shortcutting on the CounterReset/NotCounterReset hints; zero-threshold changes fold buckets first.
    bool detectReset(const TimeSeriesFloatHistogram & previous) const;

    /// Mirrors FloatHistogram.Compact: trims empty buckets at span edges, merges spans at most
    /// `max_empty_buckets` apart, splits spans with longer empty runs.
    void compact(int max_empty_buckets);

private:
    /// One bucket with resolved bounds (the port of `Bucket[float64]` from
    /// model/histogram/generic.go; the inclusivity flags are not used by the kernel paths).
    struct ResolvedBucket
    {
        Float64 lower;
        Float64 upper;
        Float64 count;
        Int64 index;
    };

    /// The port of `floatBucketIterator` in model/histogram/float_histogram.go: iterates the positive or negative
    /// buckets in span order, merging down to `target_schema`; exponential buckets with upper bound <= a nonzero `absolute_start_value` are skipped.
    struct BucketIterator
    {
        Int32 schema = 0;
        Int32 target_schema = 0;
        bool positive = true;
        Float64 absolute_start_value = 0;
        bool bound_reached_start_value = true;
        const std::vector<TimeSeriesHistogramSpan> * spans = nullptr; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> * buckets = nullptr; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> * custom_values = nullptr; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

        size_t spans_idx = 0;
        UInt32 idx_in_span = 0;
        size_t buckets_idx = 0;
        Float64 curr_count = 0;
        Int64 curr_idx = 0;
        Int64 orig_idx = 0;

        bool next();
        ResolvedBucket at() const;
        /// The port of `strippedAt`: the current bucket without bound values.
        std::pair<Float64, Int64> strippedAt() const { return {curr_count, curr_idx}; }

    private:
        bool advance();
        Float64 bound(Int64 idx) const;
    };

    /// The port of the low-level constructor `floatBucketIterator`.
    BucketIterator bucketIterator(bool positive, Float64 absolute_start_value, Int32 target_schema) const;

    /// The port of `zeroCountForLargerThreshold`: the zero count (and the compensation's, when non-null) with a
    /// larger threshold, adjusted to a populated bucket's upper bound; returns {zero_count, adjusted_threshold, compensation_zero_count}.
    std::tuple<Float64, Float64, Float64> zeroCountForLargerThreshold(
        Float64 larger_threshold, const TimeSeriesFloatHistogram * compensation = nullptr) const;

    /// The port of `trimBucketsInZeroBucket`: zeroes and compacts away the buckets within the zero bucket
    /// (with non-null `compensation` its buckets are zeroed in lockstep and `kahanCompact` is used).
    void trimBucketsInZeroBucket(TimeSeriesFloatHistogram * compensation = nullptr);

    /// The port of `reconcileZeroBuckets`: widens this histogram's zero bucket (and a non-null `compensation`'s)
    /// to fit both sides; returns the zero count `other` would have, plus its Kahan compensation accumulated from zero.
    std::pair<Float64, Float64> reconcileZeroBuckets(
        const TimeSeriesFloatHistogram & other, TimeSeriesFloatHistogram * compensation = nullptr);

    /// The port of `newCompensationHistogram`: a zero-valued compensation histogram matching this
    /// histogram's counter reset hint, schema, zero threshold, custom values and bucket layout.
    TimeSeriesFloatHistogram newCompensationHistogram() const;

    /// The port of `kahanCompact`: `compact` with the compensation buckets compacted in lockstep.
    void kahanCompact(int max_empty_buckets, TimeSeriesFloatHistogram & compensation);

    /// The port of `checkSchemaAndBounds`: throws INCORRECT_DATA when one histogram uses custom
    /// buckets and the other does not (upstream returns ErrHistogramsIncompatibleSchema).
    void checkSchemaAndBounds(const TimeSeriesFloatHistogram & other) const;

    /// The port of `adjustCounterReset`: adjusts this histogram's counter reset hint after an
    /// add/sub and returns whether a CounterReset/NotCounterReset collision occurred.
    bool adjustCounterReset(const TimeSeriesFloatHistogram & other);

    /// The common body of `add` and `sub`; upstream's Add and Sub share all logic through the `negative`
    /// flag of addBuckets/addCustomBucketsWithMismatches and the sign of the scalar and zero-count updates.
    TimeSeriesFloatHistogramAddSubOutcome addOrSub(const TimeSeriesFloatHistogram & other, bool negative);

    /// The port of the free function `detectReset`: per-direction bucket-by-bucket comparison.
    static bool detectResetInIterators(BucketIterator & curr_it, BucketIterator & prev_it);

    /// The port of `detectResetWithMismatchedCustomBounds`: true if any bucket count decreased, comparing
    /// NHCBs with mismatched custom bounds mapped to the intersected bounds on the fly.
    bool detectResetWithMismatchedCustomBounds(
        const TimeSeriesFloatHistogram & previous,
        const std::vector<Float64> & curr_bounds, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & prev_bounds) const; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `targetIdx` in model/histogram/float_histogram.go.
    static Int64 targetIdx(Int64 idx, Int32 origin_schema, Int32 target_schema)
    {
        return ((idx - 1) >> (origin_schema - target_schema)) + 1;
    }

    /// The port of `kahansum.Inc` (Kahan summation with Neumaier's improvement; the compensation
    /// resets when the sum overflows to an infinity).
    static std::pair<Float64, Float64> kahanInc(Float64 inc, Float64 sum, Float64 c)
    {
        const Float64 t = sum + inc;
        if (t > std::numeric_limits<Float64>::max() || t < -std::numeric_limits<Float64>::max())
            c = 0;
        else if (std::abs(sum) >= std::abs(inc))
            c += (sum - t) + inc;
        else
            c += (inc - t) + sum;
        return {t, c};
    }

    /// The port of `kahansum.Dec`.
    static std::pair<Float64, Float64> kahanDec(Float64 dec, Float64 sum, Float64 c)
    {
        return kahanInc(-dec, sum, c);
    }

    /// The port of `CustomBucketBoundsMatch` in model/histogram/generic.go.
    static bool customBucketBoundsMatch(const std::vector<Float64> & c1, const std::vector<Float64> & c2) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        if (c1.size() != c2.size())
            return false;
        for (size_t i = 0; i < c1.size(); ++i)
        {
            if (c1[i] != c2[i])
                return false;
        }
        return true;
    }

    /// The port of `reduceResolution` (deltaBuckets=false) wrapped in `mustReduceResolution`: throws INCORRECT_DATA
    /// instead of panicking, and the result is freshly allocated (upstream's `inplace` variants are observably equal).
    static std::pair<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>> mustReduceResolution( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<TimeSeriesHistogramSpan> & origin_spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & origin_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        Int32 origin_schema,
        Int32 target_schema);

    /// The port of `addBuckets`: adds the spans_b/buckets_b buckets to spans_a/buckets_a, creating missing
    /// buckets; B's buckets with an absolute upper limit <= `threshold` are ignored, `negative` subtracts.
    static std::pair<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>> addBuckets( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        Int32 schema,
        Float64 threshold,
        bool negative,
        std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & buckets_b); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `compactBuckets` (deltaBuckets=false); a non-null `compensation_buckets` is compacted in
    /// lockstep and its length must match `buckets` (INCORRECT_DATA where upstream panics).
    static void compactBuckets(
        std::vector<Float64> & buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<TimeSeriesHistogramSpan> & spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        int max_empty_buckets,
        std::vector<Float64> * compensation_buckets = nullptr); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `intersectCustomBucketBounds`.
    static std::vector<Float64> intersectCustomBucketBounds( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & bounds_a, const std::vector<Float64> & bounds_b); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `addCustomBucketsWithMismatches`: maps both custom-bucket histograms to the intersected
    /// layout and adds/subtracts them; non-null `buckets_c` (the KahanAdd path) also folds in A's compensation buckets.
    static std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> addCustomBucketsWithMismatches( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        bool negative,
        std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & bounds_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & buckets_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & bounds_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> * buckets_c, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & intersected_bounds); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `kahanAddBuckets`: like `addBuckets`, but every add is a compensated `kahanInc` pair
    /// maintaining `compensation_buckets_a` in lockstep and folding in a non-null `compensation_buckets_b`.
    static std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> kahanAddBuckets( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        Int32 schema,
        Float64 threshold,
        std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & buckets_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<Float64> compensation_buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> * compensation_buckets_b); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The port of `kahanReduceResolution` (freshly allocated results, like `mustReduceResolution`): reduces the
    /// bucket counts and their Kahan compensation with `kahanInc` pairs; throws INCORRECT_DATA where upstream panics.
    static std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> kahanReduceResolution( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<TimeSeriesHistogramSpan> & origin_spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & origin_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        const std::vector<Float64> & origin_compensation_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        Int32 origin_schema,
        Int32 target_schema);
};

/// Out-params of `kahanAdd`, mirroring FloatHistogram.KahanAdd's returns (its `err` maps to INCORRECT_DATA);
/// `updated_compensation` is the compensation histogram to move back into the next `kahanAdd` of a sequence.
struct TimeSeriesFloatHistogramKahanAddOutcome
{
    TimeSeriesFloatHistogram updated_compensation;
    bool counter_reset_collision = false;
    bool nhcb_bounds_reconciled = false;
};

inline void TimeSeriesFloatHistogram::validateDecodedLayout() const
{
    if (schema != HISTOGRAM_CUSTOM_BUCKETS_SCHEMA && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an invalid bucket schema: {}", schema);

    auto check_span_value_consistency = [](const std::vector<TimeSeriesHistogramSpan> & spans, const std::vector<Float64> & buckets) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        size_t covered = 0;
        for (const auto & span : spans)
            covered += span.length;
        if (covered > buckets.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has fewer bucket values than its spans cover");
        if (covered < buckets.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has more bucket values than its spans cover");
    };
    check_span_value_consistency(positive_spans, positive_buckets);
    check_span_value_consistency(negative_spans, negative_buckets);
}

inline TimeSeriesFloatHistogram TimeSeriesFloatHistogram::fromPayloadTupleRow(
    const ColumnTuple & tuple_column, const TimeSeriesHistogramPayloadPositions & positions, size_t row)
{
    namespace Idx = TimeSeriesHistogramPayloadTupleIndex;

    TimeSeriesFloatHistogram histogram;

    const UInt8 flags = typeid_cast<const ColumnUInt8 &>(tuple_column.getColumn(positions[Idx::Flags])).getData()[row];
    histogram.counter_reset_hint = counterResetHintFromFlags(flags);
    histogram.schema = static_cast<Int32>(typeid_cast<const ColumnInt8 &>(tuple_column.getColumn(positions[Idx::Schema])).getData()[row]);
    histogram.zero_threshold = typeid_cast<const ColumnFloat64 &>(tuple_column.getColumn(positions[Idx::ZeroThreshold])).getData()[row];
    histogram.count = typeid_cast<const ColumnFloat64 &>(tuple_column.getColumn(positions[Idx::Count])).getData()[row];
    histogram.sum = typeid_cast<const ColumnFloat64 &>(tuple_column.getColumn(positions[Idx::Sum])).getData()[row];
    histogram.zero_count = typeid_cast<const ColumnFloat64 &>(tuple_column.getColumn(positions[Idx::ZeroCount])).getData()[row];

    auto read_spans = [&](const IColumn & column, std::vector<TimeSeriesHistogramSpan> & out) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        const auto & array = typeid_cast<const ColumnArray &>(column);
        const auto & offsets = array.getOffsets();
        const auto & tuple = typeid_cast<const ColumnTuple &>(array.getData());
        const auto & span_offsets = typeid_cast<const ColumnInt32 &>(tuple.getColumn(0)).getData();
        const auto & span_lengths = typeid_cast<const ColumnUInt32 &>(tuple.getColumn(1)).getData();
        const size_t begin = (row == 0) ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        out.clear();
        out.reserve(end - begin);
        for (size_t i = begin; i < end; ++i)
            out.push_back(TimeSeriesHistogramSpan{span_offsets[i], span_lengths[i]});
    };

    auto read_floats = [&](const IColumn & column, std::vector<Float64> & out) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        const auto & array = typeid_cast<const ColumnArray &>(column);
        const auto & offsets = array.getOffsets();
        const auto & data = typeid_cast<const ColumnFloat64 &>(array.getData()).getData();
        const size_t begin = (row == 0) ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        out.assign(data.begin() + begin, data.begin() + end);
    };

    read_spans(tuple_column.getColumn(positions[Idx::PositiveSpans]), histogram.positive_spans);
    read_floats(tuple_column.getColumn(positions[Idx::PositiveValues]), histogram.positive_buckets);
    read_spans(tuple_column.getColumn(positions[Idx::NegativeSpans]), histogram.negative_spans);
    read_floats(tuple_column.getColumn(positions[Idx::NegativeValues]), histogram.negative_buckets);
    read_floats(tuple_column.getColumn(positions[Idx::CustomValues]), histogram.custom_values);

    histogram.validateDecodedLayout();
    return histogram;
}

inline TimeSeriesFloatHistogram TimeSeriesFloatHistogram::copyToSchema(Int32 target_schema) const
{
    if (target_schema == schema)
    {
        /// Fast path: a deep copy (upstream FloatHistogram.Copy), including the counter reset hint.
        return *this;
    }
    if (usesCustomBuckets())
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot reduce resolution to {} when there are custom buckets", target_schema);
    if (target_schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA)
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot reduce resolution to custom buckets schema");
    if (target_schema > schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot copy from schema {} to {}", schema, target_schema);

    TimeSeriesFloatHistogram reduced;
    reduced.schema = target_schema;
    reduced.zero_threshold = zero_threshold;
    reduced.zero_count = zero_count;
    reduced.count = count;
    reduced.sum = sum;
    /// Upstream CopyToSchema does not copy the counter reset hint: it stays UnknownCounterReset.
    std::tie(reduced.positive_spans, reduced.positive_buckets)
        = mustReduceResolution(positive_spans, positive_buckets, schema, target_schema);
    std::tie(reduced.negative_spans, reduced.negative_buckets)
        = mustReduceResolution(negative_spans, negative_buckets, schema, target_schema);
    return reduced;
}

inline TimeSeriesFloatHistogramAddSubOutcome TimeSeriesFloatHistogram::addOrSub(const TimeSeriesFloatHistogram & other, bool negative)
{
    checkSchemaAndBounds(other);
    TimeSeriesFloatHistogramAddSubOutcome outcome;
    outcome.counter_reset_collision = adjustCounterReset(other);
    if (!usesCustomBuckets())
    {
        const Float64 other_zero_count = reconcileZeroBuckets(other).first;
        if (negative)
            zero_count -= other_zero_count;
        else
            zero_count += other_zero_count;
    }
    if (negative)
    {
        count -= other.count;
        sum -= other.sum;
    }
    else
    {
        count += other.count;
        sum += other.sum;
    }

    if (usesCustomBuckets())
    {
        if (customBucketBoundsMatch(custom_values, other.custom_values))
        {
            std::tie(positive_spans, positive_buckets) = addBuckets(
                schema, zero_threshold, negative,
                std::move(positive_spans), std::move(positive_buckets),
                other.positive_spans, other.positive_buckets);
        }
        else
        {
            outcome.nhcb_bounds_reconciled = true;
            std::vector<Float64> intersected_bounds = intersectCustomBucketBounds(custom_values, other.custom_values); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

            /// Add/subtract with mapping - maps both histograms to the intersected layout.
            std::tie(positive_spans, positive_buckets, std::ignore) = addCustomBucketsWithMismatches(
                negative,
                std::move(positive_spans), std::move(positive_buckets), custom_values,
                other.positive_spans, other.positive_buckets, other.custom_values,
                nullptr, intersected_bounds);
            custom_values = std::move(intersected_bounds);
        }
        return outcome;
    }

    /// The other histogram's spans/buckets are only reduced (copied) when its schema is finer.
    const std::vector<TimeSeriesHistogramSpan> * other_positive_spans = &other.positive_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_positive_buckets = &other.positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> * other_negative_spans = &other.negative_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_negative_buckets = &other.negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> reduced_other_positive_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> reduced_other_negative_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    if (other.schema < schema)
    {
        std::tie(positive_spans, positive_buckets)
            = mustReduceResolution(positive_spans, positive_buckets, schema, other.schema);
        std::tie(negative_spans, negative_buckets)
            = mustReduceResolution(negative_spans, negative_buckets, schema, other.schema);
        schema = other.schema;
    }
    else if (other.schema > schema)
    {
        std::tie(reduced_other_positive_spans, reduced_other_positive_buckets)
            = mustReduceResolution(other.positive_spans, other.positive_buckets, other.schema, schema);
        std::tie(reduced_other_negative_spans, reduced_other_negative_buckets)
            = mustReduceResolution(other.negative_spans, other.negative_buckets, other.schema, schema);
        other_positive_spans = &reduced_other_positive_spans;
        other_positive_buckets = &reduced_other_positive_buckets;
        other_negative_spans = &reduced_other_negative_spans;
        other_negative_buckets = &reduced_other_negative_buckets;
    }

    std::tie(positive_spans, positive_buckets) = addBuckets(
        schema, zero_threshold, negative,
        std::move(positive_spans), std::move(positive_buckets),
        *other_positive_spans, *other_positive_buckets);
    std::tie(negative_spans, negative_buckets) = addBuckets(
        schema, zero_threshold, negative,
        std::move(negative_spans), std::move(negative_buckets),
        *other_negative_spans, *other_negative_buckets);

    return outcome;
}

inline TimeSeriesFloatHistogramKahanAddOutcome TimeSeriesFloatHistogram::kahanAdd(
    const TimeSeriesFloatHistogram & other, std::optional<TimeSeriesFloatHistogram> compensation)
{
    checkSchemaAndBounds(other);
    TimeSeriesFloatHistogramKahanAddOutcome outcome;
    outcome.counter_reset_collision = adjustCounterReset(other);

    if (!compensation)
        compensation = newCompensationHistogram();
    TimeSeriesFloatHistogram & c = *compensation;

    /// The compensation histogram's bucket layout must match the histogram's (upstream relies on
    /// this implicitly; a mismatch is an index-out-of-range panic there, so fail close here).
    if (c.positive_buckets.size() != positive_buckets.size()
        || (!usesCustomBuckets() && c.negative_buckets.size() != negative_buckets.size()))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram Kahan compensation layout ({} positive, {} negative buckets) does not match"
            " the histogram layout ({} positive, {} negative buckets)",
            c.positive_buckets.size(), c.negative_buckets.size(), positive_buckets.size(), negative_buckets.size());

    if (!usesCustomBuckets())
    {
        const auto [other_zero_count, other_c_zero_count] = reconcileZeroBuckets(other, &c);
        std::tie(zero_count, c.zero_count) = kahanInc(other_zero_count, zero_count, c.zero_count);
        std::tie(zero_count, c.zero_count) = kahanInc(other_c_zero_count, zero_count, c.zero_count);
    }
    std::tie(count, c.count) = kahanInc(other.count, count, c.count);
    std::tie(sum, c.sum) = kahanInc(other.sum, sum, c.sum);

    if (usesCustomBuckets())
    {
        if (customBucketBoundsMatch(custom_values, other.custom_values))
        {
            std::tie(positive_spans, positive_buckets, c.positive_buckets) = kahanAddBuckets(
                schema, zero_threshold,
                std::move(positive_spans), std::move(positive_buckets),
                other.positive_spans, other.positive_buckets,
                std::move(c.positive_buckets), nullptr);
        }
        else
        {
            outcome.nhcb_bounds_reconciled = true;
            std::vector<Float64> intersected_bounds = intersectCustomBucketBounds(custom_values, other.custom_values); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

            /// Add with mapping - maps both histograms to the intersected layout.
            std::tie(positive_spans, positive_buckets, c.positive_buckets) = addCustomBucketsWithMismatches(
                false,
                std::move(positive_spans), std::move(positive_buckets), custom_values,
                other.positive_spans, other.positive_buckets, other.custom_values,
                &c.positive_buckets, intersected_bounds);
            custom_values = intersected_bounds;
            c.custom_values = intersected_bounds;
        }
        c.positive_spans = positive_spans;
        outcome.updated_compensation = std::move(c);
        return outcome;
    }

    /// The other histogram's spans/buckets (and freshly zeroed compensation buckets) are reduced only when its
    /// schema is finer; otherwise its compensation buckets stay null (upstream's nil `otherCPositiveBuckets`/`otherCNegativeBuckets`).
    const std::vector<TimeSeriesHistogramSpan> * other_positive_spans = &other.positive_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_positive_buckets = &other.positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> * other_negative_spans = &other.negative_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_negative_buckets = &other.negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_c_positive_buckets = nullptr; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * other_c_negative_buckets = nullptr; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> reduced_other_positive_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_c_positive_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> reduced_other_negative_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> reduced_other_c_negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    if (other.schema < schema)
    {
        std::tie(positive_spans, positive_buckets, c.positive_buckets)
            = kahanReduceResolution(positive_spans, positive_buckets, c.positive_buckets, schema, other.schema);
        std::tie(negative_spans, negative_buckets, c.negative_buckets)
            = kahanReduceResolution(negative_spans, negative_buckets, c.negative_buckets, schema, other.schema);
        schema = other.schema;
    }
    else if (other.schema > schema)
    {
        if (!other.positive_buckets.empty())
        {
            reduced_other_c_positive_buckets.assign(other.positive_buckets.size(), 0.0);
            std::tie(reduced_other_positive_spans, reduced_other_positive_buckets, reduced_other_c_positive_buckets)
                = kahanReduceResolution(
                    other.positive_spans, other.positive_buckets, reduced_other_c_positive_buckets, other.schema, schema);
            other_positive_spans = &reduced_other_positive_spans;
            other_positive_buckets = &reduced_other_positive_buckets;
            other_c_positive_buckets = &reduced_other_c_positive_buckets;
        }
        if (!other.negative_buckets.empty())
        {
            reduced_other_c_negative_buckets.assign(other.negative_buckets.size(), 0.0);
            std::tie(reduced_other_negative_spans, reduced_other_negative_buckets, reduced_other_c_negative_buckets)
                = kahanReduceResolution(
                    other.negative_spans, other.negative_buckets, reduced_other_c_negative_buckets, other.schema, schema);
            other_negative_spans = &reduced_other_negative_spans;
            other_negative_buckets = &reduced_other_negative_buckets;
            other_c_negative_buckets = &reduced_other_c_negative_buckets;
        }
    }

    std::tie(positive_spans, positive_buckets, c.positive_buckets) = kahanAddBuckets(
        schema, zero_threshold,
        std::move(positive_spans), std::move(positive_buckets),
        *other_positive_spans, *other_positive_buckets,
        std::move(c.positive_buckets), other_c_positive_buckets);
    std::tie(negative_spans, negative_buckets, c.negative_buckets) = kahanAddBuckets(
        schema, zero_threshold,
        std::move(negative_spans), std::move(negative_buckets),
        *other_negative_spans, *other_negative_buckets,
        std::move(c.negative_buckets), other_c_negative_buckets);

    c.schema = schema;
    c.zero_threshold = zero_threshold;
    c.positive_spans = positive_spans;
    c.negative_spans = negative_spans;

    outcome.updated_compensation = std::move(c);
    return outcome;
}

inline void TimeSeriesFloatHistogram::mul(Float64 factor)
{
    zero_count *= factor;
    count *= factor;
    sum *= factor;
    for (auto & bucket : positive_buckets)
        bucket *= factor;
    for (auto & bucket : negative_buckets)
        bucket *= factor;
    if (factor < 0)
        counter_reset_hint = TimeSeriesHistogramCounterResetHint::GaugeType;
}

inline void TimeSeriesFloatHistogram::div(Float64 scalar)
{
    zero_count /= scalar;
    count /= scalar;
    sum /= scalar;
    /// Division by zero removes all buckets.
    if (scalar == 0)
    {
        positive_buckets.clear();
        negative_buckets.clear();
        positive_spans.clear();
        negative_spans.clear();
        return;
    }
    for (auto & bucket : positive_buckets)
        bucket /= scalar;
    for (auto & bucket : negative_buckets)
        bucket /= scalar;
    if (scalar < 0)
        counter_reset_hint = TimeSeriesHistogramCounterResetHint::GaugeType;
}

inline bool TimeSeriesFloatHistogram::detectReset(const TimeSeriesFloatHistogram & previous) const
{
    namespace Hint = TimeSeriesHistogramCounterResetHint;
    if (counter_reset_hint == Hint::CounterReset)
        return true;
    if (counter_reset_hint == Hint::NotCounterReset)
        return false;
    /// In all other cases of the counter reset hint (UnknownCounterReset and GaugeType), go on
    /// with the detailed checks (upstream treats gauge histograms as counter histograms here).
    if (count < previous.count)
        return true;
    if (usesCustomBuckets())
    {
        if (!previous.usesCustomBuckets())
        {
            /// Something has changed or the application has been restarted; the schema change is
            /// handled directly in the chunks and PromQL functions upstream.
            return true;
        }
        if (!customBucketBoundsMatch(custom_values, previous.custom_values))
        {
            /// Custom bounds don't match - check if any reconciled bucket value has decreased.
            return detectResetWithMismatchedCustomBounds(previous, custom_values, previous.custom_values);
        }
    }
    if (schema > previous.schema)
        return true;
    if (zero_threshold < previous.zero_threshold)
    {
        /// ZeroThreshold decreased.
        return true;
    }
    Float64 previous_zero_count = 0;
    Float64 new_threshold = 0;
    std::tie(previous_zero_count, new_threshold, std::ignore) = previous.zeroCountForLargerThreshold(zero_threshold);
    if (new_threshold != zero_threshold)
    {
        /// ZeroThreshold is within a populated bucket in the previous histogram.
        return true;
    }
    if (zero_count < previous_zero_count)
        return true;
    {
        auto curr_it = bucketIterator(true, zero_threshold, schema);
        auto prev_it = previous.bucketIterator(true, zero_threshold, schema);
        if (detectResetInIterators(curr_it, prev_it))
            return true;
    }
    auto curr_it = bucketIterator(false, zero_threshold, schema);
    auto prev_it = previous.bucketIterator(false, zero_threshold, schema);
    return detectResetInIterators(curr_it, prev_it);
}

inline void TimeSeriesFloatHistogram::compact(int max_empty_buckets)
{
    compactBuckets(positive_buckets, positive_spans, max_empty_buckets);
    compactBuckets(negative_buckets, negative_spans, max_empty_buckets);
}

inline TimeSeriesFloatHistogram::BucketIterator
TimeSeriesFloatHistogram::bucketIterator(bool positive, Float64 absolute_start_value, Int32 target_schema) const
{
    if (usesCustomBuckets() && target_schema != schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot merge from custom buckets schema to exponential schema");
    if (!usesCustomBuckets() && target_schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA)
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot merge from exponential buckets schema to custom schema");
    if (target_schema > schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "cannot merge from schema {} to {}", schema, target_schema);

    BucketIterator it;
    it.schema = schema;
    it.target_schema = target_schema;
    it.positive = positive;
    it.absolute_start_value = absolute_start_value;
    it.bound_reached_start_value = (absolute_start_value == 0);
    if (positive)
    {
        it.spans = &positive_spans;
        it.buckets = &positive_buckets;
        it.custom_values = &custom_values;
    }
    else
    {
        it.spans = &negative_spans;
        it.buckets = &negative_buckets;
    }
    return it;
}

inline bool TimeSeriesFloatHistogram::BucketIterator::next()
{
    while (true)
    {
        if (!advance())
            return false;
        /// Skip buckets before absolute_start_value for exponential schemas (mirrors the recursive
        /// call in upstream's Next, converted to a loop).
        if (!bound_reached_start_value
            && target_schema >= HISTOGRAM_EXPONENTIAL_SCHEMA_MIN && target_schema <= HISTOGRAM_EXPONENTIAL_SCHEMA_MAX
            && getHistogramBoundExponential(curr_idx, target_schema) <= absolute_start_value)
            continue;
        bound_reached_start_value = true;
        return true;
    }
}

inline bool TimeSeriesFloatHistogram::BucketIterator::advance()
{
    if (spans_idx >= spans->size())
        return false;
    TimeSeriesHistogramSpan span = (*spans)[spans_idx];

    if (schema == target_schema)
    {
        /// Fast path for the common case.
        if (buckets_idx == 0)
        {
            /// Seed curr_idx for the first bucket.
            curr_idx = span.offset;
        }
        else
            ++curr_idx;
        if (buckets_idx >= buckets->size())
        {
            /// Protects against index out of range, which can only happen with an invalid histogram.
            return false;
        }

        while (idx_in_span >= span.length)
        {
            /// We have exhausted the current span and have to find a new one. We even handle
            /// pathologic spans of length 0 here.
            idx_in_span = 0;
            ++spans_idx;
            if (spans_idx >= spans->size())
                return false;
            span = (*spans)[spans_idx];
            curr_idx += span.offset;
        }

        curr_count = (*buckets)[buckets_idx];
        ++idx_in_span;
        ++buckets_idx;
        return true;
    }

    /// Copy all of these into local variables so that we can forward to the next bucket and then
    /// roll back if needed.
    Int64 orig_idx_local = orig_idx;
    size_t spans_idx_local = spans_idx;
    UInt32 idx_in_span_local = idx_in_span;
    bool first_pass = true;
    curr_count = 0;

    /// Merge together all buckets from the original schema that fall into one bucket in the target schema.
    while (true)
    {
        if (buckets_idx == 0)
        {
            /// Seed orig_idx for the first bucket.
            orig_idx_local = span.offset;
        }
        else
            ++orig_idx_local;
        if (buckets_idx >= buckets->size())
        {
            /// Protects against index out of range, which can only happen with an invalid histogram.
            if (first_pass)
                return false;
            break;
        }
        while (idx_in_span_local >= span.length)
        {
            /// We have exhausted the current span and have to find a new one. We even handle
            /// pathologic spans of length 0 here.
            idx_in_span_local = 0;
            ++spans_idx_local;
            if (spans_idx_local >= spans->size())
            {
                if (first_pass)
                    return false;
                goto merge_done;
            }
            span = (*spans)[spans_idx_local];
            orig_idx_local += span.offset;
        }
        {
            const Int64 curr_target_idx = targetIdx(orig_idx_local, schema, target_schema);
            if (first_pass)
            {
                curr_idx = curr_target_idx;
                first_pass = false;
            }
            else if (curr_target_idx != curr_idx)
            {
                /// Reached next bucket in target_schema. Do not actually forward to the next
                /// bucket, but break out.
                break;
            }
            curr_count += (*buckets)[buckets_idx];
            ++idx_in_span_local;
            ++buckets_idx;
            orig_idx = orig_idx_local;
            spans_idx = spans_idx_local;
            idx_in_span = idx_in_span_local;
            if (schema == target_schema)
            {
                /// Don't need to test the next bucket for mergeability if we have no schema change
                /// anyway. (Unreachable in this branch, kept for parity with upstream.)
                break;
            }
        }
    }
merge_done:
    return true;
}

inline Float64 TimeSeriesFloatHistogram::BucketIterator::bound(Int64 idx) const
{
    /// The port of `getBound` in model/histogram/generic.go (upstream panics on an out-of-bounds
    /// index; we throw).
    if (target_schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA)
    {
        const Int64 length = custom_values ? static_cast<Int64>(custom_values->size()) : 0;
        if (idx > length || idx < -1)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Native histogram bucket index {} is out of bounds for {} custom bucket bounds",
                idx, length);
        if (idx == length)
            return std::numeric_limits<Float64>::infinity();
        if (idx == -1)
            return -std::numeric_limits<Float64>::infinity();
        return (*custom_values)[static_cast<size_t>(idx)];
    }
    return getHistogramBoundExponential(idx, target_schema);
}

inline TimeSeriesFloatHistogram::ResolvedBucket TimeSeriesFloatHistogram::BucketIterator::at() const
{
    /// The port of `at` in model/histogram/generic.go, always with the target schema.
    ResolvedBucket bucket{0, 0, curr_count, curr_idx};
    if (positive)
    {
        bucket.upper = bound(curr_idx);
        bucket.lower = bound(curr_idx - 1);
    }
    else
    {
        bucket.lower = -bound(curr_idx);
        bucket.upper = -bound(curr_idx - 1);
    }
    return bucket;
}

inline std::tuple<Float64, Float64, Float64> TimeSeriesFloatHistogram::zeroCountForLargerThreshold(
    Float64 larger_threshold, const TimeSeriesFloatHistogram * compensation) const
{
    Float64 c_zero_count = compensation ? compensation->zero_count : 0;
    /// Fast path.
    if (larger_threshold == zero_threshold)
        return {zero_count, larger_threshold, c_zero_count};
    if (larger_threshold < zero_threshold)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram zero threshold {} is less than the previous zero threshold {}",
            larger_threshold, zero_threshold);

    Float64 h_zero_count = 0;
    while (true)  /// `outer` in upstream
    {
        h_zero_count = zero_count;
        bool redo = false;
        {
            auto it = bucketIterator(true, 0, schema);
            size_t buckets_idx = 0;
            while (it.next())
            {
                const ResolvedBucket b = it.at();
                if (b.lower >= larger_threshold)
                    break;
                /// Bucket to be merged into zero bucket.
                std::tie(h_zero_count, c_zero_count) = kahanInc(b.count, h_zero_count, c_zero_count);
                if (compensation)
                    std::tie(h_zero_count, c_zero_count)
                        = kahanInc(compensation->positive_buckets[buckets_idx], h_zero_count, c_zero_count);
                if (b.upper > larger_threshold)
                {
                    /// The new threshold ended up within a bucket. If it's populated, we need to
                    /// adjust larger_threshold before we are done here.
                    if (b.count != 0)
                        larger_threshold = b.upper;
                    break;
                }
                ++buckets_idx;
            }
        }
        {
            auto it = bucketIterator(false, 0, schema);
            size_t buckets_idx = 0;
            while (it.next())
            {
                const ResolvedBucket b = it.at();
                if (b.upper <= -larger_threshold)
                    break;
                /// Bucket to be merged into zero bucket.
                std::tie(h_zero_count, c_zero_count) = kahanInc(b.count, h_zero_count, c_zero_count);
                if (compensation)
                    std::tie(h_zero_count, c_zero_count)
                        = kahanInc(compensation->negative_buckets[buckets_idx], h_zero_count, c_zero_count);
                if (b.lower < -larger_threshold)
                {
                    /// The new threshold ended up within a bucket: if populated, adjust larger_threshold
                    /// and redo, because the treatment of the positive buckets is invalid now.
                    if (b.count != 0)
                    {
                        larger_threshold = -b.lower;
                        redo = true;
                    }
                    break;
                }
                ++buckets_idx;
            }
        }
        if (!redo)
            return {h_zero_count, larger_threshold, c_zero_count};
    }
}

inline TimeSeriesFloatHistogram TimeSeriesFloatHistogram::newCompensationHistogram() const
{
    TimeSeriesFloatHistogram c;
    c.counter_reset_hint = counter_reset_hint;
    c.schema = schema;
    c.zero_threshold = zero_threshold;
    c.custom_values = custom_values;
    c.positive_buckets.assign(positive_buckets.size(), 0.0);
    c.positive_spans = positive_spans;
    c.negative_spans = negative_spans;
    if (!usesCustomBuckets())
        c.negative_buckets.assign(negative_buckets.size(), 0.0);
    return c;
}

inline void TimeSeriesFloatHistogram::kahanCompact(int max_empty_buckets, TimeSeriesFloatHistogram & compensation)
{
    compactBuckets(positive_buckets, positive_spans, max_empty_buckets, &compensation.positive_buckets);
    compactBuckets(negative_buckets, negative_spans, max_empty_buckets, &compensation.negative_buckets);
}

inline void TimeSeriesFloatHistogram::trimBucketsInZeroBucket(TimeSeriesFloatHistogram * compensation)
{
    {
        auto it = bucketIterator(true, 0, schema);
        size_t buckets_idx = 0;
        while (it.next())
        {
            const ResolvedBucket b = it.at();
            if (b.lower >= zero_threshold)
                break;
            positive_buckets[buckets_idx] = 0;
            if (compensation)
                compensation->positive_buckets[buckets_idx] = 0;
            ++buckets_idx;
        }
    }
    {
        auto it = bucketIterator(false, 0, schema);
        size_t buckets_idx = 0;
        while (it.next())
        {
            const ResolvedBucket b = it.at();
            if (b.upper <= -zero_threshold)
                break;
            negative_buckets[buckets_idx] = 0;
            if (compensation)
                compensation->negative_buckets[buckets_idx] = 0;
            ++buckets_idx;
        }
    }
    /// We are abusing compact to trim the buckets set to zero above. Premature compacting could
    /// cause additional cost, but this code path is probably rarely used anyway.
    if (compensation)
        kahanCompact(0, *compensation);
    else
        compact(0);
}

inline std::pair<Float64, Float64> TimeSeriesFloatHistogram::reconcileZeroBuckets(
    const TimeSeriesFloatHistogram & other, TimeSeriesFloatHistogram * compensation)
{
    Float64 other_zero_count = other.zero_count;
    Float64 other_c_zero_count = 0;
    Float64 other_zero_threshold = other.zero_threshold;

    while (other_zero_threshold != zero_threshold)
    {
        if (zero_threshold > other_zero_threshold)
            std::tie(other_zero_count, other_zero_threshold, other_c_zero_count)
                = other.zeroCountForLargerThreshold(zero_threshold);
        if (other_zero_threshold > zero_threshold)
        {
            Float64 c_zero_count = 0;
            std::tie(zero_count, zero_threshold, c_zero_count) = zeroCountForLargerThreshold(other_zero_threshold, compensation);
            if (compensation)
                compensation->zero_count = c_zero_count;
            trimBucketsInZeroBucket(compensation);
        }
    }
    return {other_zero_count, other_c_zero_count};
}

inline void TimeSeriesFloatHistogram::checkSchemaAndBounds(const TimeSeriesFloatHistogram & other) const
{
    if (usesCustomBuckets() != other.usesCustomBuckets())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "cannot apply this operation on histograms with a mix of exponential and custom bucket schemas");
}

inline bool TimeSeriesFloatHistogram::adjustCounterReset(const TimeSeriesFloatHistogram & other)
{
    namespace Hint = TimeSeriesHistogramCounterResetHint;
    if (other.counter_reset_hint == counter_reset_hint)
    {
        /// Adding apples to apples, all good. No need to change anything.
        return false;
    }
    if (counter_reset_hint == Hint::GaugeType)
    {
        /// Adding something else to a gauge. That's probably OK. Outcome is a gauge. Nothing to do
        /// since the receiver is already marked as gauge.
        return false;
    }
    if (other.counter_reset_hint == Hint::GaugeType)
    {
        /// Similar to before, but this time the receiver is "something else" and we have to change
        /// it to gauge.
        counter_reset_hint = Hint::GaugeType;
        return false;
    }
    if (counter_reset_hint == Hint::UnknownCounterReset)
    {
        /// With the receiver's hint "unknown" this could still be legitimate; the outcome is "unknown"
        /// and the receiver is already marked as such, so there is nothing to do.
        return false;
    }
    if (other.counter_reset_hint == Hint::UnknownCounterReset)
    {
        /// Similar to before, but now we have to set the receiver's counter reset hint to "unknown".
        counter_reset_hint = Hint::UnknownCounterReset;
        return false;
    }
    /// All other cases are a direct CounterReset/NotCounterReset collision: conservatively set the hint
    /// to "unknown" and report a collision (upstream additionally warns the query user).
    counter_reset_hint = Hint::UnknownCounterReset;
    return true;
}

inline bool TimeSeriesFloatHistogram::detectResetInIterators(BucketIterator & curr_it, BucketIterator & prev_it)
{
    if (!prev_it.next())
        return false;   /// If no buckets in the previous histogram, nothing can be reset.
    auto [prev_count, prev_index] = prev_it.strippedAt();
    if (!curr_it.next())
    {
        /// No bucket in the current histogram, but at least one in the previous histogram. Check
        /// if any of those are non-zero, in which case this is a reset.
        while (true)
        {
            if (prev_count != 0)
                return true;
            if (!prev_it.next())
                return false;
            std::tie(prev_count, prev_index) = prev_it.strippedAt();
        }
    }
    auto [curr_count, curr_index] = curr_it.strippedAt();
    while (true)
    {
        /// Forward curr_it until we find the bucket corresponding to the previous bucket.
        while (curr_index < prev_index)
        {
            if (!curr_it.next())
            {
                /// Reached the end of curr_it early: unless all remaining buckets of the previous
                /// histogram are unpopulated, this is a reset.
                while (true)
                {
                    if (prev_count != 0)
                        return true;
                    if (!prev_it.next())
                        return false;
                    std::tie(prev_count, prev_index) = prev_it.strippedAt();
                }
            }
            std::tie(curr_count, curr_index) = curr_it.strippedAt();
        }
        if (curr_index > prev_index)
        {
            /// The previous histogram has a bucket the current one does not have. If it's
            /// populated, it's a reset.
            if (prev_count != 0)
                return true;
        }
        else
        {
            /// We have reached corresponding buckets in both iterators. We can finally compare
            /// the counts.
            if (curr_count < prev_count)
                return true;
        }
        if (!prev_it.next())
        {
            /// Reached the end of prev_it without finding offending buckets.
            return false;
        }
        std::tie(prev_count, prev_index) = prev_it.strippedAt();
    }
}

inline bool TimeSeriesFloatHistogram::detectResetWithMismatchedCustomBounds(
    const TimeSeriesFloatHistogram & previous,
    const std::vector<Float64> & curr_bounds, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & prev_bounds) const /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    if (schema != HISTOGRAM_CUSTOM_BUCKETS_SCHEMA || previous.schema != HISTOGRAM_CUSTOM_BUCKETS_SCHEMA)
        throw Exception(ErrorCodes::INCORRECT_DATA, "detectResetWithMismatchedCustomBounds called with non-NHCB schema");

    auto curr_it = bucketIterator(true, 0, HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
    auto prev_it = previous.bucketIterator(true, 0, HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);

    struct RollupSumResult
    {
        Float64 sum;
        ResolvedBucket bucket;
        bool has_more;
    };

    /// Rolls up the iterator's buckets with an upper bound at or below `bound` and returns their
    /// sum, the first bucket above `bound`, and whether there is such a bucket.
    auto rollup_sum_for_bound = [](BucketIterator & it, bool iter_started, ResolvedBucket iter_bucket, Float64 bound) -> RollupSumResult
    {
        if (!iter_started)
        {
            if (!it.next())
                return {0, ResolvedBucket{0, 0, 0, 0}, false};
            iter_bucket = it.at();
        }
        Float64 total = 0;
        while (iter_bucket.upper <= bound)
        {
            total += iter_bucket.count;
            if (!it.next())
                return {total, ResolvedBucket{0, 0, 0, 0}, false};
            iter_bucket = it.at();
        }
        return {total, iter_bucket, true};
    };

    size_t curr_bound_idx = 0;
    size_t prev_bound_idx = 0;
    ResolvedBucket curr_bucket{0, 0, 0, 0};
    ResolvedBucket prev_bucket{0, 0, 0, 0};
    bool curr_iter_started = false;
    bool curr_has_more = false;
    bool prev_iter_started = false;
    bool prev_has_more = false;

    const Float64 infinity = std::numeric_limits<Float64>::infinity();
    while (curr_bound_idx <= curr_bounds.size() && prev_bound_idx <= prev_bounds.size())
    {
        const Float64 curr_bound = curr_bound_idx < curr_bounds.size() ? curr_bounds[curr_bound_idx] : infinity;
        const Float64 prev_bound = prev_bound_idx < prev_bounds.size() ? prev_bounds[prev_bound_idx] : infinity;

        if (curr_bound == prev_bound)
        {
            /// Check matching bound, rolling up lesser buckets that have not been accounted for yet.
            Float64 curr_rollup_sum = 0;
            if (!curr_iter_started || curr_has_more)
            {
                const auto result = rollup_sum_for_bound(curr_it, curr_iter_started, curr_bucket, curr_bound);
                curr_rollup_sum = result.sum;
                curr_bucket = result.bucket;
                curr_has_more = result.has_more;
                curr_iter_started = true;
            }

            Float64 prev_rollup_sum = 0;
            if (!prev_iter_started || prev_has_more)
            {
                const auto result = rollup_sum_for_bound(prev_it, prev_iter_started, prev_bucket, curr_bound);
                prev_rollup_sum = result.sum;
                prev_bucket = result.bucket;
                prev_has_more = result.has_more;
                prev_iter_started = true;
            }

            if (curr_rollup_sum < prev_rollup_sum)
                return true;

            ++curr_bound_idx;
            ++prev_bound_idx;
        }
        else if (curr_bound < prev_bound)
            ++curr_bound_idx;
        else
            ++prev_bound_idx;
    }

    return false;
}

inline std::pair<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>> TimeSeriesFloatHistogram::mustReduceResolution( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> & origin_spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & origin_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int32 origin_schema,
    Int32 target_schema)
{
    std::vector<TimeSeriesHistogramSpan> target_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> target_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int64 bucket_idx = 0;       /// The index of the bucket in the origin schema.
    size_t bucket_count_idx = 0;  /// The position of a bucket in the origin bucket slice.
    Int64 last_target_bucket_idx = 0;

    for (size_t n = 0; n < origin_spans.size(); ++n)
    {
        const auto & span = origin_spans[n];
        if (n > 0 && span.offset < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram span number {} has a negative offset {}", n + 1, span.offset);
        /// Determine the index of the first bucket in this span.
        bucket_idx += span.offset;
        for (UInt32 j = 0; j < span.length; ++j)
        {
            /// Protect against too few buckets in the origin.
            if (bucket_count_idx >= origin_buckets.size())
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Native histogram has {} bucket values but its spans cover more",
                    origin_buckets.size());

            /// Determine the index of the bucket in the target schema from the index in the
            /// original schema.
            const Int64 target_bucket_idx = targetIdx(bucket_idx, origin_schema, target_schema);

            if (target_spans.empty())
            {
                /// This is the first span in target_spans.
                target_spans.push_back(TimeSeriesHistogramSpan{static_cast<Int32>(target_bucket_idx), 1});
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
                last_target_bucket_idx = target_bucket_idx;
            }
            else if (last_target_bucket_idx == target_bucket_idx)
            {
                /// The current bucket has to be merged into the same target bucket as the previous bucket.
                target_buckets.back() += origin_buckets[bucket_count_idx];
            }
            else if (last_target_bucket_idx + 1 == target_bucket_idx)
            {
                /// The current bucket has to go into a new target bucket, and that bucket is next
                /// to the previous target bucket, so we add it to the current target span.
                ++target_spans.back().length;
                ++last_target_bucket_idx;
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
            }
            else if (last_target_bucket_idx + 1 < target_bucket_idx)
            {
                /// The current bucket goes into a new target bucket separated by a gap from the
                /// previous target bucket, so we need to add a new target span.
                target_spans.push_back(TimeSeriesHistogramSpan{static_cast<Int32>(target_bucket_idx - last_target_bucket_idx - 1), 1});
                last_target_bucket_idx = target_bucket_idx;
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
            }
            /// (No else: mirrors the upstream switch without a default.)
            ++bucket_idx;
            ++bucket_count_idx;
        }
    }
    if (bucket_count_idx != origin_buckets.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram spans cover {} bucket values but {} were provided",
            bucket_count_idx, origin_buckets.size());
    return {std::move(target_spans), std::move(target_buckets)};
}

inline std::pair<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>> TimeSeriesFloatHistogram::addBuckets( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int32 schema,
    Float64 threshold,
    bool negative,
    std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & buckets_b) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    const bool exponential_schema = schema >= HISTOGRAM_EXPONENTIAL_SCHEMA_MIN && schema <= HISTOGRAM_EXPONENTIAL_SCHEMA_MAX;

    Int64 i_span = -1;
    Int64 i_bucket = -1;
    Int64 i_in_span = 0;
    Int64 index_a = 0;
    Int64 index_b = 0;
    size_t b_idx_b = 0;
    Float64 bucket_b = 0;
    Int64 delta_index = 0;
    bool lower_than_threshold = true;

    for (const auto & span_b : spans_b)
    {
        index_b += span_b.offset;
        for (UInt32 j = 0; j < span_b.length; ++j)
        {
            if (lower_than_threshold && exponential_schema && getHistogramBoundExponential(index_b, schema) <= threshold)
                goto next_loop;
            lower_than_threshold = false;

            bucket_b = buckets_b[b_idx_b];
            if (negative)
                bucket_b *= -1;

            if (i_span == -1)
            {
                if (spans_a.empty() || spans_a[0].offset > index_b)
                {
                    /// Add bucket before all others.
                    buckets_a.insert(buckets_a.begin(), bucket_b);
                    if (!spans_a.empty() && spans_a[0].offset == index_b + 1)
                    {
                        ++spans_a[0].length;
                        --spans_a[0].offset;
                        goto next_loop;
                    }
                    spans_a.insert(spans_a.begin(), TimeSeriesHistogramSpan{static_cast<Int32>(index_b), 1});
                    if (spans_a.size() > 1)
                    {
                        /// Convert the absolute offset in the formerly first span to a relative offset.
                        spans_a[1].offset -= static_cast<Int32>(index_b + 1);
                    }
                    goto next_loop;
                }
                if (spans_a[0].offset == index_b)
                {
                    /// Just add to first bucket.
                    buckets_a[0] += bucket_b;
                    goto next_loop;
                }
                i_span = 0;
                i_bucket = 0;
                i_in_span = 0;
                index_a = spans_a[0].offset;
            }
            delta_index = index_b - index_a;
            while (true)
            {
                const Int64 remaining_in_span = static_cast<Int64>(spans_a[i_span].length) - i_in_span;
                if (delta_index < remaining_in_span)
                {
                    /// Bucket is in the current span.
                    i_bucket += delta_index;
                    i_in_span += delta_index;
                    buckets_a[i_bucket] += bucket_b;
                    break;
                }
                delta_index -= remaining_in_span;
                i_bucket += remaining_in_span;
                ++i_span;
                if (i_span == static_cast<Int64>(spans_a.size()) || delta_index < spans_a[i_span].offset)
                {
                    /// Bucket is in the gap behind the previous span (or there are no further spans).
                    buckets_a.insert(buckets_a.begin() + i_bucket, bucket_b);
                    if (delta_index == 0)
                    {
                        /// Directly after the previous span, extend the previous span.
                        if (i_span < static_cast<Int64>(spans_a.size()))
                            --spans_a[i_span].offset;
                        --i_span;
                        i_in_span = spans_a[i_span].length;
                        ++spans_a[i_span].length;
                        goto next_loop;
                    }
                    if (i_span < static_cast<Int64>(spans_a.size()) && delta_index == spans_a[i_span].offset - 1)
                    {
                        /// Directly before the next span, extend the next span.
                        i_in_span = 0;
                        --spans_a[i_span].offset;
                        ++spans_a[i_span].length;
                        goto next_loop;
                    }
                    /// No next span, or the next span is not directly adjacent to the new bucket.
                    /// Add a new span.
                    i_in_span = 0;
                    if (i_span < static_cast<Int64>(spans_a.size()))
                        spans_a[i_span].offset -= static_cast<Int32>(delta_index + 1);
                    spans_a.insert(spans_a.begin() + i_span, TimeSeriesHistogramSpan{static_cast<Int32>(delta_index), 1});
                    goto next_loop;
                }
                /// Try the start of the next span.
                delta_index -= spans_a[i_span].offset;
                i_in_span = 0;
            }

        next_loop:
            index_a = index_b;
            ++index_b;
            ++b_idx_b;
        }
    }

    return {std::move(spans_a), std::move(buckets_a)};
}

inline void TimeSeriesFloatHistogram::compactBuckets(
    std::vector<Float64> & buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<TimeSeriesHistogramSpan> & spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    int max_empty_buckets,
    std::vector<Float64> * compensation_buckets) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    if (compensation_buckets && compensation_buckets->size() != buckets.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram bucket layout ({} buckets) mismatch against the associated compensation buckets layout ({} buckets)",
            buckets.size(), compensation_buckets->size());

    /// Fast path: no empty buckets AND no span offset <= max_empty_buckets AND no zero-length span
    /// means nothing to do (checked first because it is cheap and presumably common).
    bool nothing_to_do = true;
    for (const Float64 bucket : buckets)
    {
        if (bucket == 0)
        {
            nothing_to_do = false;
            break;
        }
    }
    if (nothing_to_do)
    {
        for (const auto & span : spans)
        {
            if (span.offset <= max_empty_buckets || span.length == 0)
            {
                nothing_to_do = false;
                break;
            }
        }
        if (nothing_to_do)
            return;
    }

    size_t i_bucket = 0;
    size_t i_span = 0;
    UInt32 pos_in_span = 0;
    Float64 current_bucket_absolute = 0;

    /// The number of consecutive empty buckets from the current position, bounded by the end of the current
    /// span (upstream's emptyBucketsHere; a bucket counts as empty iff both its count and its compensation are zero).
    auto empty_buckets_here = [&]() -> size_t
    {
        size_t i = 0;
        Float64 absolute = current_bucket_absolute;
        Float64 comp = 0;
        if (compensation_buckets)
            comp = (*compensation_buckets)[i_bucket];
        while (i + pos_in_span < spans[i_span].length && absolute == 0 && comp == 0)
        {
            ++i;
            if (i + i_bucket >= buckets.size())
                break;
            absolute = buckets[i + i_bucket];
            if (compensation_buckets)
                comp = (*compensation_buckets)[i + i_bucket];
        }
        return i;
    };

    /// Merge spans with zero offset to avoid special cases later.
    if (spans.size() > 1)
    {
        size_t write_span = 0;
        for (size_t i = 1; i < spans.size(); ++i)
        {
            const auto span = spans[i];
            if (span.offset == 0)
            {
                spans[write_span].length += span.length;
                continue;
            }
            ++write_span;
            if (i != write_span)
                spans[write_span] = span;
        }
        spans.resize(write_span + 1);
    }
    i_span = 0;

    /// Merge spans with zero length to avoid special cases later.
    {
        size_t write_span = 0;
        for (size_t i = 0; i < spans.size(); ++i)
        {
            const auto span = spans[i];
            if (span.length == 0)
            {
                if (i + 1 < spans.size())
                    spans[i + 1].offset += span.offset;
                continue;
            }
            if (i != write_span)
                spans[write_span] = span;
            ++write_span;
        }
        spans.resize(write_span);
    }
    i_span = 0;

    /// If all spans were zero-length, no buckets remain valid.
    if (spans.empty())
    {
        buckets.clear();
        if (compensation_buckets)
            compensation_buckets->clear();
        return;
    }

    /// Cut out empty buckets at the start and end of spans unconditionally; in the middle of a span
    /// only when there are more than max_empty_buckets consecutive empty buckets.
    while (i_bucket < buckets.size() && i_span < spans.size())
    {
        current_bucket_absolute = buckets[i_bucket];
        const size_t n_empty = empty_buckets_here();
        if (n_empty > 0)
        {
            if (pos_in_span > 0
                && n_empty < static_cast<size_t>(spans[i_span].length - pos_in_span)
                && n_empty <= static_cast<size_t>(max_empty_buckets))
            {
                /// The empty buckets are in the middle of a span, and there are few enough to not
                /// bother. Just fast-forward.
                i_bucket += n_empty;
                pos_in_span += static_cast<UInt32>(n_empty);
                continue;
            }
            /// In all other cases, we cut out the empty buckets.
            buckets.erase(buckets.begin() + i_bucket, buckets.begin() + i_bucket + n_empty);
            if (compensation_buckets)
                compensation_buckets->erase(
                    compensation_buckets->begin() + i_bucket, compensation_buckets->begin() + i_bucket + n_empty);
            if (pos_in_span == 0)
            {
                /// Start of span.
                if (n_empty == spans[i_span].length)
                {
                    /// The whole span is empty.
                    const Int32 offset = spans[i_span].offset;
                    spans.erase(spans.begin() + i_span);
                    if (spans.size() > i_span)
                        spans[i_span].offset += offset + static_cast<Int32>(n_empty);
                    continue;
                }
                spans[i_span].length -= static_cast<UInt32>(n_empty);
                spans[i_span].offset += static_cast<Int32>(n_empty);
                continue;
            }
            /// It's in the middle or in the end of the span. Split the current span.
            TimeSeriesHistogramSpan new_span{
                static_cast<Int32>(n_empty),
                spans[i_span].length - pos_in_span - static_cast<UInt32>(n_empty)};
            spans[i_span].length = pos_in_span;
            /// In any case, we have to split to the next span.
            ++i_span;
            pos_in_span = 0;
            if (new_span.length == 0)
            {
                /// The span is empty, so we were already at the end of a span. We don't have to
                /// insert the new span, just adjust the next span's offset, if there is one.
                if (i_span < spans.size())
                    spans[i_span].offset += static_cast<Int32>(n_empty);
                continue;
            }
            /// Insert the new span.
            spans.insert(spans.begin() + i_span, new_span);
            continue;
        }
        ++i_bucket;
        ++pos_in_span;
        if (pos_in_span >= spans[i_span].length)
        {
            pos_in_span = 0;
            ++i_span;
        }
    }
    if (max_empty_buckets == 0 || buckets.empty())
        return;

    /// Finally, check if any offsets between spans are small enough to merge the spans.
    i_bucket = spans[0].length;
    i_span = 1;
    while (i_span < spans.size())
    {
        if (spans[i_span].offset > max_empty_buckets)
        {
            i_bucket += spans[i_span].length;
            ++i_span;
            continue;
        }
        /// Merge the span with the previous one and insert empty buckets.
        const size_t offset = static_cast<size_t>(spans[i_span].offset);
        const size_t length = spans[i_span].length;
        spans[i_span - 1].length += static_cast<UInt32>(offset) + spans[i_span].length;
        spans.erase(spans.begin() + i_span);
        std::vector<Float64> new_buckets(buckets.size() + offset, 0.0); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::copy(buckets.begin(), buckets.begin() + i_bucket, new_buckets.begin());
        std::copy(buckets.begin() + i_bucket, buckets.end(), new_buckets.begin() + i_bucket + offset);
        buckets = std::move(new_buckets);
        if (compensation_buckets)
        {
            std::vector<Float64> new_compensation_buckets(compensation_buckets->size() + offset, 0.0); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
            std::copy(compensation_buckets->begin(), compensation_buckets->begin() + i_bucket, new_compensation_buckets.begin());
            std::copy(
                compensation_buckets->begin() + i_bucket, compensation_buckets->end(),
                new_compensation_buckets.begin() + i_bucket + offset);
            *compensation_buckets = std::move(new_compensation_buckets);
        }
        i_bucket += offset;
        /// The buckets of the merged span are now part of the previous span, so i_bucket has to
        /// skip them as well to keep pointing at the first bucket of the span we look at next.
        i_bucket += length;
        /// Note that with many merges, it would be more efficient to first record all the chunks of
        /// empty buckets to insert and then do it in one go through all the buckets.
    }
}

inline std::vector<Float64> TimeSeriesFloatHistogram::intersectCustomBucketBounds( /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & bounds_a, const std::vector<Float64> & bounds_b) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<Float64> result; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (bounds_a.empty() || bounds_b.empty())
        return result;

    size_t i = 0;
    size_t j = 0;
    while (i < bounds_a.size() && j < bounds_b.size())
    {
        if (bounds_a[i] == bounds_b[j])
        {
            result.push_back(bounds_a[i]);
            ++i;
            ++j;
        }
        else if (bounds_a[i] < bounds_b[j])
            ++i;
        else
            ++j;
    }
    return result;
}

inline std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> /// STYLE_CHECK_ALLOW_STD_CONTAINERS
TimeSeriesFloatHistogram::addCustomBucketsWithMismatches(
    bool negative,
    std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & bounds_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & buckets_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & bounds_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * buckets_c, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & intersected_bounds) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<Float64> target_buckets(intersected_bounds.size() + 1, 0.0); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> c_target_buckets(intersected_bounds.size() + 1, 0.0); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    auto map_buckets = [&](const std::vector<TimeSeriesHistogramSpan> & spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
                           const std::vector<Float64> & buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
                           const std::vector<Float64> & bounds, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
                           bool negative_,
                           bool with_compensation)
    {
        Int64 src_idx = 0;
        size_t bucket_idx = 0;
        size_t intersect_idx = 0;

        for (const auto & span : spans)
        {
            src_idx += span.offset;
            for (UInt32 k = 0; k < span.length; ++k)
            {
                if (bucket_idx < buckets.size())
                {
                    const Float64 value = buckets[bucket_idx];

                    /// Find the target bucket index.
                    size_t target_idx = target_buckets.size() - 1;  /// Default to the +Inf bucket.
                    if (src_idx < static_cast<Int64>(bounds.size()))
                    {
                        const Float64 src_bound = bounds[static_cast<size_t>(src_idx)];
                        /// Since both arrays are sorted, we can continue from where we left off.
                        while (intersect_idx < intersected_bounds.size())
                        {
                            if (intersected_bounds[intersect_idx] >= src_bound)
                            {
                                target_idx = intersect_idx;
                                break;
                            }
                            ++intersect_idx;
                        }
                    }

                    if (negative_)
                        std::tie(target_buckets[target_idx], c_target_buckets[target_idx])
                            = kahanDec(value, target_buckets[target_idx], c_target_buckets[target_idx]);
                    else
                    {
                        std::tie(target_buckets[target_idx], c_target_buckets[target_idx])
                            = kahanInc(value, target_buckets[target_idx], c_target_buckets[target_idx]);
                        if (with_compensation && buckets_c)
                            std::tie(target_buckets[target_idx], c_target_buckets[target_idx])
                                = kahanInc((*buckets_c)[bucket_idx], target_buckets[target_idx], c_target_buckets[target_idx]);
                    }
                }
                ++src_idx;
                ++bucket_idx;
            }
        }
    };

    /// Map the histograms to the intersected layout.
    map_buckets(spans_a, buckets_a, bounds_a, false, true);
    map_buckets(spans_b, buckets_b, bounds_b, negative, false);

    /// Build spans and buckets, excluding zero-valued buckets from the final result. (Upstream
    /// reuses the capacity of spans_a and the target slices; the observable result is identical.)
    std::vector<TimeSeriesHistogramSpan> dest_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> dest_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> c_dest_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int64 last_idx = -1;

    for (size_t i = 0; i < target_buckets.size(); ++i)
    {
        if (target_buckets[i] == 0 && c_target_buckets[i] == 0)
            continue;

        dest_buckets.push_back(target_buckets[i]);
        c_dest_buckets.push_back(c_target_buckets[i]);
        const Int64 idx = static_cast<Int64>(i);

        if (!dest_spans.empty() && idx == last_idx + 1)
        {
            /// Consecutive bucket, extend the last span.
            ++dest_spans.back().length;
        }
        else
        {
            /// New span needed.
            Int64 offset = idx;
            if (!dest_spans.empty())
            {
                /// Convert to a relative offset from the end of the last span.
                offset = idx - last_idx - 1;
            }
            dest_spans.push_back(TimeSeriesHistogramSpan{static_cast<Int32>(offset), 1});
        }
        last_idx = idx;
    }

    return {std::move(dest_spans), std::move(dest_buckets), std::move(c_dest_buckets)};
}

inline std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> /// STYLE_CHECK_ALLOW_STD_CONTAINERS
TimeSeriesFloatHistogram::kahanAddBuckets(
    Int32 schema,
    Float64 threshold,
    std::vector<TimeSeriesHistogramSpan> spans_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<TimeSeriesHistogramSpan> & spans_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & buckets_b, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> compensation_buckets_a, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> * compensation_buckets_b) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    const bool exponential_schema = schema >= HISTOGRAM_EXPONENTIAL_SCHEMA_MIN && schema <= HISTOGRAM_EXPONENTIAL_SCHEMA_MAX;

    Int64 i_span = -1;
    Int64 i_bucket = -1;
    Int64 i_in_span = 0;
    Int64 index_a = 0;
    Int64 index_b = 0;
    size_t b_idx_b = 0;
    Float64 bucket_b = 0;
    Float64 compensation_bucket_b = 0;
    Int64 delta_index = 0;
    bool lower_than_threshold = true;

    for (const auto & span_b : spans_b)
    {
        index_b += span_b.offset;
        for (UInt32 j = 0; j < span_b.length; ++j)
        {
            if (lower_than_threshold && exponential_schema && getHistogramBoundExponential(index_b, schema) <= threshold)
                goto next_loop;
            lower_than_threshold = false;

            bucket_b = buckets_b[b_idx_b];
            if (compensation_buckets_b)
                compensation_bucket_b = (*compensation_buckets_b)[b_idx_b];

            if (i_span == -1)
            {
                if (spans_a.empty() || spans_a[0].offset > index_b)
                {
                    /// Add bucket before all others.
                    buckets_a.insert(buckets_a.begin(), bucket_b);
                    compensation_buckets_a.insert(compensation_buckets_a.begin(), compensation_bucket_b);
                    if (!spans_a.empty() && spans_a[0].offset == index_b + 1)
                    {
                        ++spans_a[0].length;
                        --spans_a[0].offset;
                        goto next_loop;
                    }
                    spans_a.insert(spans_a.begin(), TimeSeriesHistogramSpan{static_cast<Int32>(index_b), 1});
                    if (spans_a.size() > 1)
                    {
                        /// Convert the absolute offset in the formerly first span to a relative offset.
                        spans_a[1].offset -= static_cast<Int32>(index_b + 1);
                    }
                    goto next_loop;
                }
                if (spans_a[0].offset == index_b)
                {
                    /// Just add to first bucket.
                    std::tie(buckets_a[0], compensation_buckets_a[0])
                        = kahanInc(bucket_b, buckets_a[0], compensation_buckets_a[0]);
                    if (compensation_bucket_b != 0)
                        std::tie(buckets_a[0], compensation_buckets_a[0])
                            = kahanInc(compensation_bucket_b, buckets_a[0], compensation_buckets_a[0]);
                    goto next_loop;
                }
                i_span = 0;
                i_bucket = 0;
                i_in_span = 0;
                index_a = spans_a[0].offset;
            }
            delta_index = index_b - index_a;
            while (true)
            {
                const Int64 remaining_in_span = static_cast<Int64>(spans_a[i_span].length) - i_in_span;
                if (delta_index < remaining_in_span)
                {
                    /// Bucket is in the current span.
                    i_bucket += delta_index;
                    i_in_span += delta_index;
                    std::tie(buckets_a[i_bucket], compensation_buckets_a[i_bucket])
                        = kahanInc(bucket_b, buckets_a[i_bucket], compensation_buckets_a[i_bucket]);
                    if (compensation_bucket_b != 0)
                        std::tie(buckets_a[i_bucket], compensation_buckets_a[i_bucket])
                            = kahanInc(compensation_bucket_b, buckets_a[i_bucket], compensation_buckets_a[i_bucket]);
                    break;
                }
                delta_index -= remaining_in_span;
                i_bucket += remaining_in_span;
                ++i_span;
                if (i_span == static_cast<Int64>(spans_a.size()) || delta_index < spans_a[i_span].offset)
                {
                    /// Bucket is in the gap behind the previous span (or there are no further spans).
                    buckets_a.insert(buckets_a.begin() + i_bucket, bucket_b);
                    compensation_buckets_a.insert(compensation_buckets_a.begin() + i_bucket, compensation_bucket_b);
                    if (delta_index == 0)
                    {
                        /// Directly after the previous span, extend the previous span.
                        if (i_span < static_cast<Int64>(spans_a.size()))
                            --spans_a[i_span].offset;
                        --i_span;
                        i_in_span = spans_a[i_span].length;
                        ++spans_a[i_span].length;
                        goto next_loop;
                    }
                    if (i_span < static_cast<Int64>(spans_a.size()) && delta_index == spans_a[i_span].offset - 1)
                    {
                        /// Directly before the next span, extend the next span.
                        i_in_span = 0;
                        --spans_a[i_span].offset;
                        ++spans_a[i_span].length;
                        goto next_loop;
                    }
                    /// No next span, or the next span is not directly adjacent to the new bucket.
                    /// Add a new span.
                    i_in_span = 0;
                    if (i_span < static_cast<Int64>(spans_a.size()))
                        spans_a[i_span].offset -= static_cast<Int32>(delta_index + 1);
                    spans_a.insert(spans_a.begin() + i_span, TimeSeriesHistogramSpan{static_cast<Int32>(delta_index), 1});
                    goto next_loop;
                }
                /// Try the start of the next span.
                delta_index -= spans_a[i_span].offset;
                i_in_span = 0;
            }

        next_loop:
            index_a = index_b;
            ++index_b;
            ++b_idx_b;
        }
    }

    return {std::move(spans_a), std::move(buckets_a), std::move(compensation_buckets_a)};
}

inline std::tuple<std::vector<TimeSeriesHistogramSpan>, std::vector<Float64>, std::vector<Float64>> /// STYLE_CHECK_ALLOW_STD_CONTAINERS
TimeSeriesFloatHistogram::kahanReduceResolution(
    const std::vector<TimeSeriesHistogramSpan> & origin_spans, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & origin_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    const std::vector<Float64> & origin_compensation_buckets, /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int32 origin_schema,
    Int32 target_schema)
{
    if (origin_compensation_buckets.size() != origin_buckets.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram bucket layout ({} buckets) mismatch against the associated compensation buckets layout ({} buckets)",
            origin_buckets.size(), origin_compensation_buckets.size());

    std::vector<TimeSeriesHistogramSpan> target_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> target_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Float64> target_compensation_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Int64 bucket_idx = 0;         /// The index of the bucket in the origin schema.
    size_t bucket_count_idx = 0;  /// The position of a bucket in the origin bucket slice.
    Int64 last_target_bucket_idx = 0;

    for (size_t n = 0; n < origin_spans.size(); ++n)
    {
        const auto & span = origin_spans[n];
        if (n > 0 && span.offset < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram span number {} has a negative offset {}", n + 1, span.offset);
        /// Determine the index of the first bucket in this span.
        bucket_idx += span.offset;
        for (UInt32 j = 0; j < span.length; ++j)
        {
            /// Protect against too few buckets in the origin.
            if (bucket_count_idx >= origin_buckets.size())
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Native histogram has {} bucket values but its spans cover more",
                    origin_buckets.size());

            /// Determine the index of the bucket in the target schema from the index in the
            /// original schema.
            const Int64 target_bucket_idx = targetIdx(bucket_idx, origin_schema, target_schema);

            if (target_spans.empty())
            {
                /// This is the first span in target_spans.
                target_spans.push_back(TimeSeriesHistogramSpan{static_cast<Int32>(target_bucket_idx), 1});
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
                last_target_bucket_idx = target_bucket_idx;
                target_compensation_buckets.push_back(origin_compensation_buckets[bucket_count_idx]);
            }
            else if (last_target_bucket_idx == target_bucket_idx)
            {
                /// The current bucket has to be merged into the same target bucket as the previous bucket.
                std::tie(target_buckets.back(), target_compensation_buckets.back()) = kahanInc(
                    origin_buckets[bucket_count_idx], target_buckets.back(), target_compensation_buckets.back());
                std::tie(target_buckets.back(), target_compensation_buckets.back()) = kahanInc(
                    origin_compensation_buckets[bucket_count_idx], target_buckets.back(), target_compensation_buckets.back());
            }
            else if (last_target_bucket_idx + 1 == target_bucket_idx)
            {
                /// The current bucket has to go into a new target bucket, and that bucket is next
                /// to the previous target bucket, so we add it to the current target span.
                ++target_spans.back().length;
                ++last_target_bucket_idx;
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
                target_compensation_buckets.push_back(origin_compensation_buckets[bucket_count_idx]);
            }
            else if (last_target_bucket_idx + 1 < target_bucket_idx)
            {
                /// The current bucket goes into a new target bucket separated by a gap from the
                /// previous target bucket, so we need to add a new target span.
                target_spans.push_back(TimeSeriesHistogramSpan{static_cast<Int32>(target_bucket_idx - last_target_bucket_idx - 1), 1});
                last_target_bucket_idx = target_bucket_idx;
                target_buckets.push_back(origin_buckets[bucket_count_idx]);
                target_compensation_buckets.push_back(origin_compensation_buckets[bucket_count_idx]);
            }
            /// (No else: mirrors the upstream switch without a default.)

            ++bucket_idx;
            ++bucket_count_idx;
        }
    }
    if (bucket_count_idx != origin_buckets.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Native histogram spans cover {} bucket values but {} were provided",
            bucket_count_idx, origin_buckets.size());
    return {std::move(target_spans), std::move(target_buckets), std::move(target_compensation_buckets)};
}

}
