#include <algorithm>
#include <bit>
#include <limits>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/hasNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/MergeLock.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace ProfileEvents
{
    extern const Event RuntimeFiltersCreated;
    extern const Event RuntimeFilterBlocksProcessed;
    extern const Event RuntimeFilterBlocksSkipped;
    extern const Event RuntimeFilterRowsChecked;
    extern const Event RuntimeFilterRowsPassed;
    extern const Event RuntimeFilterRowsSkipped;
    extern const Event RuntimeFilterBloomFilterBuildsSkipped;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace detail
{

void RuntimeFilterBuildState::assertCanInsert() const
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");
}

void RuntimeFilterBuildState::assertCanFind() const
{
    if (!inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to lookup values in runtime filter before building it was finished");
}

void RuntimeFilterBuildState::assertCanMerge() const
{
    assertCanInsert();
    if (filters_to_merge == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge more runtime filters than expected");
}

void RuntimeFilterBuildState::finishMerge()
{
    assertCanMerge();
    --filters_to_merge;
}

void RuntimeFilterSkipBudget::replenish(UInt64 rows, UInt64 multiplier)
{
    if (rows == 0 || multiplier == 0)
        return;

    const Int64 max_rows_to_skip = std::numeric_limits<Int64>::max();
    const UInt64 max_rows_to_skip_unsigned = static_cast<UInt64>(max_rows_to_skip);
    const UInt64 rows_to_add = rows > max_rows_to_skip_unsigned / multiplier ? max_rows_to_skip_unsigned : rows * multiplier;
    const Int64 increment = static_cast<Int64>(rows_to_add);

    Int64 current_rows_to_skip = rows_to_skip.load(std::memory_order_relaxed);
    while (current_rows_to_skip < max_rows_to_skip)
    {
        const Int64 available = max_rows_to_skip - current_rows_to_skip;
        const Int64 updated_rows_to_skip = increment >= available ? max_rows_to_skip : current_rows_to_skip + increment;
        if (rows_to_skip.compare_exchange_weak(
                current_rows_to_skip, updated_rows_to_skip, std::memory_order_relaxed, std::memory_order_relaxed))
            return;
    }
}

bool RuntimeFilterSkipBudget::consume(size_t rows)
{
    Int64 current_rows_to_skip = rows_to_skip.load(std::memory_order_relaxed);
    while (current_rows_to_skip > 0)
    {
        const bool should_skip = rows < static_cast<UInt64>(current_rows_to_skip);
        const Int64 remaining_rows_to_skip = should_skip ? current_rows_to_skip - static_cast<Int64>(rows) : 0;
        if (rows_to_skip.compare_exchange_weak(
                current_rows_to_skip, remaining_rows_to_skip, std::memory_order_relaxed, std::memory_order_relaxed))
            return should_skip;
    }

    return false;
}
}

RuntimeFilterEvaluationState::RuntimeFilterEvaluationState(RuntimeFilterConfig config_)
    : config(std::move(config_))
{
}

void RuntimeFilterEvaluationState::updateStats(UInt64 rows_checked, UInt64 rows_passed) const
{
    stats.blocks_processed++;
    stats.rows_checked += rows_checked;
    stats.rows_passed += rows_passed;

    ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksProcessed);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsChecked, rows_checked);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsPassed, rows_passed);

    /// Skip the configured number of blocks if too few rows got filtered out.
    const double rows_passed_threshold = config.pass_ratio_threshold_for_disabling * static_cast<double>(rows_checked);
    if (static_cast<double>(rows_passed) > rows_passed_threshold)
        skip_budget.replenish(rows_checked, config.blocks_to_skip_before_reenabling);
}

void RuntimeFilterEvaluationState::recordSkippedBlock(size_t rows_skipped) const
{
    stats.rows_skipped += rows_skipped;
    stats.blocks_skipped++;
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsSkipped, rows_skipped);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksSkipped);
}

bool RuntimeFilterEvaluationState::shouldSkip(size_t next_block_rows) const
{
    if (!key_set_dropped.load() && !skip_budget.consume(next_block_rows))
        return false;

    recordSkippedBlock(next_block_rows);
    return true;
}

static void mergeBloomFilters(BloomFilter & destination, const BloomFilter & source)
{
    auto & destination_words = destination.getFilter();
    const auto & source_words = source.getFilter();
    constexpr size_t word_size = sizeof(source_words.front());
    if (destination_words.size() != source_words.size())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot merge Bloom Filters of different sizes: {} and {}",
            destination_words.size() * word_size, source_words.size() * word_size);

    for (size_t i = 0; i < destination_words.size(); ++i)
        destination_words[i] |= source_words[i];
}

static constexpr UInt64 BLOOM_FILTER_SEED = 42;
static constexpr size_t HASH_BATCH_SIZE = 1024;

namespace
{
/// The histogram buckets an order-preserving UInt64 coordinate, which covers the integral and
/// date types but not `DateTime64` (a `Decimal`) or `LowCardinality`. Those keep the plain envelope.
bool typeSupportsKeyRangeHistogram(const DataTypePtr & type)
{
    if (!type)
        return false;

    WhichDataType which(removeNullable(type));
    return which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64()
        || which.isInt8() || which.isInt16() || which.isInt32() || which.isInt64()
        || which.isDate() || which.isDate32() || which.isDateTime();
}

bool typeSupportsMinMaxRange(const DataTypePtr & type)
{
    if (!type)
        return false;

    DataTypePtr inner = removeNullable(recursiveRemoveLowCardinality(type));
    WhichDataType which(inner);
    return which.isInteger() || which.isDateOrDate32OrDateTimeOrDateTime64();
}

void extendRange(bool & has_range, Field & range_min, Field & range_max, const Field & new_min, const Field & new_max)
{
    if (!has_range)
    {
        range_min = new_min;
        range_max = new_max;
        has_range = true;
        return;
    }

    if (accurateLess(new_min, range_min))
        range_min = new_min;
    if (accurateLess(range_max, new_max))
        range_max = new_max;
}

using KeyInterval = std::pair<Field, Field>;
using KeyCover = std::vector<KeyInterval>;

/// Only ever used to compare interval and gap sizes; the emitted bounds stay the exact `Field`s.
Float64 toNumber(const Field & field)
{
    return applyVisitor(FieldVisitorConvertToNumber<Float64>(), field);
}

/// Keys the gap actually excludes; every supported key type is integral, so neighbours exclude nothing.
Float64 excludedBetween(const KeyInterval & left, const KeyInterval & right)
{
    return std::max(0.0, toNumber(right.first) - toNumber(left.second) - 1);
}

/// Sort by left bound and fuse intervals that overlap or touch.
void normalize(KeyCover & cover)
{
    if (cover.size() < 2)
        return;

    ::sort(cover.begin(), cover.end(), [](const auto & lhs, const auto & rhs) { return accurateLess(lhs.first, rhs.first); });

    size_t kept = 0;
    for (size_t i = 1; i < cover.size(); ++i)
    {
        if (excludedBetween(cover[kept], cover[i]) > 0)
            cover[++kept] = std::move(cover[i]);
        else if (accurateLess(cover[kept].second, cover[i].second))
            cover[kept].second = std::move(cover[i].second);
    }
    cover.resize(kept + 1);
}

/// Keep the `budget - 1` widest gaps; one pass, since the histogram can hand this thousands of runs.
void coalesceToBudget(KeyCover & cover, size_t budget)
{
    if (cover.size() <= budget || budget == 0)
        return;

    /// Gap i separates interval i from i + 1.
    std::vector<std::pair<Float64, size_t>> gaps;
    gaps.reserve(cover.size() - 1);
    for (size_t i = 0; i + 1 < cover.size(); ++i)
        gaps.emplace_back(excludedBetween(cover[i], cover[i + 1]), i);

    const size_t keep = budget - 1;
    std::nth_element(
        gaps.begin(),
        gaps.begin() + keep,
        gaps.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.first > rhs.first; });
    gaps.resize(keep);
    ::sort(gaps.begin(), gaps.end(), [](const auto & lhs, const auto & rhs) { return lhs.second < rhs.second; });

    KeyCover result;
    result.reserve(budget);
    size_t begin = 0;
    for (const auto & [gap, split_after] : gaps)
    {
        result.emplace_back(std::move(cover[begin].first), std::move(cover[split_after].second));
        begin = split_after + 1;
    }
    result.emplace_back(std::move(cover[begin].first), std::move(cover.back().second));
    cover = std::move(result);
}

/// A split only earns its extra OR branch if it excludes a real part of the span.
constexpr Float64 min_excluded_ratio_to_split = 0.01;

/// Undo splits not worth making; when none qualifies this leaves the plain [min, max] envelope.
void dropUselessSplits(KeyCover & cover)
{
    if (cover.size() < 2)
        return;

    const Float64 threshold = min_excluded_ratio_to_split * (toNumber(cover.back().second) - toNumber(cover.front().first));

    size_t kept = 0;
    for (size_t i = 1; i < cover.size(); ++i)
    {
        if (excludedBetween(cover[kept], cover[i]) < threshold)
            cover[kept].second = std::move(cover[i].second);
        else
            cover[++kept] = std::move(cover[i]);
    }
    cover.resize(kept + 1);
}


void hashFixedSizeColumn(const char * raw_data, size_t value_size, size_t row_count, UInt64 seed, BloomFilterHashPair * out_hashes)
{
    const char * position = raw_data;
    for (size_t row = 0; row < row_count; ++row)
    {
        out_hashes[row] = BloomFilter::computeHashPair(position, value_size, seed);
        position += value_size;
    }
}

template <typename ProcessBatch>
void forEachColumnHashBatch(const IColumn & column, UInt64 seed, ProcessBatch && process_batch)
{
    const size_t row_count = column.size();
    if (row_count == 0)
        return;

    std::vector<BloomFilterHashPair> hash_pairs(std::min(HASH_BATCH_SIZE, row_count));

    if (!isColumnConst(column) && column.isFixedAndContiguous())
    {
        const size_t value_size = column.sizeOfValueIfFixed();
        const std::string_view raw_data = column.getRawData();

        chassert(value_size == 0 || raw_data.size() / value_size >= row_count);

        size_t start_row = 0;
        while (start_row < row_count)
        {
            const size_t batch_size = std::min(hash_pairs.size(), row_count - start_row);
            const char * batch_data = raw_data.data() + start_row * value_size;
            hashFixedSizeColumn(batch_data, value_size, batch_size, seed, hash_pairs.data());
            process_batch(hash_pairs.data(), batch_size, start_row);
            start_row += batch_size;
        }
        return;
    }

    size_t start_row = 0;
    while (start_row < row_count)
    {
        const size_t batch_size = std::min(hash_pairs.size(), row_count - start_row);
        for (size_t index = 0; index < batch_size; ++index)
        {
            const auto value = column.getDataAt(start_row + index);
            hash_pairs[index] = BloomFilter::computeHashPair(value.data(), value.size(), seed);
        }
        process_batch(hash_pairs.data(), batch_size, start_row);
        start_row += batch_size;
    }
}

template <typename... Ts>
struct Overloaded : Ts...
{
    using Ts::operator()...;
};

template <typename... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

}

/// Bitmap over a bucketed key domain; recording a key is one bit set, hence order-independent.
struct KeyRangeHistogram
{
    /// 1 KiB of bits; resolves a 55M-id span to ~16k, far finer than the clusters it must separate.
    static constexpr size_t buckets = 8192;
    static constexpr size_t words = buckets / 64;
    /// Flipping the sign bit maps a signed key to a UInt64 of the same ordering.
    static constexpr UInt64 sign_bias = UInt64(1) << 63;

    UInt64 bits[words] = {};
    UInt64 base = 0;        /// coordinate where bucket 0 starts
    unsigned shift = 0;     /// bucket width is 1 << shift
    UInt64 min_coordinate = 0;
    UInt64 max_coordinate = 0;
    bool is_signed = false;
    bool initialized = false;

    bool isSet(size_t bucket) const { return (bits[bucket >> 6] >> (bucket & 63)) & 1; }
    size_t lastBucket() const { return std::min<size_t>((max_coordinate - base) >> shift, buckets - 1); }
    Field toField(UInt64 coordinate) const
    {
        return is_signed ? Field(static_cast<Int64>(coordinate ^ sign_bias)) : Field(coordinate);
    }

    /// Widen the buckets, and move the base down, until `coordinate` fits; then remap what we have.
    void rescale(UInt64 coordinate)
    {
        const UInt64 low = std::min(coordinate, min_coordinate);
        const UInt64 high = std::max(coordinate, max_coordinate);

        /// Window twice the span, starting a quarter in, or descending input rescales on every key.
        unsigned new_shift = shift;
        while (new_shift < 63 && ((high - low) >> new_shift) >= buckets / 2)
            ++new_shift;

        const UInt64 margin = static_cast<UInt64>(buckets / 4) << new_shift;
        const UInt64 new_base = low > margin ? low - margin : 0;

        const UInt64 width = UInt64(1) << shift;
        UInt64 remapped[words] = {};
        for (size_t bucket = 0, last = lastBucket(); bucket <= last; ++bucket)
        {
            if (!isSet(bucket))
                continue;

            /// A widened bucket can straddle two new ones; set both, the cover must stay a superset.
            const UInt64 bucket_low = base + (static_cast<UInt64>(bucket) << shift);
            /// Saturate: a wrapped high edge would remap the bucket wrongly, or drop its keys.
            const UInt64 bucket_high = std::numeric_limits<UInt64>::max() - bucket_low < width - 1
                ? std::numeric_limits<UInt64>::max()
                : bucket_low + width - 1;
            const size_t from = static_cast<size_t>((bucket_low - new_base) >> new_shift);
            const size_t to = std::min<size_t>((bucket_high - new_base) >> new_shift, buckets - 1);
            for (size_t i = from; i <= to; ++i)
                remapped[i >> 6] |= UInt64(1) << (i & 63);
        }

        memcpy(bits, remapped, sizeof(bits));
        base = new_base;
        shift = new_shift;
    }

    void addCoordinate(UInt64 coordinate)
    {
        if (!initialized)
        {
            initialized = true;
            base = min_coordinate = max_coordinate = coordinate;
        }

        min_coordinate = std::min(min_coordinate, coordinate);
        max_coordinate = std::max(max_coordinate, coordinate);

        if (coordinate < base || ((coordinate - base) >> shift) >= buckets)
            rescale(coordinate);

        const size_t bucket = static_cast<size_t>((coordinate - base) >> shift);
        bits[bucket >> 6] |= UInt64(1) << (bucket & 63);
    }

    template <typename T>
    bool addTypedColumn(const IColumn & column, const NullMap * null_map)
    {
        const auto * typed = typeid_cast<const ColumnVector<T> *>(&column);
        if (!typed)
            return false;

        is_signed = is_signed_v<T>;
        const auto & data = typed->getData();
        for (size_t i = 0, size = data.size(); i < size; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;

            if constexpr (is_signed_v<T>)
                addCoordinate(static_cast<UInt64>(static_cast<Int64>(data[i])) ^ sign_bias);
            else
                addCoordinate(static_cast<UInt64>(data[i]));
        }
        return true;
    }

    /// False for a key type it cannot bucket, so the caller falls back to the per-block extremes.
    bool add(const IColumn & column)
    {
        const IColumn * values = &column;
        const NullMap * null_map = nullptr;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(values))
        {
            null_map = &nullable->getNullMapData();
            values = &nullable->getNestedColumn();
        }

        return addTypedColumn<UInt8>(*values, null_map) || addTypedColumn<UInt16>(*values, null_map)
            || addTypedColumn<UInt32>(*values, null_map) || addTypedColumn<UInt64>(*values, null_map)
            || addTypedColumn<Int8>(*values, null_map) || addTypedColumn<Int16>(*values, null_map)
            || addTypedColumn<Int32>(*values, null_map) || addTypedColumn<Int64>(*values, null_map);
    }

    /// Every run of set buckets becomes one interval, clamped to the exact extremes at the edges.
    void appendIntervals(KeyCover & cover) const
    {
        if (!initialized)
            return;

        for (size_t bucket = 0, last = lastBucket(); bucket <= last;)
        {
            if (!isSet(bucket))
            {
                ++bucket;
                continue;
            }

            const size_t from = bucket;
            while (bucket <= last && isSet(bucket))
                ++bucket;

            /// From the last set bucket, and saturated: either would wrap and invert the interval.
            const UInt64 width = UInt64(1) << shift;
            const UInt64 run_last_low = base + (static_cast<UInt64>(bucket - 1) << shift);
            const UInt64 run_high = std::numeric_limits<UInt64>::max() - run_last_low < width - 1
                ? std::numeric_limits<UInt64>::max()
                : run_last_low + width - 1;

            const UInt64 low = std::max(min_coordinate, base + (static_cast<UInt64>(from) << shift));
            const UInt64 high = std::min(max_coordinate, run_high);
            cover.emplace_back(toField(low), toField(high));
        }
    }
};

static size_t countPassedStats(ColumnPtr values);

template <bool negate>
ExactSetRuntimeFilter<negate>::ExactSetRuntimeFilter(
    const DataTypePtr & filter_column_target_type_, UInt64 bytes_limit_, UInt64 exact_values_limit_)
    : filter_column_target_type(filter_column_target_type_)
    , argument_can_have_nulls(hasTypeThatCanContainNulls(filter_column_target_type_))
    , bytes_limit(bytes_limit_)
    , exact_values_limit(exact_values_limit_)
    , lookup_state(Many{std::make_shared<Set>(SizeLimits{}, -1, argument_can_have_nulls)})
{
    ColumnsWithTypeAndName set_header = {ColumnWithTypeAndName(filter_column_target_type_, String())};
    getExactValues().setHeader(set_header);
    getExactValues().fillSetElements(); /// Save the values, not just hashes.
}

template <bool negate>
Set & ExactSetRuntimeFilter<negate>::getExactValues()
{
    auto * many = std::get_if<Many>(&lookup_state);
    if (!many || !many->exact_values)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter exact values are not available");
    return *many->exact_values;
}

template <bool negate>
const Set & ExactSetRuntimeFilter<negate>::getExactValues() const
{
    const auto * many = std::get_if<Many>(&lookup_state);
    if (!many || !many->exact_values)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter exact values are not available");
    return *many->exact_values;
}

template <bool negate>
void ExactSetRuntimeFilter<negate>::insert(ColumnPtr values)
{
    if (is_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter exact set after it was marked as finished");

    if (is_full)
        return;

    auto & set = getExactValues();
    set.insertFromColumns({values});
    is_full = set.getTotalRowCount() > exact_values_limit || set.getTotalByteCount() > bytes_limit;
}

template <bool negate>
void ExactSetRuntimeFilter<negate>::finishInsert()
{
    if (is_finished)
        return;

    auto & set = getExactValues();
    set.finishInsert();
    is_finished = true;

    /// If the set is empty just return a constant false column.
    if (set.getTotalRowCount() == 0)
    {
        lookup_state = Empty{set.getSetElements().front()};
        return;
    }

    /// If only one element is in the set then use `equals` instead of set lookup.
    /// If the argument is `Nullable`, use `Set` because it can handle `NULL` values.
    if (set.getTotalRowCount() == 1 && !argument_can_have_nulls)
    {
        lookup_state = Single{set.getSetElements().front()};
        return;
    }

    /// Keep the set-backed state for normal set lookups.
}

template <bool negate>
void ExactSetRuntimeFilter<negate>::finishInsert(RuntimeFilterEvaluationState & evaluation_state)
{
    finishInsert();

    if constexpr (!negate)
    {
        if (isFull())
        {
            /// Some keys were dropped so we cannot filter by a partial set of keys.
            evaluation_state.markKeySetDropped();
            releaseExactValues();
        }
    }
}

template <bool negate>
ColumnPtr ExactSetRuntimeFilter<negate>::find(const ColumnWithTypeAndName & values, std::optional<size_t> & /*rows_passed*/) const
{
    if (!is_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter set is not ready for lookups");

    return std::visit(
        Overloaded{
            [&](const Empty &) -> ColumnPtr { return DataTypeUInt8().createColumnConst(values.column->size(), negate); },
            [&](const Single & single) -> ColumnPtr
            {
                /// If only one element is in the set then use `equals` instead of set lookup.
                /// Use the column directly from `Set` to avoid lossy `Field` roundtrip.
                ColumnPtr const_column = ColumnConst::create(single.column, values.column->size());
                ColumnsWithTypeAndName arguments = {values, ColumnWithTypeAndName(const_column, values.type, String())};
                auto single_element_equals_function
                    = FunctionFactory::instance().get(negate ? "notEquals" : "equals", nullptr)->build(arguments);
                return single_element_equals_function->execute(
                    arguments, single_element_equals_function->getResultType(), values.column->size(), /* dry_run = */ false);
            },
            [&](const Many & many) -> ColumnPtr
            {
                if (!many.exact_values)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter exact values are not available");

                return many.exact_values->execute({values}, negate);
            },
        },
        lookup_state);
}

template <bool negate>
ColumnPtr ExactSetRuntimeFilter<negate>::getValuesColumn() const
{
    return std::visit(
        Overloaded{
            [](const Empty & empty) -> ColumnPtr { return empty.column; },
            [](const Single & single) -> ColumnPtr { return single.column; },
            [](const Many & many) -> ColumnPtr
            {
                if (!many.exact_values)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter exact values are not available");

                many.exact_values->finishInsert();
                return many.exact_values->getSetElements().front();
            },
        },
        lookup_state);
}

template <bool negate>
void ExactSetRuntimeFilter<negate>::releaseExactValues()
{
    if (auto * many = std::get_if<Many>(&lookup_state))
        many->exact_values.reset();
}

template <bool negate>
ColumnPtr ExactSetRuntimeFilter<negate>::getRecordedKeyValues() const
{
    if constexpr (negate)
        return nullptr;
    if (is_full || !is_finished)
        return nullptr;
    return getValuesColumn();
}

template <bool negate>
void ExactSetRuntimeFilter<negate>::mergeFrom(const ExactSetRuntimeFilter & source)
{
    insert(source.getValuesColumn());
}

bool ApproximateSetRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    /// Runtime BloomFilter hashing uses byte representation from either fixed contiguous column storage or getDataAt().
    /// LowCardinality reports a contiguous representation unconditionally, but its getDataAt() delegates to the
    /// dictionary column; for LowCardinality(Nullable(...)) that is ColumnNullable::getDataAt(), which throws on a NULL.
    /// Strip LowCardinality and test the inner type so LC(Nullable(...)) falls back to the exact (NULL-safe) Set path,
    /// exactly like a plain Nullable(...) key already does.
    return removeLowCardinality(data_type)->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
}

ApproximateSetRuntimeFilter::ApproximateSetRuntimeFilter(UInt64 bytes_limit_, UInt64 bloom_filter_hash_functions_)
    : bloom_filter(bytes_limit_, bloom_filter_hash_functions_, BLOOM_FILTER_SEED)
{
}

void ApproximateSetRuntimeFilter::insert(ColumnPtr values)
{
    insertIntoBloomFilter(values);
}

void ApproximateSetRuntimeFilter::insertIntoBloomFilter(const ColumnPtr & values)
{
    forEachColumnHashBatch(
        *values,
        bloom_filter.getSeed(),
        [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t /* start_row */)
        { bloom_filter.addHashPairs(hash_pairs, count); });
}

ColumnPtr ApproximateSetRuntimeFilter::find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const
{
    auto dst = ColumnVector<UInt8>::create();
    auto & dst_data = dst->getData();
    dst_data.resize(values.column->size());

    /// `findHashPairs` counts the matches while filling the mask; report that count through
    /// `rows_passed` so the caller does not rescan the mask to collect stats.
    size_t found_count = 0;
    forEachColumnHashBatch(
        *values.column,
        bloom_filter.getSeed(),
        [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t start_row)
        { found_count += bloom_filter.findHashPairs(hash_pairs, count, dst_data.data() + start_row); });

    rows_passed = found_count;
    return dst;
}

void ApproximateSetRuntimeFilter::mergeFrom(const ApproximateSetRuntimeFilter & source)
{
    mergeBloomFilters(bloom_filter, source.bloom_filter);
}

bool ApproximateSetRuntimeFilter::isWorthUsing(Float64 max_ratio_of_set_bits_in_bloom_filter) const
{
    const auto & raw_filter_words = bloom_filter.getFilter();
    const size_t total_bits = raw_filter_words.size() * sizeof(raw_filter_words[0]) * 8;
    size_t set_bits = 0;
    for (auto word : raw_filter_words)
        set_bits += std::popcount(word);

    /// If too many bits are set then it is likely that the filter will not filter out much.
    return static_cast<double>(set_bits) <= max_ratio_of_set_bits_in_bloom_filter * static_cast<double>(total_bits);
}

bool AdaptiveSetRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    return ApproximateSetRuntimeFilter::isDataTypeSupported(data_type);
}

AdaptiveSetRuntimeFilter::AdaptiveSetRuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    std::optional<UInt64> distinct_keys_hint_,
    bool distinct_keys_hint_matches_filter_key_)
    : filter_column_target_type(filter_column_target_type_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , distinct_keys_hint(distinct_keys_hint_)
    , distinct_keys_hint_matches_filter_key(distinct_keys_hint_matches_filter_key_)
    , filter(std::in_place_type<ExactFilter>, filter_column_target_type_, bytes_limit_, exact_values_limit_)
{
}

void AdaptiveSetRuntimeFilter::insert(ColumnPtr values)
{
    insert(std::move(values), filter);
}

void AdaptiveSetRuntimeFilter::insert(ColumnPtr values, Filter & filter_)
{
    if (std::holds_alternative<KeySetDropped>(filter_))
        return;

    if (auto * approximate_filter = std::get_if<ApproximateSetRuntimeFilter>(&filter_))
    {
        approximate_filter->insert(std::move(values));
        return;
    }

    auto * exact_filter = std::get_if<ExactFilter>(&filter_);
    if (!exact_filter || exact_filter->isFull())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of AdaptiveSetRuntimeFilter");

    exact_filter->insert(std::move(values));

    if (exact_filter->isFull())
        switchToApproximateFilter(filter_);
}

void AdaptiveSetRuntimeFilter::finishInsert(RuntimeFilterEvaluationState & evaluation_state)
{
    std::visit(
        Overloaded{
            [](ExactFilter & exact_filter) { exact_filter.finishInsert(); },
            [&](ApproximateSetRuntimeFilter & approximate_filter)
            { checkApproximateFilterWorthiness(evaluation_state, approximate_filter); },
            [&](KeySetDropped &) { evaluation_state.markKeySetDropped(); },
        },
        filter);
}

static size_t countPassedStats(ColumnPtr values)
{
    if (const auto * column_bool = typeid_cast<const ColumnUInt8 *>(values.get()))
    {
        return countBytesInFilter(column_bool->getData());
    }
    else if (const auto * column_const = typeid_cast<const ColumnConst *>(values.get()))
    {
        const bool all_true = column_const->getValue<UInt8>();
        return all_true ? values->size() : 0;
    }
    /// If for some reason value column type is unexpected then just assume that all rows passed
    return values->size();
}

ColumnPtr AdaptiveSetRuntimeFilter::find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const
{
    return std::visit(
        Overloaded{
            [&](const ExactFilter & exact_filter) -> ColumnPtr { return exact_filter.find(values, rows_passed); },
            [&](const ApproximateSetRuntimeFilter & approximate_filter) -> ColumnPtr
            { return approximate_filter.find(values, rows_passed); },
            [&](const KeySetDropped &) -> ColumnPtr
            {
                rows_passed = values.column->size();
                return DataTypeUInt8().createColumnConst(values.column->size(), true);
            },
        },
        filter);
}

ColumnPtr AdaptiveSetRuntimeFilter::getRecordedKeyValues() const
{
    if (const auto * exact_filter = std::get_if<ExactFilter>(&filter))
        return exact_filter->getRecordedKeyValues();
    return nullptr;
}

void AdaptiveSetRuntimeFilter::mergeFrom(const AdaptiveSetRuntimeFilter & source)
{
    std::visit(
        Overloaded{
            [&](const ExactFilter & source_exact_filter) { insert(source_exact_filter.getValuesColumn(), filter); },
            [&](const ApproximateSetRuntimeFilter & source_approximate_filter)
            {
                if (auto * destination_approximate_filter = switchToApproximateFilter(filter))
                    destination_approximate_filter->mergeFrom(source_approximate_filter);
            },
            [&](const KeySetDropped &) { dropKeySet(filter); },
        },
        source.filter);
}

void AdaptiveSetRuntimeFilter::dropKeySet(Filter & filter_)
{
    filter_.emplace<KeySetDropped>();
}

ApproximateSetRuntimeFilter * AdaptiveSetRuntimeFilter::switchToApproximateFilter(Filter & filter_)
{
    if (auto * approximate_filter = std::get_if<ApproximateSetRuntimeFilter>(&filter_))
        return approximate_filter;
    if (std::holds_alternative<KeySetDropped>(filter_))
        return nullptr;

    auto * exact_filter = std::get_if<ExactFilter>(&filter_);
    if (!exact_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of AdaptiveSetRuntimeFilter");
    auto values = exact_filter->getValuesColumn();
    UInt64 bytes_limit = exact_filter->getBytesLimit();

    if (distinct_keys_hint)
    {
        bytes_limit = growRuntimeBloomFilterBytesFromStats(
            *distinct_keys_hint, bloom_filter_hash_functions, bytes_limit, max_ratio_of_set_bits_in_bloom_filter);

        /// The filter size is capped, so a build side with more distinct keys would produce a Bloom filter
        /// that `checkApproximateFilterWorthiness` discards. Predict that fill rate before constructing it.
        if (distinct_keys_hint_matches_filter_key)
        {
            const double least_distinct_keys
                = static_cast<double>(*distinct_keys_hint) / HashJoinEntry::MAX_OVERESTIMATION_FACTOR;
            const double predicted_fill_rate = estimateRuntimeBloomFilterSetBitsRatio(
                least_distinct_keys, RuntimeBloomFilterParameters{bytes_limit, bloom_filter_hash_functions});
            if (predicted_fill_rate > max_ratio_of_set_bits_in_bloom_filter)
            {
                ProfileEvents::increment(ProfileEvents::RuntimeFilterBloomFilterBuildsSkipped);
                dropKeySet(filter_);
                return nullptr;
            }
        }
    }

    auto & approximate_filter = filter_.emplace<ApproximateSetRuntimeFilter>(bytes_limit, bloom_filter_hash_functions);
    approximate_filter.insert(values);
    return &approximate_filter;
}

void AdaptiveSetRuntimeFilter::checkApproximateFilterWorthiness(
    RuntimeFilterEvaluationState & evaluation_state, const ApproximateSetRuntimeFilter & approximate_filter) const
{
    if (!approximate_filter.isWorthUsing(max_ratio_of_set_bits_in_bloom_filter))
        evaluation_state.markKeySetDropped();
}

SharedFixedHashTableRuntimeFilter::SharedFixedHashTableRuntimeFilter(
    const DataTypePtr & filter_column_target_type_, ProbeFn probe_fn_, std::vector<Range> key_ranges_, ColumnPtr recorded_key_values_)
    : filter_column_target_type(filter_column_target_type_)
    , probe_fn(std::move(probe_fn_))
    , key_ranges(std::move(key_ranges_))
    , recorded_key_values(std::move(recorded_key_values_))
{
}

ColumnPtr SharedFixedHashTableRuntimeFilter::find(const ColumnWithTypeAndName & values, std::optional<size_t> & /*rows_passed*/) const
{
    return probe_fn(values);
}

RuntimeFilter::RuntimeFilter(RuntimeFilterConfig config_, Data data_)
    : filter_column_target_type(std::visit([](const auto & filter) { return filter.getTargetType(); }, data_.filter))
    , range_supported(typeSupportsMinMaxRange(filter_column_target_type))
    , range_positive(!std::holds_alternative<ExactNotContains>(data_.filter))
    , range_histogram_supported(typeSupportsKeyRangeHistogram(filter_column_target_type))
    , evaluation_state(std::move(config_))
    , data(std::move(data_))
{
    if (!range_supported)
    {
        std::lock_guard lock(mutex);
        data.has_range = false;
        data.range_cover.clear();
    }
}

void RuntimeFilter::insert(ColumnPtr values)
{
    std::lock_guard lock(mutex);
    std::visit(
        [&](auto & filter) TSA_REQUIRES(mutex)
        {
            using FilterType = std::decay_t<decltype(filter)>;
            if constexpr (!FilterType::is_prebuilt)
            {
                data.build_state.assertCanInsert();
                if (data.index_analysis_enabled && range_supported && range_positive && !values->empty())
                {
                    /// The histogram sees the keys; per-block extremes are blind within a block.
                    bool recorded_in_histogram = false;
                    if (range_histogram_supported)
                    {
                        if (!data.range_histogram)
                            data.range_histogram = std::make_shared<KeyRangeHistogram>();
                        recorded_in_histogram = data.range_histogram->add(*values);
                    }

                    /// Keys the histogram declined must still land somewhere, or the cover misses them.
                    if (!recorded_in_histogram)
                    {
                        Field column_min;
                        Field column_max;
                        values->getExtremes(column_min, column_max, 0, values->size());
                        if (!column_min.isNull() && !column_max.isNull())
                            extendRange(data.has_range, data.range_min, data.range_max, column_min, column_max);
                    }
                }
                filter.insert(std::move(values));
            }
        },
        data.filter);
}

void RuntimeFilter::finishInsert()
{
    std::lock_guard lock(mutex);
    if (data.build_state.hasPendingMerges())
        return;

    std::visit([&](auto & filter) { filter.finishInsert(evaluation_state); }, data.filter);
    data.build_state.finishInserts();
}

ColumnPtr RuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    SharedLockGuard lock(mutex);
    data.build_state.assertCanFind();

    const size_t rows_in_block = values.column->size();
    if (evaluation_state.shouldSkip(rows_in_block))
        return DataTypeUInt8().createColumnConst(rows_in_block, true);

    std::optional<size_t> rows_passed;
    auto result = std::visit([&](const auto & filter) -> ColumnPtr { return filter.find(values, rows_passed); }, data.filter);
    evaluation_state.updateStats(rows_in_block, rows_passed ? *rows_passed : countPassedStats(result));
    return result;
}

void RuntimeFilter::merge(const RuntimeFilter & source)
{
    if (&source == this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge a runtime filter with itself");

    MergeLock lock(source.mutex, mutex);

    /// `HashJoin::publishSharedRuntimeFilters` may have already replaced this lookup entry with a
    /// prebuilt shared fixed-hash-table filter: the publication step can run as soon as the last
    /// build-side port is closed, while `BuildRuntimeFilterTransform::finish()` (which reaches this
    /// merge via `IRuntimeFilterLookup::add`) only runs afterwards in `prepare()`. The shared filter
    /// probes the complete build-side hash table, i.e. a superset of anything a late set/bloom
    /// filter could contribute, so ignore the merge (the pre-refactor no-op behavior of
    /// `SharedFixedHashTableRuntimeFilter::merge`) instead of failing the query.
    if (std::holds_alternative<SharedFixedHashTable>(data.filter))
        return;

    if (data.filter.index() != source.data.filter.index())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    data.build_state.assertCanMerge();
    std::visit(
        [](auto & destination_filter, const auto & source_filter)
        {
            using DestinationFilter = std::decay_t<decltype(destination_filter)>;
            using SourceFilter = std::decay_t<decltype(source_filter)>;
            if constexpr (std::is_same_v<DestinationFilter, SourceFilter>)
            {
                destination_filter.mergeFrom(source_filter);
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");
            }
        },
        data.filter,
        source.data.filter);
    if (data.index_analysis_enabled && range_supported && range_positive)
    {
        if (source.data.has_range)
            extendRange(data.has_range, data.range_min, data.range_max, source.data.range_min, source.data.range_max);

        /// Separate grids, so merge as intervals; reducing here would cost a walk and sort per stream.
        source.appendRangeCover(data.range_cover);
    }
    data.build_state.finishMerge();
}

void RuntimeFilter::enableIndexAnalysis()
{
    std::lock_guard lock(mutex);
    data.build_state.assertCanInsert();
    data.index_analysis_enabled = true;
}

ColumnPtr RuntimeFilter::getRecordedKeyValues() const
{
    if (!range_positive)
        return nullptr;

    SharedLockGuard lock(mutex);
    if (!data.index_analysis_enabled || !data.build_state.isFinished())
        return nullptr;
    return std::visit([](const auto & filter) { return filter.getRecordedKeyValues(); }, data.filter);
}

/// Everything recorded so far, unreduced: histogram runs, merged-in intervals, and the envelope.
void RuntimeFilter::appendRangeCover(std::vector<std::pair<Field, Field>> & out) const
{
    SharedLockGuard lock(mutex);
    out.insert(out.end(), data.range_cover.begin(), data.range_cover.end());
    if (data.range_histogram)
        data.range_histogram->appendIntervals(out);
    if (data.has_range && !data.range_min.isNull() && !data.range_max.isNull())
        out.emplace_back(data.range_min, data.range_max);
}

std::vector<std::pair<Field, Field>> RuntimeFilter::effectiveRangeCover() const
{
    std::vector<std::pair<Field, Field>> cover = data.range_cover;
    if (data.range_histogram)
        data.range_histogram->appendIntervals(cover);

    if (data.has_range && !data.range_min.isNull() && !data.range_max.isNull())
        cover.emplace_back(data.range_min, data.range_max);

    normalize(cover);
    coalesceToBudget(cover, max_key_range_intervals);
    return cover;
}

std::vector<Range> RuntimeFilter::getRecordedKeyRanges() const
{
    if (!range_supported || !range_positive)
        return {};

    /// Called once per part, and the cover cannot change after the build finished, so memoize it.
    {
        SharedLockGuard lock(mutex);
        if (!data.build_state.isFinished())
            return {};
        if (data.recorded_key_ranges)
            return *data.recorded_key_ranges;
    }

    std::lock_guard lock(mutex);
    if (data.recorded_key_ranges)
        return *data.recorded_key_ranges;

    auto cover = effectiveRangeCover();
    dropUselessSplits(cover);

    std::vector<Range> ranges;
    ranges.reserve(cover.size());
    for (const auto & [low, high] : cover)
    {
        if (low.isNull() || high.isNull())
            return {};
        ranges.emplace_back(low, /*left_included=*/true, high, /*right_included=*/true);
    }

    data.recorded_key_ranges = ranges;
    return ranges;
}

template class ExactSetRuntimeFilter<false>;
template class ExactSetRuntimeFilter<true>;

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & key, const String & display_name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard lock(mutex);
        auto & filter = filters_by_name[key];
        if (!filter)
        {
            ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
            filter.reset(runtime_filter.release()); /// Save new filter.
            /// Record the readable structural name once because the map is keyed by the opaque rendezvous key.
            display_names.emplace(key, display_name);
        }
        else
        {
            filter->merge(*runtime_filter); /// Add all new keys to an existing filter.
        }
        filter->finishInsert();
    }

    void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard lock(mutex);
        auto & filter = filters_by_name[name];
        if (!filter)
            ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
        filter.reset(runtime_filter.release());
    }

    RuntimeFilterConstPtr find(const String & name) const override
    {
        SharedLockGuard lock(mutex);
        auto it = filters_by_name.find(name);
        if (it == filters_by_name.end())
            return nullptr;
        return it->second;
    }

    void logStats() const override
    {
        SharedLockGuard lock(mutex);
        for (const auto & [filter_key, filter] : filters_by_name)
        {
            const auto & stats = filter->getStats();
            /// `filter_key` is the opaque random rendezvous key; prefer the readable structural name.
            auto name_it = display_names.find(filter_key);
            const String & name = (name_it != display_names.end() && !name_it->second.empty()) ? name_it->second : filter_key;
            LOG_TRACE(
                getLogger("RuntimeFilter"),
                "Stats for '{}': rows skipped {}, rows checked {}, rows passed {}, blocks skipped {}, blocks processed {}",
                name,
                stats.rows_skipped.load(),
                stats.rows_checked.load(),
                stats.rows_passed.load(),
                stats.blocks_skipped.load(),
                stats.blocks_processed.load());
        }
    }

private:
    mutable SharedMutex mutex;
    std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(mutex);
    /// Readable structural name per rendezvous key, for logging. Kept under the same lock and
    /// preserved across `replace` because the replacement keeps the original registration's name.
    std::unordered_map<String, String> display_names TSA_GUARDED_BY(mutex);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

/// Build a pruning predicate on the column: exact IN values when available, otherwise the recorded
/// key ranges.
static const ActionsDAG::Node * convertRuntimeFilterToKeyConditionDAG(
    const RuntimeFilter & filter, const String & column_name, const DataTypePtr & column_type, ActionsDAG & dag, const ContextPtr & context)
{
    auto exact_values = filter.getRecordedKeyValues();
    auto ranges = exact_values ? std::vector<Range>{} : filter.getRecordedKeyRanges();
    if (!exact_values && ranges.empty())
        return nullptr;

    const auto target_type = filter.getFilterColumnTargetType();
    const auto & key_node = dag.addInput(column_name, column_type);
    const auto & key_casted = column_type->equals(*target_type)
        ? key_node
        : dag.addCast(key_node, target_type, {}, context);

    if (exact_values)
    {
        ColumnWithTypeAndName set_values(exact_values, target_type, "__runtime_filter_in_values_" + column_name);
        auto future_set = std::make_shared<FutureSetFromTuple>(
            CityHash_v1_0_2::uint128{}, ASTPtr{}, ColumnsWithTypeAndName{set_values}, false, SizeLimits{});
        auto set_column = ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0);
        const auto & set_node
            = dag.addColumn(std::move(set_column), std::make_shared<DataTypeSet>(), "__runtime_filter_in_set_" + column_name);
        LOG_DEBUG(
            getLogger("JoinRuntimeFilterIndexAnalysis"),
            "Index analysis engaged on join key '{}': pruning by exact IN-set of {} value(s)",
            column_name,
            exact_values->size());
        return &dag.addFunction(FunctionFactory::instance().get("in", context), {&key_casted, &set_node}, {});
    }

    {
        WriteBufferFromOwnString ranges_description;
        for (size_t i = 0; i < ranges.size(); ++i)
            ranges_description << (i ? " OR " : "") << ranges[i].toString();
        LOG_DEBUG(
            getLogger("JoinRuntimeFilterIndexAnalysis"),
            "Index analysis engaged on join key '{}': pruning by {} range(s) {}",
            column_name,
            ranges.size(),
            ranges_description.str());
    }

    auto ge_func = FunctionFactory::instance().get("greaterOrEquals", context);
    auto le_func = FunctionFactory::instance().get("lessOrEquals", context);
    FunctionOverloadResolverPtr and_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    /// One `BETWEEN` per interval, ORed together. `KeyCondition` turns a disjunction of ranges over
    /// a primary key column into a union of mark ranges, and a `minmax` index keeps a granule when
    /// its own [min, max] intersects any of the intervals.
    ActionsDAG::NodeRawConstPtrs or_args;
    or_args.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto suffix = "_" + toString(i) + "_" + column_name;
        const auto & min_node
            = dag.addColumn(target_type->createColumnConst(1, ranges[i].left), target_type, "__runtime_filter_min" + suffix);
        const auto & max_node
            = dag.addColumn(target_type->createColumnConst(1, ranges[i].right), target_type, "__runtime_filter_max" + suffix);
        const auto & ge_node = dag.addFunction(ge_func, {&key_casted, &min_node}, {});
        const auto & le_node = dag.addFunction(le_func, {&key_casted, &max_node}, {});
        or_args.push_back(&dag.addFunction(and_func, {&ge_node, &le_node}, {}));
    }

    if (or_args.size() == 1)
        return or_args.front();

    FunctionOverloadResolverPtr or_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
    return &dag.addFunction(or_func, std::move(or_args), {});
}

const ActionsDAG::Node * buildRuntimeRangePredicate(
    const IRuntimeFilterLookup & lookup,
    const std::vector<RuntimeFilterIndexAnalysisDescriptor> & descriptors,
    ActionsDAG & dag,
    const ContextPtr & context)
{
    ActionsDAG::NodeRawConstPtrs and_args;
    for (const auto & descriptor : descriptors)
    {
        auto filter = lookup.find(descriptor.filter_id);
        if (!filter)
            continue;
        if (const auto * predicate
            = convertRuntimeFilterToKeyConditionDAG(*filter, descriptor.key_column_name, descriptor.key_column_type, dag, context))
            and_args.push_back(predicate);
    }

    if (and_args.empty())
        return nullptr;
    if (and_args.size() == 1)
        return and_args.front();

    FunctionOverloadResolverPtr and_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return &dag.addFunction(and_func, std::move(and_args), {});
}

}
