#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeSet.h>
#include <Interpreters/PreparedSets.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <base/sort.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <vector>

namespace ProfileEvents
{
    extern const Event RuntimeFiltersCreated;
    extern const Event RuntimeFilterBlocksProcessed;
    extern const Event RuntimeFilterBlocksSkipped;
    extern const Event RuntimeFilterRowsChecked;
    extern const Event RuntimeFilterRowsPassed;
    extern const Event RuntimeFilterRowsSkipped;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
/// Whether a min/max envelope over the key is usable (int/Date keys only).
bool typeSupportsMinMaxRange(const DataTypePtr & type)
{
    if (!type)
        return false;

    DataTypePtr inner = removeNullable(recursiveRemoveLowCardinality(type));
    WhichDataType which(inner);
    return which.isInt() || which.isUInt()
        || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64();
}


using KeyInterval = std::pair<Field, Field>;
using KeyCover = std::vector<KeyInterval>;

/// Only ever used to compare interval and gap sizes; the emitted bounds stay the exact `Field`s.
Float64 toNumber(const Field & field)
{
    return applyVisitor(FieldVisitorConvertToNumber<Float64>(), field);
}

/// How many keys the gap between two neighbouring intervals actually excludes. Every supported key
/// type is integral, so neighbours one apart exclude nothing and splitting a run of them is pointless.
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

/// Fuse the neighbouring pair that excludes the fewest keys until the cover fits the budget, which
/// is the same as keeping the `budget - 1` widest gaps: the tightest cover available for that budget.
void coalesceToBudget(KeyCover & cover, size_t budget)
{
    while (cover.size() > budget)
    {
        size_t narrowest = 0;
        for (size_t i = 1; i + 1 < cover.size(); ++i)
            if (excludedBetween(cover[i], cover[i + 1]) < excludedBetween(cover[narrowest], cover[narrowest + 1]))
                narrowest = i;

        cover[narrowest].second = std::move(cover[narrowest + 1].second);
        cover.erase(cover.begin() + narrowest + 1);
    }
}

/// A split only earns its extra OR branch if it excludes a real part of the span.
constexpr Float64 min_excluded_ratio_to_split = 0.01;

/// Undo the splits that are not worth making. When no gap qualifies this leaves a single interval,
/// i.e. the plain [min, max] envelope, which is what a dense or evenly scattered key set deserves.
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
}

/// Bitmap over a bucketed key domain, filled once the exact value set has overflowed. A key maps to
/// an order-preserving UInt64 coordinate, so bucketing is a subtract and a shift, and recording one
/// is a single bit set - commutative, so the clusters found do not depend on the order the build
/// side arrives in or on how it is split into blocks.
struct KeyRangeHistogram
{
    /// Sized against the workload this exists for. On the Stack Overflow dataset `posts.Id` is an
    /// IDENTITY column, so it ascends with `CreationDate` (measured correlation 0.9995). Joining
    /// 239M `votes` (ORDER BY PostId) to the posts of January 2011 and January 2020 gives a build
    /// side of 588k ids in two clusters, 4.57M-5.37M and 59.5M-60.1M, with 97% of the span between
    /// them empty: the envelope spans 55.5M ids (72% of the table), the two intervals cover 2.8%.
    /// 4096 buckets is 512 bytes, and resolves that span to ~13k ids - far finer than the ~800k-wide
    /// clusters that have to be told apart, while the exact extremes below keep the outer bounds
    /// tight. Fewer buckets would start fusing clusters; more would only sharpen edges that
    /// `dropUselessSplits` rounds off anyway.
    static constexpr size_t buckets = 4096;
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

        unsigned new_shift = shift;
        while (new_shift < 63 && ((high - low) >> new_shift) >= buckets)
            ++new_shift;

        UInt64 remapped[words] = {};
        for (size_t bucket = 0, last = lastBucket(); bucket <= last; ++bucket)
        {
            if (!isSet(bucket))
                continue;

            /// A widened bucket can straddle two new ones; set both, the cover must stay a superset.
            const UInt64 bucket_low = base + (static_cast<UInt64>(bucket) << shift);
            const size_t from = static_cast<size_t>((bucket_low - low) >> new_shift);
            const size_t to = std::min<size_t>((bucket_low + ((UInt64(1) << shift) - 1) - low) >> new_shift, buckets - 1);
            for (size_t i = from; i <= to; ++i)
                remapped[i >> 6] |= UInt64(1) << (i & 63);
        }

        memcpy(bits, remapped, sizeof(bits));
        base = low;
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

    /// False for a key type it cannot bucket (`DateTime64`, `LowCardinality`), so that the caller
    /// keeps using the per-block extremes for it.
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

            const UInt64 low = std::max(min_coordinate, base + (static_cast<UInt64>(from) << shift));
            const UInt64 high = std::min(max_coordinate, base + (static_cast<UInt64>(bucket) << shift) - 1);
            cover.emplace_back(toField(low), toField(high));
        }
    }
};

IRuntimeFilter::~IRuntimeFilter() = default;

IRuntimeFilter::IRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_)
    : filters_to_merge(filters_to_merge_)
    , filter_column_target_type(filter_column_target_type_)
    , pass_ratio_threshold_for_disabling(pass_ratio_threshold_for_disabling_)
    , blocks_to_skip_before_reenabling(blocks_to_skip_before_reenabling_)
{
    range_supported = typeSupportsMinMaxRange(filter_column_target_type);
}

std::vector<Range> IRuntimeFilter::getRecordedKeyRanges() const
{
    /// inserts_are_finished (seq_cst) publishes the cover without a lock.
    if (!range_supported || !range_positive || !inserts_are_finished.load())
        return {};

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
    return ranges;
}

/// Everything recorded so far as intervals: what the histogram holds, plus the per-block extremes
/// kept before it existed and for key types it cannot bucket.
KeyCover IRuntimeFilter::effectiveRangeCover() const
{
    KeyCover cover = range_cover;
    if (range_histogram)
        range_histogram->appendIntervals(cover);

    normalize(cover);
    coalesceToBudget(cover, max_key_range_intervals);
    return cover;
}

void IRuntimeFilter::updateRange(const IColumn & column)
{
    if (!index_analysis_enabled || !range_supported || !range_positive || column.size() == 0)
        return;

    /// Once the histogram exists it sees the keys themselves. Until then, and for key types it
    /// cannot bucket, record one interval per block instead: cheap, but blind to structure inside a
    /// block, so it only finds clusters when all of a block's rows sit in the same one.
    if (range_histogram && range_histogram->add(column))
        return;

    Field low;
    Field high;
    column.getExtremes(low, high, 0, column.size());
    if (low.isNull() || high.isNull())
        return;

    range_cover.emplace_back(std::move(low), std::move(high));
    normalize(range_cover);
    coalesceToBudget(range_cover, max_key_range_intervals);
}

void IRuntimeFilter::recordOverflowedKeys(const IColumn & column)
{
    if (!index_analysis_enabled || !range_supported || !range_positive || range_histogram)
        return;

    range_histogram = std::make_unique<KeyRangeHistogram>();
    if (!range_histogram->add(column))
    {
        range_histogram.reset();    /// Key type it cannot bucket; stay on the per-block extremes.
        return;
    }

    /// The histogram now holds every key the set had, so the coarse per-block intervals collected on
    /// the way here are redundant and strictly looser. Dropping them matters: a block that spanned
    /// several clusters left an interval that would swallow everything the histogram found.
    range_cover.clear();
}

void IRuntimeFilter::mergeRange(const IRuntimeFilter & source)
{
    if (!index_analysis_enabled || !range_supported || !range_positive)
        return;

    /// The two histograms have their own bucket grids, so they merge as intervals, not as bitmaps.
    const auto source_cover = source.effectiveRangeCover();
    range_cover.insert(range_cover.end(), source_cover.begin(), source_cover.end());
    normalize(range_cover);
    coalesceToBudget(range_cover, max_key_range_intervals);
}


void IRuntimeFilter::updateStats(UInt64 rows_checked, UInt64 rows_passed) const
{
    stats.blocks_processed++;
    stats.rows_checked += rows_checked;
    stats.rows_passed += rows_passed;

    ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksProcessed);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsChecked, rows_checked);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsPassed, rows_passed);

    /// Skip next 30 blocks if too few rows got filtered out
    if (static_cast<double>(rows_passed) > pass_ratio_threshold_for_disabling * static_cast<double>(rows_checked))
        rows_to_skip += rows_checked * blocks_to_skip_before_reenabling;
}

bool IRuntimeFilter::shouldSkip(size_t next_block_rows) const
{
    if (is_fully_disabled)
    {
        stats.rows_skipped += next_block_rows;
        stats.blocks_skipped++;
        ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsSkipped, next_block_rows);
        ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksSkipped);
        return true;
    }

    rows_to_skip -= next_block_rows;
    if (rows_to_skip > 0)
    {
        stats.rows_skipped += next_block_rows;
        stats.blocks_skipped++;
        ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsSkipped, next_block_rows);
        ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksSkipped);
        return true;
    }

    rows_to_skip = 0;
    return false;
}

void IRuntimeFilter::finishInsert()
{
    if (filters_to_merge != 0)
        return;

    inserts_are_finished = true;

    finishInsertImpl();
}

ColumnPtr IRuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    if (!inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to lookup values in runtime filter before building it was finished");

    const size_t rows_in_block = values.column->size();
    if (shouldSkip(rows_in_block))
        return DataTypeUInt8().createColumnConst(rows_in_block, true);

    return findImpl(values);
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
/// Max size up to which the bloom filter grows before the false positive rate starts degrading.
static constexpr UInt64 MAX_STATS_SIZED_BLOOM_FILTER_BYTES = 4 * 1024 * 1024;
/// At 3 hash functions achieves a 12.5% false positive rate
static constexpr Float64 RUNTIME_BLOOM_FILTER_TARGET_FILL_RATE = 0.5;

namespace
{
void hashFixedSizeColumn(
    const char * raw_data,
    size_t value_size,
    size_t row_count,
    UInt64 seed,
    BloomFilterHashPair * out_hashes)
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

/// Grow the bloom filter bytes to hold `distinct_keys` keys at the target fill rate using
/// `hash_functions` hash functions: filter_bits = -hash_functions * distinct_keys / ln(1 - fill_rate)
/// The formula is built on the following logic:
/// - distinct_keys * hash_functions: total bit-inserts into the filter
/// - filter_bits: the size of the filter in bits (what we solve for)
/// - 1/filter_bits: probability that one bit-insert sets a given bit
/// - (1 - 1/filter_bits)^(distinct_keys * hash_functions): probability that a given bit is not set after all inserts
/// - e^(-distinct_keys * hash_functions / filter_bits) is used to approximate the above probability
/// - 1 - e^(-distinct_keys * hash_functions / filter_bits): expected fraction of bits that end up set (= fill_rate)
/// For more infomation check: https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf
UInt64 growBloomFilterBytes(UInt64 distinct_keys, UInt64 hash_functions, UInt64 default_bloom_filter_bytes, Float64 max_ratio_of_set_bits)
{
    const Float64 target_fill_rate = std::min(RUNTIME_BLOOM_FILTER_TARGET_FILL_RATE, max_ratio_of_set_bits);
    const double ideal_bloom_filter_bytes = std::ceil(-static_cast<double>(hash_functions) * static_cast<double>(distinct_keys) / std::log1p(-target_fill_rate) / 8.0);
    const double clamped_bloom_filter_bytes = std::clamp(ideal_bloom_filter_bytes, 0.0, static_cast<double>(MAX_STATS_SIZED_BLOOM_FILTER_BYTES));
    return std::max(static_cast<UInt64>(clamped_bloom_filter_bytes), default_bloom_filter_bytes);
}
}

void ExactContainsRuntimeFilter::merge(const IRuntimeFilter * source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after it was marked as finished");

    const auto * source_typed = typeid_cast<const ExactContainsRuntimeFilter *>(source);
    if (!source_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    insert(source_typed->getValuesColumn());
    /// `getValuesColumn` returns only the keys the source kept, so once the source overflowed its
    /// exact-values limit those values no longer describe its whole key range. Merge the source's
    /// cover explicitly, otherwise the left side would be pruned by a too narrow range.
    mergeRange(*source_typed);
    --filters_to_merge;
}

void ExactContainsRuntimeFilter::finishInsertImpl()
{
    Base::finishInsertImpl();

    if (isFull())
    {
        /// Some keys were dropped so we cannot filter by partial set of keys
        setFullyDisabled();
        releaseExactValues();
    }
}

void ExactNotContainsRuntimeFilter::merge(const IRuntimeFilter * source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after it was marked as finished");

    const auto * source_typed = typeid_cast<const ExactNotContainsRuntimeFilter *>(source);
    if (!source_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    insert(source_typed->getValuesColumn());
    --filters_to_merge;
}

bool ApproximateRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    /// Runtime BloomFilter hashing uses byte representation from either fixed contiguous column storage or getDataAt().
    /// LowCardinality reports a contiguous representation unconditionally, but its getDataAt() delegates to the
    /// dictionary column; for LowCardinality(Nullable(...)) that is ColumnNullable::getDataAt(), which throws on a NULL.
    /// Strip LowCardinality and test the inner type so LC(Nullable(...)) falls back to the exact (NULL-safe) Set path,
    /// exactly like a plain Nullable(...) key already does.
    return removeLowCardinality(data_type)->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
}

ApproximateRuntimeFilter::ApproximateRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    std::optional<UInt64> distinct_keys_hint_)
    : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_, bytes_limit_, exact_values_limit_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , distinct_keys_hint(distinct_keys_hint_)
    , bloom_filter(nullptr)
{}

void ApproximateRuntimeFilter::insert(ColumnPtr values)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

    if (bloom_filter)
    {
        /// Bloom mode dropped the values; track the envelope here.
        updateRange(*values);
        insertIntoBloomFilter(values);
    }
    else
    {
        if (isFull())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected 'full' state of ApproximateRuntimeFilter");

        Base::insert(std::move(values));

        if (isFull())
            switchToBloomFilter();
    }
}

void ApproximateRuntimeFilter::finishInsertImpl()
{
    if (bloom_filter)
    {
        checkBloomFilterWorthiness();
        return;
    }

    Base::finishInsertImpl();
}

/// Add all keys from one filter to the other so that destination filter contains the union of both filters.
void ApproximateRuntimeFilter::merge(const IRuntimeFilter * source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after it was marked as finished");

    const auto * source_typed = typeid_cast<const ApproximateRuntimeFilter *>(source);
    if (!source_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    if (source_typed->bloom_filter)
    {
        switchToBloomFilter();
        mergeBloomFilters(*bloom_filter, *source_typed->bloom_filter);
    }
    else
    {
        insert(source_typed->getValuesColumn());
    }
    /// Also merge the source's envelope (bloom mode loses source values).
    mergeRange(*source);
    --filters_to_merge;
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

template <bool negate>
ColumnPtr RuntimeFilterBase<negate>::findImpl(const ColumnWithTypeAndName & values) const
{
    chassert(inserts_are_finished);

    switch (values_count)
    {
        case ValuesCount::UNKNOWN:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Run time filter set is not ready for lookups");
        case ValuesCount::ZERO:
            updateStats(values.column->size(), negate ? values.column->size() : 0);
            return DataTypeUInt8().createColumnConst(values.column->size(), negate);
        case ValuesCount::ONE:
        {
            /// If only 1 element in the set then use "value == const" instead of set lookup.
            /// Use the column directly from Set to avoid lossy Field roundtrip.
            ColumnPtr const_column = ColumnConst::create(single_element_column, values.column->size());
            ColumnsWithTypeAndName arguments = {
                values,
                ColumnWithTypeAndName(const_column, filter_column_target_type, String())
            };
            auto single_element_equals_function = FunctionFactory::instance().get(negate ? "notEquals" : "equals", nullptr)->build(arguments);
            auto result = single_element_equals_function->execute(arguments, single_element_equals_function->getResultType(), values.column->size(), /* dry_run = */ false);
            updateStats(values.column->size(), countPassedStats(result));
            return result;
        }
        case ValuesCount::MANY:
        {
            auto result = exact_values->execute({values}, negate);
            updateStats(values.column->size(), countPassedStats(result));
            return result;
        }
    }
    UNREACHABLE();
}

ColumnPtr ApproximateRuntimeFilter::findImpl(const ColumnWithTypeAndName & values) const
{
    chassert(inserts_are_finished);

    if (bloom_filter)
    {
        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(values.column->size());

        size_t found_count = 0;
        forEachColumnHashBatch(*values.column, bloom_filter->getSeed(),
            [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t start_row)
            {
                found_count += bloom_filter->findHashPairs(hash_pairs, count, dst_data.data() + start_row);
            });
        updateStats(values.column->size(), found_count);

        return dst;
    }
    else
    {
        return Base::findImpl(values);
    }
}

void ApproximateRuntimeFilter::insertIntoBloomFilter(ColumnPtr values)
{
    forEachColumnHashBatch(*values, bloom_filter->getSeed(),
        [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t /* start_row */)
        {
            bloom_filter->addHashPairs(hash_pairs, count);
        });
}

void ApproximateRuntimeFilter::switchToBloomFilter()
{
    if (bloom_filter)
        return;

    UInt64 bloom_filter_bytes = getBytesLimit();
    if (distinct_keys_hint)
        bloom_filter_bytes = growBloomFilterBytes(*distinct_keys_hint, bloom_filter_hash_functions, getBytesLimit(), max_ratio_of_set_bits_in_bloom_filter);

    bloom_filter = std::make_unique<BloomFilter>(bloom_filter_bytes, bloom_filter_hash_functions, BLOOM_FILTER_SEED);
    insertIntoBloomFilter(getValuesColumn());

    releaseExactValues();
}

void ApproximateRuntimeFilter::checkBloomFilterWorthiness()
{
    const auto & raw_filter_words = bloom_filter->getFilter();
    const size_t total_bits = raw_filter_words.size() * sizeof(raw_filter_words[0]) * 8;
    size_t set_bits = 0;
    for (auto word : raw_filter_words)
        set_bits += std::popcount(word);
    /// If too many bits are set then it is likely that the filter will not filter out much
    if (static_cast<double>(set_bits) > max_ratio_of_set_bits_in_bloom_filter * static_cast<double>(total_bits))
        setFullyDisabled();
}

SharedFixedHashTableRuntimeFilter::SharedFixedHashTableRuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    ProbeFn probe_fn_,
    std::vector<Range> key_ranges_,
    ColumnPtr recorded_key_values_)
    : IRuntimeFilter(
        /*filters_to_merge_=*/0,
        filter_column_target_type_,
        pass_ratio_threshold_for_disabling_,
        blocks_to_skip_before_reenabling_)
    , probe_fn(std::move(probe_fn_))
    , recorded_key_values(std::move(recorded_key_values_))
{
    /// Build was already done elsewhere; nothing left to insert.
    inserts_are_finished = true;

    /// Carry over the key-range cover recorded by the filter this one replaces, so that granule
    /// pruning on the left side survives the switch to the shared hash table.
    if (range_supported)
    {
        range_cover.reserve(key_ranges_.size());
        for (const auto & range : key_ranges_)
            range_cover.emplace_back(range.left, range.right);
    }
}

ColumnPtr SharedFixedHashTableRuntimeFilter::findImpl(const ColumnWithTypeAndName & values) const
{
    chassert(inserts_are_finished);
    auto result = probe_fn(values);
    updateStats(values.column->size(), countPassedStats(result));
    return result;
}

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & key, const String & display_name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[key];
        if (!filter)
        {
            ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
            filter.reset(runtime_filter.release());   /// Save new filter
            /// Record the readable structural name once (the map is keyed by the opaque rendezvous key).
            display_names.emplace(key, display_name);
        }
        else
        {
            filter->merge(runtime_filter.get());    /// Add all new keys to a existing filter
        }
        filter->finishInsert();
    }

    void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[name];
        if (!filter)
            ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
        filter.reset(runtime_filter.release());
    }

    RuntimeFilterConstPtr find(const String & name) const override
    {
        SharedLockGuard g(rw_lock);
        auto it = filters_by_name.find(name);
        if (it == filters_by_name.end())
            return nullptr;
        else
            return it->second;
    }

    void logStats() const override
    {
        SharedLockGuard g(rw_lock);
        for (const auto & [filter_key, filter] : filters_by_name)
        {
            const auto & stats = filter->getStats();
            /// `filter_key` is the opaque random rendezvous key; prefer the readable structural name.
            auto name_it = display_names.find(filter_key);
            const String & name = (name_it != display_names.end() && !name_it->second.empty()) ? name_it->second : filter_key;
            LOG_TRACE(getLogger("RuntimeFilter"),
                "Stats for '{}': rows skipped {}, rows checked {}, rows passed {}, blocks skipped {}, blocks processed {}",
                name, stats.rows_skipped.load(), stats.rows_checked.load(), stats.rows_passed.load(), stats.blocks_skipped.load(), stats.blocks_processed.load());
        }
    }

private:
    mutable SharedMutex rw_lock;
    std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(rw_lock);
    /// Readable structural name per rendezvous key, for logging. Kept under the same lock and
    /// preserved across `replace` (the replacement keeps the original registration's name).
    std::unordered_map<String, String> display_names TSA_GUARDED_BY(rw_lock);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

/// Build a pruning predicate on the column: IN (exact values) else BETWEEN.
static const ActionsDAG::Node * convertRuntimeFilterToKeyConditionDAG(
    const IRuntimeFilter & filter,
    const String & column_name,
    const DataTypePtr & column_type,
    ActionsDAG & dag,
    const ContextPtr & context)
{
    auto exact_values = filter.getRecordedKeyValues();
    auto ranges = exact_values ? std::vector<Range>{} : filter.getRecordedKeyRanges();
    if (!exact_values && ranges.empty())
        return nullptr;

    /// Work in the filter's target type; cast the column to avoid overflow.
    const auto & target_type = filter.getFilterColumnTargetType();
    const auto & key_node = dag.addInput(column_name, column_type);
    const auto & key_casted = column_type->equals(*target_type)
        ? key_node
        : dag.addCast(key_node, target_type, {}, context);

    if (exact_values)
    {
        LOG_DEBUG(
            getLogger("JoinRuntimeFilterIndexAnalysis"),
            "Index analysis engaged on join key '{}': pruning by exact IN-set of {} value(s)",
            column_name, exact_values->size());

        ColumnWithTypeAndName set_values(exact_values, target_type, "__runtime_filter_in_values_" + column_name);
        auto future_set = std::make_shared<FutureSetFromTuple>(
            CityHash_v1_0_2::uint128{}, ASTPtr{}, ColumnsWithTypeAndName{set_values}, /*transform_null_in=*/false, SizeLimits{});
        auto set_column = ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0);
        const auto & set_node = dag.addColumn(std::move(set_column), std::make_shared<DataTypeSet>(), "__runtime_filter_in_set_" + column_name);

        auto in_func = FunctionFactory::instance().get("in", context);
        return &dag.addFunction(in_func, {&key_casted, &set_node}, {});
    }

    {
        WriteBufferFromOwnString ranges_description;
        for (size_t i = 0; i < ranges.size(); ++i)
            ranges_description << (i ? " OR " : "") << ranges[i].toString();
        LOG_DEBUG(
            getLogger("JoinRuntimeFilterIndexAnalysis"),
            "Index analysis engaged on join key '{}': pruning by {} range(s) {}",
            column_name, ranges.size(), ranges_description.str());
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
        const auto & min_node = dag.addColumn(
            target_type->createColumnConst(1, ranges[i].left), target_type, "__runtime_filter_min" + suffix);
        const auto & max_node = dag.addColumn(
            target_type->createColumnConst(1, ranges[i].right), target_type, "__runtime_filter_max" + suffix);

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
    for (const auto & descr : descriptors)
    {
        /// Fail-open: skip a filter that isn't built yet or lacks a range.
        auto filter = lookup.find(descr.filter_id);
        if (!filter)
            continue;

        if (const auto * predicate = convertRuntimeFilterToKeyConditionDAG(*filter, descr.key_column_name, descr.key_column_type, dag, context))
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
