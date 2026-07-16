#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <fmt/format.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
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

bool typeContainsFloatImpl(const DataTypePtr & type)
{
    const auto nested_type = removeNullable(removeLowCardinality(type));
    if (WhichDataType(nested_type).isNativeFloat())
        return true;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get()))
    {
        for (const auto & element_type : tuple_type->getElements())
        {
            if (typeContainsFloatImpl(element_type))
                return true;
        }
    }

    return false;
}

}

bool runtimeFilterTypeContainsFloat(const DataTypePtr & type)
{
    return typeContainsFloatImpl(type);
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
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot merge Bloom Filters of different sizes: {} and {}",
            destination_words.size() * word_size,
            source_words.size() * word_size);

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

/// Grow the bloom filter bytes to hold `distinct_keys` keys at the target fill rate using
/// `hash_functions` hash functions: filter_bits = -hash_functions * distinct_keys / ln(1 - fill_rate)
/// The formula is built on the following logic:
/// - distinct_keys * hash_functions: total bit-inserts into the filter
/// - filter_bits: the size of the filter in bits (what we solve for)
/// - 1/filter_bits: probability that one bit-insert sets a given bit
/// - (1 - 1/filter_bits)^(distinct_keys * hash_functions): probability that a given bit is not set after all inserts
/// - e^(-distinct_keys * hash_functions / filter_bits) is used to approximate the above probability
/// - 1 - e^(-distinct_keys * hash_functions / filter_bits): expected fraction of bits that end up set (= fill_rate)
/// For more information check: https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf
UInt64 growBloomFilterBytes(UInt64 distinct_keys, UInt64 hash_functions, UInt64 default_bloom_filter_bytes, Float64 max_ratio_of_set_bits)
{
    const Float64 target_fill_rate = std::min(RUNTIME_BLOOM_FILTER_TARGET_FILL_RATE, max_ratio_of_set_bits);
    const double ideal_bloom_filter_bytes
        = std::ceil(-static_cast<double>(hash_functions) * static_cast<double>(distinct_keys) / std::log1p(-target_fill_rate) / 8.0);
    const double clamped_bloom_filter_bytes
        = std::clamp(ideal_bloom_filter_bytes, 0.0, static_cast<double>(MAX_STATS_SIZED_BLOOM_FILTER_BYTES));
    return std::max(static_cast<UInt64>(clamped_bloom_filter_bytes), default_bloom_filter_bytes);
}
}

namespace
{

template <typename T>
T getColumnValue(const IColumn & column, size_t row)
{
    if constexpr (std::is_same_v<T, UInt64>)
        return column.getUInt(row);
    else if constexpr (std::is_same_v<T, Int64>)
        return column.getInt(row);
    else if constexpr (std::is_same_v<T, Float32>)
        return column.getFloat32(row);
    else
        return column.getFloat64(row);
}

}

template <typename T>
ApproximateNumericRuntimeFilter<T>::ApproximateNumericRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    std::optional<UInt64> distinct_keys_hint_)
    : ApproximateGenericRuntimeFilter(
          filters_to_merge_,
          filter_column_target_type_,
          pass_ratio_threshold_for_disabling_,
          blocks_to_skip_before_reenabling_,
          bytes_limit_,
          exact_values_limit_,
          bloom_filter_hash_functions_,
          max_ratio_of_set_bits_in_bloom_filter_,
          distinct_keys_hint_)
    , min_value(std::numeric_limits<T>::max())
    , max_value(std::numeric_limits<T>::lowest())
{
}

template <typename T>
void ApproximateNumericRuntimeFilter<T>::finishInsertImpl()
{
    if (isApproximate())
    {
        /// If the Bloom filter is saturated, keep the min/max range as a safe over-approximation
        /// instead of disabling the whole numeric runtime filter. This is still useful for highly
        /// selective ranges and cannot introduce false negatives.
        use_range_only = !isBloomFilterWorthwhile();
        return;
    }

    Base::finishInsertImpl();
}

template <typename T>
ColumnPtr ApproximateNumericRuntimeFilter<T>::findImpl(const ColumnWithTypeAndName & values) const
{
    chassert(inserts_are_finished);

    if (isApproximate())
    {
        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(values.column->size());

        size_t found_count = 0;
        for (size_t row = 0; row < values.column->size(); ++row)
        {
            T value = getColumnValue<T>(*values.column, row);
            /// The range check is a safe over-approximation of the build-side keys. For floating
            /// point values, `mayContain` handles NaN separately because ordinary comparisons with
            /// NaN are always false while JOIN key membership can still match NaN keys.
            bool found = mayContain(value);
            if (found && !use_range_only)
            {
                if constexpr (!std::is_floating_point_v<T>)
                    found = lookupInBloomFilter(values.column, row);
                else if (!std::isnan(value))
                    found = lookupInBloomFilter(values.column, row);
            }
            found_count += found ? 1 : 0;
            dst_data[row] = found;
        }
        updateStats(values.column->size(), found_count);

        return dst;
    }
    else
    {
        return lookupInExactSet(values);
    }
}

template <typename T>
void ApproximateNumericRuntimeFilter<T>::merge(const IRuntimeFilter * source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after it was marked as finished");

    const auto * source_typed = typeid_cast<const ApproximateNumericRuntimeFilter<T> *>(source);
    if (!source_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    auto merge_approximate_filters = [this](const ApproximateNumericRuntimeFilter<T> * other)
    {
        /// Merge ranges.
        if (other->has_comparable_value)
        {
            if (!has_comparable_value || other->min_value < min_value)
                min_value = other->min_value;
            if (!has_comparable_value || other->max_value > max_value)
                max_value = other->max_value;
            has_comparable_value = true;
        }
        has_nan = has_nan || other->has_nan;
        use_range_only = use_range_only || other->use_range_only;

        /// Merge bloom filters.
        Base::mergeImpl(other);
    };

    if (isApproximate())
    {
        /// This filter is in minmax mode
        if (source_typed->isApproximate())
        {
            merge_approximate_filters(source_typed);
        }
        else
        {
            /// Source is in exact mode, insert its values to update min/max
            insert(source_typed->getValuesColumn());
        }
    }
    else
    {
        /// This filter is in exact mode, insert source values
        if (source_typed->isApproximate())
        {
            /// Source is in minmax mode, switch to minmax and merge
            switchToApproximateSet();
            merge_approximate_filters(source_typed);
        }
        else
        {
            /// Both are in exact mode, insert source values
            insert(source_typed->getValuesColumn());
        }
    }

    --filters_to_merge;
}

template <typename T>
void ApproximateNumericRuntimeFilter<T>::insertIntoApproximateSet(ColumnPtr values, size_t row)
{
    T value = getColumnValue<T>(*values, row);
    if constexpr (std::is_floating_point_v<T>)
    {
        if (std::isnan(value))
        {
            has_nan = true;
            Base::insertIntoApproximateSet(values, row);
            return;
        }
    }

    if (!has_comparable_value || value < min_value)
        min_value = value;
    if (!has_comparable_value || value > max_value)
        max_value = value;
    has_comparable_value = true;

    Base::insertIntoApproximateSet(values, row);
}

template <typename T>
bool ApproximateNumericRuntimeFilter<T>::mayContain(T value) const
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (std::isnan(value))
            return has_nan;
    }

    return has_comparable_value && min_value <= value && value <= max_value;
}

template <typename T>
String ApproximateNumericRuntimeFilter<T>::getModeForLogs() const
{
    if (!isApproximate())
        return "exact";
    return use_range_only ? "minmax" : "bloom_minmax";
}

template <typename T>
String ApproximateNumericRuntimeFilter<T>::getExtraInfoForLogs() const
{
    if (!isApproximate())
        return {};

    if constexpr (std::is_floating_point_v<T>)
    {
        return fmt::format("min={} max={} has_comparable_value={} has_nan={}", min_value, max_value, has_comparable_value, has_nan);
    }
    else
    {
        return fmt::format("min={} max={} has_comparable_value={}", min_value, max_value, has_comparable_value);
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

bool ApproximateGenericRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    /// Runtime BloomFilter hashing uses byte representation from either fixed contiguous column storage or getDataAt().
    /// LowCardinality reports a contiguous representation unconditionally, but its getDataAt() delegates to the
    /// dictionary column; for LowCardinality(Nullable(...)) that is ColumnNullable::getDataAt(), which throws on a NULL.
    /// Strip LowCardinality and test the inner type so LC(Nullable(...)) falls back to the exact (NULL-safe) Set path,
    /// exactly like a plain Nullable(...) key already does.
    return removeLowCardinality(data_type)->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
}

ApproximateGenericRuntimeFilter::ApproximateGenericRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    std::optional<UInt64> distinct_keys_hint_)
    : RuntimeFilterBase(
          filters_to_merge_,
          filter_column_target_type_,
          pass_ratio_threshold_for_disabling_,
          blocks_to_skip_before_reenabling_,
          bytes_limit_,
          exact_values_limit_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , distinct_keys_hint(distinct_keys_hint_)
    , bloom_filter(nullptr)
{
}

void ApproximateGenericRuntimeFilter::insert(ColumnPtr values)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

    if (bloom_filter)
    {
        build_rows += values->size();
        insertIntoBloomFilter(values);
    }
    else
    {
        if (isFull())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected 'full' state of ApproximateRuntimeFilter");

        Base::insert(std::move(values));

        if (isFull())
            switchToApproximateSet();
    }
}

void ApproximateGenericRuntimeFilter::finishInsertImpl()
{
    if (bloom_filter)
    {
        checkBloomFilterWorthiness();
        return;
    }

    Base::finishInsertImpl();
}

/// Add all keys from one filter to the other so that destination filter contains the union of both filters.
void ApproximateGenericRuntimeFilter::merge(const IRuntimeFilter * source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after it was marked as finished");

    const auto * source_typed = typeid_cast<const ApproximateGenericRuntimeFilter *>(source);
    if (!source_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    mergeImpl(source_typed);
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
        case ValuesCount::UNKNOWN: throw Exception(ErrorCodes::LOGICAL_ERROR, "Run time filter set is not ready for lookups");
        case ValuesCount::ZERO:
            updateStats(values.column->size(), negate ? values.column->size() : 0);
            return DataTypeUInt8().createColumnConst(values.column->size(), negate);
        case ValuesCount::ONE: {
            /// If only 1 element in the set then use "value == const" instead of set lookup.
            /// Use the column directly from Set to avoid lossy Field roundtrip.
            ColumnPtr const_column = ColumnConst::create(single_element_column, values.column->size());
            ColumnsWithTypeAndName arguments = {values, ColumnWithTypeAndName(const_column, filter_column_target_type, String())};
            auto single_element_equals_function
                = FunctionFactory::instance().get(negate ? "notEquals" : "equals", nullptr)->build(arguments);
            auto result = single_element_equals_function->execute(
                arguments, single_element_equals_function->getResultType(), values.column->size(), /* dry_run = */ false);
            updateStats(values.column->size(), countPassedStats(result));
            return result;
        }
        case ValuesCount::MANY: {
            auto result = exact_values->execute({values}, negate);
            updateStats(values.column->size(), countPassedStats(result));
            return result;
        }
    }
    UNREACHABLE();
}

ColumnPtr ApproximateGenericRuntimeFilter::findImpl(const ColumnWithTypeAndName & values) const
{
    chassert(inserts_are_finished);

    if (bloom_filter)
    {
        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(values.column->size());

        size_t found_count = 0;
        forEachColumnHashBatch(
            *values.column,
            bloom_filter->getSeed(),
            [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t start_row)
            { found_count += bloom_filter->findHashPairs(hash_pairs, count, dst_data.data() + start_row); });
        updateStats(values.column->size(), found_count);

        return dst;
    }
    else
    {
        return Base::findImpl(values);
    }
}

void ApproximateGenericRuntimeFilter::insertIntoApproximateSet(ColumnPtr values, size_t row)
{
    const auto value = values->getDataAt(row);
    bloom_filter->add(value.data(), value.size());
}

void ApproximateGenericRuntimeFilter::mergeImpl(const ApproximateGenericRuntimeFilter * source)
{
    if (source->bloom_filter)
    {
        switchToApproximateSet();
        mergeBloomFilters(*bloom_filter, *source->bloom_filter);
    }
    else
    {
        insert(source->getValuesColumn());
    }
}

bool ApproximateGenericRuntimeFilter::lookupInBloomFilter(ColumnPtr values, size_t row) const
{
    /// TODO: optimize: consider replacing hash calculation with vectorized version
    auto value = values->getDataAt(row);
    return bloom_filter->find(value.data(), value.size());
}

void ApproximateGenericRuntimeFilter::switchToApproximateSet()
{
    if (bloom_filter)
        return;

    UInt64 bloom_filter_bytes = getBytesLimit();
    if (distinct_keys_hint)
        bloom_filter_bytes = growBloomFilterBytes(
            *distinct_keys_hint, bloom_filter_hash_functions, getBytesLimit(), max_ratio_of_set_bits_in_bloom_filter);

    bloom_filter = std::make_unique<BloomFilter>(bloom_filter_bytes, bloom_filter_hash_functions, BLOOM_FILTER_SEED);
    insertIntoBloomFilter(getValuesColumn());

    releaseExactValues();
}

void ApproximateGenericRuntimeFilter::insertIntoBloomFilter(ColumnPtr values)
{
    const size_t num_rows = values->size();
    for (size_t row = 0; row < num_rows; ++row)
    {
        insertIntoApproximateSet(values, row);
    }
}

bool ApproximateGenericRuntimeFilter::isBloomFilterWorthwhile() const
{
    const auto & raw_filter_words = bloom_filter->getFilter();
    const size_t total_bits = raw_filter_words.size() * sizeof(raw_filter_words[0]) * 8;
    size_t set_bits = 0;
    for (auto word : raw_filter_words)
        set_bits += std::popcount(word);

    /// If too many bits are set then it is likely that the filter will not filter out much.
    return static_cast<double>(set_bits) <= max_ratio_of_set_bits_in_bloom_filter * static_cast<double>(total_bits);
}

void ApproximateGenericRuntimeFilter::checkBloomFilterWorthiness()
{
    if (!isBloomFilterWorthwhile())
        setFullyDisabled();
}

String ApproximateGenericRuntimeFilter::getModeForLogs() const
{
    return bloom_filter ? "bloom" : "exact";
}

String ApproximateGenericRuntimeFilter::getExtraInfoForLogs() const
{
    if (!bloom_filter)
        return {};

    const auto & raw_filter_words = bloom_filter->getFilter();
    return fmt::format(
        "bloom_filter_bytes={} bloom_filter_hash_functions={}",
        raw_filter_words.size() * sizeof(raw_filter_words.front()),
        bloom_filter_hash_functions);
}

SharedFixedHashTableRuntimeFilter::SharedFixedHashTableRuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    ProbeFn probe_fn_)
    : IRuntimeFilter(
          /*filters_to_merge_=*/0, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_)
    , probe_fn(std::move(probe_fn_))
{
    /// Build was already done elsewhere; nothing left to insert.
    inserts_are_finished = true;
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
            filter.reset(runtime_filter.release()); /// Save new filter
            /// Record the readable structural name once (the map is keyed by the opaque rendezvous key).
            display_names.emplace(key, display_name);
        }
        else
        {
            filter->merge(runtime_filter.get()); /// Add all new keys to a existing filter
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
            const String extra_info = filter->getExtraInfoForLogs();
            LOG_TRACE(
                getLogger("RuntimeFilter"),
                "Stats for '{}': mode {}, target_type {}, build_rows {}, rows skipped {}, rows checked {}, rows passed {}, blocks skipped "
                "{}, blocks processed {}, fully_disabled {}, details: {}",
                name,
                filter->getModeForLogs(),
                filter->getFilterColumnTargetType()->getName(),
                filter->getBuildRows(),
                stats.rows_skipped.load(),
                stats.rows_checked.load(),
                stats.rows_passed.load(),
                stats.blocks_skipped.load(),
                stats.blocks_processed.load(),
                filter->isFullyDisabled(),
                extra_info.empty() ? "-" : extra_info);
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

template class ApproximateNumericRuntimeFilter<UInt64>;
template class ApproximateNumericRuntimeFilter<Int64>;
template class ApproximateNumericRuntimeFilter<Float32>;
template class ApproximateNumericRuntimeFilter<Float64>;

namespace
{

template <typename T>
UniqueRuntimeFilterPtr createApproximateNumericRuntimeFilterImpl(
    size_t filters_to_merge,
    const DataTypePtr & filter_column_target_type,
    Float64 pass_ratio_threshold_for_disabling,
    UInt64 blocks_to_skip_before_reenabling,
    UInt64 bytes_limit,
    UInt64 exact_values_limit,
    UInt64 bloom_filter_hash_functions,
    Float64 max_ratio_of_set_bits_in_bloom_filter,
    std::optional<UInt64> distinct_keys_hint)
{
    return std::make_unique<ApproximateNumericRuntimeFilter<T>>(
        filters_to_merge,
        filter_column_target_type,
        pass_ratio_threshold_for_disabling,
        blocks_to_skip_before_reenabling,
        bytes_limit,
        exact_values_limit,
        bloom_filter_hash_functions,
        max_ratio_of_set_bits_in_bloom_filter,
        distinct_keys_hint);
}

}

UniqueRuntimeFilterPtr createApproximateNumericRuntimeFilter(
    size_t filters_to_merge,
    const DataTypePtr & filter_column_target_type,
    Float64 pass_ratio_threshold_for_disabling,
    UInt64 blocks_to_skip_before_reenabling,
    UInt64 bytes_limit,
    UInt64 exact_values_limit,
    UInt64 bloom_filter_hash_functions,
    Float64 max_ratio_of_set_bits_in_bloom_filter,
    std::optional<UInt64> distinct_keys_hint)
{
    WhichDataType which(filter_column_target_type);
    if (which.isNativeUInt())
        return createApproximateNumericRuntimeFilterImpl<UInt64>(
            filters_to_merge,
            filter_column_target_type,
            pass_ratio_threshold_for_disabling,
            blocks_to_skip_before_reenabling,
            bytes_limit,
            exact_values_limit,
            bloom_filter_hash_functions,
            max_ratio_of_set_bits_in_bloom_filter,
            distinct_keys_hint);
    if (which.isNativeInt())
        return createApproximateNumericRuntimeFilterImpl<Int64>(
            filters_to_merge,
            filter_column_target_type,
            pass_ratio_threshold_for_disabling,
            blocks_to_skip_before_reenabling,
            bytes_limit,
            exact_values_limit,
            bloom_filter_hash_functions,
            max_ratio_of_set_bits_in_bloom_filter,
            distinct_keys_hint);
    if (which.isFloat32())
        return createApproximateNumericRuntimeFilterImpl<Float32>(
            filters_to_merge,
            filter_column_target_type,
            pass_ratio_threshold_for_disabling,
            blocks_to_skip_before_reenabling,
            bytes_limit,
            exact_values_limit,
            bloom_filter_hash_functions,
            max_ratio_of_set_bits_in_bloom_filter,
            distinct_keys_hint);
    if (which.isFloat64())
        return createApproximateNumericRuntimeFilterImpl<Float64>(
            filters_to_merge,
            filter_column_target_type,
            pass_ratio_threshold_for_disabling,
            blocks_to_skip_before_reenabling,
            bytes_limit,
            exact_values_limit,
            bloom_filter_hash_functions,
            max_ratio_of_set_bits_in_bloom_filter,
            distinct_keys_hint);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported type for ApproximateNumericRuntimeFilter");
}

}
