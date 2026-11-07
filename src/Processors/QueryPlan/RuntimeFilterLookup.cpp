#include <algorithm>
#include <bit>
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/hasNullable.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/castTypeToEither.h>
#include <Functions/IFunctionAdaptors.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/FailPoint.h>
#include <Common/MergeLock.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <base/TypeLists.h>
#include <base/scope_guard.h>

namespace ProfileEvents
{
    extern const Event RuntimeFiltersCreated;
    extern const Event RuntimeFilterBlocksProcessed;
    extern const Event RuntimeFilterBlocksSkipped;
    extern const Event RuntimeFilterRowsChecked;
    extern const Event RuntimeFilterRowsPassed;
    extern const Event RuntimeFilterRowsSkipped;
    extern const Event RuntimeFilterBloomFilterBuildsSkipped;
    extern const Event RuntimeFilterDenseBitsetsBuilt;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char runtime_filter_skip_finish_insert[];
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

void RuntimeFilterBuildState::finishMerge(size_t filters)
{
    assertCanMerge();
    chassert(filters != 0);
    if (filters > filters_to_merge)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge more runtime filters than expected");
    filters_to_merge -= filters;
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

bool detail::RuntimeFilterIndexAnalysis::supportsDataType(const DataTypePtr & data_type)
{
    return runtimeFilterKeySupportsMinMaxRange(data_type);
}

bool runtimeFilterKeySupportsMinMaxRange(const DataTypePtr & data_type)
{
    if (!data_type)
        return false;

    DataTypePtr inner = removeNullable(recursiveRemoveLowCardinality(data_type));
    WhichDataType which(inner);
    return which.isInteger() || which.isDateOrDate32OrDateTimeOrDateTime64();
}

detail::RuntimeFilterIndexAnalysis::RuntimeFilterIndexAnalysis(const DataTypePtr & data_type, bool positive_filter_)
    : range_supported(supportsDataType(data_type))
    , positive_filter(positive_filter_)
{
}

void detail::RuntimeFilterIndexAnalysis::setRange(const Range & range)
{
    if (!range_supported || !positive_filter)
        return;

    range_min = range.left;
    range_max = range.right;
    has_range = true;
}

void detail::RuntimeFilterIndexAnalysis::extendRange(const Field & new_min, const Field & new_max)
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

void detail::RuntimeFilterIndexAnalysis::insert(const IColumn & values)
{
    if (!enabled || !range_supported || !positive_filter || values.empty())
        return;

    Field column_min;
    Field column_max;
    values.getExtremes(column_min, column_max, 0, values.size());
    if (!column_min.isNull() && !column_max.isNull())
        extendRange(column_min, column_max);
}

void detail::RuntimeFilterIndexAnalysis::mergeFrom(const RuntimeFilterIndexAnalysis & source)
{
    if (enabled && range_supported && positive_filter && source.has_range)
        extendRange(source.range_min, source.range_max);
}

std::optional<Range> detail::RuntimeFilterIndexAnalysis::getRange() const
{
    if (!enabled || !range_supported || !positive_filter || !has_range || range_min.isNull() || range_max.isNull())
        return {};
    return Range(range_min, true, range_max, true);
}

namespace
{

/// Whether `equals` can answer differently from the bitwise comparison a hash table performs on keys:
/// NaN is not equal to itself, and -0.0 is equal to 0.0. A JSON column counts as a whole, because a
/// float can appear on a path discovered while reading, which is not among the type's static children.
bool equalsCanDisagreeWithHashTable(const IDataType & type)
{
    return anyInTypeTree(type, [](const IDataType & nested) { return isFloat(nested) || isObject(nested); });
}

/// A runtime filter key comes from an `=` predicate, which never matches NULL, so a `Nullable` integer key keeps
/// only its non-NULL values, in the set of the nested type.
bool canDropNullKeys(const DataTypePtr & type)
{
    if (!type->isNullable())
        return false;
    const auto nested = removeNullable(type);
    return nested->isValueRepresentedByInteger() && nested->getSizeOfValueInMemory() <= 16;
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

constexpr UInt64 BITSET_WORD_BITS = 64;

using NativeIntDataTypes = TypeListMap<DataTypeNumber, TypeListNativeInt>;
using NativeIntColumns = TypeListMap<ColumnVector, TypeListNativeInt>;

bool isDenseBitsetKeyType(const DataTypePtr & type)
{
    return castTypeToEither(NativeIntDataTypes{}, type.get(), [](const auto &) { return true; });
}

/// Calls `op` with the data of a native integer column. Returns false for any other column.
template <typename Op>
bool visitNativeIntColumnData(const IColumn & column, Op && op)
{
    return castTypeToEither(NativeIntColumns{}, &column, [&](const auto & typed_column)
    {
        op(typed_column.getData());
        return true;
    });
}

/// Aligns down to a word boundary. In two's complement this also floors negative values.
Int64 alignDownToWord(Int64 value)
{
    return static_cast<Int64>(static_cast<UInt64>(value) & ~(BITSET_WORD_BITS - 1));
}

/// Words of a bitset that covers [min_value, max_value] from the word boundary below `min_value`.
UInt64 bitsetWordsForRange(Int64 min_value, Int64 max_value)
{
    return (static_cast<UInt64>(max_value) - static_cast<UInt64>(alignDownToWord(min_value))) / BITSET_WORD_BITS + 1;
}

/// Range of a native integer column in Int64. Returns false for another column, an empty one, or keys
/// that do not form an ordered Int64 interval away from the Int64 bounds.
bool getNativeIntColumnRange(const IColumn & column, Int64 & out_min, Int64 & out_max)
{
    bool representable = true;
    bool empty = true;
    Int64 min_value = 0;
    Int64 max_value = 0;
    const bool is_native_int = visitNativeIntColumnData(column, [&](const auto & data)
    {
        using T = std::decay_t<decltype(data[0])>;
        if (data.empty())
            return;
        T typed_min = data[0];
        T typed_max = data[0];
        for (const T value : data)
        {
            typed_min = std::min(typed_min, value);
            typed_max = std::max(typed_max, value);
        }
        if constexpr (std::is_unsigned_v<T> && sizeof(T) == sizeof(Int64))
            representable = typed_max <= static_cast<T>(std::numeric_limits<Int64>::max());
        min_value = static_cast<Int64>(typed_min);
        max_value = static_cast<Int64>(typed_max);
        empty = false;
    });
    if (!is_native_int || !representable || empty)
        return false;
    /// Keep the range away from the Int64 bounds, so that widening it cannot overflow.
    static constexpr Int64 SAFE_BOUND = std::numeric_limits<Int64>::max() / 4;
    if (min_value < -SAFE_BOUND || max_value > SAFE_BOUND)
        return false;
    out_min = min_value;
    out_max = max_value;
    return true;
}

template <typename... Ts>
struct Overloaded : Ts...
{
    using Ts::operator()...;
};

template <typename... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

}

static size_t countPassedStats(ColumnPtr values);

template <bool negate>
ExactSetRuntimeFilter<negate>::ExactSetRuntimeFilter(
    const DataTypePtr & filter_column_target_type_, UInt64 bytes_limit_, UInt64 exact_values_limit_)
    : filter_column_target_type(filter_column_target_type_)
    , argument_can_have_nulls(hasTypeThatCanContainNulls(filter_column_target_type_))
    , drop_null_keys(canDropNullKeys(filter_column_target_type_))
    , bytes_limit(bytes_limit_)
    , exact_values_limit(exact_values_limit_)
    , lookup_state(Many{std::make_shared<Set>(SizeLimits{}, -1, argument_can_have_nulls && !drop_null_keys)})
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
ColumnPtr ExactSetRuntimeFilter<negate>::toFilterColumnType(const ColumnPtr & set_elements) const
{
    return drop_null_keys ? makeNullable(set_elements) : set_elements;
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
    const size_t set_bytes = bound_key_bytes ? set.getSetElementsBytes() : set.getTotalByteCount();
    is_full = set.getTotalRowCount() > exact_values_limit || set_bytes > bytes_limit;
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
        lookup_state = Empty{toFilterColumnType(set.getSetElements().front())};
        return;
    }

    /// If only one element is in the set then use `equals` instead of set lookup.
    /// If the argument is `Nullable`, use `Set` because it can handle `NULL` values.
    /// If `equals` can disagree with the hash table, use `Set`: this filter must not reject a row the join matches.
    if (set.getTotalRowCount() == 1 && !argument_can_have_nulls
        && !equalsCanDisagreeWithHashTable(*filter_column_target_type))
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
            [this](const Many & many) -> ColumnPtr
            {
                if (!many.exact_values)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter exact values are not available");

                many.exact_values->finishInsert();
                return toFilterColumnType(many.exact_values->getSetElements().front());
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

void ApproximateSetRuntimeFilter::serialize(WriteBuffer & out) const
{
    writeVarUInt(bloom_filter.getFilterSizeBytes(), out);
    writeVarUInt(bloom_filter.getHashes(), out);
    writeVarUInt(bloom_filter.getSeed(), out);
    const auto & words = bloom_filter.getFilter();
    out.write(reinterpret_cast<const char *>(words.data()), words.size() * sizeof(words[0]));
}

ApproximateSetRuntimeFilter ApproximateSetRuntimeFilter::deserialize(ReadBuffer & in, const RuntimeFilterGeometry & geometry)
{
    UInt64 size_bytes{};
    UInt64 hash_functions{};
    UInt64 seed{};
    readVarUInt(size_bytes, in);
    readVarUInt(hash_functions, in);
    readVarUInt(seed, in);
    /// The bloom size comes from `bloom_filter_bytes`, never from the exact-phase byte bound: the
    /// sender may have been allowed a bigger exact set than the plain settings geometry, but every
    /// receiver of the same exchange has to allocate the same bits or the merged bits are garbage.
    if (size_bytes != geometry.bloom_filter_bytes || hash_functions != geometry.bloom_filter_hash_functions
        || seed != BLOOM_FILTER_SEED)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Runtime filter bloom parameters ({}, {}, {}) do not match the expected ({}, {}, {})",
            size_bytes,
            hash_functions,
            seed,
            geometry.bloom_filter_bytes,
            geometry.bloom_filter_hash_functions,
            BLOOM_FILTER_SEED);

    ApproximateSetRuntimeFilter filter(size_bytes, hash_functions);
    auto & words = filter.bloom_filter.getFilter();
    in.readStrict(reinterpret_cast<char *>(words.data()), words.size() * sizeof(words[0]));
    return filter;
}

bool DenseBitsetRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    return isDenseBitsetKeyType(data_type);
}

DenseBitsetRuntimeFilter::DenseBitsetRuntimeFilter(const DataTypePtr & filter_column_target_type_)
    : filter_column_target_type(filter_column_target_type_)
    , pending_keys(filter_column_target_type_->createColumn())
{
}

bool DenseBitsetRuntimeFilter::insert(const ColumnPtr & values, UInt64 max_bytes)
{
    const ColumnPtr block = values->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    if (block->empty())
        return true;

    Int64 block_min = 0;
    Int64 block_max = 0;
    if (!getNativeIntColumnRange(*block, block_min, block_max))
        return false;
    const Int64 new_min = has_keys ? std::min(min_key, block_min) : block_min;
    const Int64 new_max = has_keys ? std::max(max_key, block_max) : block_max;
    if (bitsetWordsForRange(new_min, new_max) > max_bytes / sizeof(UInt64))
        return false;

    has_keys = true;
    min_key = new_min;
    max_key = new_max;

    const Int64 words_end = words_begin + static_cast<Int64>(words.size() * BITSET_WORD_BITS);
    if (!words.empty() && block_min >= words_begin && block_max < words_end)
    {
        setBits(*block);
        return true;
    }

    {
        /// `pending_keys` stays valid if the append throws: `ISimpleTransform` keeps inserting after an exception.
        auto keys = IColumn::mutate(std::move(pending_keys));
        SCOPE_EXIT({ pending_keys = std::move(keys); });
        keys->insertRangeFrom(*block, 0, block->size());
    }
    if (pending_keys->byteSize() >= bitsetWordsForRange(min_key, max_key) * sizeof(UInt64))
        setPendingBits();
    return true;
}

bool DenseBitsetRuntimeFilter::mergeFrom(const DenseBitsetRuntimeFilter & source, UInt64 max_bytes)
{
    if (!source.has_keys)
        return true;

    const Int64 new_min = has_keys ? std::min(min_key, source.min_key) : source.min_key;
    const Int64 new_max = has_keys ? std::max(max_key, source.max_key) : source.max_key;
    if (bitsetWordsForRange(new_min, new_max) > max_bytes / sizeof(UInt64))
        return false;

    has_keys = true;
    min_key = new_min;
    max_key = new_max;

    if (!words.empty() || !source.words.empty()
        || pending_keys->byteSize() + source.pending_keys->byteSize() >= bitsetWordsForRange(min_key, max_key) * sizeof(UInt64))
    {
        /// Our bits cover the range of all keys after this, so they also cover the keys and the bits of the source.
        setPendingBits();
        if (!source.pending_keys->empty())
            setBits(*source.pending_keys);
        if (!source.words.empty())
        {
            const size_t offset = static_cast<size_t>((source.words_begin - words_begin) / static_cast<Int64>(BITSET_WORD_BITS));
            chassert(source.words_begin >= words_begin && offset + source.words.size() <= words.size());
            for (size_t i = 0; i < source.words.size(); ++i)
                words[offset + i] |= source.words[i];
        }
    }
    else if (!source.pending_keys->empty())
    {
        auto keys = IColumn::mutate(std::move(pending_keys));
        SCOPE_EXIT({ pending_keys = std::move(keys); });
        keys->reserve(keys->size() + source.pending_keys->size());
        keys->insertRangeFrom(*source.pending_keys, 0, source.pending_keys->size());
    }
    return true;
}

void DenseBitsetRuntimeFilter::finishInsert()
{
    if (!has_keys || (pending_keys->empty() && !words.empty()))
        return;

    setPendingBits();
    LOG_TRACE(
        getLogger("RuntimeFilter"),
        "Dense bitset runtime filter over [{}, {}], {} bytes",
        min_key,
        max_key,
        getBytes());
}

void DenseBitsetRuntimeFilter::setPendingBits()
{
    /// Keys only widen the range, so the new bits contain the old ones.
    const Int64 new_begin = alignDownToWord(min_key);
    const UInt64 new_size = bitsetWordsForRange(min_key, max_key);
    if (new_begin != words_begin || new_size != words.size())
    {
        VectorWithMemoryTracking<UInt64> widened(new_size, 0);
        if (!words.empty())
        {
            const UInt64 offset = (static_cast<UInt64>(words_begin) - static_cast<UInt64>(new_begin)) / BITSET_WORD_BITS;
            chassert(words_begin >= new_begin && offset + words.size() <= new_size);
            std::copy(words.begin(), words.end(), widened.begin() + offset);
        }
        words.swap(widened);
        words_begin = new_begin;
    }

    if (!pending_keys->empty())
    {
        setBits(*pending_keys);
        pending_keys = filter_column_target_type->createColumn();
    }
}

void DenseBitsetRuntimeFilter::setBits(const IColumn & column)
{
    UInt64 * const bitset = words.data();
    const UInt64 begin = static_cast<UInt64>(words_begin);
    const bool is_native_int = visitNativeIntColumnData(column, [&](const auto & data)
    {
        for (const auto value : data)
        {
            const UInt64 index = static_cast<UInt64>(static_cast<Int64>(value)) - begin;
            chassert(index < words.size() * BITSET_WORD_BITS);
            bitset[index / BITSET_WORD_BITS] |= 1ULL << (index % BITSET_WORD_BITS);
        }
    });
    if (!is_native_int)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in a dense bitset runtime filter", column.getName());
}

ColumnPtr DenseBitsetRuntimeFilter::find(const ColumnWithTypeAndName & values, std::optional<size_t> & rows_passed) const
{
    const ColumnPtr probe = values.column->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    const size_t rows = probe->size();

    if (!pending_keys->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dense bitset runtime filter is not ready for lookups");
    if (words.empty())
    {
        rows_passed = 0;
        return DataTypeUInt8().createColumnConst(rows, false);
    }

    auto result = ColumnUInt8::create(rows);
    auto & result_data = result->getData();
    const UInt64 * const bitset = words.data();
    const UInt64 begin = static_cast<UInt64>(words_begin);
    const UInt64 bits = words.size() * BITSET_WORD_BITS;
    size_t passed = 0;

    /// A value outside the covered range maps to an index of at least `bits` and is a definite miss.
    const bool is_native_int = visitNativeIntColumnData(*probe, [&](const auto & data)
    {
        for (size_t row = 0; row < rows; ++row)
        {
            const UInt64 index = static_cast<UInt64>(static_cast<Int64>(data[row])) - begin;
            const bool in_range = index < bits;
            const UInt64 safe_index = in_range ? index : 0;
            const UInt8 found = static_cast<UInt8>(in_range) & static_cast<UInt8>((bitset[safe_index / BITSET_WORD_BITS] >> (safe_index % BITSET_WORD_BITS)) & 1);
            result_data[row] = found;
            passed += found;
        }
    });

    if (!is_native_int)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in a dense bitset runtime filter lookup", probe->getName());

    rows_passed = passed;
    return result;
}

void DenseBitsetRuntimeFilter::forEachValuesBatch(size_t max_rows, const std::function<void(ColumnPtr)> & callback) const
{
    chassert(max_rows > 0);
    const bool supported = castTypeToEither(NativeIntDataTypes{}, filter_column_target_type.get(), [&](const auto & data_type)
    {
        using DataType = std::decay_t<decltype(data_type)>;
        using T = typename DataType::FieldType;
        using ColumnType = typename DataType::ColumnType;

        auto batch = ColumnType::create();
        batch->getData().reserve(max_rows);
        const UInt64 begin = static_cast<UInt64>(words_begin);
        for (size_t word_index = 0; word_index < words.size(); ++word_index)
        {
            for (UInt64 word = words[word_index]; word != 0; word &= word - 1)
            {
                batch->getData().push_back(static_cast<T>(begin + word_index * BITSET_WORD_BITS + std::countr_zero(word)));
                if (batch->size() == max_rows)
                {
                    callback(std::move(batch));
                    batch = ColumnType::create();
                    batch->getData().reserve(max_rows);
                }
            }
        }
        if (!batch->empty())
            callback(std::move(batch));
        return true;
    });
    if (!supported)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Dense bitset runtime filter has key type {}", filter_column_target_type->getName());

    for (size_t offset = 0; offset < pending_keys->size(); offset += max_rows)
        callback(pending_keys->cut(offset, std::min(max_rows, pending_keys->size() - offset)));
}

UInt64 DenseBitsetRuntimeFilter::countDistinctKeys()
{
    if (!has_keys)
        return 0;
    setPendingBits();
    UInt64 keys = 0;
    for (const UInt64 word : words)
        keys += std::popcount(word);
    return keys;
}

bool AdaptiveSetRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    return ApproximateSetRuntimeFilter::isDataTypeSupported(data_type);
}

AdaptiveSetRuntimeFilter::AdaptiveSetRuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    const RuntimeFilterGeometry & geometry_,
    std::optional<UInt64> distinct_keys_hint_,
    bool distinct_keys_hint_matches_filter_key_)
    : filter_column_target_type(filter_column_target_type_)
    , bloom_filter_bytes(geometry_.bloom_filter_bytes)
    , bloom_filter_hash_functions(geometry_.bloom_filter_hash_functions)
    , max_ratio_of_set_bits_in_bloom_filter(geometry_.max_ratio_of_set_bits_in_bloom_filter)
    , distinct_keys_hint(distinct_keys_hint_)
    , distinct_keys_hint_matches_filter_key(distinct_keys_hint_matches_filter_key_)
    , key_range(filter_column_target_type_, /*positive_filter_=*/true)
    , filter(std::in_place_type<ExactFilter>, filter_column_target_type_, geometry_.exact_bytes_limit, geometry_.exact_values_limit)
{
}

void AdaptiveSetRuntimeFilter::boundExactSetByKeyBytes()
{
    dense_bitset_allowed = false;
    if (auto * exact_filter = std::get_if<ExactFilter>(&filter))
        exact_filter->boundKeyBytes();
}

bool AdaptiveSetRuntimeFilter::hasPendingKeys() const
{
    const auto * bitset_filter = std::get_if<DenseBitsetRuntimeFilter>(&filter);
    return bitset_filter && bitset_filter->hasPendingKeys();
}

void AdaptiveSetRuntimeFilter::insert(ColumnPtr values)
{
    if (state_serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after its state was serialized");

    key_range.insert(*values);
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

    if (auto * bitset_filter = std::get_if<DenseBitsetRuntimeFilter>(&filter_))
    {
        if (bitset_filter->insert(values, denseBitsetMaxBytes()))
            return;

        /// The key range no longer fits: `switchToApproximateFilter` moves the recorded keys.
        if (auto * approximate_filter = switchToApproximateFilter(filter_))
            approximate_filter->insert(std::move(values));
        return;
    }

    auto * exact_filter = std::get_if<ExactFilter>(&filter_);
    if (!exact_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of AdaptiveSetRuntimeFilter");

    if (exact_filter->isFull())
    {
        /// The switch away from the full set threw and the caller kept inserting. The full set holds every key, so retry it.
        switchFromExactFilter(filter_);
        insert(std::move(values), filter_);
        return;
    }

    exact_filter->insert(std::move(values));

    if (exact_filter->isFull())
        switchFromExactFilter(filter_);
}

void AdaptiveSetRuntimeFilter::finishInsert(RuntimeFilterEvaluationState & evaluation_state)
{
    std::visit(
        Overloaded{
            [](ExactFilter & exact_filter) { exact_filter.finishInsert(); },
            /// Exact, so there is no fill rate to check. Low selectivity is caught by the pass ratio.
            [](DenseBitsetRuntimeFilter & bitset_filter) { bitset_filter.finishInsert(); },
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
            [&](const DenseBitsetRuntimeFilter & bitset_filter) -> ColumnPtr { return bitset_filter.find(values, rows_passed); },
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
    if (state_serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after its state was serialized");

    /// Without a range from the source, the merged range would miss its keys.
    if (source.key_range.isEnabled())
        key_range.mergeFrom(source.key_range);
    else
        key_range.disable();

    std::visit(
        Overloaded{
            [&](const ExactFilter & source_exact_filter) { insert(source_exact_filter.getValuesColumn(), filter); },
            [&](const DenseBitsetRuntimeFilter & source_bitset_filter)
            {
                if (const auto * exact_filter = std::get_if<ExactFilter>(&filter); exact_filter && dense_bitset_allowed)
                {
                    /// The source holds more keys than an exact set may: continue from its keys and add ours.
                    ColumnPtr own_values = exact_filter->getValuesColumn();
                    DenseBitsetRuntimeFilter merged(filter_column_target_type);
                    if (merged.mergeFrom(source_bitset_filter, denseBitsetMaxBytes()))
                    {
                        filter.emplace<DenseBitsetRuntimeFilter>(std::move(merged));
                        insert(std::move(own_values), filter);
                        return;
                    }
                }
                else if (auto * bitset_filter = std::get_if<DenseBitsetRuntimeFilter>(&filter))
                {
                    if (bitset_filter->mergeFrom(source_bitset_filter, denseBitsetMaxBytes()))
                        return;
                }
                /// Otherwise the ordinary insert path switches to an approximate set if the range does not fit.
                if (std::holds_alternative<KeySetDropped>(filter))
                    return;
                source_bitset_filter.forEachValuesBatch(DEFAULT_BLOCK_SIZE, [&](ColumnPtr batch) { insert(std::move(batch), filter); });
            },
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
    if (!std::holds_alternative<ExactFilter>(filter_) && !std::holds_alternative<DenseBitsetRuntimeFilter>(filter_))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of AdaptiveSetRuntimeFilter");

    /// Only a filter that stays in one pipeline grows its bloom from statistics. A transported
    /// partial carries no hint: its bloom must match the plan geometry on every receiver and must
    /// not cost more on the wire than the settings-sized bloom.
    UInt64 grown_bloom_filter_bytes = bloom_filter_bytes;

    if (distinct_keys_hint)
    {
        grown_bloom_filter_bytes = growRuntimeBloomFilterBytesFromStats(
            *distinct_keys_hint, bloom_filter_hash_functions, bloom_filter_bytes, max_ratio_of_set_bits_in_bloom_filter);

        /// The filter size is capped, so a build side with more distinct keys would produce a Bloom filter
        /// that `checkApproximateFilterWorthiness` discards. Predict that fill rate before constructing it.
        if (distinct_keys_hint_matches_filter_key)
        {
            const double least_distinct_keys
                = static_cast<double>(*distinct_keys_hint) / HashJoinEntry::MAX_OVERESTIMATION_FACTOR;
            const double predicted_fill_rate = estimateRuntimeBloomFilterSetBitsRatio(
                least_distinct_keys, RuntimeBloomFilterParameters{grown_bloom_filter_bytes, bloom_filter_hash_functions});
            if (predicted_fill_rate > max_ratio_of_set_bits_in_bloom_filter)
            {
                ProfileEvents::increment(ProfileEvents::RuntimeFilterBloomFilterBuildsSkipped);
                dropKeySet(filter_);
                return nullptr;
            }
        }
    }

    /// A dense bitset knows its distinct keys exactly, so a Bloom filter that they alone would saturate is not built.
    if (auto * bitset_filter = std::get_if<DenseBitsetRuntimeFilter>(&filter_))
    {
        const UInt64 distinct_keys = bitset_filter->countDistinctKeys();
        if (estimateRuntimeBloomFilterSetBitsRatio(
                static_cast<Float64>(distinct_keys),
                RuntimeBloomFilterParameters{grown_bloom_filter_bytes, bloom_filter_hash_functions})
            > max_ratio_of_set_bits_in_bloom_filter)
        {
            ProfileEvents::increment(ProfileEvents::RuntimeFilterBloomFilterBuildsSkipped);
            dropKeySet(filter_);
            return nullptr;
        }
    }

    /// The keys are read before emplacing the Bloom filter, which destroys their representation.
    ApproximateSetRuntimeFilter approximate_filter(grown_bloom_filter_bytes, bloom_filter_hash_functions);
    if (const auto * exact_filter = std::get_if<ExactFilter>(&filter_))
        approximate_filter.insert(exact_filter->getValuesColumn());
    else
        std::get<DenseBitsetRuntimeFilter>(filter_).forEachValuesBatch(
            DEFAULT_BLOCK_SIZE, [&](ColumnPtr batch) { approximate_filter.insert(std::move(batch)); });
    return &filter_.emplace<ApproximateSetRuntimeFilter>(std::move(approximate_filter));
}

void AdaptiveSetRuntimeFilter::switchFromExactFilter(Filter & filter_)
{
    if (!trySwitchToDenseBitsetFilter(filter_))
        switchToApproximateFilter(filter_);
}

bool AdaptiveSetRuntimeFilter::trySwitchToDenseBitsetFilter(Filter & filter_)
{
    if (!dense_bitset_allowed || !DenseBitsetRuntimeFilter::isDataTypeSupported(filter_column_target_type))
        return false;

    const auto * exact_filter = std::get_if<ExactFilter>(&filter_);
    if (!exact_filter)
        return false;

    const ColumnPtr values = exact_filter->getValuesColumn();
    DenseBitsetRuntimeFilter bitset_filter(filter_column_target_type);
    const UInt64 max_bytes = denseBitsetMaxBytes();
    if (!bitset_filter.insert(values, max_bytes) || bitset_filter.empty())
        return false;

    ProfileEvents::increment(ProfileEvents::RuntimeFilterDenseBitsetsBuilt);
    LOG_TRACE(getLogger("RuntimeFilter"), "Switching {} exact key rows to a dense bitset, allowance {} bytes", values->size(), max_bytes);
    filter_.emplace<DenseBitsetRuntimeFilter>(std::move(bitset_filter));
    return true;
}

UInt64 AdaptiveSetRuntimeFilter::denseBitsetMaxBytes() const
{
    /// The allowance is raised only by the distinct keys of the statistics, never by the build rows, which count duplicates.
    static constexpr UInt64 MAX_DENSE_BITSET_BYTES = 32 * 1024 * 1024;
    static constexpr UInt64 DENSE_BITSET_BYTES_PER_BLOOM_FILTER_BYTE = 16;
    static constexpr UInt64 MAX_TRUSTED_DISTINCT_KEYS_HINT = 1'000'000'000;

    UInt64 allowance = std::min(bloom_filter_bytes, MAX_DENSE_BITSET_BYTES) * DENSE_BITSET_BYTES_PER_BLOOM_FILTER_BYTE;
    if (distinct_keys_hint && distinct_keys_hint_matches_filter_key && *distinct_keys_hint <= MAX_TRUSTED_DISTINCT_KEYS_HINT)
        allowance = std::max(allowance, *distinct_keys_hint * filter_column_target_type->getSizeOfValueInMemory());
    return std::min(allowance, MAX_DENSE_BITSET_BYTES);
}

void AdaptiveSetRuntimeFilter::checkApproximateFilterWorthiness(
    RuntimeFilterEvaluationState & evaluation_state, const ApproximateSetRuntimeFilter & approximate_filter) const
{
    if (!approximate_filter.isWorthUsing(max_ratio_of_set_bits_in_bloom_filter))
        evaluation_state.markKeySetDropped();
}

static constexpr UInt64 RUNTIME_FILTER_STATE_VERSION = 1;

void AdaptiveSetRuntimeFilter::serialize(WriteBuffer & out)
{
    state_serialized = true;

    writeVarUInt(RUNTIME_FILTER_STATE_VERSION, out);

    /// 0: the range is not tracked, 1: tracked, but no keys yet, 2: tracked, followed by min and max.
    const auto range = key_range.getRange();
    writeBinary(UInt8(!key_range.isEnabled() ? 0 : (range ? 2 : 1)), out);
    if (range)
    {
        writeFieldBinary(range->left, out);
        writeFieldBinary(range->right, out);
    }

    /// A transported partial never builds a dense bitset (see `boundExactSetByKeyBytes`), and the wire format has none.
    if (std::holds_alternative<DenseBitsetRuntimeFilter>(filter))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to serialize a runtime filter with a dense bitset");

    if (const auto * approximate_filter = std::get_if<ApproximateSetRuntimeFilter>(&filter))
    {
        writeBinary(UInt8(1), out);
        approximate_filter->serialize(out);
        return;
    }

    /// A filter that gave up its key set passes all rows; there is no partial state to transport
    /// and the wire format has no way to say so. It is unreachable: the key set is only dropped
    /// from the statistics hint, which a transported partial never carries.
    const auto * exact_filter = std::get_if<ExactFilter>(&filter);
    if (!exact_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to serialize a runtime filter that gave up its key set");

    writeBinary(UInt8(0), out);

    /// Set elements have `LowCardinality` removed, so the block uses the same type. The row count goes
    /// first so the reader can reject it before allocating.
    auto values = exact_filter->getValuesColumn();
    writeVarUInt(values->size(), out);
    Block block({ColumnWithTypeAndName(values, recursiveRemoveLowCardinality(filter_column_target_type), "values")});
    NativeWriter writer(out, DBMS_TCP_PROTOCOL_VERSION, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
}

AdaptiveSetRuntimeFilter AdaptiveSetRuntimeFilter::deserialize(
    ReadBuffer & in, const DataTypePtr & filter_column_target_type_, const RuntimeFilterGeometry & geometry_)
{
    UInt64 version{};
    readVarUInt(version, in);
    if (version != RUNTIME_FILTER_STATE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown runtime filter state version {}", version);

    UInt8 key_range_state{};
    readBinary(key_range_state, in);
    if (key_range_state > 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed runtime filter key range");
    std::optional<Range> range;
    if (key_range_state == 2)
    {
        Field range_min = readFieldBinary(in);
        Field range_max = readFieldBinary(in);
        if (range_min.isNull() || range_max.isNull() || range_min.getType() != range_max.getType() || accurateLess(range_max, range_min))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed runtime filter key range");
        range = Range(std::move(range_min), true, std::move(range_max), true);
    }

    UInt8 has_bloom_filter{};
    readBinary(has_bloom_filter, in);
    if (has_bloom_filter > 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed runtime filter state");

    AdaptiveSetRuntimeFilter filter(
        filter_column_target_type_,
        geometry_,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    filter.boundExactSetByKeyBytes();
    if (key_range_state != 0)
        filter.key_range.enable();
    if (range)
        filter.key_range.setRange(*range);

    if (has_bloom_filter)
    {
        filter.filter.emplace<ApproximateSetRuntimeFilter>(ApproximateSetRuntimeFilter::deserialize(in, geometry_));
    }
    else
    {
        UInt64 rows{};
        readVarUInt(rows, in);
        if (rows > geometry_.exact_values_limit)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Runtime filter declares {} exact values, more than the limit {}",
                rows,
                geometry_.exact_values_limit);

        /// The row count does not bound variable-width keys. Without this check a sender could make
        /// us parse far more than `exact_bytes_limit`. The slack covers the Native block header and
        /// the type name.
        const auto element_type = recursiveRemoveLowCardinality(filter_column_target_type_);
        constexpr size_t native_framing_slack_bytes = 64 * 1024;
        const size_t max_state_bytes = geometry_.exact_bytes_limit + element_type->getName().size() + native_framing_slack_bytes;
        if (in.available() > max_state_bytes)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Runtime filter exact state of {} bytes exceeds the limit of {} bytes",
                in.available(),
                max_state_bytes);

        Block block;
        {
            LimitReadBuffer limited(in, {.read_no_more = max_state_bytes});
            block = NativeReader(limited, DBMS_TCP_PROTOCOL_VERSION).read();
        }
        if (block.columns() != 1 || block.rows() != rows || !block.getByPosition(0).type->equals(*element_type))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Runtime filter values have unexpected structure: {}", block.dumpStructure());
        filter.insert(block.getByPosition(0).column);
    }

    if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the runtime filter state");

    return filter;
}

SharedFixedHashTableRuntimeFilter::SharedFixedHashTableRuntimeFilter(
    const DataTypePtr & filter_column_target_type_, ProbeFn probe_fn_, std::optional<Range> key_range_, ColumnPtr recorded_key_values_)
    : filter_column_target_type(filter_column_target_type_)
    , probe_fn(std::move(probe_fn_))
    , key_range(std::move(key_range_))
    , recorded_key_values(std::move(recorded_key_values_))
{
}

ColumnPtr SharedFixedHashTableRuntimeFilter::find(const ColumnWithTypeAndName & values, std::optional<size_t> & /*rows_passed*/) const
{
    return probe_fn(values);
}

RuntimeFilter::RuntimeFilter(RuntimeFilterConfig config_, Data data_)
    : filter_column_target_type(std::visit([](const auto & filter) { return filter.getTargetType(); }, data_.filter))
    , evaluation_state(std::move(config_))
    , data(std::move(data_))
{
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
                data.index_analysis.insert(*values);
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

void RuntimeFilter::merge(const RuntimeFilter & source, size_t source_filters)
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
    data.index_analysis.mergeFrom(source.data.index_analysis);
    data.build_state.finishMerge(source_filters);
}

void RuntimeFilter::enableIndexAnalysis()
{
    std::lock_guard lock(mutex);
    data.build_state.assertCanInsert();
    data.index_analysis.enable();
}

ColumnPtr RuntimeFilter::getRecordedKeyValues() const
{
    SharedLockGuard lock(mutex);
    if (!data.index_analysis.canUseExactValues() || !data.build_state.isFinished())
        return nullptr;
    return std::visit([](const auto & filter) { return filter.getRecordedKeyValues(); }, data.filter);
}

std::optional<Range> RuntimeFilter::getRecordedKeyRanges() const
{
    SharedLockGuard lock(mutex);
    if (!data.build_state.isFinished())
        return {};
    return data.index_analysis.getRange();
}

template class ExactSetRuntimeFilter<false>;
template class ExactSetRuntimeFilter<true>;

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & key, const String & display_name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        /// A partial that still keeps keys as values waits for the next one, and the two are merged outside the lock,
        /// so the bits of all streams are not set under the lock. Other partials are merged directly unless one waits.
        bool merge_in_pairs = runtime_filter->hasPendingKeys();
        size_t partials = 1; /// Expected filters whose keys `runtime_filter` holds.
        while (true)
        {
            UniqueRuntimeFilterPtr other;
            size_t other_partials = 0;
            {
                std::lock_guard lock(mutex);
                auto & filter = filters_by_name[key];
                if (!filter)
                {
                    ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
                    filter.reset(runtime_filter.release()); /// Save new filter.
                    /// Record the readable structural name once because the map is keyed by the opaque rendezvous key.
                    display_names.emplace(key, display_name);
                    LOG_TRACE(getLogger("RuntimeFilter"), "Registered runtime filter '{}' under key '{}'", display_name, key);
                }
                else
                {
                    if (auto parked = parked_partials.find(key); parked != parked_partials.end())
                    {
                        other = std::move(parked->second.filter);
                        other_partials = parked->second.partials;
                        parked_partials.erase(parked);
                    }

                    if (!other)
                    {
                        if (merge_in_pairs && filter->getFiltersToMerge() > partials)
                        {
                            /// A partial that is not added yet, or is being merged now, will take this one.
                            parked_partials.emplace(key, ParkedPartial{std::move(runtime_filter), partials});
                            return;
                        }
                        filter->merge(*runtime_filter, partials); /// Add all new keys to an existing filter.
                        LOG_TRACE(
                            getLogger("RuntimeFilter"),
                            "Merged {} partials into runtime filter '{}' under key '{}'",
                            partials,
                            display_name,
                            key);
                    }
                }

                if (!other)
                {
                    /// The registration above already makes the filter findable by the probe side, so skipping
                    /// the call below holds it in the registered-but-unfinished state that is otherwise transient.
                    fiu_do_on(FailPoints::runtime_filter_skip_finish_insert, { return; });
                    filter->finishInsert();
                    return;
                }
            }

            if (other_partials > partials)
                std::swap(runtime_filter, other);
            runtime_filter->merge(*other);
            partials += other_partials;
            merge_in_pairs = true;
        }
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
            /// Several filters can share one display name, so the key is logged too.
            auto name_it = display_names.find(filter_key);
            const String & name = (name_it != display_names.end() && !name_it->second.empty()) ? name_it->second : filter_key;
            LOG_TRACE(
                getLogger("RuntimeFilter"),
                "Stats for '{}' (key '{}'): rows skipped {}, rows checked {}, rows passed {}, blocks skipped {}, blocks processed {}",
                name,
                filter_key,
                stats.rows_skipped.load(),
                stats.rows_checked.load(),
                stats.rows_passed.load(),
                stats.blocks_skipped.load(),
                stats.blocks_processed.load());
        }
    }

private:
    struct ParkedPartial
    {
        UniqueRuntimeFilterPtr filter;
        size_t partials = 0;
    };

    mutable SharedMutex mutex;
    std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(mutex);
    /// At most one partial per key waits here for the next one to be merged with it.
    std::unordered_map<String, ParkedPartial> parked_partials TSA_GUARDED_BY(mutex);
    /// Readable structural name per rendezvous key, for logging. Kept under the same lock and
    /// preserved across `replace` because the replacement keeps the original registration's name.
    std::unordered_map<String, String> display_names TSA_GUARDED_BY(mutex);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

/// Build a pruning predicate on the column: exact IN values when available, otherwise a range.
static const ActionsDAG::Node * convertRuntimeFilterToKeyConditionDAG(
    const RuntimeFilter & filter, const String & column_name, const DataTypePtr & column_type, ActionsDAG & dag, const ContextPtr & context)
{
    auto exact_values = filter.getRecordedKeyValues();
    auto range = exact_values ? std::optional<Range>{} : filter.getRecordedKeyRanges();
    if (!exact_values && !range)
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

    if (range)
    {
        const auto & min_node
            = dag.addColumn(target_type->createColumnConst(1, range->left), target_type, "__runtime_filter_min_" + column_name);
        const auto & max_node
            = dag.addColumn(target_type->createColumnConst(1, range->right), target_type, "__runtime_filter_max_" + column_name);
        const auto & ge_node = dag.addFunction(FunctionFactory::instance().get("greaterOrEquals", context), {&key_casted, &min_node}, {});
        const auto & le_node = dag.addFunction(FunctionFactory::instance().get("lessOrEquals", context), {&key_casted, &max_node}, {});
        LOG_DEBUG(
            getLogger("JoinRuntimeFilterIndexAnalysis"),
            "Index analysis engaged on join key '{}': pruning by range {}",
            column_name,
            range->toString());
        FunctionOverloadResolverPtr and_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        return &dag.addFunction(and_func, {&ge_node, &le_node}, {});
    }

    return nullptr;
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
