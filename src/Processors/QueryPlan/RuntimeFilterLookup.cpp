#include <algorithm>
#include <cmath>
#include <optional>
#include <vector>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Common/FieldAccurateComparison.h>
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
}

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

std::optional<Range> IRuntimeFilter::getRecordedKeyRanges() const
{
    /// inserts_are_finished (seq_cst) publishes the range without a lock.
    if (!range_supported || !range_positive || !has_range || !inserts_are_finished.load())
        return {};
    if (range_min.isNull() || range_max.isNull())
        return {};
    return Range(range_min, /*left_included=*/true, range_max, /*right_included=*/true);
}

void IRuntimeFilter::updateRange(const IColumn & column)
{
    if (!index_analysis_enabled || !range_supported || !range_positive)
        return;

    const size_t rows = column.size();
    if (rows == 0)
        return;

    Field cmin;
    Field cmax;
    column.getExtremes(cmin, cmax, 0, rows);
    if (cmin.isNull() || cmax.isNull())
        return;

    if (!has_range)
    {
        range_min = std::move(cmin);
        range_max = std::move(cmax);
        has_range = true;
        return;
    }

    if (accurateLess(cmin, range_min))
        range_min = std::move(cmin);
    if (accurateLess(range_max, cmax))
        range_max = std::move(cmax);
}

void IRuntimeFilter::mergeRange(const IRuntimeFilter & source)
{
    if (!index_analysis_enabled || !range_supported || !range_positive || !source.has_range)
        return;

    if (!has_range)
    {
        range_min = source.range_min;
        range_max = source.range_max;
        has_range = true;
        return;
    }

    if (accurateLess(source.range_min, range_min))
        range_min = source.range_min;
    if (accurateLess(range_max, source.range_max))
        range_max = source.range_max;
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
    const RuntimeFilterGeometry & geometry_,
    std::optional<UInt64> distinct_keys_hint_)
    : RuntimeFilterBase(
          filters_to_merge_,
          filter_column_target_type_,
          geometry_.pass_ratio_threshold_for_disabling,
          geometry_.blocks_to_skip_before_reenabling,
          geometry_.exact_bytes_limit,
          geometry_.exact_values_limit)
    , bloom_filter_bytes(geometry_.bloom_filter_bytes)
    , bloom_filter_hash_functions(geometry_.bloom_filter_hash_functions)
    , max_ratio_of_set_bits_in_bloom_filter(geometry_.max_ratio_of_set_bits_in_bloom_filter)
    , distinct_keys_hint(distinct_keys_hint_)
    , bloom_filter(nullptr)
{}

void ApproximateRuntimeFilter::insert(ColumnPtr values)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");
    if (state_serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after its state was serialized");

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
    if (state_serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge into runtime filter after its state was serialized");

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

static constexpr UInt64 RUNTIME_FILTER_STATE_VERSION = 1;

void ApproximateRuntimeFilter::serialize(WriteBuffer & out)
{
    state_serialized = true;

    writeVarUInt(RUNTIME_FILTER_STATE_VERSION, out);
    writeBinary(UInt8(bloom_filter ? 1 : 0), out);

    if (bloom_filter)
    {
        writeVarUInt(bloom_filter->getFilterSizeBytes(), out);
        writeVarUInt(bloom_filter->getHashes(), out);
        writeVarUInt(bloom_filter->getSeed(), out);
        const auto & words = bloom_filter->getFilter();
        out.write(reinterpret_cast<const char *>(words.data()), words.size() * sizeof(words[0]));
    }
    else
    {
        /// The Set stores its elements with LowCardinality stripped (see `Set::getElementTypes`),
        /// so the block must be typed the same way; `insert` on the receiving side accepts full
        /// columns. The row count goes first so the reader can reject an oversized declaration
        /// before the column gets allocated.
        auto values = getValuesColumn();
        writeVarUInt(values->size(), out);
        Block block({ColumnWithTypeAndName(values, recursiveRemoveLowCardinality(filter_column_target_type), "values")});
        NativeWriter writer(out, DBMS_TCP_PROTOCOL_VERSION, std::make_shared<const Block>(block.cloneEmpty()));
        writer.write(block);
    }
}

std::unique_ptr<ApproximateRuntimeFilter> ApproximateRuntimeFilter::deserialize(
    ReadBuffer & in, size_t filters_to_merge_, const DataTypePtr & filter_column_target_type_, const RuntimeFilterGeometry & geometry_)
{
    UInt64 version{};
    readVarUInt(version, in);
    if (version != RUNTIME_FILTER_STATE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown runtime filter state version {}", version);

    UInt8 has_bloom_filter{};
    readBinary(has_bloom_filter, in);
    if (has_bloom_filter > 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed runtime filter state");

    auto filter = std::make_unique<ApproximateRuntimeFilter>(
        filters_to_merge_, filter_column_target_type_, geometry_, /*distinct_keys_hint_=*/std::nullopt);

    if (has_bloom_filter)
    {
        UInt64 size_bytes{};
        UInt64 hash_functions{};
        UInt64 seed{};
        readVarUInt(size_bytes, in);
        readVarUInt(hash_functions, in);
        readVarUInt(seed, in);
        if (size_bytes != geometry_.bloom_filter_bytes || hash_functions != geometry_.bloom_filter_hash_functions
            || seed != BLOOM_FILTER_SEED)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Runtime filter bloom parameters ({}, {}, {}) do not match the expected ({}, {}, {})",
                size_bytes,
                hash_functions,
                seed,
                geometry_.bloom_filter_bytes,
                geometry_.bloom_filter_hash_functions,
                BLOOM_FILTER_SEED);

        filter->bloom_filter = std::make_shared<BloomFilter>(size_bytes, hash_functions, seed);
        auto & words = filter->bloom_filter->getFilter();
        in.readStrict(reinterpret_cast<char *>(words.data()), words.size() * sizeof(words[0]));
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

        /// Declared row count does not bound variable-width keys. Reject before consuming payload if
        /// remaining bytes exceed `exact_bytes_limit` plus Native framing (block info, column name,
        /// serialization kind) plus the actual type-name length (`Enum` names are unbounded).
        /// Hostile row counts still die on the memory limit, loudly.
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
        filter->insert(block.getByPosition(0).column);
    }

    if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the runtime filter state");

    return filter;
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

    /// The stats-sized growth applies only to a filter that lives and dies in one pipeline. A
    /// transported partial never carries the hint: its serialized state must match the plan's
    /// geometry on the receiving side and must never cost more on the wire than the
    /// settings-sized bloom.
    UInt64 grown_bloom_filter_bytes = bloom_filter_bytes;
    if (distinct_keys_hint)
        grown_bloom_filter_bytes = growBloomFilterBytes(
            *distinct_keys_hint, bloom_filter_hash_functions, bloom_filter_bytes, max_ratio_of_set_bits_in_bloom_filter);

    bloom_filter = std::make_unique<BloomFilter>(grown_bloom_filter_bytes, bloom_filter_hash_functions, BLOOM_FILTER_SEED);
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
    std::optional<Range> key_range_,
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

    /// The fixed hash map knows its exact [min, max] key envelope; expose it for granule pruning.
    if (key_range_ && range_supported)
    {
        range_min = key_range_->left;
        range_max = key_range_->right;
        has_range = true;
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
            LOG_TRACE(getLogger("RuntimeFilter"), "Registered runtime filter '{}' under key '{}'", display_name, key);
        }
        else
        {
            filter->merge(runtime_filter.get());    /// Add all new keys to a existing filter
            LOG_TRACE(getLogger("RuntimeFilter"), "Merged a partial into runtime filter '{}' under key '{}'", display_name, key);
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
        /// Parallel build streams register one by one, and until the last one arrives the filter
        /// is half-built. It must stay invisible: a reader that races the registrations (a scan's
        /// PREWHERE applying one filter while another one is being built in the same task) would
        /// otherwise look up a filter whose `find` throws. Not ready means not there yet - the
        /// reader passes all rows, exactly as before the first registration.
        if (!it->second->isReady())
            return nullptr;
        return it->second;
    }

    void logStats() const override
    {
        SharedLockGuard g(rw_lock);
        for (const auto & [filter_key, filter] : filters_by_name)
        {
            const auto & stats = filter->getStats();
            /// `filter_key` is the opaque random rendezvous key; the readable structural name comes
            /// first, but the key stays in the line: several filter instances (e.g. a transported
            /// union and a worker-re-added local pair) share one structural name and only the key
            /// tells them apart.
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
    auto range = exact_values ? std::optional<Range>{} : filter.getRecordedKeyRanges();
    if (!exact_values && !range)
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

    LOG_DEBUG(
        getLogger("JoinRuntimeFilterIndexAnalysis"),
        "Index analysis engaged on join key '{}': pruning by range {}",
        column_name, range->toString());

    const auto & min_node = dag.addColumn(
        target_type->createColumnConst(1, range->left), target_type, "__runtime_filter_min_" + column_name);
    const auto & max_node = dag.addColumn(
        target_type->createColumnConst(1, range->right), target_type, "__runtime_filter_max_" + column_name);

    auto ge_func = FunctionFactory::instance().get("greaterOrEquals", context);
    auto le_func = FunctionFactory::instance().get("lessOrEquals", context);
    const auto & ge_node = dag.addFunction(ge_func, {&key_casted, &min_node}, {});
    const auto & le_node = dag.addFunction(le_func, {&key_casted, &max_node}, {});

    FunctionOverloadResolverPtr and_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return &dag.addFunction(and_func, {&ge_node, &le_node}, {});
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
