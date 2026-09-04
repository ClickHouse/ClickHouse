#include <algorithm>
#include <bit>
#include <cmath>
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
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Common/FieldAccurateComparison.h>
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
        rows_to_skip += rows_checked * config.blocks_to_skip_before_reenabling;
}

bool RuntimeFilterEvaluationState::shouldSkip(size_t next_block_rows) const
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
            evaluation_state.setFullyDisabled();
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
    std::optional<UInt64> distinct_keys_hint_)
    : filter_column_target_type(filter_column_target_type_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , distinct_keys_hint(distinct_keys_hint_)
    , filter(std::in_place_type<ExactFilter>, filter_column_target_type_, bytes_limit_, exact_values_limit_)
{
}

void AdaptiveSetRuntimeFilter::insert(ColumnPtr values)
{
    insert(std::move(values), filter);
}

void AdaptiveSetRuntimeFilter::insert(ColumnPtr values, Filter & filter_)
{
    if (auto * approximate_filter = std::get_if<ApproximateSetRuntimeFilter>(&filter_))
    {
        approximate_filter->insert(std::move(values));
        return;
    }

    auto & exact_filter = std::get<ExactFilter>(filter_);
    if (exact_filter.isFull())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected 'full' state of AdaptiveSetRuntimeFilter");

    exact_filter.insert(std::move(values));

    if (exact_filter.isFull())
        switchToApproximateFilter(filter_);
}

void AdaptiveSetRuntimeFilter::finishInsert(RuntimeFilterEvaluationState & evaluation_state)
{
    std::visit(
        Overloaded{
            [](ExactFilter & exact_filter) { exact_filter.finishInsert(); },
            [&](ApproximateSetRuntimeFilter & approximate_filter)
            { checkApproximateFilterWorthiness(evaluation_state, approximate_filter); },
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
                auto & destination_approximate_filter = switchToApproximateFilter(filter);
                destination_approximate_filter.mergeFrom(source_approximate_filter);
            },
        },
        source.filter);
}

ApproximateSetRuntimeFilter & AdaptiveSetRuntimeFilter::switchToApproximateFilter(Filter & filter_)
{
    if (auto * approximate_filter = std::get_if<ApproximateSetRuntimeFilter>(&filter_))
        return *approximate_filter;

    auto * exact_filter = std::get_if<ExactFilter>(&filter_);
    if (!exact_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of AdaptiveSetRuntimeFilter");
    auto values = exact_filter->getValuesColumn();
    UInt64 bytes_limit = exact_filter->getBytesLimit();

    if (distinct_keys_hint)
        bytes_limit
            = growBloomFilterBytes(*distinct_keys_hint, bloom_filter_hash_functions, bytes_limit, max_ratio_of_set_bits_in_bloom_filter);

    auto & approximate_filter = filter_.emplace<ApproximateSetRuntimeFilter>(bytes_limit, bloom_filter_hash_functions);
    approximate_filter.insert(values);
    return approximate_filter;
}

void AdaptiveSetRuntimeFilter::checkApproximateFilterWorthiness(
    RuntimeFilterEvaluationState & evaluation_state, const ApproximateSetRuntimeFilter & approximate_filter) const
{
    if (!approximate_filter.isWorthUsing(max_ratio_of_set_bits_in_bloom_filter))
        evaluation_state.setFullyDisabled();
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
    : evaluation_state(std::move(config_))
    , data(std::move(data_))
{
    auto filter_data = data.getWriteEnabled();
    filter_data->range_supported = typeSupportsMinMaxRange(filter_data->filter_column_target_type);
    if (!filter_data->range_supported)
        filter_data->has_range = false;
}

void RuntimeFilter::insert(ColumnPtr values)
{
    auto filter_data = data.getWriteEnabled();
    std::visit(
        [&](auto & filter)
        {
            using FilterType = std::decay_t<decltype(filter)>;
            if constexpr (!FilterType::is_prebuilt)
            {
                filter_data->build_state.assertCanInsert();
                if (filter_data->index_analysis_enabled && filter_data->range_supported && filter_data->range_positive && !values->empty())
                {
                    Field column_min;
                    Field column_max;
                    values->getExtremes(column_min, column_max, 0, values->size());
                    if (!column_min.isNull() && !column_max.isNull())
                        extendRange(filter_data->has_range, filter_data->range_min, filter_data->range_max, column_min, column_max);
                }
                filter.insert(std::move(values));
            }
        },
        filter_data->filter);
}

void RuntimeFilter::finishInsert()
{
    auto filter_data = data.getWriteEnabled();
    if (filter_data->build_state.hasPendingMerges())
        return;

    std::visit([&](auto & filter) { filter.finishInsert(evaluation_state); }, filter_data->filter);
    filter_data->build_state.finishInserts();
}

ColumnPtr RuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    auto filter_data = data.getReadOnly();
    filter_data->build_state.assertCanFind();

    const size_t rows_in_block = values.column->size();
    if (evaluation_state.shouldSkip(rows_in_block))
        return DataTypeUInt8().createColumnConst(rows_in_block, true);

    std::optional<size_t> rows_passed;
    auto result = std::visit([&](const auto & filter) -> ColumnPtr { return filter.find(values, rows_passed); }, filter_data->filter);
    evaluation_state.updateStats(rows_in_block, rows_passed ? *rows_passed : countPassedStats(result));
    return result;
}

void RuntimeFilter::merge(const RuntimeFilter * source)
{
    if (!source)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    auto source_data = source->data.getReadOnly();
    auto destination_data = data.getWriteEnabled();

    /// `HashJoin::publishSharedRuntimeFilters` may have already replaced this lookup entry with a
    /// prebuilt shared fixed-hash-table filter: the publication step can run as soon as the last
    /// build-side port is closed, while `BuildRuntimeFilterTransform::finish()` (which reaches this
    /// merge via `IRuntimeFilterLookup::add`) only runs afterwards in `prepare()`. The shared filter
    /// probes the complete build-side hash table, i.e. a superset of anything a late set/bloom
    /// filter could contribute, so ignore the merge (the pre-refactor no-op behavior of
    /// `SharedFixedHashTableRuntimeFilter::merge`) instead of failing the query.
    if (std::holds_alternative<SharedFixedHashTable>(destination_data->filter))
        return;

    if (destination_data->filter.index() != source_data->filter.index())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    std::visit(
        [&](auto & destination_filter, const auto & source_filter)
        {
            using DestinationFilter = std::decay_t<decltype(destination_filter)>;
            using SourceFilter = std::decay_t<decltype(source_filter)>;
            if constexpr (std::is_same_v<DestinationFilter, SourceFilter>)
            {
                if constexpr (!DestinationFilter::is_prebuilt)
                    destination_data->build_state.assertCanMerge();
                destination_filter.mergeFrom(source_filter);
                if (destination_data->index_analysis_enabled && destination_data->range_supported && destination_data->range_positive
                    && source_data->has_range)
                    extendRange(
                        destination_data->has_range,
                        destination_data->range_min,
                        destination_data->range_max,
                        source_data->range_min,
                        source_data->range_max);
                if constexpr (!DestinationFilter::is_prebuilt)
                    destination_data->build_state.finishMerge();
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");
            }
        },
        destination_data->filter,
        source_data->filter);
}

void RuntimeFilter::enableIndexAnalysis()
{
    auto filter_data = data.getWriteEnabled();
    filter_data->build_state.assertCanInsert();
    filter_data->index_analysis_enabled = true;
}

ColumnPtr RuntimeFilter::getRecordedKeyValues() const
{
    auto filter_data = data.getReadOnly();
    if (!filter_data->index_analysis_enabled || !filter_data->range_positive || !filter_data->build_state.isFinished())
        return nullptr;
    return std::visit([](const auto & filter) { return filter.getRecordedKeyValues(); }, filter_data->filter);
}

std::optional<Range> RuntimeFilter::getRecordedKeyRanges() const
{
    auto filter_data = data.getReadOnly();
    if (!filter_data->range_supported || !filter_data->range_positive || !filter_data->has_range || !filter_data->build_state.isFinished()
        || filter_data->range_min.isNull() || filter_data->range_max.isNull())
        return {};
    return Range(filter_data->range_min, true, filter_data->range_max, true);
}

DataTypePtr RuntimeFilter::getFilterColumnTargetType() const
{
    auto filter_data = data.getReadOnly();
    return filter_data->filter_column_target_type;
}

template class ExactSetRuntimeFilter<false>;
template class ExactSetRuntimeFilter<true>;

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & key, const String & display_name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        data.accessWriteEnabled(
            [&](Data * lookup_data)
            {
                auto & filter = lookup_data->filters_by_name[key];
                if (!filter)
                {
                    ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
                    filter.reset(runtime_filter.release()); /// Save new filter.
                    /// Record the readable structural name once because the map is keyed by the opaque rendezvous key.
                    lookup_data->display_names.emplace(key, display_name);
                }
                else
                {
                    filter->merge(runtime_filter.get()); /// Add all new keys to an existing filter.
                }
                filter->finishInsert();
            });
    }

    void replace(const String & name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        data.accessWriteEnabled(
            [&](Data * lookup_data)
            {
                auto & filter = lookup_data->filters_by_name[name];
                if (!filter)
                    ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
                filter.reset(runtime_filter.release());
            });
    }

    RuntimeFilterConstPtr find(const String & name) const override
    {
        return data.accessReadOnly(
            [&](const Data * lookup_data) -> RuntimeFilterConstPtr
            {
                auto it = lookup_data->filters_by_name.find(name);
                if (it == lookup_data->filters_by_name.end())
                    return nullptr;
                return it->second;
            });
    }

    void logStats() const override
    {
        data.accessReadOnly(
            [](const Data * lookup_data)
            {
                for (const auto & [filter_key, filter] : lookup_data->filters_by_name)
                {
                    const auto & stats = filter->getStats();
                    /// `filter_key` is the opaque random rendezvous key; prefer the readable structural name.
                    auto name_it = lookup_data->display_names.find(filter_key);
                    const String & name
                        = (name_it != lookup_data->display_names.end() && !name_it->second.empty()) ? name_it->second : filter_key;
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
            });
    }

private:
    struct Data
    {
        std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name;
        /// Readable structural name per rendezvous key, for logging. Kept under the same lock and
        /// preserved across `replace` because the replacement keeps the original registration's name.
        std::unordered_map<String, String> display_names;
    };

    MutexProtected<Data> data;
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
