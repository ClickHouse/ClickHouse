#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <DataTypes/hasNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <algorithm>
#include <bit>
#include <unordered_map>
#include <utility>
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
    if (static_cast<double>(rows_passed) > config.pass_ratio_threshold_for_disabling * static_cast<double>(rows_checked))
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
    const DataTypePtr & filter_column_target_type_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_)
    : argument_can_have_nulls(hasTypeThatCanContainNulls(filter_column_target_type_))
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
Set & ExactSetRuntimeFilter<negate>::getExactValues() const
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
ColumnPtr ExactSetRuntimeFilter<negate>::find(const ColumnWithTypeAndName & values) const
{
    if (!is_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter set is not ready for lookups");

    return std::visit(
        Overloaded
        {
            [&](const Empty &) -> ColumnPtr
            {
                return DataTypeUInt8().createColumnConst(values.column->size(), negate);
            },
            [&](const Single & single) -> ColumnPtr
            {
                /// If only one element is in the set then use `equals` instead of set lookup.
                /// Use the column directly from `Set` to avoid lossy `Field` roundtrip.
                ColumnPtr const_column = ColumnConst::create(single.column, values.column->size());
                ColumnsWithTypeAndName arguments = {
                    values,
                    ColumnWithTypeAndName(const_column, values.type, String())
                };
                auto single_element_equals_function = FunctionFactory::instance().get(negate ? "notEquals" : "equals", nullptr)->build(arguments);
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
        Overloaded
        {
            [](const Empty & empty) -> ColumnPtr
            {
                return empty.column;
            },
            [](const Single & single) -> ColumnPtr
            {
                return single.column;
            },
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
void ExactSetRuntimeFilter<negate>::mergeFrom(const ExactSetRuntimeFilter & source)
{
    insert(source.getValuesColumn());
}

bool ApproximateSetRuntimeFilter::isDataTypeSupported(const DataTypePtr & data_type)
{
    /// Runtime `BloomFilter` hashing uses byte representation from either fixed contiguous column storage or `getDataAt`.
    return data_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
}

ApproximateSetRuntimeFilter::ApproximateSetRuntimeFilter(
    UInt64 bytes_limit_,
    UInt64 bloom_filter_hash_functions_)
    : bloom_filter(bytes_limit_, bloom_filter_hash_functions_, BLOOM_FILTER_SEED)
{
}

void ApproximateSetRuntimeFilter::insert(ColumnPtr values)
{
    insertIntoBloomFilter(values);
}

void ApproximateSetRuntimeFilter::insertIntoBloomFilter(const ColumnPtr & values)
{
    forEachColumnHashBatch(*values, bloom_filter.getSeed(),
        [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t /* start_row */)
        {
            bloom_filter.addHashPairs(hash_pairs, count);
        });
}

ColumnPtr ApproximateSetRuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    auto dst = ColumnVector<UInt8>::create();
    auto & dst_data = dst->getData();
    dst_data.resize(values.column->size());

    forEachColumnHashBatch(*values.column, bloom_filter.getSeed(),
        [&](const BloomFilterHashPair * hash_pairs, size_t count, size_t start_row)
        {
            bloom_filter.findHashPairs(hash_pairs, count, dst_data.data() + start_row);
        });

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
    Float64 max_ratio_of_set_bits_in_bloom_filter_)
    : bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , filter(
        std::in_place_type<ExactFilter>,
        filter_column_target_type_,
        bytes_limit_,
        exact_values_limit_)
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
        Overloaded
        {
            [](ExactFilter & exact_filter)
            {
                exact_filter.finishInsert();
            },
            [&](ApproximateSetRuntimeFilter & approximate_filter)
            {
                checkApproximateFilterWorthiness(evaluation_state, approximate_filter);
            },
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

ColumnPtr AdaptiveSetRuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    return std::visit(
        Overloaded
        {
            [&](const ExactFilter & exact_filter) -> ColumnPtr
            {
                return exact_filter.find(values);
            },
            [&](const ApproximateSetRuntimeFilter & approximate_filter) -> ColumnPtr
            {
                return approximate_filter.find(values);
            },
        },
        filter);
}

void AdaptiveSetRuntimeFilter::mergeFrom(const AdaptiveSetRuntimeFilter & source)
{
    std::visit(
        Overloaded
        {
            [&](const ExactFilter & source_exact_filter)
            {
                insert(source_exact_filter.getValuesColumn(), filter);
            },
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
    const auto bytes_limit = exact_filter->getBytesLimit();

    auto & approximate_filter = filter_.emplace<ApproximateSetRuntimeFilter>(bytes_limit, bloom_filter_hash_functions);
    approximate_filter.insert(values);
    return approximate_filter;
}

void AdaptiveSetRuntimeFilter::checkApproximateFilterWorthiness(
    RuntimeFilterEvaluationState & evaluation_state,
    const ApproximateSetRuntimeFilter & approximate_filter)
{
    if (!approximate_filter.isWorthUsing(max_ratio_of_set_bits_in_bloom_filter))
        evaluation_state.setFullyDisabled();
}

SharedFixedHashTableRuntimeFilterImpl::SharedFixedHashTableRuntimeFilterImpl(ProbeFn probe_fn_)
    : probe_fn(std::move(probe_fn_))
{
}

ColumnPtr SharedFixedHashTableRuntimeFilterImpl::find(const ColumnWithTypeAndName & values) const
{
    return probe_fn(values);
}

void RuntimeFilter::insert(ColumnPtr values)
{
    data.accessWriteEnabled(
        [&](Data * filter_data)
        {
            std::visit(
                [&](auto & filter)
                {
                    using FilterType = std::decay_t<decltype(filter)>;
                    if constexpr (!FilterType::is_prebuilt)
                        filter_data->build_state.assertCanInsert();
                    filter.insert(std::move(values));
                },
                filter_data->filter);
        });
}

void RuntimeFilter::finishInsert()
{
    data.accessWriteEnabled(
        [&](Data * filter_data)
        {
            if (filter_data->build_state.hasPendingMerges())
                return;

            filter_data->build_state.finishInserts();
            std::visit(
                [&](auto & filter)
                {
                    filter.finishInsert(evaluation_state);
                },
                filter_data->filter);
        });
}

ColumnPtr RuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    return data.accessReadOnly(
        [&](const Data * filter_data) -> ColumnPtr
        {
            filter_data->build_state.assertCanFind();

            const size_t rows_in_block = values.column->size();
            if (evaluation_state.shouldSkip(rows_in_block))
                return DataTypeUInt8().createColumnConst(rows_in_block, true);

            auto result = std::visit(
                [&](const auto & filter) -> ColumnPtr
                {
                    return filter.find(values);
                },
                filter_data->filter);
            evaluation_state.updateStats(values.column->size(), countPassedStats(result));
            return result;
        });
}

void RuntimeFilter::merge(const RuntimeFilter * source)
{
    if (!source)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to merge runtime filters with different types");

    source->data.accessReadOnly(
        [&](const Data * source_data)
        {
            data.accessWriteEnabled(
                [&](Data * destination_data)
                {
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
                });
        });
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
                    const String & name = (name_it != lookup_data->display_names.end() && !name_it->second.empty()) ? name_it->second : filter_key;
                    LOG_TRACE(getLogger("RuntimeFilter"),
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

}
