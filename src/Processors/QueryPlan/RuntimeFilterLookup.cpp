#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>

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

void IRuntimeFilter::updateStats(UInt64 rows_checked, UInt64 rows_passed) const
{
    stats.blocks_processed++;
    stats.rows_checked += rows_checked;
    stats.rows_passed += rows_passed;

    ProfileEvents::increment(ProfileEvents::RuntimeFilterBlocksProcessed);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsChecked, rows_checked);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterRowsPassed, rows_passed);

    /// Skip next 30 blocks if too few rows got filtered out
    if (rows_passed > pass_ratio_threshold_for_disabling * rows_checked)
        rows_to_skip = rows_checked * blocks_to_skip_before_reenabling;
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

ColumnPtr IRuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    if (!inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to lookup values in runtime filter before builiding it was finished");

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

ApproximateRuntimeFilter::ApproximateRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_)
    : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_, bytes_limit_, exact_values_limit_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
    , bloom_filter(nullptr)
{}

void ApproximateRuntimeFilter::insert(ColumnPtr values)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

    if (bloom_filter)
    {
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

void ApproximateRuntimeFilter::finishInsert()
{
    if (filters_to_merge != 0)
        return;

    inserts_are_finished = true;

    if (bloom_filter)
    {
        checkBloomFilterWorthiness();
        return;
    }

    Base::finishInsert();
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
            /// If only 1 element in the set then use "value == const" instead of set lookup
            auto const_column = filter_column_target_type->createColumnConst(values.column->size(), *single_element_in_set);
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
        for (size_t row = 0; row < values.column->size(); ++row)
        {
            /// TODO: optimize: consider replacing hash calculation with vectorized version
            const auto & value = values.column->getDataAt(row);
            const bool found = bloom_filter->find(value.data(), value.size());
            found_count += found ? 1 : 0;
            dst_data[row] = found;
        }
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
    const size_t num_rows = values->size();
    for (size_t row = 0; row < num_rows; ++row)
    {
        /// TODO: make this efficient: compute hashes in vectorized manner
        auto value = values->getDataAt(row);
        bloom_filter->add(value.data(), value.size());
    }
}

void ApproximateRuntimeFilter::switchToBloomFilter()
{
    if (bloom_filter)
        return;

    bloom_filter = std::make_unique<BloomFilter>(getBytesLimit(), bloom_filter_hash_functions, BLOOM_FILTER_SEED);
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
    if (set_bits > max_ratio_of_set_bits_in_bloom_filter * total_bits)
        setFullyDisabled();
}

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[name];
        if (!filter)
        {
            ProfileEvents::increment(ProfileEvents::RuntimeFiltersCreated);
            filter.reset(runtime_filter.release());   /// Save new filter
        }
        else
        {
            filter->merge(runtime_filter.get());    /// Add all new keys to a existing filter
        }
        filter->finishInsert();
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
        for (const auto & [filter_name, filter] : filters_by_name)
        {
            const auto & stats = filter->getStats();
            LOG_TRACE(getLogger("RuntimeFilter"),
                "Stats for '{}': rows skipped {}, rows checked {}, rows passed {}, blocks skipped {}, blocks processed {}",
                filter_name, stats.rows_skipped.load(), stats.rows_checked.load(), stats.rows_passed.load(), stats.blocks_skipped.load(), stats.blocks_processed.load());
        }
    }

private:
    mutable SharedMutex rw_lock;
    std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(rw_lock);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

}
