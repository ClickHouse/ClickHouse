#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <DataTypes/DataTypesNumber.h>
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

static constexpr UInt64 BLOOM_FILTER_SEED = 42;

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
    return data_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
}

ApproximateRuntimeFilter::ApproximateRuntimeFilter(
    size_t filters_to_merge_,
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 /* bloom_filter_hash_functions_ */,
    Float64 max_ratio_of_set_bits_in_bloom_filter_)
    : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, pass_ratio_threshold_for_disabling_, blocks_to_skip_before_reenabling_, bytes_limit_, exact_values_limit_)
    , max_ratio_of_set_bits_in_bloom_filter(max_ratio_of_set_bits_in_bloom_filter_)
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
        bloom_filter->merge(*source_typed->bloom_filter);
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
            ColumnPtr const_column = filter_column_target_type->createColumnConst(values.column->size(), *single_element_in_set);
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
        const size_t num_rows = values.column->size();
        dst_data.resize(num_rows);

        size_t found_count = bloom_filter->findBatch(*values.column, num_rows, dst_data.data());
        updateStats(num_rows, found_count);

        return dst;
    }
    else
    {
        return Base::findImpl(values);
    }
}

void ApproximateRuntimeFilter::insertIntoBloomFilter(ColumnPtr values)
{
    bloom_filter->addBatch(*values, values->size());
}

void ApproximateRuntimeFilter::switchToBloomFilter()
{
    if (bloom_filter)
        return;

    bloom_filter = std::make_unique<BlockedBloomFilter>(getBytesLimit(), BLOOM_FILTER_SEED);
    insertIntoBloomFilter(getValuesColumn());

    releaseExactValues();
}

void ApproximateRuntimeFilter::checkBloomFilterWorthiness()
{
    const size_t set_bits = bloom_filter->countSetBits();
    const size_t total_bits = bloom_filter->totalBits();
    /// If too many bits are set then it is likely that the filter will not filter out much
    if (static_cast<double>(set_bits) > max_ratio_of_set_bits_in_bloom_filter * static_cast<double>(total_bits))
        setFullyDisabled();
}

SharedFixedHashTableRuntimeFilter::SharedFixedHashTableRuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    ProbeFn probe_fn_)
    : IRuntimeFilter(
        /*filters_to_merge_=*/0,
        filter_column_target_type_,
        pass_ratio_threshold_for_disabling_,
        blocks_to_skip_before_reenabling_)
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

}
