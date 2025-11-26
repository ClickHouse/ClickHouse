#include <Processors/QueryPlan/RuntimeFilterLookup.h>

#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
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
    UInt64 bytes_limit_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_hash_functions_)
    : RuntimeFilterBase(filters_to_merge_, filter_column_target_type_, bytes_limit_, exact_values_limit_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
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
        return;

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

ColumnPtr ApproximateRuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    if (!inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to lookup values in runtime filter before builiding it was finished");

    if (bloom_filter)
    {
        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(values.column->size());

        for (size_t row = 0; row < values.column->size(); ++row)
        {
            /// TODO: optimize: consider replacing hash calculation with vectorized version
            const auto & value = values.column->getDataAt(row);
            dst_data[row] = bloom_filter->find(value.data, value.size);
        }

        return dst;
    }
    else
    {
        return Base::find(values);
    }
}

void ApproximateRuntimeFilter::insertIntoBloomFilter(ColumnPtr values)
{
    const size_t num_rows = values->size();
    for (size_t row = 0; row < num_rows; ++row)
    {
        /// TODO: make this efficient: compute hashes in vectorized manner
        auto value = values->getDataAt(row);
        bloom_filter->add(value.data, value.size);
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

class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & name, UniqueRuntimeFilterPtr runtime_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[name];
        if (!filter)
        {
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

private:
    mutable SharedMutex rw_lock;
    std::unordered_map<String, SharedRuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(rw_lock);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

}
