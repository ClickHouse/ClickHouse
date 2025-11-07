#include <Interpreters/BloomFilter.h>
#include <Interpreters/Set.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>

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

RuntimeFilter::RuntimeFilter(
    const DataTypePtr & filter_column_target_type_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_bytes_,
    UInt64 bloom_filter_hash_functions_)
    : exact_values_limit(exact_values_limit_)
    , bloom_filter_bytes(bloom_filter_bytes_)
    , bloom_filter_hash_functions(bloom_filter_hash_functions_)
    , filter_column_target_type(filter_column_target_type_)
    , result_type(std::make_shared<DataTypeUInt8>())
    , bloom_filter(nullptr)
    , exact_values(std::make_shared<Set>(SizeLimits{}, -1, false))
{
    ColumnsWithTypeAndName set_header;
    set_header.emplace_back(ColumnWithTypeAndName(filter_column_target_type, String()));
    exact_values->setHeader(set_header);
    exact_values->fillSetElements();    /// Save the values, not just hashes
}

void RuntimeFilter::insert(ColumnPtr values)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

    if (exact_values)
    {
        exact_values->insertFromColumns({values});
        if (exact_values->getTotalRowCount() > exact_values_limit || exact_values->getTotalByteCount() > bloom_filter_bytes)
            switchToBloomFilter();
    }
    else
        insertIntoBloomFilter(values);
}

void RuntimeFilter::finishInsert()
{
    /// Only one thread will do the actual finishing logic and any concurrent caller will wait for it to complete
    std::lock_guard g(finish_mutex);

    if (inserts_are_finished)
        return;

    inserts_are_finished = true;

    if (exact_values)
    {
        exact_values->finishInsert();

        /// If the set is empty just return Const False column
        if (exact_values->getTotalRowCount() == 0)
        {
            no_elements_in_set = true;
            return;
        }

        /// If only 1 element in the set then use " == const" instead of set lookup
        if (exact_values->getTotalRowCount() == 1)
        {
            single_element_in_set = (*exact_values->getSetElements().front())[0];
            return;
        }
    }
}

/// Add all keys from one filter to the other so that destination filter contains the union of both filters.
void RuntimeFilter::addAllFrom(const RuntimeFilter & source)
{
    if (inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert into runtime filter after it was marked as finished");

    if (source.exact_values)
    {
        insert(source.exact_values->getSetElements().front());
    }
    else
    {
        switchToBloomFilter();
        mergeBloomFilters(*bloom_filter, *source.bloom_filter);
    }
}

ColumnPtr RuntimeFilter::find(const ColumnWithTypeAndName & values) const
{
    if (!inserts_are_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to lookup values in runtime filter before builiding it was finished");

    if (no_elements_in_set)
    {
        /// If the set is empty just return Const False column
        return result_type->createColumnConst(values.column->size(), false);
    }
    else if (single_element_in_set)
    {
        /// If only 1 element in the set then use "value == const" instead of set lookup
        auto const_column = filter_column_target_type->createColumnConst(values.column->size(), *single_element_in_set);
        ColumnsWithTypeAndName equals_args = {
            values,
            ColumnWithTypeAndName(const_column, filter_column_target_type, String())
        };
        auto single_element_equals_function = FunctionFactory::instance().get("equals", nullptr)->build(equals_args);
        return single_element_equals_function->execute(equals_args, single_element_equals_function->getResultType(), values.column->size(), /* dry_run = */ false);
    }
    else if (exact_values)
    {
        return exact_values->execute({values}, false);
    }
    else
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
}

void RuntimeFilter::insertIntoBloomFilter(ColumnPtr values)
{
    const size_t num_rows = values->size();
    for (size_t row = 0; row < num_rows; ++row)
    {
        /// TODO: make this efficient: compute hashes in vectorized manner
        auto value = values->getDataAt(row);
        bloom_filter->add(value.data, value.size);
    }
}

void RuntimeFilter::switchToBloomFilter()
{
    if (bloom_filter)
        return;

    exact_values->finishInsert();

    bloom_filter = std::make_unique<BloomFilter>(bloom_filter_bytes, bloom_filter_hash_functions, BLOOM_FILTER_SEED);
    insertIntoBloomFilter(exact_values->getSetElements().front());

    exact_values.reset();
}

using RuntimeFilterPtr = std::shared_ptr<RuntimeFilter>;
class RuntimeFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & name, std::unique_ptr<RuntimeFilter> runtime_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[name];
        if (!filter)
        {
            filter.reset(runtime_filter.release());   /// Save new filter
        }
        else
        {
            runtime_filter->finishInsert();
            filter->addAllFrom(*runtime_filter);    /// Add all new keys to a existing filter
        }
    }

    RuntimeFilterConstPtr find(const String & name) const override
    {
        SharedLockGuard g(rw_lock);
        auto it = filters_by_name.find(name);
        if (it == filters_by_name.end())
            return nullptr;
        else
        {
            it->second->finishInsert();
            return it->second;
        }
    }

private:
    mutable SharedMutex rw_lock;
    std::unordered_map<String, RuntimeFilterPtr> filters_by_name TSA_GUARDED_BY(rw_lock);
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<RuntimeFilterLookup>();
}

}
