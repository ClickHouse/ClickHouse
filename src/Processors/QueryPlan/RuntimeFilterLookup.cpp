#include <Interpreters/BloomFilter.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Common/SharedMutex.h>
#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

class BloomFilterLookup : public IRuntimeFilterLookup
{
public:
    void add(const String & name, std::unique_ptr<BloomFilter> bloom_filter) override
    {
        std::lock_guard g(rw_lock);
        auto & filter = filters_by_name[name];
        if (!filter)
            filter.reset(bloom_filter.release());   /// Save new filter
        else
            mergeFilter(*filter, *bloom_filter);    /// Add all new keys to a existing filter
    }

    BloomFilterConstPtr find(const String & name) const override
    {
        std::shared_lock g(rw_lock);
        auto it = filters_by_name.find(name);
        if (it == filters_by_name.end())
            return nullptr;
        else
            return it->second;
    }

private:
    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    static void mergeFilter(BloomFilter & destination, const BloomFilter & source)
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

    mutable SharedMutex rw_lock;
    std::unordered_map<String, BloomFilterPtr> filters_by_name;
};

RuntimeFilterLookupPtr createRuntimeFilterLookup()
{
    return std::make_shared<BloomFilterLookup>();
}

}
