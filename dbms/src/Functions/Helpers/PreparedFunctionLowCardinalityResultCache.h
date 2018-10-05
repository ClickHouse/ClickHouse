#include <Columns/IColumnUnique.h>
#include <memory>

namespace DB
{

/// Cache for functions result if it was executed on low cardinality column.
/// It's LRUCache which stores function result executed on dictionary and index mapping.
/// It's expected that cache_size is a number of reading streams (so, will store single cached value per thread).
class PreparedFunctionLowCardinalityResultCache
{
public:
    /// Will assume that dictionaries with same hash has the same keys.
    /// Just in case, check that they have also the same size.
    struct DictionaryKey
    {
        UInt128 hash;
        UInt64 size;

        bool operator== (const DictionaryKey & other) const { return hash == other.hash && size == other.size; }
    };

    struct DictionaryKeyHash
    {
        size_t operator()(const DictionaryKey & key) const
        {
            SipHash hash;
            hash.update(key.hash.low);
            hash.update(key.hash.high);
            hash.update(key.size);
            return hash.get64();
        }
    };

    struct CachedValues
    {
        /// Store ptr to dictionary to be sure it won't be deleted.
        ColumnPtr dictionary_holder;
        ColumnUniquePtr function_result;
        /// Remap positions. new_pos = index_mapping->index(old_pos);
        ColumnPtr index_mapping;
    };

    using CachedValuesPtr = std::shared_ptr<CachedValues>;

    explicit PreparedFunctionLowCardinalityResultCache(size_t cache_size) : cache(cache_size) {}

    CachedValuesPtr get(const DictionaryKey & key) { return cache.get(key); }
    void set(const DictionaryKey & key, const CachedValuesPtr & mapped) { cache.set(key, mapped); }
    CachedValuesPtr getOrSet(const DictionaryKey & key, const CachedValuesPtr & mapped)
    {
        return cache.getOrSet(key, [&]() { return mapped; }).first;
    }

private:
    using Cache = LRUCache<DictionaryKey, CachedValues, DictionaryKeyHash>;
    Cache cache;
};

using PreparedFunctionLowCardinalityResultCachePtr = std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

}
