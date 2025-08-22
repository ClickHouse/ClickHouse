#pragma once

#include <Interpreters/Cache/QueryResultCache.h>
#include <Common/CacheBase.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <optional>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
struct Settings;

class LocalQueryResultCache : public QueryResultCache
{
public:
    /// query --> query result
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    LocalQueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);
    ~LocalQueryResultCache() override = default;

    void updateConfiguration(const Poco::Util::AbstractConfiguration & config) override;

    std::shared_ptr<QueryResultCacheReader> createReader(const Key & key) override;
    std::shared_ptr<QueryResultCacheWriter> createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota) override;

    void clear(const std::optional<String> & tag) override;

    size_t sizeInBytes() const override;
    size_t count() const override;

    /// Record new execution of query represented by key. Returns number of executions so far.
    size_t recordQueryRun(const Key & key) override;

    std::vector<KeyMapped> dump() const override;
private:
    void updateConfigurationImpl(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);
    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

    Cache cache; /// has its own locking --> not protected by mutex

    mutable std::mutex mutex;

    /// query --> query execution count
    using TimesExecuted = std::unordered_map<Key, size_t, KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    /// Cache configuration
    size_t max_entry_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t max_entry_size_in_rows TSA_GUARDED_BY(mutex) = 0;

    friend class StorageSystemQueryResultCache;
    friend class LocalQueryResultCacheWriter;
    friend class LocalQueryResultCacheReader;
};

class LocalQueryResultCacheWriter : public QueryResultCacheWriter
{
public:
    using Cache = LocalQueryResultCache::Cache;
    explicit LocalQueryResultCacheWriter(
        Cache & cache_,
        const Cache::Key & key_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_)
        : QueryResultCacheWriter(key_, max_entry_size_in_bytes_, max_entry_size_in_rows_, min_query_runtime_, squash_partial_results_, max_block_size_)
        , cache(cache_)
    {
        initializeWriter<Cache, LocalQueryResultCache::IsStale>(cache, key_);
    }
    LocalQueryResultCacheWriter(const LocalQueryResultCacheWriter &) = default;
    ~LocalQueryResultCacheWriter() override = default;
    void finalizeWrite() override
    {
        finalizeWriteImpl<Cache, LocalQueryResultCache::IsStale>(cache);
    }
private:
    Cache & cache;
};

class LocalQueryResultCacheReader : public QueryResultCacheReader
{
public:
    using Cache = LocalQueryResultCache::Cache;
    LocalQueryResultCacheReader(Cache & cache_, const Cache::Key & key_, const std::lock_guard<std::mutex> &)
    {
        initializeReader<Cache, LocalQueryResultCache::IsStale>(cache_, key_);
    }
    ~LocalQueryResultCacheReader() override = default;
};

using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
