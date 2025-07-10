#pragma once

#include <Interpreters/Cache/QueryResultCache.h>
#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Cache/QueryResultCacheUsage.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Block.h>
#include <Parsers/IASTHash.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <base/UUID.h>
#include <Common/RemoteCacheBase.h>

#include <optional>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
struct Settings;

class RemoteQueryResultCache : public QueryResultCache
{
public:
    /// query --> query result
    using Cache = RemoteCacheBase<Key, Entry, KeyHasher, QueryResultCacheEntryWeight>;
    RemoteQueryResultCache(std::shared_ptr<RedisConfiguration> config, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);
    virtual ~RemoteQueryResultCache() override {}

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
    void updateConfigurationImpl(std::shared_ptr<RedisConfiguration> config, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);
    struct IsStale
    {
        bool operator()(const Key &) const { return false; }
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
    friend class RemoteQueryResultCacheWriter;
    friend class RemoteQueryResultCacheReader;
};

class RemoteQueryResultCacheWriter : public QueryResultCacheWriter
{
public:
    using Cache = RemoteQueryResultCache::Cache;
    explicit RemoteQueryResultCacheWriter(
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
        initializeWriter<Cache, RemoteQueryResultCache::IsStale>(cache_, key_);
    }
    RemoteQueryResultCacheWriter(const RemoteQueryResultCacheWriter & other)
        : QueryResultCacheWriter(other)
        , cache(other.cache)
    {
    }
    virtual ~RemoteQueryResultCacheWriter() override = default;
    void finalizeWrite() override
    {
        finalizeWriteImpl<Cache, RemoteQueryResultCache::IsStale>(cache);
    }
private:
    Cache & cache;
};

class RemoteQueryResultCacheReader : public QueryResultCacheReader
{
public:
    using Cache = RemoteQueryResultCache::Cache;
    RemoteQueryResultCacheReader(Cache & cache_, const Cache::Key & key_, const std::lock_guard<std::mutex> &)
    {
        initializeReader<Cache, RemoteQueryResultCache::IsStale>(cache_, key_);
    }
    ~RemoteQueryResultCacheReader() override = default;
};

}
