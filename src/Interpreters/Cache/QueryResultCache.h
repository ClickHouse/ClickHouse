#pragma once

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Parsers/IAST.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

/// Maps queries to query results. Useful to avoid repeated query calculation.
///
/// The cache does not aim to be transactionally consistent (which is difficult to get right). For example, the cache is not invalidated
/// when data is inserted/deleted into/from tables referenced by queries in the cache. In such situations, incorrect results may be
/// returned. In order to still obtain sufficiently up-to-date query results, a expiration time must be specified for each cache entry after
/// which it becomes stale and is ignored. Stale entries are removed opportunistically from the cache, they are only evicted when a new
/// entry is inserted and the cache has insufficient capacity.
class QueryResultCache
{
public:
    /// Represents a query in the cache.
    struct Key
    {
        /// The actual key, i.e. data which gets hashed:

        /// Unlike the query string, the AST is agnostic to lower/upper case (SELECT vs. select)
        const ASTPtr ast;

        /// It is unlikely that different users pose the same queries. More importantly, sharing query results between users potentially
        /// breaches security. E.g. User A must not be able to bypass row policies on some table by running the same queries as user B for
        /// whom no row policies exist.
        const String username;

        /// Identifies a (virtual) cache partition. Can be used to cache the same query multiple times with different timeouts.
        const String partition_key;

        /// Note: For a transactionally consistent cache, we would need to include the system settings in the cache key or invalidate the
        /// cache whenever the settings change. This is because certain settings (e.g. "additional_table_filters") can affect the query
        /// result.

        /// Additional stuff data stored in the key, not hashed:

        /// For constructing the pipe.
        const Block header;

        /// When does the entry expire?
        const std::chrono::time_point<std::chrono::system_clock> expires_at;

        bool operator==(const Key & other) const;
        String queryStringFromAst() const;
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    using Entry = std::shared_ptr<Chunk>;

    /// query --> query result
    using Cache = std::unordered_map<Key, Entry, KeyHasher>;

    /// query --> query execution count
    using TimesExecutedMap = std::unordered_map<Key, size_t, KeyHasher>;

public:
    /// Buffers multiple result chunks and stores them during destruction as a cache entry.
    class Writer
    {
    public:
        Writer(std::mutex & mutex_, Cache & cache_, const Key & key_,
            size_t & cache_size_in_bytes_, size_t max_cache_size_in_bytes_, size_t max_entries_, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);
        ~Writer();
        void buffer(Chunk && chunk);
    private:
        std::mutex & mutex;
        Cache & cache;
        const Key key;
        size_t & cache_size_in_bytes;
        const size_t max_cache_size_in_bytes;
        const size_t max_entries;
        size_t new_entry_size_in_bytes;
        const size_t max_entry_size_in_bytes;
        size_t new_entry_size_in_rows;
        const size_t max_entry_size_in_rows;
        Chunks chunks;
        bool skip_insert;
    };

    /// Looks up a result chunk for a key in the cache and (if found) constructs a pipe with the chunk as source.
    class Reader
    {
    public:
        Reader(const Cache & cache_, std::mutex & mutex, const Key & key);
        bool hasEntryForKey() const;
        Pipe && getPipe();
    private:
        Pipe pipe;
    };

    explicit QueryResultCache(size_t max_cache_size_in_bytes_);

    Reader createReader(const Key & key);
    Writer createWriter(const Key & key, size_t max_entries, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows);

    void reset();

    /// Record new execution of query represented by key. Returns number of executions so far.
    size_t recordQueryRun(const Key & key);

private:
    /// Implementation note: The query result implements a custom caching mechanism and doesn't make use of CacheBase, unlike many other
    /// internal caches in ClickHouse. The main reason is that we don't need standard CacheBase (S)LRU eviction as the expiration times
    /// associated with cache entries provide a "natural" eviction criterion. As a future TODO, we could make an expiration-based eviction
    /// policy and use that with CacheBase.
    mutable std::mutex mutex;
    Cache cache;
    TimesExecutedMap times_executed;

    size_t cache_size_in_bytes;
    const size_t max_cache_size_in_bytes;

    friend class StorageSystemQueryResultCache;
};

using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
