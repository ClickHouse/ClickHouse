#pragma once

#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

/// Does AST contain non-deterministic functions like rand() and now()?
bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context);

/// Maps queries to query results. Useful to avoid repeated query calculation.
///
/// The cache does not aim to be transactionally consistent (which is difficult to get right). For example, the cache is not invalidated
/// when data is inserted/deleted into/from tables referenced by queries in the cache. In such situations, incorrect results may be
/// returned. In order to still obtain sufficiently up-to-date query results, a expiry time (TTL) must be specified for each cache entry
/// after which it becomes stale and is ignored. Stale entries are removed opportunistically from the cache, they are only evicted when a
/// new entry is inserted and the cache has insufficient capacity.
class QueryResultCache
{
public:
    /// Represents a query result in the cache.
    struct Key
    {
        /// ----------------------------------------------------
        /// The actual key (data which gets hashed):

        /// Unlike the query string, the AST is agnostic to lower/upper case (SELECT vs. select)
        const ASTPtr ast;

        /// Note: For a transactionally consistent cache, we would need to include the system settings in the cache key or invalidate the
        /// cache whenever the settings change. This is because certain settings (e.g. "additional_table_filters") can affect the query
        /// result.

        /// ----------------------------------------------------
        /// Additional stuff data stored in the key, not hashed:

        /// Result metadata for constructing the pipe.
        const Block header;

        /// Std::nullopt means that the associated entry can be read by other users. In general, sharing is a bad idea: First, it is
        /// unlikely that different users pose the same queries. Second, sharing potentially breaches security. E.g. User A should not be
        /// able to bypass row policies on some table by running the same queries as user B for whom no row policies exist.
        const std::optional<String> username;

        /// When does the entry expire?
        const std::chrono::time_point<std::chrono::system_clock> expires_at;

        Key(ASTPtr ast_,
            Block header_, const std::optional<String> & username_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_);

        bool operator==(const Key & other) const;
        String queryStringFromAst() const;
    };

    struct QueryResult
    {
        std::shared_ptr<Chunks> chunks = std::make_shared<Chunks>();
        size_t sizeInBytes() const;

        /// Notes: 1. For performance reasons, we cache the original result chunks as-is (no concatenation during cache insert or lookup).
        ///        2. Ref-counting (shared_ptr) ensures that eviction of an entry does not affect queries which still read from the cache.
        ///           (this can also be achieved by copying the chunks during lookup but that would be under the cache lock --> too slow)
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    /// query --> query result
    using Cache = std::unordered_map<Key, QueryResult, KeyHasher>;

    /// query --> query execution count
    using TimesExecuted = std::unordered_map<Key, size_t, KeyHasher>;

public:
    /// Buffers multiple partial query result chunks (buffer()) and eventually stores them as cache entry (finalizeWrite()).
    ///
    /// Implementation note: Queries may throw exceptions during runtime, e.g. out-of-memory errors. In this case, no query result must be
    /// written into the query result cache. Unfortunately, neither the Writer nor the special transform added on top of the query pipeline
    /// which holds the Writer know whether they are destroyed because the query ended successfully or because of an exception (otherwise,
    /// we could simply implement a check in their destructors). To handle exceptions correctly nevertheless, we do the actual insert in
    /// finalizeWrite() as opposed to the Writer destructor. This function is then called only for successful queries in finish_callback()
    /// which runs before the transform and the Writer are destroyed, whereas for unsuccessful queries we do nothing (the Writer is
    /// destroyed w/o inserting anything).
    /// Queries may also be cancelled by the user, in which case IProcessor's cancel bit is set. FinalizeWrite() is only called if the
    /// cancel bit is not set.
    class Writer
    {
    public:
        void buffer(Chunk && partial_query_result);
        void finalizeWrite();
    private:
        std::mutex & mutex;
        Cache & cache TSA_GUARDED_BY(mutex);
        const Key key;
        size_t & cache_size_in_bytes TSA_GUARDED_BY(mutex);
        const size_t max_cache_size_in_bytes;
        const size_t max_cache_entries;
        size_t new_entry_size_in_bytes = 0;
        const size_t max_entry_size_in_bytes;
        size_t new_entry_size_in_rows = 0;
        const size_t max_entry_size_in_rows;
        const std::chrono::time_point<std::chrono::system_clock> query_start_time = std::chrono::system_clock::now(); /// Writer construction and finalizeWrite() coincide with query start/end
        const std::chrono::milliseconds min_query_runtime;
        QueryResult query_result;
        std::atomic<bool> skip_insert = false;

        Writer(std::mutex & mutex_, Cache & cache_, const Key & key_,
            size_t & cache_size_in_bytes_, size_t max_cache_size_in_bytes_,
            size_t max_cache_entries_,
            size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_,
            std::chrono::milliseconds min_query_runtime_);

        friend class QueryResultCache; /// for createWriter()
    };

    /// Looks up a query result for a key in the cache and (if found) constructs a pipe with the query result chunks as source.
    class Reader
    {
    public:
        bool hasCacheEntryForKey() const;
        Pipe && getPipe(); /// must be called only if hasCacheEntryForKey() returns true
    private:
        Reader(const Cache & cache_, const Key & key, size_t & cache_size_in_bytes_, const std::lock_guard<std::mutex> &);
        Pipe pipe;
        friend class QueryResultCache; /// for createReader()
    };

    QueryResultCache(size_t max_cache_size_in_bytes_, size_t max_cache_entries_, size_t max_cache_entry_size_in_bytes_, size_t max_cache_entry_size_in_rows_);

    Reader createReader(const Key & key);
    Writer createWriter(const Key & key, std::chrono::milliseconds min_query_runtime);

    void reset();

    /// Record new execution of query represented by key. Returns number of executions so far.
    size_t recordQueryRun(const Key & key);

private:
    /// Implementation note: The query result implements a custom caching mechanism and doesn't make use of CacheBase, unlike many other
    /// internal caches in ClickHouse. The main reason is that we don't need standard CacheBase (S)LRU eviction as the expiry times
    /// associated with cache entries provide a "natural" eviction criterion. As a future TODO, we could make an expiry-based eviction
    /// policy and use that with CacheBase (e.g. see #23706)
    /// TODO To speed up removal of stale entries, we could also add another container sorted on expiry times which maps keys to iterators
    /// into the cache. To insert an entry, add it to the cache + add the iterator to the sorted container. To remove stale entries, do a
    /// binary search on the sorted container and erase all left of the found key.
    mutable std::mutex mutex;
    Cache cache TSA_GUARDED_BY(mutex);
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    size_t cache_size_in_bytes TSA_GUARDED_BY(mutex) = 0; /// updated in each cache insert/delete
    const size_t max_cache_size_in_bytes;
    const size_t max_cache_entries;
    const size_t max_cache_entry_size_in_bytes;
    const size_t max_cache_entry_size_in_rows;

    friend class StorageSystemQueryResultCache;
};

using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
