#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/QueryResultCacheUsage.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Block.h>
#include <Parsers/IASTHash.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <base/UUID.h>

#include <optional>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
struct Settings;

/// Does AST contain non-deterministic functions like rand() and now()?
bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context);

/// Does AST contain system tables like "system.processes"?
bool astContainsSystemTables(ASTPtr ast, ContextPtr context);

class QueryResultCacheWriter;
class QueryResultCacheReader;

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
    /// Key + Entry represents a query result in the cache.
    struct Key
    {
        /// ----------------------------------------------------
        /// The actual key (data which gets hashed):


        /// The hash of the query AST.
        /// Unlike the query string, the AST is agnostic to lower/upper case (SELECT vs. select).
        IASTHash ast_hash;

        /// Note: For a transactionally consistent cache, we would need to include the system settings in the cache key or invalidate the
        /// cache whenever the settings change. This is because certain settings (e.g. "additional_table_filters") can affect the query
        /// result.

        /// ----------------------------------------------------
        /// Additional stuff data stored in the key, not hashed:

        /// Result metadata for constructing the pipe.
        const Block header;

        /// The id and current roles of the user who executed the query.
        /// These members are necessary to ensure that a (non-shared, see below) entry can only be written and read by the same user with
        /// the same roles. Example attack scenarios:
        /// - after DROP USER, it must not be possible to create a new user with with the dropped user name and access the dropped user's
        ///   query result cache entries
        /// - different roles of the same user may be tied to different row-level policies. It must not be possible to switch role and
        ///   access another role's cache entries
        std::optional<UUID> user_id;
        std::vector<UUID> current_user_roles;

        /// If the associated entry can be read by other users. In general, sharing is a bad idea: First, it is unlikely that different
        /// users pose the same queries. Second, sharing potentially breaches security. E.g. User A should not be able to bypass row
        /// policies on some table by running the same queries as user B for whom no row policies exist.
        const bool is_shared;

        /// When does the entry expire?
        const std::chrono::time_point<std::chrono::system_clock> expires_at;

        /// Are the chunks in the entry compressed?
        /// (we could theoretically apply compression also to the totals and extremes but it's an obscure use case)
        const bool is_compressed;

        /// The SELECT query as plain string, displayed in SYSTEM.QUERY_CACHE. Stored explicitly, i.e. not constructed from the AST, for the
        /// sole reason that QueryResultCache-related SETTINGS are pruned from the AST (see removeQueryResultCacheSettings()) which would otherwise look
        /// ugly in SYSTEM.QUERY_CACHE.
        const String query_string;

        /// ID of the query.
        const String query_id;

        /// A tag (namespace) for distinguish multiple entries of the same query.
        /// This member has currently no use besides that SYSTEM.QUERY_CACHE can populate the 'tag' column conveniently without having to
        /// compute the tag from the query AST.
        const String tag;

        /// Ctor to construct a Key for writing into query result cache.
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            Block header_,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed);

        /// Ctor to construct a Key for reading from query result cache (this operation only needs the AST + user name).
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_);

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        Chunks chunks;
        std::optional<Chunk> totals = std::nullopt;
        std::optional<Chunk> extremes = std::nullopt;
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct QueryResultCacheEntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

public:
    /// query --> query result
    using Cache = CacheBase<Key, Entry, KeyHasher, QueryResultCacheEntryWeight>;

    QueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    QueryResultCacheReader createReader(const Key & key);
    QueryResultCacheWriter createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota);

    void clear(const std::optional<String> & tag);

    size_t sizeInBytes() const;
    size_t count() const;

    /// Record new execution of query represented by key. Returns number of executions so far.
    size_t recordQueryRun(const Key & key);

    /// For debugging and system tables
    std::vector<QueryResultCache::Cache::KeyMapped> dump() const;

private:
    Cache cache; /// has its own locking --> not protected by mutex

    mutable std::mutex mutex;

    /// query --> query execution count
    using TimesExecuted = std::unordered_map<Key, size_t, KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    /// Cache configuration
    size_t max_entry_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t max_entry_size_in_rows TSA_GUARDED_BY(mutex) = 0;

    friend class StorageSystemQueryResultCache;
    friend class QueryResultCacheWriter;
    friend class QueryResultCacheReader;
};

/// Buffers multiple partial query result chunks (buffer()) and eventually stores them as cache entry (finalizeWrite()).
///
/// Implementation note: Queries may throw exceptions during runtime, e.g. out-of-memory errors. In this case, no query result must be
/// written into the query result cache. Unfortunately, neither the Writer nor the special transform added on top of the query pipeline
/// which holds the Writer know whether they are destroyed because the query ended successfully or because of an exception (otherwise, we
/// could simply implement a check in their destructors). To handle exceptions correctly nevertheless, we do the actual insert in
/// finalizeWrite() as opposed to the Writer destructor. This function is then called only for successful queries in finish_callback() which
/// runs before the transform and the Writer are destroyed, whereas for unsuccessful queries we do nothing (the Writer is destroyed w/o
/// inserting anything).
/// Queries may also be cancelled by the user, in which case IProcessor's cancel bit is set. FinalizeWrite() is only called if the
/// cancel bit is not set.
class QueryResultCacheWriter
{
public:
    QueryResultCacheWriter(const QueryResultCacheWriter & other);

    enum class ChunkType : uint8_t
    {
        Result,
        Totals,
        Extremes
    };
    void buffer(Chunk && chunk, ChunkType chunk_type);

    void finalizeWrite();
private:
    using Cache = QueryResultCache::Cache;

    std::mutex mutex;
    Cache & cache;
    const QueryResultCache::Key key;
    const size_t max_entry_size_in_bytes;
    const size_t max_entry_size_in_rows;
    const std::chrono::time_point<std::chrono::system_clock> query_start_time = std::chrono::system_clock::now(); /// Writer construction and finalizeWrite() coincide with query start/end
    const std::chrono::milliseconds min_query_runtime;
    const bool squash_partial_results;
    const size_t max_block_size;
    Cache::MappedPtr query_result TSA_GUARDED_BY(mutex) = std::make_shared<QueryResultCache::Entry>();
    std::atomic<bool> skip_insert = false;
    bool was_finalized = false;
    LoggerPtr logger = getLogger("QueryResultCache");

    QueryResultCacheWriter(
        Cache & cache_,
        const Cache::Key & key_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_);

    friend class QueryResultCache; /// for createWriter()
};

/// Reader's constructor looks up a query result for a key in the cache. If found, it constructs source processors (that generate the
/// cached result) for use in a pipe or query pipeline.
class QueryResultCacheReader
{
public:
    bool hasCacheEntryForKey() const;
    /// getSource*() moves source processors out of the Reader. Call each of these method just once.
    std::unique_ptr<SourceFromChunks> getSource();
    std::unique_ptr<SourceFromChunks> getSourceTotals();
    std::unique_ptr<SourceFromChunks> getSourceExtremes();
private:
    using Cache = QueryResultCache::Cache;

    QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &);
    void buildSourceFromChunks(Block header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);
    std::unique_ptr<SourceFromChunks> source_from_chunks;
    std::unique_ptr<SourceFromChunks> source_from_chunks_totals;
    std::unique_ptr<SourceFromChunks> source_from_chunks_extremes;
    LoggerPtr logger = getLogger("QueryResultCache");
    friend class QueryResultCache; /// for createReader()
};


using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
