#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Core/ResultColumnOrigin.h>
#include <Interpreters/Cache/QueryResultCacheUsage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IASTHash.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/IAST_fwd.h>
#include <base/UUID.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB
{

struct Settings;

class QueryResultCacheWriter;
class QueryResultCacheReader;

class QueryResultCacheOnDisk;
using QueryResultCacheOnDiskPtr = std::shared_ptr<const QueryResultCacheOnDisk>;

/// Is the in-memory query result cache enabled for writes by the settings and able to store entries under the server configuration?
bool canWriteToQueryResultCacheInMemory(ContextPtr context);

/// Is the on-disk query result cache `on_disk_cache` (nullptr if there is none) enabled for writes by the settings and able to store
/// entries under the server configuration (the maximum entry size of the query result cache applies to both backends)?
bool canWriteToQueryResultCacheOnDisk(ContextPtr context, QueryResultCacheOnDiskPtr on_disk_cache);

/// Can any backend of the query result cache store the result of the query? A configured but unavailable on-disk backend does not count,
/// and neither does an in-memory cache with zero limits.
bool hasQueryResultCacheWriteBackend(ContextPtr context, QueryResultCacheOnDiskPtr on_disk_cache);

/// Checks that query cache can be used for query.
/// Only use the query cache if the query does not contain non-deterministic functions or system tables (which are typically non-deterministic)
/// Throws if ast contains non-deterministic functions or system tables and appropriate handling setting is set to throw.
/// `on_disk_cache` is the on-disk query result cache backend resolved for the query (nullptr if there is none): the checks must apply
/// only if some backend actually accepts writes (see `hasQueryResultCacheWriteBackend`).
/// When skip_context_check is true, the context's canUseQueryResultCache flag is not checked.
/// This is used for explicit per-subquery opt-in where the subquery has SETTINGS use_query_cache = true
/// but the outer query context may not have the flag set.
/// When throw_on_error is false, returns false instead of throwing. Used for cheap speculative probes
/// (e.g. deciding if herd coalescing is worthwhile) before the authoritative check after query runs.
bool checkCanWriteQueryResultCache(
    ASTPtr ast, ContextPtr context, QueryResultCacheOnDiskPtr on_disk_cache, bool skip_context_check = false, bool throw_on_error = true);

/// Bug 67476: If the query runs with a non-THROW overflow mode and hits a limit, the query result cache will incorrectly store a truncated
/// result (if enabled).It is hard to detect from the perspective of the query result cache that
/// the query result is truncated. Notify the user through an exception to disable either the query result cache or use
/// another overflow mode. Called by executeQuery() for the top-level cache and by Planner for the subquery-level cache (explicit
/// per-subquery opt-in), since the outer query's use_query_cache flag and executeQuery()'s own check may be false
/// even though the Planner-level cache is used for that subquery.
void throwIfQueryResultCacheUsedWithNonThrowOverflowMode(const Settings & settings);

class QueryResultCache;

using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

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
        SharedHeader header;

        /// The table columns the result columns come from, see `QueryResultDetails::result_column_origins`. Stored so that a cache hit
        /// can report them without analyzing the query again, which would evaluate scalar subqueries against the current data.
        ResultColumnOrigins result_column_origins = {};

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

        /// When was the entry created?
        const std::chrono::time_point<std::chrono::system_clock> created_at;

        /// When does the entry expire?
        const std::chrono::time_point<std::chrono::system_clock> expires_at;

        /// Are the chunks in the entry compressed?
        /// (we could theoretically apply compression also to the totals and extremes but it's an obscure use case)
        const bool is_compressed;

        /// The SELECT query as plain string, displayed in SYSTEM.QUERY_CACHE. Stored explicitly, i.e. not constructed from the AST, for the
        /// sole reason that QueryResultCache-related SETTINGS are pruned from the AST (see removeQueryResultCacheSettings()) which would otherwise look
        /// ugly in SYSTEM.QUERY_CACHE.
        String query_string;

        /// ID of the query.
        const String query_id;

        /// A tag (namespace) for distinguish multiple entries of the same query.
        /// This member has currently no use besides that SYSTEM.QUERY_CACHE can populate the 'tag' column conveniently without having to
        /// compute the tag from the query AST.
        const String tag;

        /// Is it subquery entry? Displayed in SYSTEM.QUERY_CACHE.
        const bool is_subquery;

        /// Was the query that wrote the entry exempt from quotas (see `IInterpreter::ignoreQuota`)? A hit does not create an
        /// interpreter, so it takes the decision from here and keeps the exemption of a query served from the cache.
        bool ignore_quota = false;

        /// Ctor to construct a Key for writing into query result cache.
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            SharedHeader header_,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> created_at_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed,
            bool is_subquery_);

        /// Ctor to construct a Key for reading from query result cache (this operation only needs the AST + user name).
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_subquery_);

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        Chunks chunks;
        std::optional<Chunk> totals = std::nullopt;
        std::optional<Chunk> extremes = std::nullopt;
    };

    /// Identifies concurrent identical queries that should coalesce. Unlike Key, this ignores TTL/creation time.
    /// Includes user_id/roles (unless share_between_users is set) so different users never wait on each other.
    struct CoalescingKey
    {
        IASTHash ast_hash;
        std::optional<UUID> user_id;
        std::vector<UUID> current_user_roles;
        bool share_between_users = false;
        bool is_subquery = false;

        bool operator==(const CoalescingKey & other) const;
    };

    struct CoalescingKeyHasher
    {
        size_t operator()(const CoalescingKey & key) const;
    };

    /// One in-flight query result computation. The query which creates the token (the "executor") runs the query and
    /// eventually completes the token (successfully or not); concurrent identical queries (the "waiters") wait for that.
    ///
    /// Completion is signalled through a state and a condition variable rather than by holding a mutex for the whole
    /// execution: the token is created on the thread that plans the query, but it may be completed or destroyed on any
    /// other thread (e.g. a pipeline thread, or another worker of the Arrow Flight server polling the query).
    struct HerdToken
    {
        enum class State : uint8_t
        {
            Running,
            /// The executor finished successfully. Its result is in the cache unless it could not be stored there
            /// (e.g. it exceeds the entry size limits), in which case the waiters must not try again.
            Succeeded,
            /// The executor threw, was cancelled, or was destroyed without finishing. A waiter may take over.
            Failed,
        };

        HerdToken(String owner_query_id_, UInt64 generation_)
            : owner_query_id(std::move(owner_query_id_)), generation(generation_)
        {
        }

        std::mutex mutex;
        std::condition_variable completed;
        State state = State::Running; /// Protected by `mutex`.

        /// Query id of the token owner. Prevents a query from waiting on itself (e.g. same subquery appears twice).
        const String owner_query_id;

        /// Snapshot of clear_generation at creation. Mismatches mean SYSTEM CLEAR ran after so new queries skip it.
        const UInt64 generation;
    };
    using HerdTokenPtr = std::shared_ptr<HerdToken>;

    enum class HerdWaitOutcome : uint8_t
    {
        /// This call became the executor, `token` is set.
        Acquired,
        /// The executor finished successfully. The caller should re-probe the cache and, on a miss, execute the query
        /// independently (without a token): a successful execution whose result was not cached would not be cached
        /// by a retry either, so retrying would only serialize the queries.
        OwnerSucceeded,
        /// The executor failed. The caller should re-probe the cache and, on a miss, call `acquireOrWaitHerdToken`
        /// again to take over as the executor (or to wait for another waiter which took over first).
        OwnerFailed,
        /// The caller must not wait and should execute the query independently (without a token). This happens when
        /// the query already owns the token for `key` (the same subquery appears twice in a query), when the query owns
        /// a token of another subquery (waiting could deadlock with a query which acquires the same subqueries in a
        /// different order), when the wait of this caller timed out (the token stays registered for the others), or when
        /// `is_cancelled` returned true. In the latter case the caller is expected to detect and report the
        /// cancellation itself (e.g. via `QueryStatus::throwIfKilled`), the same way it would without coalescing.
        Independent,
    };

    struct HerdWaitResult
    {
        HerdWaitOutcome outcome;
        HerdTokenPtr token; /// Only set for `HerdWaitOutcome::Acquired`.
    };

    /// Tries to become the herd "executor" for `key`, waiting up to `timeout` for a concurrent, identical query
    /// to finish if one is already in flight. `is_cancelled` is polled periodically while waiting, it may throw.
    ///
    /// If the call became the executor, the caller must run the query and call `releaseHerdToken` exactly once,
    /// from both the success and the exception path (see `QueryResultCacheHerdTokenHolder`).
    HerdWaitResult acquireOrWaitHerdToken(
        const CoalescingKey & key,
        std::chrono::milliseconds timeout,
        const String & query_id,
        const std::function<bool()> & is_cancelled);

    /// Releases a token obtained from `acquireOrWaitHerdToken`. Must be called exactly once by the executor, from any
    /// thread. Wakes up the waiters, telling them whether the execution succeeded, and, if the token is still the current
    /// entry for `key` in the coalescing map, removes it so that future queries don't wait on a finished execution.
    void releaseHerdToken(const CoalescingKey & key, const HerdTokenPtr & token, bool succeeded);

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

public:
    /// query --> query result
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    QueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    QueryResultCacheReader createReader(const Key & key);
    QueryResultCacheWriter createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota,
        bool write_to_memory_cache = true,
        QueryResultCacheOnDiskPtr on_disk_cache = nullptr);

    void clear(const std::optional<String> & tag);

    /// Can the in-memory cache store an entry at all under the current server configuration? It can not if one of its limits is 0,
    /// e.g. in `clickhouse-local` or with `query_cache.max_size_in_bytes = 0`.
    bool canStoreEntries() const;

    /// Do the maximum entry sizes allow any entry? A limit of 0 means that nothing may be cached, in both backends.
    bool entrySizeLimitsAllowEntries() const;

    size_t maxSizeInBytes() const;
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

    /// Currently in-flight query executions, keyed by CoalescingKey, used to avoid the "thundering herd" effect
    /// (many concurrent identical queries each computing and inserting the same result). See HerdToken.
    using HerdTokenMap = std::unordered_map<CoalescingKey, HerdTokenPtr, CoalescingKeyHasher>;
    HerdTokenMap herd_tokens TSA_GUARDED_BY(mutex);

    /// Bumped once per clear() call. Snapshotted into each new HerdToken so that lookups can tell apart tokens
    /// created before vs. after the most recent SYSTEM CLEAR QUERY CACHE.
    UInt64 clear_generation TSA_GUARDED_BY(mutex) = 0;

    /// Number of current subquery tokens in `herd_tokens` per owner query id. A query owning a subquery token must not
    /// wait for another subquery token: two queries acquiring the same subquery tokens in a different order would deadlock.
    /// Top-level tokens are not counted: a query waits for a top-level token only before it owns any token, so a query
    /// owning only its top-level token can wait for a subquery token without creating a cycle.
    std::unordered_map<String, size_t> subquery_herd_tokens_by_owner TSA_GUARDED_BY(mutex);

    HerdTokenPtr createHerdToken(const CoalescingKey & key, const String & query_id) TSA_REQUIRES(mutex);
    void eraseHerdToken(HerdTokenMap::iterator it) TSA_REQUIRES(mutex);

    friend class StorageSystemQueryResultCache;
    friend class QueryResultCacheWriter;
    friend class QueryResultCacheReader;
};

/// RAII owner of a herd token (see `QueryResultCache::acquireOrWaitHerdToken`).
///
/// The token is acquired while the query (or, in the Planner, a subquery) is planned, but it must be released only once
/// the query has actually executed, which happens at a very different place in the code, possibly on another thread,
/// or never (e.g. the subquery is replaced by a `ReadFromQueryResultCacheStep`). `release` is idempotent, so it is safe
/// to call from both a normal completion path and a cleanup path. If the holder is destroyed without an explicit
/// `release`, the execution is considered to have failed, so that a waiter takes over.
///
/// Not thread-safe: `release` must not be called concurrently on the same holder.
class QueryResultCacheHerdTokenHolder
{
public:
    QueryResultCacheHerdTokenHolder() = default;

    QueryResultCacheHerdTokenHolder(QueryResultCachePtr cache_, QueryResultCache::CoalescingKey key_, QueryResultCache::HerdTokenPtr token_)
        : cache(std::move(cache_)), key(std::move(key_)), token(std::move(token_))
    {
    }

    QueryResultCacheHerdTokenHolder(const QueryResultCacheHerdTokenHolder &) = delete;
    QueryResultCacheHerdTokenHolder & operator=(const QueryResultCacheHerdTokenHolder &) = delete;

    QueryResultCacheHerdTokenHolder(QueryResultCacheHerdTokenHolder && other) noexcept
        : cache(std::move(other.cache)), key(std::move(other.key)), token(std::move(other.token))
    {
        other.token.reset();
    }

    QueryResultCacheHerdTokenHolder & operator=(QueryResultCacheHerdTokenHolder && other) noexcept
    {
        if (this != &other)
        {
            release(/*succeeded=*/ false);
            cache = std::move(other.cache);
            key = std::move(other.key);
            token = std::move(other.token);
            other.token.reset();
        }
        return *this;
    }

    ~QueryResultCacheHerdTokenHolder() { release(/*succeeded=*/ false); }

    /// Idempotent and safe to call on a default-constructed (no-op) holder.
    void release(bool succeeded) noexcept
    {
        if (!token)
            return;
        cache->releaseHerdToken(key, token, succeeded);
        token.reset();
    }

private:
    QueryResultCachePtr cache;
    QueryResultCache::CoalescingKey key;
    QueryResultCache::HerdTokenPtr token;
};

/// Runs the "thundering herd" avoidance protocol for a query (or a subquery) whose result is not in the query result cache:
/// waits (up to `timeout` in total) while a concurrent, identical query computes the result, then calls `try_read_from_cache`.
/// Returns true if `try_read_from_cache` returned true, i.e. the result was served from the cache. Otherwise the caller must
/// execute the query; if it became the herd executor, `holder` owns the token, else the query runs independently.
/// While waiting, the query reacts to `KILL QUERY` and to the cancellation by the client (via the interactive cancel callback
/// of the native protocol and the client connection check of the HTTP interface).
bool coalesceQueryResultCacheHerd(
    const QueryResultCachePtr & cache,
    const QueryResultCache::CoalescingKey & key,
    ContextPtr context,
    std::chrono::milliseconds timeout,
    const std::function<bool()> & try_read_from_cache,
    QueryResultCacheHerdTokenHolder & holder);

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

    /// Called when a consumer stopped reading the query result before the end, so only a prefix of it was buffered.
    /// finalizeWrite() then does not insert anything.
    void markResultIncomplete();

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
    const bool write_to_memory_cache; /// if false, the result is written only to the on-disk cache
    const QueryResultCacheOnDiskPtr on_disk_cache; /// nullptr if the result is not written to the on-disk cache
    Cache::MappedPtr query_result TSA_GUARDED_BY(mutex) = std::make_shared<QueryResultCache::Entry>();
    bool skip_memory_insert = false; /// (set in the ctor, then accessed only in finalizeWrite)
    bool skip_disk_insert = false; /// (set in the ctor, then accessed only in finalizeWrite)
    std::atomic<bool> skip_insert = false; /// both of the above: don't even buffer the result
    std::atomic<bool> result_is_incomplete = false; /// a consumer stopped reading before the end, see markResultIncomplete()
    std::atomic<bool> was_finalized = false;
    LoggerPtr logger = getLogger("QueryResultCache");

    QueryResultCacheWriter(
        Cache & cache_,
        const Cache::Key & key_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_,
        bool write_to_memory_cache_,
        QueryResultCacheOnDiskPtr on_disk_cache_);

    friend class QueryResultCache; /// for createWriter()
};

/// Reader's constructor looks up a query result for a key in the cache. If found, it constructs source processors (that generate the
/// cached result) for use in a pipe or query pipeline.
class QueryResultCacheReader
{
public:
    using Cache = QueryResultCache::Cache;

    /// Where the reader looked the query result up. Only determines which profile events are incremented.
    enum class Source
    {
        Memory,
        OnDisk
    };

    bool hasCacheEntryForKey(bool update_profile_events = true, bool record_miss = true) const;

    /// Records the overall outcome of the query result cache lookup in `QueryCacheHits` / `QueryCacheMisses`. Must be called
    /// exactly once per query that probes the cache, after every enabled backend has been probed, so that these events keep
    /// describing the query result cache as a whole (see `QueryCacheOnDiskHits` / `QueryCacheOnDiskMisses` for the on-disk
    /// backend alone).
    static void recordProbeResult(bool has_entry);

    /// Records a miss of the query result cache whose probes ran with `hasCacheEntryForKey(false)`: the per-backend
    /// `QueryCacheOnDiskMisses` if the on-disk backend was probed, and `QueryCacheMisses` (via `recordProbeResult`). Lets the
    /// caller record the probe only after it has decided that the query is allowed to run.
    static void recordMiss(bool probed_on_disk);

    /// Must only be called if hasCacheEntryForKey is true
    std::chrono::time_point<std::chrono::system_clock> entryCreatedAt();
    std::chrono::time_point<std::chrono::system_clock> entryExpiresAt();
    bool entryIgnoresQuota() const;
    const ResultColumnOrigins & entryResultColumnOrigins() const;

    /// getSource*() moves source processors out of the Reader. Call each of these method just once.
    std::unique_ptr<SourceFromChunks> getSource();
    std::unique_ptr<SourceFromChunks> getSourceExtremes();
    std::unique_ptr<SourceFromChunks> getSourceTotals();

private:
    QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &);

    /// Ctor for a cache miss from the given source.
    explicit QueryResultCacheReader(Source source_);

    /// Ctor for a cache hit served by the on-disk query result cache.
    QueryResultCacheReader(
        Source source_,
        SharedHeader header,
        QueryResultCache::Entry && entry,
        ResultColumnOrigins && result_column_origins_,
        std::chrono::time_point<std::chrono::system_clock> created_at_,
        std::chrono::time_point<std::chrono::system_clock> expires_at_,
        bool ignore_quota_,
        size_t on_disk_read_bytes_);

    void buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);

    Source source = Source::Memory;

    std::unique_ptr<SourceFromChunks> source_from_chunks;
    std::unique_ptr<SourceFromChunks> source_from_chunks_totals;
    std::unique_ptr<SourceFromChunks> source_from_chunks_extremes;

    std::chrono::time_point<std::chrono::system_clock> created_at;
    std::chrono::time_point<std::chrono::system_clock> expires_at;
    bool ignore_quota = false;
    ResultColumnOrigins result_column_origins;

    /// What reading the entry amounts to, recorded in `QueryCacheReadRows` / `QueryCacheReadBytes` / `QueryCacheOnDiskReadBytes`
    /// by `hasCacheEntryForKey`.
    size_t read_rows = 0;
    size_t read_bytes = 0;
    size_t on_disk_read_bytes = 0;

    LoggerPtr logger = getLogger("QueryResultCache");

    friend class QueryResultCache; /// for createReader()
    friend class QueryResultCacheOnDisk; /// for the on-disk ctors
};

}
