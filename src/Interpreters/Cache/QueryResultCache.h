#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
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
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB
{

struct Settings;

/// Checks that query cache can be used for query.
/// Only use the query cache if the query does not contain non-deterministic functions or system tables (which are typically non-deterministic)
/// Throws if ast contains non-deterministic functions or system tables and appropriate handling setting is set to throw
/// (unless throw_on_error is false, see below).
/// When skip_context_check is true, the context's canUseQueryResultCache flag is not checked.
/// This is used for explicit per-subquery opt-in where the subquery has SETTINGS use_query_cache = true
/// but the outer query context may not have the flag set.
/// When throw_on_error is false, returns false instead of throwing for AST/setting combinations that would
/// otherwise throw. This is used to cheaply and speculatively probe write-eligibility (e.g. to decide whether
/// thundering-herd coalescing is worthwhile) without pre-empting the authoritative throwing call made once the
/// query has actually run.
bool checkCanWriteQueryResultCache(ASTPtr ast, ContextPtr context, bool skip_context_check = false, bool throw_on_error = true);

/// Bug 67476: If the query runs with a non-THROW overflow mode and hits a limit, the query result cache will store a truncated
/// result (if enabled). This is incorrect. Unfortunately it is hard to detect from the perspective of the query result cache that
/// the query result is truncated. Therefore throw an exception, to notify the user to disable either the query result cache or use
/// another overflow mode. Called by executeQuery() for the top-level cache and by Planner for the subquery-level cache (explicit
/// per-subquery opt-in), since the outer query's use_query_cache flag - and therefore executeQuery()'s own check - may be false
/// even though the Planner-level cache is used for that subquery.
void throwIfQueryResultCacheUsedWithNonThrowOverflowMode(const Settings & settings);

class QueryResultCacheWriter;
class QueryResultCacheReader;
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

    /// Identifies a group of concurrent, identical queries that should coalesce onto a single execution
    /// ("thundering herd" avoidance). Unlike Key, equality here does not consider the entry's TTL/creation time
    /// etc. - it only needs to tell apart queries which would race to compute and insert the very same Key.
    /// user_id/current_user_roles are part of the key (unless share_between_users is set) so that queries of
    /// different users/roles never wait on each other's execution, mirroring the read-side access check.
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

    /// Represents one in-flight computation of a query result. The query which creates the token (the
    /// "executor") holds `mutex` locked until it is done (successfully or not); concurrent identical queries
    /// (the "waiters") block on `mutex` instead of redundantly re-computing the same result.
    struct HerdToken
    {
        HerdToken(String owner_query_id_, UInt64 generation_)
            : owner_query_id(std::move(owner_query_id_)), generation(generation_)
        {
        }

        std::timed_mutex mutex;

        /// Set by a waiter that gave up on its own timeout, after it removed this token from the coalescing map.
        /// Lets every other waiter still polling this same token bail out immediately instead of waiting for
        /// their own (possibly much later) timeout to elapse.
        std::atomic<bool> abandoned{false};

        /// Query id of the query which created this token. Used to detect the case where a query would end up
        /// waiting on its own in-flight execution (e.g. the same subquery appears twice in one query) which
        /// would otherwise deadlock.
        const String owner_query_id;

        /// Snapshot of QueryResultCache::clear_generation at creation time. A token whose generation does not
        /// match the cache's current clear_generation is considered stale: SYSTEM CLEAR QUERY CACHE ran after it
        /// was created, so new queries must not coalesce onto it (even though the query it belongs to may still
        /// be legitimately running and will insert its result normally once done).
        const UInt64 generation;
    };
    using HerdTokenPtr = std::shared_ptr<HerdToken>;

    /// Tries to become the herd "executor" for `key`, waiting up to `timeout` for a concurrent, identical query
    /// to finish if one is already in flight.
    ///
    /// Returns a token (already locked) if this call became the executor. The caller must run the query and
    /// call releaseHerdToken() exactly once, from both the success and the exception path.
    ///
    /// Returns nullptr if this call did not become the executor. This happens when: the wait succeeded (the
    /// other query's execution finished, a cache hit is likely - the caller should re-probe the cache), the
    /// wait gave up (this or another waiter's timeout elapsed), or `is_cancelled` started returning true. In all
    /// of these cases, the caller should re-probe the cache and, if still empty, call tryBecomeHerdExecutor() to
    /// attempt taking over as executor; if the query was in fact cancelled, the caller is expected to detect and
    /// report that itself (e.g. via QueryStatus::throwIfKilled()), the same way it would without coalescing.
    HerdTokenPtr acquireOrWaitHerdToken(
        const CoalescingKey & key,
        std::chrono::milliseconds timeout,
        const String & query_id,
        const std::function<bool()> & is_cancelled);

    /// Non-blocking counterpart of acquireOrWaitHerdToken(): tries to become the herd executor for `key`
    /// immediately. Returns a locked token on success, or nullptr if another query already owns `key` (that
    /// query is the executor).
    HerdTokenPtr tryBecomeHerdExecutor(const CoalescingKey & key, const String & query_id);

    /// Releases a token obtained from tryBecomeHerdExecutor()/acquireOrWaitHerdToken(). Must be called exactly
    /// once by whichever call became the executor. Unblocks waiters and, if the token is still the current entry
    /// for `key` in the coalescing map, removes it so that future queries don't wait on a finished execution.
    void releaseHerdToken(const CoalescingKey & key, const HerdTokenPtr & token);

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
        size_t max_query_result_cache_entries_quota);

    void clear(const std::optional<String> & tag);

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

    HerdTokenPtr tryBecomeHerdExecutorImpl(const CoalescingKey & key, const String & query_id) TSA_REQUIRES(mutex);

    friend class StorageSystemQueryResultCache;
    friend class QueryResultCacheWriter;
    friend class QueryResultCacheReader;
};

/// RAII bridge for a herd token acquired while building the query plan for a subquery (in the Planner), which
/// must be released once the subquery has actually executed (successfully or not) - or immediately, if the
/// subquery's result was served from a plain cache hit, or its plan wasn't cached at all and the token was never
/// acquired in the first place.
///
/// This indirection exists because "acquired" (during planning) and "released" (whenever/if the subquery
/// pipeline finishes executing, which may be much later, or never if e.g. the query is fully replaced by a
/// ReadFromQueryResultCacheStep) happen at very different places in the code. A holder is created unconditionally
/// for every subquery plan; release() is idempotent so it is safe to call from both a normal completion path and
/// a cleanup/exception path without extra bookkeeping at the call sites.
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
    QueryResultCacheHerdTokenHolder(QueryResultCacheHerdTokenHolder &&) = default;
    QueryResultCacheHerdTokenHolder & operator=(QueryResultCacheHerdTokenHolder &&) = default;

    ~QueryResultCacheHerdTokenHolder() { release(); }

    /// Idempotent: safe to call more than once (e.g. once from a SCOPE_EXIT and once from an explicit early-exit
    /// path) and safe to call on a default-constructed (no-op) holder.
    void release()
    {
        if (!token)
            return;
        cache->releaseHerdToken(key, token);
        token.reset();
    }

private:
    QueryResultCachePtr cache;
    QueryResultCache::CoalescingKey key;
    QueryResultCache::HerdTokenPtr token;
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
    std::atomic<bool> was_finalized = false;
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
    using Cache = QueryResultCache::Cache;

    bool hasCacheEntryForKey(bool update_profile_events = true) const;

    /// Must only be called if hasCacheEntryForKey is true
    std::chrono::time_point<std::chrono::system_clock> entryCreatedAt();
    std::chrono::time_point<std::chrono::system_clock> entryExpiresAt();

    /// getSource*() moves source processors out of the Reader. Call each of these method just once.
    std::unique_ptr<SourceFromChunks> getSource();
    std::unique_ptr<SourceFromChunks> getSourceExtremes();
    std::unique_ptr<SourceFromChunks> getSourceTotals();

private:
    QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &);
    void buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);

    std::unique_ptr<SourceFromChunks> source_from_chunks;
    std::unique_ptr<SourceFromChunks> source_from_chunks_totals;
    std::unique_ptr<SourceFromChunks> source_from_chunks_extremes;

    std::chrono::time_point<std::chrono::system_clock> created_at;
    std::chrono::time_point<std::chrono::system_clock> expires_at;

    LoggerPtr logger = getLogger("QueryResultCache");

    friend class QueryResultCache; /// for createReader()
};

}
