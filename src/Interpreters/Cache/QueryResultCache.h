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

#include <optional>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}
// Forward declarations for serialization (defined here to avoid including heavy IO headers in this header).

namespace DB
{

struct Settings;

/// Does AST contain non-deterministic functions like rand() and now()?
bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context);

/// Does AST contain system tables like "system.processes"?
bool astContainsSystemTables(ASTPtr ast, ContextPtr context);

class QueryResultCacheWriter;
class QueryResultCacheReader;

/// Common key/entry types shared between all cache implementations.
/// These are defined as members of a common base to avoid circular dependencies.

/// Holds the key/entry structs that are shared by both LocalQueryResultCache and RemoteQueryResultCache.
/// We keep them inside a namespace struct so existing code using QueryResultCache::Key still compiles
/// after the rename.
struct QueryResultCache
{
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
            SharedHeader header_,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> created_at_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed);

        /// Ctor to construct a Key for reading from query result cache (this operation only needs the AST + user name).
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_);

        bool operator==(const Key & other) const;

        /// Encode this key as a Redis key string: {tag}{high64_hex}{low64_hex}
        String encodeToRedisKey() const;

        /// Serialize Key metadata (all non-hashed fields) to a binary buffer.
        /// Format: ast_hash(16) | header(NativeWriter) | user_id flag(1) | [user_id(16)] |
        ///         roles_count(varuint) | [roles(16 each)] | is_shared(1) | is_compressed(1) |
        ///         expires_at(8) | created_at(8) | query_string | query_id | tag
        void serializeTo(WriteBuffer & buf) const;

        /// Deserialize Key metadata from binary buffer. The returned Key has a dummy ast_hash that
        /// matches the one embedded in the buffer — callers must not use it as a cache lookup key.
        static Key deserializeFrom(ReadBuffer & buf);

    private:
        /// Private constructor used only by deserializeFrom to reconstruct a Key
        /// without going through calculateASTHash. All fields are set directly.
        struct DeserializeTag {};
        Key(DeserializeTag,
            IASTHash ast_hash_,
            SharedHeader header_,
            std::optional<UUID> user_id_,
            std::vector<UUID> current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> created_at_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed_,
            String query_string_,
            String query_id_,
            String tag_);
    };

    struct Entry
    {
        Chunks chunks;
        std::optional<Chunk> totals = std::nullopt;
        std::optional<Chunk> extremes = std::nullopt;

        /// Serialize all chunks (and optional totals/extremes) using NativeWriter format.
        /// header is needed by NativeWriter to write column type information.
        void serializeTo(WriteBuffer & buf, const Block & header) const;

        /// Deserialize from binary buffer. header provides column type context for NativeReader.
        static Entry deserializeFrom(ReadBuffer & buf, const Block & header);
    };

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

    /// query --> query result
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;
};

/// Internal interface used by QueryResultCacheWriter to write into the cache without depending on
/// the concrete cache type (local vs. remote). Both LocalQueryResultCache and RemoteQueryResultCache
/// implement this interface.
struct IQueryResultCacheStorage
{
    virtual ~IQueryResultCacheStorage() = default;

    /// Store an entry. Must be non-throwing; failures should be logged and silently dropped.
    virtual void setEntry(
        const QueryResultCache::Key & key,
        std::shared_ptr<QueryResultCache::Entry> entry) = 0;

    /// Return true if the cache already contains a non-stale entry for the given key.
    /// Used by the Writer constructor and finalizeWrite to avoid redundant writes.
    virtual bool hasNonStaleEntry(const QueryResultCache::Key & key) = 0;
};

/// Abstract interface for all query result cache implementations.
/// Concrete implementations: LocalQueryResultCache (in-process) and RemoteQueryResultCache (Redis).
class IQueryResultCache
{
public:
    using Key = QueryResultCache::Key;
    using Entry = QueryResultCache::Entry;

    virtual ~IQueryResultCache() = default;

    virtual QueryResultCacheReader createReader(const Key & key) = 0;
    virtual QueryResultCacheWriter createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota) = 0;

    virtual void clear(const std::optional<String> & tag) = 0;
    virtual size_t sizeInBytes() const = 0;
    virtual size_t count() const = 0;

    /// Record new execution of query represented by key. Returns number of executions so far.
    virtual size_t recordQueryRun(const Key & key) = 0;

    /// For debugging and system tables
    virtual std::vector<QueryResultCache::Cache::KeyMapped> dump() const = 0;
};

using QueryResultCachePtr = std::shared_ptr<IQueryResultCache>;

/// Maps queries to query results. Useful to avoid repeated query calculation.
///
/// The cache does not aim to be transactionally consistent (which is difficult to get right). For example, the cache is not invalidated
/// when data is inserted/deleted into/from tables referenced by queries in the cache. In such situations, incorrect results may be
/// returned. In order to still obtain sufficiently up-to-date query results, a expiry time (TTL) must be specified for each cache entry
/// after which it becomes stale and is ignored. Stale entries are removed opportunistically from the cache, they are only evicted when a
/// new entry is inserted and the cache has insufficient capacity.
class LocalQueryResultCache : public IQueryResultCache, private IQueryResultCacheStorage
{
public:
    using Cache = QueryResultCache::Cache;

    LocalQueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    QueryResultCacheReader createReader(const Key & key) override;
    QueryResultCacheWriter createWriter(
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

    /// For debugging and system tables
    std::vector<QueryResultCache::Cache::KeyMapped> dump() const override;

private:
    Cache cache; /// has its own locking --> not protected by mutex

    mutable std::mutex mutex;

    /// query --> query execution count
    using TimesExecuted = std::unordered_map<Key, size_t, QueryResultCache::KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    /// Cache configuration
    size_t max_entry_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t max_entry_size_in_rows TSA_GUARDED_BY(mutex) = 0;

    /// IQueryResultCacheStorage implementation
    void setEntry(const Key & key, std::shared_ptr<Entry> entry) override;
    bool hasNonStaleEntry(const Key & key) override;

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
    std::mutex mutex;
    IQueryResultCacheStorage & storage;
    const QueryResultCache::Key key;
    const size_t max_entry_size_in_bytes;
    const size_t max_entry_size_in_rows;
    const std::chrono::time_point<std::chrono::system_clock> query_start_time = std::chrono::system_clock::now(); /// Writer construction and finalizeWrite() coincide with query start/end
    const std::chrono::milliseconds min_query_runtime;
    const bool squash_partial_results;
    const size_t max_block_size;
    std::shared_ptr<QueryResultCache::Entry> query_result TSA_GUARDED_BY(mutex) = std::make_shared<QueryResultCache::Entry>();
    std::atomic<bool> skip_insert = false;
    bool was_finalized = false;
    LoggerPtr logger = getLogger("QueryResultCache");

    QueryResultCacheWriter(
        IQueryResultCacheStorage & storage_,
        const QueryResultCache::Key & key_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_);

    friend class LocalQueryResultCache; /// for createWriter()
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

    friend class LocalQueryResultCache; /// for createReader()
};

}
