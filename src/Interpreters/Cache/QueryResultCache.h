#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Columns/IColumn.h>
#include <Interpreters/Cache/QueryResultCacheUsage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IASTHash.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <base/UUID.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>

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
        bool is_shared;

        /// When does the entry expire?
        std::chrono::time_point<std::chrono::system_clock> expires_at;

        /// Are the chunks in the entry compressed?
        /// (we could theoretically apply compression also to the totals and extremes but it's an obscure use case)
        bool is_compressed;

        /// The SELECT query as plain string, displayed in SYSTEM.QUERY_CACHE. Stored explicitly, i.e. not constructed from the AST, for the
        /// sole reason that QueryResultCache-related SETTINGS are pruned from the AST (see removeQueryResultCacheSettings()) which would otherwise look
        /// ugly in SYSTEM.QUERY_CACHE.
        String query_string;

        /// ID of the query.
        String query_id;

        /// A tag (namespace) for distinguish multiple entries of the same query.
        /// This member has currently no use besides that SYSTEM.QUERY_CACHE can populate the 'tag' column conveniently without having to
        /// compute the tag from the query AST.
        String tag;

        uint32_t ttl_seconds;
        /// Ctor to construct a Key for deserialization from buffer.
        Key() = default;
        /// Ctor to construct a Key for writing into query result cache.
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            SharedHeader header_,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            const uint32_t ttl_seconds,
            bool is_compressed);

        /// Ctor to construct a Key for reading from query result cache (this operation only needs the AST + user name).
        Key(ASTPtr ast_,
            const String & current_database,
            const Settings & settings,
            const String & query_id_,
            std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_);

        bool operator==(const Key & other) const;
        void serialize(WriteBuffer &) const;
        void deserialize(ReadBuffer &);
        String encodeTo() const;
    };

    struct Entry
    {
        Chunks chunks;
        std::optional<Chunk> totals = std::nullopt;
        std::optional<Chunk> extremes = std::nullopt;
        void serializeWithKey(const Key &, WriteBuffer &);
        void deserializeWithKey(Key &, ReadBuffer &);
    };
    using MappedPtr = std::shared_ptr<Entry>;

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

public:
    QueryResultCache();
    virtual ~QueryResultCache();

    virtual void updateConfiguration(const Poco::Util::AbstractConfiguration & config) = 0;

    virtual std::shared_ptr<QueryResultCacheReader> createReader(const Key & key) = 0;
    virtual std::shared_ptr<QueryResultCacheWriter> createWriter(
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
    struct KeyMapped
    {
        Key key;
        std::shared_ptr<Entry> mapped;
    };
    virtual std::vector<KeyMapped> dump() const = 0;
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
    explicit QueryResultCacheWriter(
        const QueryResultCache::Key & key_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        std::chrono::milliseconds min_query_runtime_,
        bool squash_partial_results_,
        size_t max_block_size_);
    QueryResultCacheWriter(const QueryResultCacheWriter & other);
    virtual ~QueryResultCacheWriter() {}

    enum class ChunkType : uint8_t
    {
        Result,
        Totals,
        Extremes
    };
    void buffer(Chunk && chunk, ChunkType chunk_type);

    virtual void finalizeWrite() = 0;
protected:
    template<typename Cache, typename IsStale>
    void initializeWriter(Cache & cache, const QueryResultCache::Key & key_)
    {
        if (auto entry = cache.getWithKey(key_); entry.has_value() && !IsStale()(entry->key))
        {
            skip_insert = true; /// Key already contained in cache and did not expire yet --> don't replace it
            LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
        }
    }

    template<typename Cache, typename IsStale>
    void finalizeWriteImpl(Cache & cache)
    {
        if (skip_insert)
            return;

        std::lock_guard lock(mutex);

        chassert(!was_finalized);

        /// Check some reasons why the entry must not be cached:

        if (auto query_runtime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - query_start_time); query_runtime < min_query_runtime)
        {
            LOG_TRACE(logger, "Skipped insert because the query is not expensive enough, query runtime: {} msec (minimum query runtime: {} msec), query: {}",
                      query_runtime.count(), min_query_runtime.count(), doubleQuoteString(key.query_string));
            return;
        }

        if (auto entry = cache.getWithKey(key); entry.has_value() && !IsStale()(entry->key))
        {
            /// Same check as in ctor because a parallel Writer could have inserted the current key in the meantime
            LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
            return;
        }

        if (squash_partial_results)
        {
            /// Squash partial result chunks to chunks of size 'max_block_size' each. This costs some performance but provides a more natural
            /// compression of neither too small nor big blocks. Also, it will look like 'max_block_size' is respected when the query result is
            /// served later on from the query result cache.

            Chunks squashed_chunks;
            size_t rows_remaining_in_squashed = 0; /// how many further rows can the last squashed chunk consume until it reaches max_block_size

            for (auto & chunk : query_result->chunks)
            {
                convertToFullIfSparse(chunk);
                convertToFullIfConst(chunk);

                const size_t rows_chunk = chunk.getNumRows();
                if (rows_chunk == 0)
                    continue;

                size_t rows_chunk_processed = 0;
                while (true)
                {
                    if (rows_remaining_in_squashed == 0)
                    {
                        Chunk empty_chunk = Chunk(chunk.cloneEmptyColumns(), 0);
                        squashed_chunks.push_back(std::move(empty_chunk));
                        rows_remaining_in_squashed = max_block_size;
                    }

                    const size_t rows_to_append = std::min(rows_chunk - rows_chunk_processed, rows_remaining_in_squashed);
                    squashed_chunks.back().append(chunk, rows_chunk_processed, rows_to_append);
                    rows_chunk_processed += rows_to_append;
                    rows_remaining_in_squashed -= rows_to_append;

                    if (rows_chunk_processed == rows_chunk)
                        break;
                }
            }

            query_result->chunks = std::move(squashed_chunks);
        }

        if (key.is_compressed)
        {
            /// Compress result chunks. Reduces the space consumption of the cache but means reading from it will be slower due to decompression.

            Chunks compressed_chunks;

            for (const auto & chunk : query_result->chunks)
            {
                const Columns & columns = chunk.getColumns();
                Columns compressed_columns;
                for (const auto & column : columns)
                {
                    auto compressed_column = column->compress(/*force_compression=*/false);
                    compressed_columns.push_back(compressed_column);
                }
                Chunk compressed_chunk(compressed_columns, chunk.getNumRows());
                compressed_chunks.push_back(std::move(compressed_chunk));
            }
            query_result->chunks = std::move(compressed_chunks);
        }

        /// Check more reasons why the entry must not be cached.

        auto count_rows_in_chunks = [](const QueryResultCache::Entry & entry)
        {
            size_t res = 0;
            for (const auto & chunk : entry.chunks)
                res += chunk.getNumRows();
            res += entry.totals.has_value() ? entry.totals->getNumRows() : 0;
            res += entry.extremes.has_value() ? entry.extremes->getNumRows() : 0;
            return res;
        };

        size_t new_entry_size_in_bytes = QueryResultCache::QueryResultCacheEntryWeight()(*query_result);
        size_t new_entry_size_in_rows = count_rows_in_chunks(*query_result);

        if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
        {
            LOG_TRACE(logger, "Skipped insert because the query result is too big, query result size: {} (maximum size: {}), query result size in rows: {} (maximum size: {}), query: {}",
                      formatReadableSizeWithBinarySuffix(new_entry_size_in_bytes, 0), formatReadableSizeWithBinarySuffix(max_entry_size_in_bytes, 0), new_entry_size_in_rows, max_entry_size_in_rows, doubleQuoteString(key.query_string));
            return;
        }

        cache.set(key, query_result);

        LOG_TRACE(logger, "Stored query result of query {}", doubleQuoteString(key.query_string));

        was_finalized = true;
    }
protected:
    std::mutex mutex;
    const QueryResultCache::Key key;
    const size_t max_entry_size_in_bytes;
    const size_t max_entry_size_in_rows;
    const std::chrono::time_point<std::chrono::system_clock> query_start_time = std::chrono::system_clock::now(); /// Writer construction and finalizeWrite() coincide with query start/end
    const std::chrono::milliseconds min_query_runtime;
    const bool squash_partial_results;
    const size_t max_block_size;
    QueryResultCache::MappedPtr query_result TSA_GUARDED_BY(mutex) = std::make_shared<QueryResultCache::Entry>();
    std::atomic<bool> skip_insert = false;
    bool was_finalized = false;
    LoggerPtr logger = getLogger("QueryResultCache");
};

/// Reader's constructor looks up a query result for a key in the cache. If found, it constructs source processors (that generate the
/// cached result) for use in a pipe or query pipeline.
class QueryResultCacheReader
{
public:
    QueryResultCacheReader() = default;
    virtual ~QueryResultCacheReader() = default;
    bool hasCacheEntryForKey() const;
    /// getSource*() moves source processors out of the Reader. Call each of these method just once.
    std::unique_ptr<SourceFromChunks> getSource();
    std::unique_ptr<SourceFromChunks> getSourceTotals();
    std::unique_ptr<SourceFromChunks> getSourceExtremes();
protected:
    template<typename Cache, typename IsStale>
    void initializeReader(Cache & cache_, const QueryResultCache::Key key_)
    {
        auto entry = cache_.getWithKey(key_);

<<<<<<< HEAD
    QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &);
    void buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);
=======
        if (!entry.has_value())
        {
            LOG_TRACE(logger, "No query result found for query {}", doubleQuoteString(key_.query_string));
            return;
        }

        const auto & entry_key = entry->key;
        const auto & entry_mapped = entry->mapped;

        const bool is_same_user_id = ((!entry_key.user_id.has_value() && !key_.user_id.has_value()) || (entry_key.user_id.has_value() && key_.user_id.has_value() && *entry_key.user_id == *key_.user_id));
        const bool is_same_current_user_roles = (entry_key.current_user_roles == key_.current_user_roles);
        if (!entry_key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
        {
            LOG_TRACE(logger, "Inaccessible query result found for query {}", doubleQuoteString(key_.query_string));
            return;
        }

        if (IsStale()(entry_key))
        {
            LOG_TRACE(logger, "Stale query result found for query {}", doubleQuoteString(key_.query_string));
            return;
        }

        if (!entry_key.is_compressed)
        {
            // Cloning chunks isn't exactly great. It could be avoided by another indirection, i.e. wrapping Entry's members chunks, totals and
            // extremes into shared_ptrs and assuming that the lifecycle of these shared_ptrs coincides with the lifecycle of the Entry
            // shared_ptr. This is not done 1. to keep things simple 2. this case (uncompressed chunks) is the exceptional case, in the other
            // case (the default case aka. compressed chunks) we need to decompress the entry anyways and couldn't apply the potential
            // optimization.

            Chunks cloned_chunks;
            for (const auto & chunk : entry_mapped->chunks)
                cloned_chunks.push_back(chunk.clone());

            buildSourceFromChunks(entry_key.header, std::move(cloned_chunks), entry_mapped->totals, entry_mapped->extremes);
        }
        else
        {
            Chunks decompressed_chunks;
            const Chunks & chunks = entry_mapped->chunks;
            for (const auto & chunk : chunks)
            {
                const Columns & columns = chunk.getColumns();
                Columns decompressed_columns;
                for (const auto & column : columns)
                {
                    auto decompressed_column = column->decompress();
                    decompressed_columns.push_back(decompressed_column);
                }
                Chunk decompressed_chunk(decompressed_columns, chunk.getNumRows());
                decompressed_chunks.push_back(std::move(decompressed_chunk));
            }

            buildSourceFromChunks(entry_key.header, std::move(decompressed_chunks), entry_mapped->totals, entry_mapped->extremes);
        }

        LOG_TRACE(logger, "Query result found for query {}", doubleQuoteString(key_.query_string));
    }
    void buildSourceFromChunks(Block header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes);
protected:
    std::unique_ptr<SourceFromChunks> source_from_chunks;
    std::unique_ptr<SourceFromChunks> source_from_chunks_totals;
    std::unique_ptr<SourceFromChunks> source_from_chunks_extremes;
    LoggerPtr logger = getLogger("QueryResultCache");
};

using QueryResultCachePtr = std::shared_ptr<QueryResultCache>;

}
