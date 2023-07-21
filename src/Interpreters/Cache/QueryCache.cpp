#include "Interpreters/Cache/QueryCache.h"

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/formatAST.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Core/Settings.h>
#include <base/defines.h> /// chassert


namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
};

namespace DB
{

namespace
{

struct HasNonDeterministicFunctionsMatcher
{
    struct Data
    {
        const ContextPtr context;
        bool has_non_deterministic_functions = false;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.has_non_deterministic_functions)
            return;

        if (const auto * function = node->as<ASTFunction>())
        {
            const auto func = FunctionFactory::instance().tryGet(function->name, data.context);
            if (func && !func->isDeterministic())
                data.has_non_deterministic_functions = true;
        }
    }
};

using HasNonDeterministicFunctionsVisitor = InDepthNodeVisitor<HasNonDeterministicFunctionsMatcher, true>;

}

bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

namespace
{

class RemoveQueryCacheSettingsMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * set_clause = ast->as<ASTSetQuery>())
        {
            chassert(!set_clause->is_standalone);

            auto is_query_cache_related_setting = [](const auto & change)
            {
                return change.name.starts_with("query_cache_") || change.name.ends_with("_query_cache");
            };

            std::erase_if(set_clause->changes, is_query_cache_related_setting);
        }
    }

    /// TODO further improve AST cleanup, e.g. remove SETTINGS clause completely if it is empty
    /// E.g. SELECT 1 SETTINGS use_query_cache = true
    /// and  SELECT 1;
    /// currently don't match.
};

using RemoveQueryCacheSettingsVisitor = InDepthNodeVisitor<RemoveQueryCacheSettingsMatcher, true>;

/// Consider
///   (1) SET use_query_cache = true;
///       SELECT expensiveComputation(...) SETTINGS max_threads = 64, query_cache_ttl = 300;
///       SET use_query_cache = false;
/// and
///   (2) SELECT expensiveComputation(...) SETTINGS max_threads = 64, use_query_cache = true;
///
/// The SELECT queries in (1) and (2) are basically the same and the user expects that the second invocation is served from the query
/// cache. However, query results are indexed by their query ASTs and therefore no result will be found. Insert and retrieval behave overall
/// more natural if settings related to the query cache are erased from the AST key. Note that at this point the settings themselves
/// have been parsed already, they are not lost or discarded.
ASTPtr removeQueryCacheSettings(ASTPtr ast)
{
    ASTPtr transformed_ast = ast->clone();

    RemoveQueryCacheSettingsMatcher::Data visitor_data;
    RemoveQueryCacheSettingsVisitor(visitor_data).visit(transformed_ast);

    return transformed_ast;
}

String queryStringFromAST(ASTPtr ast)
{
    WriteBufferFromOwnString buf;
    formatAST(*ast, buf, /*hilite*/ false, /*one_line*/ true, /*show_secrets*/ false);
    return buf.str();
}

}

QueryCache::Key::Key(
    ASTPtr ast_,
    Block header_,
    const String & user_name_, bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_)
    : ast(removeQueryCacheSettings(ast_))
    , header(header_)
    , user_name(user_name_)
    , is_shared(is_shared_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_string(queryStringFromAST(ast_))
{
}

QueryCache::Key::Key(ASTPtr ast_, const String & user_name_)
    : QueryCache::Key(ast_, {}, user_name_, false, std::chrono::system_clock::from_time_t(1), false) /// dummy values for everything != AST or user name
{
}

bool QueryCache::Key::operator==(const Key & other) const
{
    return ast->getTreeHash() == other.ast->getTreeHash();
}

size_t QueryCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.ast->getTreeHash());
    auto res = hash.get64();
    return res;
}

size_t QueryCache::QueryCacheEntryWeight::operator()(const Entry & entry) const
{
    size_t res = 0;
    for (const auto & chunk : entry.chunks)
        res += chunk.allocatedBytes();
    res += entry.totals.has_value() ? entry.totals->allocatedBytes() : 0;
    res += entry.extremes.has_value() ? entry.extremes->allocatedBytes() : 0;
    return res;
}

bool QueryCache::IsStale::operator()(const Key & key) const
{
    return (key.expires_at < std::chrono::system_clock::now());
};

QueryCache::Writer::Writer(
    Cache & cache_, const Key & key_,
    size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_runtime_,
    bool squash_partial_results_,
    size_t max_block_size_)
    : cache(cache_)
    , key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
    , squash_partial_results(squash_partial_results_)
    , max_block_size(max_block_size_)
{
    if (auto entry = cache.getWithKey(key); entry.has_value() && !IsStale()(entry->key))
    {
        skip_insert = true; /// Key already contained in cache and did not expire yet --> don't replace it
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Skipped insert (non-stale entry found), query: {}", key.query_string);
    }
}

QueryCache::Writer::Writer(const Writer & other)
    : cache(other.cache)
    , key(other.key)
    , max_entry_size_in_bytes(other.max_entry_size_in_bytes)
    , max_entry_size_in_rows(other.max_entry_size_in_rows)
    , min_query_runtime(other.min_query_runtime)
    , squash_partial_results(other.squash_partial_results)
    , max_block_size(other.max_block_size)
{
}

void QueryCache::Writer::buffer(Chunk && chunk, ChunkType chunk_type)
{
    if (skip_insert)
        return;

    /// Reading from the query cache is implemented using processor `SourceFromChunks` which inherits from `ISource`.
    /// The latter has logic which finishes processing (= calls `.finish()` on the output port + returns `Status::Finished`)
    /// when the derived class returns an empty chunk. If this empty chunk is not the last chunk,
    /// i.e. if it is followed by non-empty chunks, the query result will be incorrect.
    /// This situation should theoretically never occur in practice but who knows...
    /// To be on the safe side, writing into the query cache now rejects empty chunks and thereby avoids this scenario.
    if (chunk.empty())
        return;

    std::lock_guard lock(mutex);

    switch (chunk_type)
    {
        case ChunkType::Result:
        {
            /// Normal query result chunks are simply buffered. They are squashed and compressed later in finalizeWrite().
            query_result->chunks.emplace_back(std::move(chunk));
            break;
        }
        case ChunkType::Totals:
        case ChunkType::Extremes:
        {
            /// For simplicity, totals and extremes chunks are immediately squashed (totals/extremes are obscure and even if enabled, few
            /// such chunks are expected).
            auto & buffered_chunk = (chunk_type == ChunkType::Totals) ? query_result->totals : query_result->extremes;

            convertToFullIfSparse(chunk);
            convertToFullIfConst(chunk);

            if (!buffered_chunk.has_value())
                buffered_chunk = std::move(chunk);
            else
                buffered_chunk->append(chunk);

            break;
        }
    }
}

void QueryCache::Writer::finalizeWrite()
{
    if (skip_insert)
        return;

    std::lock_guard lock(mutex);

    chassert(!was_finalized);

    /// Check some reasons why the entry must not be cached:

    if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - query_start_time) < min_query_runtime)
    {
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Skipped insert (query not expensive enough), query: {}", key.query_string);
        return;
    }

    if (auto entry = cache.getWithKey(key); entry.has_value() && !IsStale()(entry->key))
    {
        /// Same check as in ctor because a parallel Writer could have inserted the current key in the meantime
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Skipped insert (non-stale entry found), query: {}", key.query_string);
        return;
    }

    if (squash_partial_results)
    {
        /// Squash partial result chunks to chunks of size 'max_block_size' each. This costs some performance but provides a more natural
        /// compression of neither too small nor big blocks. Also, it will look like 'max_block_size' is respected when the query result is
        /// served later on from the query cache.

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
                auto compressed_column = column->compress();
                compressed_columns.push_back(compressed_column);
            }
            Chunk compressed_chunk(compressed_columns, chunk.getNumRows());
            compressed_chunks.push_back(std::move(compressed_chunk));
        }
        query_result->chunks = std::move(compressed_chunks);
    }

    /// Check more reasons why the entry must not be cached.

    auto count_rows_in_chunks = [](const Entry & entry)
    {
        size_t res = 0;
        for (const auto & chunk : entry.chunks)
            res += chunk.getNumRows();
        res += entry.totals.has_value() ? entry.totals->getNumRows() : 0;
        res += entry.extremes.has_value() ? entry.extremes->getNumRows() : 0;
        return res;
    };

    size_t new_entry_size_in_bytes = QueryCacheEntryWeight()(*query_result);
    size_t new_entry_size_in_rows = count_rows_in_chunks(*query_result);

    if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
    {
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Skipped insert (query result too big), new_entry_size_in_bytes: {} ({}), new_entry_size_in_rows: {} ({}), query: {}", new_entry_size_in_bytes, max_entry_size_in_bytes, new_entry_size_in_rows, max_entry_size_in_rows, key.query_string);
        return;
    }

    cache.set(key, query_result);

    was_finalized = true;
}

/// Creates a source processor which serves result chunks stored in the query cache, and separate sources for optional totals/extremes.
void QueryCache::Reader::buildSourceFromChunks(Block header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes)
{
    source_from_chunks = std::make_unique<SourceFromChunks>(header, std::move(chunks));

    if (totals.has_value())
    {
        Chunks chunks_totals;
        chunks_totals.emplace_back(totals->clone());
        source_from_chunks_totals = std::make_unique<SourceFromChunks>(header, std::move(chunks_totals));
    }

    if (extremes.has_value())
    {
        Chunks chunks_extremes;
        chunks_extremes.emplace_back(extremes->clone());
        source_from_chunks_extremes = std::make_unique<SourceFromChunks>(header, std::move(chunks_extremes));
    }
}

QueryCache::Reader::Reader(Cache & cache_, const Key & key, const std::lock_guard<std::mutex> &)
{
    auto entry = cache_.getWithKey(key);

    if (!entry.has_value())
    {
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "No entry found for query {}", key.query_string);
        return;
    }

    const auto & entry_key = entry->key;
    const auto & entry_mapped = entry->mapped;

    if (!entry_key.is_shared && entry_key.user_name != key.user_name)
    {
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Inaccessible entry found for query {}", key.query_string);
        return;
    }

    if (IsStale()(entry_key))
    {
        LOG_TRACE(&Poco::Logger::get("QueryCache"), "Stale entry found for query {}", key.query_string);
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

    LOG_TRACE(&Poco::Logger::get("QueryCache"), "Entry found for query {}", key.query_string);
}

bool QueryCache::Reader::hasCacheEntryForKey() const
{
    bool has_entry = (source_from_chunks != nullptr);

    if (has_entry)
        ProfileEvents::increment(ProfileEvents::QueryCacheHits);
    else
        ProfileEvents::increment(ProfileEvents::QueryCacheMisses);

    return has_entry;
}

std::unique_ptr<SourceFromChunks> QueryCache::Reader::getSource()
{
    return std::move(source_from_chunks);
}

std::unique_ptr<SourceFromChunks> QueryCache::Reader::getSourceTotals()
{
    return std::move(source_from_chunks_totals);
}

std::unique_ptr<SourceFromChunks> QueryCache::Reader::getSourceExtremes()
{
    return std::move(source_from_chunks_extremes);
}

QueryCache::Reader QueryCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return Reader(cache, key, lock);
}

QueryCache::Writer QueryCache::createWriter(const Key & key, std::chrono::milliseconds min_query_runtime, bool squash_partial_results, size_t max_block_size, size_t max_query_cache_size_in_bytes_quota, size_t max_query_cache_entries_quota)
{
    /// Update the per-user cache quotas with the values stored in the query context. This happens per query which writes into the query
    /// cache. Obviously, this is overkill but I could find the good place to hook into which is called when the settings profiles in
    /// users.xml change.
    cache.setQuotaForUser(key.user_name, max_query_cache_size_in_bytes_quota, max_query_cache_entries_quota);

    std::lock_guard lock(mutex);
    return Writer(cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

void QueryCache::reset()
{
    cache.reset();
    std::lock_guard lock(mutex);
    times_executed.clear();
    cache_size_in_bytes = 0;
}

size_t QueryCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    static constexpr size_t TIMES_EXECUTED_MAX_SIZE = 10'000;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

std::vector<QueryCache::Cache::KeyMapped> QueryCache::dump() const
{
    return cache.dump();
}

QueryCache::QueryCache()
    : cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, QueryCacheEntryWeight, IsStale>>(std::make_unique<PerUserTTLCachePolicyUserQuota>()))
{
}

void QueryCache::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);

    size_t max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", 1_GiB);
    cache.setMaxSize(max_size_in_bytes);

    size_t max_entries = config.getUInt64("query_cache.max_entries", 1024);
    cache.setMaxCount(max_entries);

    max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", 1_MiB);
    max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", 30'000'000);
}

}
