#include "Interpreters/Cache/QueryCache.h"

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <base/defines.h> /// chassert


namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
};

namespace DB
{
namespace Setting
{
    extern const SettingsString query_cache_tag;
}

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

struct HasSystemTablesMatcher
{
    struct Data
    {
        const ContextPtr context;
        bool has_system_tables = false;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.has_system_tables)
            return;

        String database_table; /// or whatever else we get, e.g. just a table

        /// SELECT [...] FROM <table>
        if (const auto * table_identifier = node->as<ASTTableIdentifier>())
        {
            database_table = table_identifier->name();
        }
        /// SELECT [...] FROM clusterAllReplicas(<cluster>, <table>)
        else if (const auto * identifier = node->as<ASTIdentifier>())
        {
            database_table = identifier->name();
        }
        /// SELECT [...] FROM clusterAllReplicas(<cluster>, '<table>')
        /// This SQL syntax is quite common but we need to be careful. A naive attempt to cast 'node' to an ASTLiteral will be too general
        /// and introduce false positives in queries like
        ///     'SELECT * FROM users WHERE name = 'system.metrics' SETTINGS use_query_cache = true;'
        /// Therefore, make sure we are really in `clusterAllReplicas`. EXPLAIN AST for
        ///     'SELECT * FROM clusterAllReplicas('default', system.one) SETTINGS use_query_cache = 1'
        /// returns:
        ///     [...]
        ///     Function clusterAllReplicas (children 1)
        ///       ExpressionList (children 2)
        ///         Literal 'test_shard_localhost'
        ///         Literal 'system.one'
        ///     [...]
        else if (const auto * function = node->as<ASTFunction>())
        {
            if (function->name == "clusterAllReplicas")
            {
                const ASTs & function_children = function->children;
                if (!function_children.empty())
                {
                    if (const auto * expression_list = function_children[0]->as<ASTExpressionList>())
                    {
                        const ASTs & expression_list_children = expression_list->children;
                        if (expression_list_children.size() >= 2)
                        {
                            if (const auto * literal = expression_list_children[1]->as<ASTLiteral>())
                            {
                                const auto & value = literal->value;
                                database_table = toString(value);
                            }
                        }
                    }
                }
            }
        }

        Tokens tokens(database_table.c_str(), database_table.c_str() + database_table.size(), /*max_query_size*/ 2048, /*skip_insignificant*/ true);
        IParser::Pos pos(tokens, /*max_depth*/ 42, /*max_backtracks*/ 42);
        Expected expected;
        String database;
        String table;
        bool successfully_parsed = parseDatabaseAndTableName(pos, expected, database, table);
        if (successfully_parsed)
            if (DatabaseCatalog::isPredefinedDatabase(database))
                data.has_system_tables = true;
    }
};

using HasNonDeterministicFunctionsVisitor = InDepthNodeVisitor<HasNonDeterministicFunctionsMatcher, true>;
using HasSystemTablesVisitor = InDepthNodeVisitor<HasSystemTablesMatcher, true>;

}

bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

bool astContainsSystemTables(ASTPtr ast, ContextPtr context)
{
    HasSystemTablesMatcher::Data finder_data{context};
    HasSystemTablesVisitor(finder_data).visit(ast);
    return finder_data.has_system_tables;
}

namespace
{

bool isQueryCacheRelatedSetting(const String & setting_name)
{
    return (setting_name.starts_with("query_cache_") || setting_name.ends_with("_query_cache")) && setting_name != "query_cache_tag";
}

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
                return isQueryCacheRelatedSetting(change.name);
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

IAST::Hash calculateAstHash(ASTPtr ast, const String & current_database, const Settings & settings)
{
    ast = removeQueryCacheSettings(ast);

    /// Hash the AST, we must consider aliases (issue #56258)
    SipHash hash;
    ast->updateTreeHash(hash, /*ignore_aliases=*/ false);

    /// Also hash the database specified via SQL `USE db`, otherwise identifiers in same query (AST) may mean different columns in different
    /// tables (issue #64136)
    hash.update(current_database);

    /// Finally, hash the (changed) settings as they might affect the query result (e.g. think of settings `additional_table_filters` and `limit`).
    /// Note: allChanged() returns the settings in random order. Also, update()-s of the composite hash must be done in deterministic order.
    ///       Therefore, collect and sort the settings first, then hash them.
    auto changed_settings = settings.changes();
    std::vector<std::pair<String, String>> changed_settings_sorted; /// (name, value)
    for (const auto & change : changed_settings)
    {
        const String & name = change.name;
        if (!isQueryCacheRelatedSetting(name)) /// see removeQueryCacheSettings() why this is a good idea
            changed_settings_sorted.push_back({name, Settings::valueToStringUtil(change.name, change.value)});
    }
    std::sort(changed_settings_sorted.begin(), changed_settings_sorted.end(), [](auto & lhs, auto & rhs) { return lhs.first < rhs.first; });
    for (const auto & setting : changed_settings_sorted)
    {
        hash.update(setting.first);
        hash.update(setting.second);
    }

    return getSipHash128AsPair(hash);
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
    const String & current_database,
    const Settings & settings,
    Block header_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_)
    : ast_hash(calculateAstHash(ast_, current_database, settings))
    , header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_string(queryStringFromAST(ast_))
    , tag(settings[Setting::query_cache_tag])
{
}

QueryCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_)
    : QueryCache::Key(ast_, current_database, settings, {}, user_id_, current_user_roles_, false, std::chrono::system_clock::from_time_t(1), false)
    /// ^^ dummy values for everything != AST, current database, user name/roles
{
}

bool QueryCache::Key::operator==(const Key & other) const
{
    return ast_hash == other.ast_hash;
}

size_t QueryCache::KeyHasher::operator()(const Key & key) const
{
    return key.ast_hash.low64;
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
        LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
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
        LOG_TRACE(logger, "Skipped insert because the query result is too big, query result size: {} (maximum size: {}), query result size in rows: {} (maximum size: {}), query: {}",
                formatReadableSizeWithBinarySuffix(new_entry_size_in_bytes, 0), formatReadableSizeWithBinarySuffix(max_entry_size_in_bytes, 0), new_entry_size_in_rows, max_entry_size_in_rows, doubleQuoteString(key.query_string));
        return;
    }

    cache.set(key, query_result);

    LOG_TRACE(logger, "Stored query result of query {}", doubleQuoteString(key.query_string));

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
        LOG_TRACE(logger, "No query result found for query {}", doubleQuoteString(key.query_string));
        return;
    }

    const auto & entry_key = entry->key;
    const auto & entry_mapped = entry->mapped;

    const bool is_same_user_id = ((!entry_key.user_id.has_value() && !key.user_id.has_value()) || (entry_key.user_id.has_value() && key.user_id.has_value() && *entry_key.user_id == *key.user_id));
    const bool is_same_current_user_roles = (entry_key.current_user_roles == key.current_user_roles);
    if (!entry_key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
    {
        LOG_TRACE(logger, "Inaccessible query result found for query {}", doubleQuoteString(key.query_string));
        return;
    }

    if (IsStale()(entry_key))
    {
        LOG_TRACE(logger, "Stale query result found for query {}", doubleQuoteString(key.query_string));
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

    LOG_TRACE(logger, "Query result found for query {}", doubleQuoteString(key.query_string));
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

QueryCache::QueryCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, QueryCacheEntryWeight, IsStale>>(std::make_unique<PerUserTTLCachePolicyUserQuota>()))
{
    updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_);
}

void QueryCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache.setMaxSizeInBytes(max_size_in_bytes);
    cache.setMaxCount(max_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
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
    /// user_id == std::nullopt is the internal user for which no quota can be configured
    if (key.user_id.has_value())
        cache.setQuotaForUser(*key.user_id, max_query_cache_size_in_bytes_quota, max_query_cache_entries_quota);

    std::lock_guard lock(mutex);
    return Writer(cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

void QueryCache::clear(const std::optional<String> & tag)
{
    if (tag)
    {
        auto predicate = [tag](const Key & key, const Cache::MappedPtr &) { return key.tag == tag.value(); };
        cache.remove(predicate);
    }
    else
    {
        cache.clear();
    }

    std::lock_guard lock(mutex);
    times_executed.clear();
}

size_t QueryCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t QueryCache::count() const
{
    return cache.count();
}

size_t QueryCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    static constexpr auto TIMES_EXECUTED_MAX_SIZE = 10'000uz;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

std::vector<QueryCache::Cache::KeyMapped> QueryCache::dump() const
{
    return cache.dump();
}

}
