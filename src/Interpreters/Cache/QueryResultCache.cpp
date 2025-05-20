#include "Interpreters/Cache/QueryResultCache.h"

#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
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
#include <Parsers/parseDatabaseAndTableName.h>
#include <Columns/IColumn.h>
#include "Common/LRUCachePolicy.h"
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include "Processors/Chunk.h"
#include "Processors/Formats/Impl/TemplateBlockOutputFormat.h"
#include <Core/Settings.h>
#include <base/defines.h> /// chassert

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>

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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
            if (const auto func = FunctionFactory::instance().tryGet(function->name, data.context))
            {
                if (!func->isDeterministic())
                    data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto udf_sql = UserDefinedSQLFunctionFactory::instance().tryGet(function->name))
            {
                /// ClickHouse currently doesn't know if SQL-based UDFs are deterministic or not. We must assume they are non-deterministic.
                data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto udf_executable = UserDefinedExecutableFunctionFactory::tryGet(function->name, data.context))
            {
                if (!udf_executable->isDeterministic())
                    data.has_non_deterministic_functions = true;
                return;
            }
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

bool isQueryResultCacheRelatedSetting(const String & setting_name)
{
    return (setting_name.starts_with("query_cache_") || setting_name.ends_with("_query_cache")) && setting_name != "query_cache_tag";
}

class RemoveQueryResultCacheSettingsMatcher
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
                return isQueryResultCacheRelatedSetting(change.name);
            };

            std::erase_if(set_clause->changes, is_query_cache_related_setting);
        }
    }

    /// TODO further improve AST cleanup, e.g. remove SETTINGS clause completely if it is empty
    /// E.g. SELECT 1 SETTINGS use_query_cache = true
    /// and  SELECT 1;
    /// currently don't match.
};

using RemoveQueryResultCacheSettingsVisitor = InDepthNodeVisitor<RemoveQueryResultCacheSettingsMatcher, true>;

/// Consider
///   (1) SET use_query_cache = true;
///       SELECT expensiveComputation(...) SETTINGS max_threads = 64, query_cache_ttl = 300;
///       SET use_query_cache = false;
/// and
///   (2) SELECT expensiveComputation(...) SETTINGS max_threads = 64, use_query_cache = true;
///
/// The SELECT queries in (1) and (2) are basically the same and the user expects that the second invocation is served from the query
/// cache. However, query results are indexed by their query ASTs and therefore no result will be found. Insert and retrieval behave overall
/// more natural if settings related to the query result cache are erased from the AST key. Note that at this point the settings themselves
/// have been parsed already, they are not lost or discarded.
ASTPtr removeQueryResultCacheSettings(ASTPtr ast)
{
    ASTPtr transformed_ast = ast->clone();

    RemoveQueryResultCacheSettingsMatcher::Data visitor_data;
    RemoveQueryResultCacheSettingsVisitor(visitor_data).visit(transformed_ast);

    return transformed_ast;
}

IASTHash calculateAstHash(ASTPtr ast, const String & current_database, const Settings & settings)
{
    ast = removeQueryResultCacheSettings(ast);

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
        if (!isQueryResultCacheRelatedSetting(name)) /// see removeQueryResultCacheSettings() why this is a good idea
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
    return ast->formatForLogging();
}

}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    Block header_,
    const String & query_id_,
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
    , query_id(query_id_)
    , tag(settings[Setting::query_cache_tag])
{
}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_)
    : QueryResultCache::Key(ast_, current_database, settings, {}, query_id_, user_id_, current_user_roles_, false, std::chrono::system_clock::from_time_t(1), false)
    /// ^^ dummy values for everything except AST, current database, query_id, user name/roles
{
}

QueryResultCache::Key::Key(IASTHash ast_hash_,
    const Block & header_,
    std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
    bool is_shared_, 
    const std::chrono::time_point<std::chrono::system_clock> & expires_at_,
    bool is_compressed_,
    const String & query_string_,
    const String & query_id_,
    const String & tag_)
    : ast_hash(ast_hash_)
    , header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_string(query_string_)
    , query_id(query_id_)
    , tag(tag_)
{
}

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast_hash == other.ast_hash;
}

size_t QueryResultCache::KeyHasher::operator()(const Key & key) const
{
    return key.ast_hash.low64;
}

size_t QueryResultCache::QueryResultCacheEntryWeight::operator()(const Entry & entry) const
{
    size_t res = 0;
    for (const auto & chunk : entry.chunks)
        res += chunk.allocatedBytes();
    res += entry.totals.has_value() ? entry.totals->allocatedBytes() : 0;
    res += entry.extremes.has_value() ? entry.extremes->allocatedBytes() : 0;
    return res;
}

bool QueryResultCache::IsStale::operator()(const Key & key) const
{
    return (key.expires_at < std::chrono::system_clock::now());
};

QueryResultCacheWriter::QueryResultCacheWriter(
    Cache & cache_,
    std::optional<OnDiskCache> & disk_cache_,
    const QueryResultCache::Key & key_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_runtime_,
    bool squash_partial_results_,
    size_t max_block_size_)
    : cache(cache_)
    , disk_cache(disk_cache_)
    , key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
    , squash_partial_results(squash_partial_results_)
    , max_block_size(max_block_size_)
{
    if (auto entry = cache.getWithKey(key); entry.has_value() && !QueryResultCache::IsStale()(entry->key))
    {
        skip_insert = true; /// Key already contained in cache and did not expire yet --> don't replace it
        LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
    }
}

QueryResultCacheWriter::QueryResultCacheWriter(const QueryResultCacheWriter & other)
    : cache(other.cache)
    , disk_cache(other.disk_cache)
    , key(other.key)
    , max_entry_size_in_bytes(other.max_entry_size_in_bytes)
    , max_entry_size_in_rows(other.max_entry_size_in_rows)
    , min_query_runtime(other.min_query_runtime)
    , squash_partial_results(other.squash_partial_results)
    , max_block_size(other.max_block_size)
{
}

void QueryResultCacheWriter::buffer(Chunk && chunk, ChunkType chunk_type)
{
    if (skip_insert)
        return;

    /// Reading from the query result cache is implemented using processor `SourceFromChunks` which inherits from `ISource`. The latter has
    /// logic which finishes processing (= calls `.finish()` on the output port + returns `Status::Finished`) when the derived class returns
    /// an empty chunk. If this empty chunk is not the last chunk, i.e. if it is followed by non-empty chunks, the query result will be
    /// incorrect. This situation should theoretically never occur in practice but who knows... To be on the safe side, writing into the
    /// query result cache now rejects empty chunks and thereby avoids this scenario.
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

namespace
{

/// Combine N (usually small) chunks to M chunks with max_chunk_size rows each.
/// The input chunks are non-const to save unnecessary copies for convertToFullIfSparse and convertToFullIfConst.
Chunks squashChunks(Chunks & chunks, size_t max_chunk_size)
{
    Chunks squashed_chunks;
    size_t rows_remaining_in_squashed = 0; /// how many further rows can the last squashed chunk consume until it reaches max_block_size

    for (auto & chunk : chunks)
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
                rows_remaining_in_squashed = max_chunk_size;
            }

            const size_t rows_to_append = std::min(rows_chunk - rows_chunk_processed, rows_remaining_in_squashed);
            squashed_chunks.back().append(chunk, rows_chunk_processed, rows_to_append);
            rows_chunk_processed += rows_to_append;
            rows_remaining_in_squashed -= rows_to_append;

            if (rows_chunk_processed == rows_chunk)
                break;
        }
    }

    return squashed_chunks;
}

Chunks compressChunks(Chunks & chunks)
{
    Chunks compressed_chunks;

    for (const auto & chunk : chunks)
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

    return compressed_chunks;
}

size_t countRowsInChunks(const QueryResultCache::Entry & entry)
{
    size_t res = 0;
    for (const auto & chunk : entry.chunks)
        res += chunk.getNumRows();

    res += entry.totals.has_value() ? entry.totals->getNumRows() : 0;
    res += entry.extremes.has_value() ? entry.extremes->getNumRows() : 0;

    return res;
}

QueryResultCache::Cache::MappedPtr cloneQueryResult(const QueryResultCache::Cache::MappedPtr & entry)
{
    auto result = std::make_shared<QueryResultCache::Entry>();
    for (const auto& chunk : entry->chunks) 
        result->chunks.push_back(chunk.clone());
    
    if (entry->extremes)
        result->extremes = entry->extremes->clone();

    if (entry->totals) 
        result->totals = entry->totals->clone();
    return result;
}
}

void QueryResultCacheWriter::finalizeWrite()
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

    if (auto entry = cache.getWithKey(key); entry.has_value() && !QueryResultCache::IsStale()(entry->key))
    {
        /// Same check as in ctor because a parallel Writer could have inserted the current key in the meantime
        LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
        return;
    }

    /// Write result to disk cache
    if (disk_cache)
        disk_cache->set(key, cloneQueryResult(query_result));


    if (squash_partial_results)
    {
        /// Squash partial result chunks to chunks of size 'max_block_size' each. This costs some performance but provides a more natural
        /// compression of neither too small nor big blocks. Also, it will look like 'max_block_size' is respected when the query result is
        /// served later on from the query result cache.
        Chunks squashed_chunks = squashChunks(query_result->chunks, max_block_size);
        query_result->chunks = std::move(squashed_chunks);
    }

    /// Need to keep uncompressed chunks for query result cache on disk,
    /// because it can squash it with different max_chunks_size,
    /// but can't squash already compressed chunks

    if (key.is_compressed)
    {
        /// Compress result chunks. Reduces the space consumption of the cache but means reading from it will be slower due to decompression.
        Chunks compressed_chunks = compressChunks(query_result->chunks);
        query_result->chunks = std::move(compressed_chunks);
    }

    /// Check more reasons why the entry must not be cached.

    size_t new_entry_size_in_bytes = QueryResultCache::QueryResultCacheEntryWeight()(*query_result);
    size_t new_entry_size_in_rows = countRowsInChunks(*query_result);

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

/// Creates a source processor which serves result chunks stored in the query result cache, and separate sources for optional totals/extremes.
void QueryResultCacheReader::buildSourceFromChunks(Block header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes)
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

QueryResultCacheReader::QueryResultCacheReader(Cache & cache_, std::optional<OnDiskCache> & disk_cache_, const Cache::Key & key, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows, const std::lock_guard<std::mutex> &)
{
    auto entry = cache_.getWithKey(key);

    if (!entry.has_value())
    {
        if (disk_cache_)
        { /// If disk cache enabled, try to find entry there
            auto disk_entry = disk_cache_->getWithKey(key);
            if (disk_entry)
            {
                LOG_TRACE(logger, "Query result found in disk cache for query {}", doubleQuoteString(key.query_string));
                entry.emplace(std::move(disk_entry.value()));

                /// Should check constraints for in-memory cache, before adding entry
                size_t entry_size_in_bytes = QueryResultCache::QueryResultCacheEntryWeight()(*entry->mapped);
                size_t entry_size_in_rows = countRowsInChunks(*entry->mapped);

                if (entry_size_in_bytes < max_entry_size_in_bytes && entry_size_in_rows < max_entry_size_in_rows && !QueryResultCache::IsStale()(entry->key))
                {
                    cache_.set(entry->key, entry->mapped); /// Add entry to in-memory cache   
                }
            }
            else
            {
                LOG_TRACE(logger, "No query result found for query {}", doubleQuoteString(key.query_string));
                return;
            }
        }
        else
        {
            LOG_TRACE(logger, "No disk cache and no query result found for query {}", doubleQuoteString(key.query_string));
            return;
        }
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

    if (QueryResultCache::IsStale()(entry_key))
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

bool QueryResultCacheReader::hasCacheEntryForKey() const
{
    bool has_entry = (source_from_chunks != nullptr);

    if (has_entry)
        ProfileEvents::increment(ProfileEvents::QueryCacheHits);
    else
        ProfileEvents::increment(ProfileEvents::QueryCacheMisses);

    return has_entry;
}

std::unique_ptr<SourceFromChunks> QueryResultCacheReader::getSource()
{
    return std::move(source_from_chunks);
}

std::unique_ptr<SourceFromChunks> QueryResultCacheReader::getSourceTotals()
{
    return std::move(source_from_chunks_totals);
}

std::unique_ptr<SourceFromChunks> QueryResultCacheReader::getSourceExtremes()
{
    return std::move(source_from_chunks_extremes);
}

QueryResultCache::QueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_, size_t disk_cache_max_size_in_bytes, size_t disk_cache_max_entries, size_t disk_cache_max_entry_size_in_bytes_, size_t disk_cache_max_entry_size_in_rows_, const std::optional<std::filesystem::path> & disk_cache_path_)
    : cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, QueryResultCacheEntryWeight, IsStale>>(
          std::make_unique<PerUserTTLCachePolicyUserQuota>()))
{
    if (disk_cache_path_)
        disk_cache.emplace(disk_cache_path_.value(), disk_cache_max_size_in_bytes, disk_cache_max_entries, disk_cache_max_entry_size_in_bytes_, disk_cache_max_entry_size_in_rows_);
    
    updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_, disk_cache_max_size_in_bytes, disk_cache_max_entries, disk_cache_max_entry_size_in_bytes_, disk_cache_max_entry_size_in_rows_);
}

void QueryResultCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_, size_t disk_cache_max_size_in_bytes, size_t disk_cache_max_entries, size_t disk_cache_max_entry_size_in_bytes_, size_t disk_cache_max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache.setMaxSizeInBytes(max_size_in_bytes);
    cache.setMaxCount(max_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;

    if (disk_cache)
    {
        disk_cache->setMaxSizeInBytes(disk_cache_max_size_in_bytes);
        disk_cache->setMaxCount(disk_cache_max_entries);

        disk_cache_max_entry_size_in_bytes = disk_cache_max_entry_size_in_bytes_;
        disk_cache_max_entry_size_in_rows = disk_cache_max_entry_size_in_rows_;
    }
}

QueryResultCacheReader QueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return QueryResultCacheReader(cache, disk_cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, lock);
}

QueryResultCacheWriter QueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t max_query_result_cache_size_in_bytes_quota,
    size_t max_query_result_cache_entries_quota)
{
    /// Update the per-user cache quotas with the values stored in the query context. This happens per query which writes into the query
    /// cache. Obviously, this is overkill but I could find the good place to hook into which is called when the settings profiles in
    /// users.xml change.
    /// user_id == std::nullopt is the internal user for which no quota can be configured
    if (key.user_id.has_value())
        cache.setQuotaForUser(*key.user_id, max_query_result_cache_size_in_bytes_quota, max_query_result_cache_entries_quota);

    std::lock_guard lock(mutex);
    return QueryResultCacheWriter(cache, disk_cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

void QueryResultCache::clear(const std::optional<String> & tag)
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

size_t QueryResultCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t QueryResultCache::count() const
{
    return cache.count();
}

size_t QueryResultCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    static constexpr auto TIMES_EXECUTED_MAX_SIZE = 10'000uz;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

std::vector<QueryResultCache::Cache::KeyMapped> QueryResultCache::dump() const
{
    return cache.dump();
}

namespace FormatTokens {
    static constexpr std::string_view format_version_txt = "format_version.txt";
    static constexpr uint32_t current_version = 1;

    static constexpr auto * token_user_id = "user_id: ";
    static constexpr auto * token_current_user_roles = "current_user_roles: ";
    static constexpr auto * token_is_shared = "is_shared: ";
    static constexpr auto * token_expires_at = "expires_at: ";
    static constexpr auto * token_is_compressed = "is_compressed: ";
    static constexpr auto * token_query_string = "query_string: ";
    static constexpr auto * token_has_totals = "has_totals: ";
    static constexpr auto * token_has_extremes = "has_extremes: ";
};

QueryResultCache::OnDiskCache::OnDiskCache(const std::filesystem::path& path_, size_t max_size_in_bytes_, size_t max_entries_, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : query_cache_path(path_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
{
    auto on_weight_loss_function = [](size_t){};
    auto on_evict_function = [&](CachePolicy::MappedPtr mapped) { onEvictFunction(mapped); };

    cache_policy = std::make_unique<LRUCachePolicy<Key, DiskEntryMetadata, KeyHasher, DiskEntryWeight>>(max_size_in_bytes_, max_entries_, on_weight_loss_function, on_evict_function);
    
    try
    {
        checkFormatVersion();
        readCacheEntriesMetaData();
    }
    catch (...)
    {
        // Remove old cache files and create new format_version.txt
        namespace fs = std::filesystem;

        fs::remove_all(query_cache_path);
        fs::create_directory(query_cache_path);

        fs::path format_version_path = query_cache_path / FormatTokens::format_version_txt;
        WriteBufferFromFile format_version_file(format_version_path);
        writeIntText(FormatTokens::current_version, format_version_file);
        format_version_file.finalize();
    }
}

void QueryResultCache::OnDiskCache::readCacheEntriesMetaData()
{
    std::lock_guard lock(mutex);

    try
    {
        namespace fs = std::filesystem;

        fs::path format_version_path = query_cache_path / FormatTokens::format_version_txt;
        ReadBufferFromFile format_version_file(format_version_path); /// throws if file can't be opened
        uint32_t version;

        readIntText(version, format_version_file);
        if (version != FormatTokens::current_version)
            return;

        for (const auto & entry_file_it : fs::directory_iterator(query_cache_path))
        {
            const fs::path & entry_path = entry_file_it.path();

            if (entry_path == format_version_path)
                continue; /// ignore format_version.txt

            String ast_hash_str = entry_path.filename();

            auto cache_entry = readCacheEntry(ast_hash_str);

            if (cache_entry)
            {
                auto entry_weight = QueryResultCacheEntryWeight()(*cache_entry->mapped);
                auto metadata = std::make_shared<DiskEntryMetadata>(entry_weight, entry_path);
                cache_policy->set(cache_entry->key, metadata);
            }
        }
    }
    catch (const Exception& e)
    {
        LOG_TRACE(logger, "Exception on reading metadata for QueryResultCache on disk. Error: {}", e.what());
    }
    catch (...)
    {
        LOG_TRACE(logger, "Unknown exception on reading metadata for QueryResultCache on disk.");
    }
}

std::optional<QueryResultCache::OnDiskCache::KeyMapped> QueryResultCache::OnDiskCache::getWithKey(const Key & key)
{
    std::lock_guard lock(mutex);
    
    IASTHash ast_hash = key.ast_hash;
    String ast_hash_str = std::to_string(ast_hash.low64) + '_' + std::to_string(ast_hash.high64);

    if (!cache_policy->contains(key))
    {
        LOG_TRACE(logger, "No entry on disk, key not found in metadata");
        return std::nullopt;
    }

    return readCacheEntry(ast_hash_str);
}

void QueryResultCache::OnDiskCache::set(const Key & key, const MappedPtr & mapped)
{
    std::lock_guard lock(mutex);
    
    IASTHash ast_hash = key.ast_hash;
    String ast_hash_str = std::to_string(ast_hash.low64) + '_' + std::to_string(ast_hash.high64);

    Chunks & chunks = mapped->chunks;

    /// To keep the file format simple, squash the result chunks to a single chunk.
    chunks = squashChunks(chunks, std::numeric_limits<size_t>::max());
    if (key.is_compressed)
        chunks = compressChunks(chunks);

    size_t new_entry_size_in_bytes = QueryResultCache::QueryResultCacheEntryWeight()(*mapped);
    size_t new_entry_size_in_rows = countRowsInChunks(*mapped);

    if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
    {
        LOG_TRACE(logger, "Skipped insert to disk because the query result is too big, query result size: {} (maximum size: {}), query result size in rows: {} (maximum size: {}), query: {}",
                formatReadableSizeWithBinarySuffix(new_entry_size_in_bytes, 0), formatReadableSizeWithBinarySuffix(max_entry_size_in_bytes, 0), new_entry_size_in_rows, max_entry_size_in_rows, doubleQuoteString(key.query_string));
        return;
    }

    if (cache_policy->contains(key))
    { 
        LOG_TRACE(logger, "Entry already in disk cache, skip inserting");
        return;
    }

    std::filesystem::path entry_file_path = query_cache_path / ast_hash_str;

    auto entry_weight = QueryResultCacheEntryWeight()(*mapped);
    auto metadata = std::make_shared<DiskEntryMetadata>(entry_weight, entry_file_path);

    cache_policy->set(key, metadata);
    
    writeCacheEntry(key, mapped);
}

void QueryResultCache::OnDiskCache::writeCacheEntry(const Key & entry_key, const MappedPtr & entry_mapped)
{
    try
    {
        /// Store query cache entries to persistence:
        namespace fs = std::filesystem;

        // check format version again in case of format_version.txt changed
        checkFormatVersion();

        IASTHash ast_hash = entry_key.ast_hash;
        String ast_hash_str = std::to_string(ast_hash.low64) + '_' + std::to_string(ast_hash.high64);

        fs::path entry_file_path = query_cache_path / ast_hash_str;
        WriteBufferFromFile entry_file(entry_file_path.string());

        writeText(FormatTokens::token_user_id, entry_file);
        UUID user_id = entry_key.user_id ? *entry_key.user_id : UUIDHelpers::Nil;
        writeUUIDText(user_id, entry_file);
        writeText("\n", entry_file);

        writeText(FormatTokens::token_current_user_roles, entry_file);
        for (size_t i = 0; i < entry_key.current_user_roles.size(); ++i)
        {
            writeUUIDText(entry_key.current_user_roles[i], entry_file);
            if (i != entry_key.current_user_roles.size() - 1)
                writeText(",", entry_file);
        }
        writeText("\n", entry_file);

        writeText(FormatTokens::token_is_shared, entry_file);
        writeBoolText(entry_key.is_shared, entry_file);
        writeText("\n", entry_file);

        writeText(FormatTokens::token_expires_at, entry_file);

        auto duration = entry_key.expires_at.time_since_epoch();
        Int64 nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

        writeIntText(nanoseconds, entry_file);
        writeText("\n", entry_file);

        writeText(FormatTokens::token_is_compressed, entry_file);
        writeBoolText(entry_key.is_compressed, entry_file);
        writeText("\n", entry_file);

        writeText(FormatTokens::token_query_string, entry_file);
        writeText(entry_key.query_string, entry_file);
        writeText("\n", entry_file);

        Chunks & chunks = entry_mapped->chunks;
        const std::optional<Chunk> & totals = entry_mapped->totals;
        const std::optional<Chunk> & extremes = entry_mapped->extremes;
        
        /// Get only one chunk after squash
        auto& chunk = chunks[0];

        Block header = entry_key.header;

        NativeWriter block_writer(entry_file, 0, header);

        Block block = header.cloneWithColumns(chunk.getColumns());
        block_writer.write(block);

        writeText(FormatTokens::token_has_totals, entry_file);

        if (totals)
        {
            writeBoolText(true, entry_file);
            writeText("\n", entry_file);

            Block block_totals = header.cloneWithColumns(totals->getColumns());
            block_writer.write(block_totals);
        } 
        else
        {
            writeBoolText(false, entry_file);
            writeText("\n", entry_file);
        }
        
        writeText(FormatTokens::token_has_extremes, entry_file);

        if (extremes)
        {
            writeBoolText(true, entry_file);
            writeText("\n", entry_file);

            Block block_extremes = header.cloneWithColumns(extremes->getColumns());
            block_writer.write(block_extremes);
        }
        else
        {
            writeBoolText(false, entry_file);
            writeText("\n", entry_file);
        }

        entry_file.finalize();
    }
    catch (const Exception& e)
    {
        LOG_TRACE(logger, "Exception on writing entry to disk cache {}", e.what());
    }
    catch (...)
    {
        LOG_TRACE(logger, "Unknown exception on writing entry to disk cache");
    }
}

std::optional<QueryResultCache::OnDiskCache::KeyMapped> QueryResultCache::OnDiskCache::readCacheEntry(String ast_hash_str)
{
    try
    {
        namespace fs = std::filesystem;

        checkFormatVersion();

        fs::path entry_file_path = query_cache_path / ast_hash_str;
        ReadBufferFromFile entry_file(entry_file_path.string());

        size_t separator_pos = ast_hash_str.find('_');
        chassert(separator_pos != String::npos);
        String low64_str = ast_hash_str.substr(0, separator_pos);
        String high64_str = ast_hash_str.substr(separator_pos + 1, ast_hash_str.size());
        IASTHash ast_hash(std::stoull(low64_str), std::stoull(high64_str));

        assertString(FormatTokens::token_user_id, entry_file);
        UUID user_id;
        readUUIDText(user_id, entry_file);
        /// can be UUIDHelpers::Nil

        assertChar('\n', entry_file);

        assertString(FormatTokens::token_current_user_roles, entry_file);
        std::vector<UUID> current_user_roles;
        while (!checkChar('\n', entry_file))
        {
            UUID user_role;
            readUUIDText(user_role, entry_file);
            current_user_roles.push_back(user_role);
            assertChar(',', entry_file);
        }

        assertString(FormatTokens::token_is_shared, entry_file);
        bool is_shared;
        readBoolText(is_shared, entry_file);
        assertChar('\n', entry_file);

        assertString(FormatTokens::token_expires_at, entry_file);
        int64_t duration;
        readIntText(duration, entry_file);
        std::chrono::nanoseconds nanoseconds(duration);
        std::chrono::time_point<std::chrono::system_clock> expires_at{
            std::chrono::duration_cast<std::chrono::system_clock::duration>(nanoseconds)};
        assertChar('\n', entry_file);
        
        assertString(FormatTokens::token_is_compressed, entry_file);
        bool is_compressed;
        readBoolText(is_compressed, entry_file);
        assertChar('\n', entry_file);

        assertString(FormatTokens::token_query_string, entry_file);
        String query_string;
        readStringUntilNewlineInto(query_string, entry_file);
        assertChar('\n', entry_file);

        NativeReader block_reader(entry_file, 0);
        Block block = block_reader.read();
        block.checkNumberOfRows();

        Block header = block.cloneEmpty();
        Chunk chunk = Chunk(block.getColumns(), block.rows());

        Chunks chunks;
        chunks.push_back(std::move(chunk));

        assertString(FormatTokens::token_has_totals, entry_file);
        bool has_totals;
        readBoolText(has_totals, entry_file);
        assertChar('\n', entry_file);

        std::optional<Chunk> totals;

        if (has_totals)
        {
            Block block_totals = block_reader.read();
            block_totals.checkNumberOfRows();

            totals = Chunk(block_totals.getColumns(), block_totals.rows());
        }

        assertString(FormatTokens::token_has_extremes, entry_file);
        bool has_extremes;
        readBoolText(has_extremes, entry_file);
        assertChar('\n', entry_file);

        std::optional<Chunk> extremes;

        if (has_extremes)
        {
            Block block_extremes = block_reader.read();
            block_extremes.checkNumberOfRows();

            extremes = Chunk(block_extremes.getColumns(), block_extremes.rows());
        }

        String query_id; /// dummy value
        String tag; /// dummy value 

        Key key(ast_hash, header, user_id, current_user_roles, is_shared, expires_at, is_compressed, query_string, query_id, tag);
        MappedPtr entry = std::make_shared<Mapped>(std::move(chunks), std::move(totals), std::move(extremes));

        return KeyMapped{key, entry};
    }
    catch (const Exception& e)
    {
        LOG_TRACE(logger, "Exception on reading entry from disk cache {}", e.what());
    }
    catch (...)
    {
        LOG_TRACE(logger, "Unknown exception on reading entry from disk cache");
    }

    return std::nullopt;
}

void QueryResultCache::OnDiskCache::checkFormatVersion()
{
    namespace fs = std::filesystem;
    fs::path format_version_path = query_cache_path / FormatTokens::format_version_txt;
    ReadBufferFromFile format_version_file(format_version_path); /// throws if file can't be opened
    uint32_t version;

    readIntText(version, format_version_file);
    if (version != FormatTokens::current_version)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On disk query result cache format_version mismatch");
}

void QueryResultCache::OnDiskCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    std::lock_guard lock(mutex);
    cache_policy->setMaxSizeInBytes(max_size_in_bytes);
}

void QueryResultCache::OnDiskCache::setMaxCount(size_t max_count)
{
    std::lock_guard lock(mutex);
    cache_policy->setMaxCount(max_count);
}

void QueryResultCache::OnDiskCache::onEvictFunction(CachePolicy::MappedPtr mapped)
{
    std::filesystem::remove(mapped->path);
}

}
