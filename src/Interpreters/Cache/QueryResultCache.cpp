#include <Interpreters/Cache/QueryResultCache.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
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
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
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

QueryResultCache::Key::Key(IASTHash ast_hash_)
    : ast_hash(ast_hash_)
{
}

QueryResultCache::Key::Key(
    IASTHash ast_hash_,
    Block header_,
    std::optional<UUID> user_id_, const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_,
    String & query_string_,
    String & query_id_,
    String & tag_)
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

// static constexpr std::string_view format_version_txt = "format_version.txt";
// static constexpr uint32_t current_version = 1;

static constexpr auto * token_user_id = "user_id: ";
static constexpr auto * token_current_user_roles = "current_user_roles: ";
static constexpr auto * token_is_shared = "is_shared: ";
static constexpr auto * token_expires_at = "expires_at: ";
static constexpr auto * token_is_compressed = "is_compressed: ";
static constexpr auto * token_query_string = "query_string: ";
static constexpr auto * token_query_id = "query_id: ";
static constexpr auto * token_tag = "query_tag: ";

void QueryResultCache::Key::serialize(WriteBuffer & buf) const
{
    writeText(token_user_id, buf);
    UUID user_id_ = user_id ? *user_id : UUIDHelpers::Nil;
    writeUUIDText(user_id_, buf);
    writeText("\n", buf);

    writeText(token_current_user_roles, buf);
    for (size_t i = 0; i < current_user_roles.size(); ++i)
    {
        writeUUIDText(current_user_roles[i], buf);
        if (i != current_user_roles.size() - 1)
            writeText(",", buf);
    }
    writeText("\n", buf);

    writeText(token_is_shared, buf);
    writeBoolText(is_shared, buf);
    writeText("\n", buf);

    writeText(token_expires_at, buf);
    writeVarUInt(std::chrono::system_clock::to_time_t(expires_at), buf);
    writeText("\n", buf);

    writeText(token_is_compressed, buf);
    writeBoolText(is_compressed, buf);
    writeText("\n", buf);

    writeText(token_query_id, buf);
    writeText(query_id, buf);
    writeText("\n", buf);

    writeText(token_tag, buf);
    writeText(tag, buf);
    writeText("\n", buf);

    writeText(token_query_string, buf);
    writeText(query_string, buf);
    writeText("\n", buf);
}

void QueryResultCache::Key::deserialize(ReadBuffer & buf)
{
    assertString(token_user_id, buf);
    UUID user_id_;
    readUUIDText(user_id_, buf);
    if (user_id_ != UUIDHelpers::Nil)
        user_id = user_id_;

    assertChar('\n', buf);
    assertString(token_current_user_roles, buf);
    std::vector<UUID> current_user_roles_;
    while (!checkChar('\n', buf))
    {
        UUID user_role;
        readUUIDText(user_role, buf);
        current_user_roles_.push_back(user_role);
        assertChar(',', buf);
    }
    if (!current_user_roles_.empty())
        current_user_roles = current_user_roles_;

    assertString(token_is_shared, buf);
    readBoolText(is_shared, buf);

    assertChar('\n', buf);
    assertString(token_expires_at, buf);
    std::time_t timestamp;
    readVarUInt(timestamp, buf);
    expires_at = std::chrono::system_clock::from_time_t(timestamp);

    assertChar('\n', buf);
    assertString(token_is_compressed, buf);
    readBoolText(is_compressed, buf);

    assertChar('\n', buf);
    assertString(token_query_id, buf);
    readString(query_id, buf);

    assertChar('\n', buf);
    assertString(token_tag, buf);
    readString(tag, buf);

    assertChar('\n', buf);
    assertString(token_query_string, buf);
    readStringUntilNewlineInto(query_string, buf);
}

String QueryResultCache::Key::getKeyPath() const
{
    String ast_hash_str = std::to_string(ast_hash.low64) + '_' + std::to_string(ast_hash.high64);
    return fs::path(ast_hash_str.substr(0, 3)) / ast_hash_str;
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

size_t QueryResultCache::QueryResultCacheDiskEntryWeight::operator()(const DiskEntry & entry) const
{
    return entry.bytes_on_disk;
}

bool QueryResultCache::IsStale::operator()(const Key & key) const
{
    return (key.expires_at < std::chrono::system_clock::now());
};

QueryResultCacheWriter::QueryResultCacheWriter(
    QueryResultCachePtr cache_,
    const QueryResultCache::Key & key_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_runtime_,
    bool squash_partial_results_,
    size_t max_block_size_,
    bool enable_writes_to_query_cache_disk_)
    : cache(cache_)
    , key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
    , squash_partial_results(squash_partial_results_)
    , max_block_size(max_block_size_)
    , enable_writes_to_query_cache_disk(enable_writes_to_query_cache_disk_)
{
    if (!cache_->isStale(key))
    {
        skip_insert = true; /// Key already contained in cache and did not expire yet --> don't replace it
        LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query result for query {}", doubleQuoteString(key.query_string));
    }
}

QueryResultCacheWriter::QueryResultCacheWriter(const QueryResultCacheWriter & other)
    : cache(other.cache)
    , key(other.key)
    , max_entry_size_in_bytes(other.max_entry_size_in_bytes)
    , max_entry_size_in_rows(other.max_entry_size_in_rows)
    , min_query_runtime(other.min_query_runtime)
    , squash_partial_results(other.squash_partial_results)
    , max_block_size(other.max_block_size)
    , enable_writes_to_query_cache_disk(other.enable_writes_to_query_cache_disk)
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

    if (!cache->isStale(key))
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

    Chunks uncompressed_chunks;
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
        uncompressed_chunks = std::move(query_result->chunks);
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

    cache->writeMemory(key, query_result);

    if (enable_writes_to_query_cache_disk)
    {
        if (key.is_compressed)
            query_result->chunks = std::move(uncompressed_chunks);
    
        cache->writeDisk(key, query_result);
    }

    LOG_TRACE(logger, "Stored query result of query {}", doubleQuoteString(key.query_string));

    was_finalized = true;
}

/// Creates a source processor which serves result chunks stored in the query result cache, and separate sources for optional totals/extremes.
void QueryResultCacheReader::buildSourceFromChunks(Block header, Chunks && chunks, std::optional<Chunk> & totals, std::optional<Chunk> & extremes)
{
    source_from_chunks = std::make_unique<SourceFromChunks>(header, std::move(chunks));

    if (totals.has_value())
    {
        Chunks chunks_totals;
        chunks_totals.emplace_back(std::move(*totals));
        source_from_chunks_totals = std::make_unique<SourceFromChunks>(header, std::move(chunks_totals));
    }

    if (extremes.has_value())
    {
        Chunks chunks_extremes;
        chunks_extremes.emplace_back(std::move(*extremes));
        source_from_chunks_extremes = std::make_unique<SourceFromChunks>(header, std::move(chunks_extremes));
    }
}

QueryResultCacheReader::QueryResultCacheReader(QueryResultCachePtr cache_, const Cache::Key & key, bool enable_reads_from_query_cache_disk, const std::lock_guard<std::mutex> &)
{
    auto entry = cache_->readFromMemory(key);
    if (!entry.has_value() && enable_reads_from_query_cache_disk)
        entry = cache_->readFromDisk(key);

    if (!entry.has_value())
    {
        // LOG_TRACE(logger, "No query result found for query {}", doubleQuoteString(key.query_string));
        return;
    }

    const auto & entry_key = entry->key;
    const auto & entry_mapped = entry->mapped;

    buildSourceFromChunks(entry_key.header, std::move(entry_mapped->chunks), entry_mapped->totals, entry_mapped->extremes);
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

QueryResultCache::QueryResultCache(
    size_t max_size_in_bytes,
    size_t max_entries,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    size_t max_disk_size_in_bytes, 
    size_t max_disk_entries,
    DiskPtr & disk_,
    const String & path_)
    : memory_cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, QueryResultCacheEntryWeight, IsStale>>(
          std::make_unique<PerUserTTLCachePolicyUserQuota>()))
    , disk_cache(std::make_unique<TTLCachePolicy<Key, DiskEntry, KeyHasher, QueryResultCacheDiskEntryWeight, IsStale>>(
        std::make_unique<PerUserTTLCachePolicyUserQuota>()))
    , disk(disk_)
    , path(path_)
{
    updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_, max_disk_size_in_bytes, max_disk_entries);

    loadEntrysFromDisk();
}

QueryResultCache::~QueryResultCache()
{
    shutdown = true;
}

void QueryResultCache::updateConfiguration(
    size_t max_size_in_bytes,
    size_t max_entries,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    size_t max_disk_size_in_bytes, 
    size_t max_disk_entries)
{
    std::lock_guard lock(mutex);
    memory_cache.setMaxSizeInBytes(max_size_in_bytes);
    memory_cache.setMaxCount(max_entries);
    disk_cache.setMaxSizeInBytes(max_disk_size_in_bytes);
    disk_cache.setMaxCount(max_disk_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
}

QueryResultCacheReader QueryResultCache::createReader(const Key & key, bool enable_reads_from_query_cache_disk)
{
    std::lock_guard lock(mutex);
    return QueryResultCacheReader(shared_from_this(), key, enable_reads_from_query_cache_disk, lock);
}

QueryResultCacheWriter QueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t max_query_result_cache_size_in_bytes_quota,
    size_t max_query_result_cache_entries_quota,
    bool enable_writes_to_query_cache_disk)
{
    /// Update the per-user cache quotas with the values stored in the query context. This happens per query which writes into the query
    /// cache. Obviously, this is overkill but I could find the good place to hook into which is called when the settings profiles in
    /// users.xml change.
    /// user_id == std::nullopt is the internal user for which no quota can be configured
    if (key.user_id.has_value())
        memory_cache.setQuotaForUser(*key.user_id, max_query_result_cache_size_in_bytes_quota, max_query_result_cache_entries_quota);

    std::lock_guard lock(mutex);
    return QueryResultCacheWriter(shared_from_this(),
        key,
        max_entry_size_in_bytes,
        max_entry_size_in_rows,
        min_query_runtime,
        squash_partial_results,
        max_block_size,
        enable_writes_to_query_cache_disk);
}

bool QueryResultCache::isStale(const Key & key)
{
    if (auto entry = memory_cache.getWithKey(key); entry.has_value() && !QueryResultCache::IsStale()(entry->key)){
        return false;
    }
    return true;
}

void QueryResultCache::writeMemory(const Key & key, const QueryResultCache::Cache::MappedPtr & entry)
{
    memory_cache.set(key, entry);
}

void QueryResultCache::writeDisk(const Key & key, const QueryResultCache::Cache::MappedPtr & entry)
{
    if (!disk)
        return;
    
    auto entry_path = fs::path(path) / key.getKeyPath();
    auto disk_entry = std::shared_ptr<DiskEntry>(
        new DiskEntry,
        [this, entry_path](DiskEntry * e)
        {
            try
            {
                if (!shutdown)
                    disk->removeRecursive(path / entry_path);
            }
            catch (...)
            {
            }
            delete e;
        }
    );

    disk->createDirectories(entry_path);
    
    {
        auto entry_file = disk->writeFile(entry_path / "key_metadata.txt");
        key.serialize(*entry_file);
        entry_file->finalize();
    }
    
    serializeEntry(key, entry, disk_entry);
    disk_cache.set(key, disk_entry);
}

std::optional<QueryResultCache::Cache::KeyMapped> QueryResultCache::readFromMemory(const Key & key)
{
    auto entry = memory_cache.getWithKey(key);
    if (!entry.has_value())
        return std::nullopt;

    const auto & entry_key = entry->key;
    const auto & entry_mapped = entry->mapped;

    if (!checkAccess(entry_key, key)) {
        return std::nullopt;
    }

    auto res = std::make_shared<Entry>();

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

        res->chunks = std::move(cloned_chunks);
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
        res->chunks = std::move(decompressed_chunks);
    }

    if (entry->mapped->totals.has_value())
        res->totals.emplace(entry->mapped->totals->clone());

    if (entry->mapped->extremes.has_value())
        res->extremes.emplace(entry->mapped->extremes->clone());

    return {{entry->key, res}};
}

std::optional<QueryResultCache::Cache::KeyMapped> QueryResultCache::readFromDisk(const Key & key)
{
    auto disk_entry = disk_cache.getWithKey(key);
    if (!disk_entry.has_value())
        return std::nullopt;

    const auto & disk_entry_key = disk_entry->key;

    if (!checkAccess(disk_entry_key, key))
        return std::nullopt;

    auto [header, entry_, disk_entry_]  = deserializeEntry(key);
    if (!entry_)
        return std::nullopt;

    if (key.is_compressed)
        compressEntry(entry_);

    memory_cache.set(key, entry_);
    return {{disk_entry->key, entry_}};
}

void QueryResultCache::serializeEntry(const Key & key, const QueryResultCache::Cache::MappedPtr & entry, QueryResultCache::DiskCache::MappedPtr & disk_entry) const
{
    auto entry_path = path / key.getKeyPath();
    auto writeChunk = [](const Chunk & chunk, NativeWriter & writer)
    {
        Block block = writer.getHeader();
        const Columns & columns = chunk.getColumns();
        block.setColumns(columns);
        // for (size_t i = 0; i < block.columns(); ++i)
        // {
        //     auto column = block.getByPosition(i);
        //     column.column = columns[i];
        //     block.insert(column);
        // }
        writer.write(block);
    };

    {
        auto out = disk->writeFile(entry_path / "results.bin");
        auto compress_out = std::make_unique<CompressedWriteBuffer>(*out, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size);
        NativeWriter writer(*compress_out, 0, key.header);

        for (const auto & chunk : entry->chunks)
            writeChunk(chunk, writer);

        compress_out->finalize();
        out->finalize();
        disk_entry->bytes_on_disk += out->count();
    }

    if (entry->totals.has_value())
    {
        auto out = disk->writeFile(entry_path / "totals.bin");
        auto compress_out = std::make_unique<CompressedWriteBuffer>(*out, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size);
        NativeWriter writer(*compress_out, 0, key.header);
        writeChunk(*entry->totals, writer);
        compress_out->finalize();
        out->finalize();
        disk_entry->bytes_on_disk += out->count();
    }

    if (entry->extremes.has_value())
    {
        auto out = disk->writeFile(entry_path / "extremes.bin");
        auto compress_out = std::make_unique<CompressedWriteBuffer>(*out, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size);
        NativeWriter writer(*compress_out, 0, key.header);
        writeChunk(*entry->extremes, writer);
        compress_out->finalize();
        out->finalize();
        disk_entry->bytes_on_disk += out->count();
    }
}

std::tuple<Block, QueryResultCache::Cache::MappedPtr, QueryResultCache::DiskCache::MappedPtr> QueryResultCache::deserializeEntry(const Key & key)
{
    try
    {
        auto entry = std::make_shared<Entry>();
        auto disk_entry = std::make_shared<DiskEntry>();
        std::optional<Block> header;
        auto key_path = path / key.getKeyPath();
        {
            auto result_path = key_path / "results.bin";
            auto in = disk->readFile(result_path, {});
            auto compress_in = std::make_shared<CompressedReadBuffer>(*in);
            NativeReader reader(*compress_in, 0);

            while (!compress_in->eof())
            {
                Block res = reader.read();
                entry->chunks.emplace_back(res.getColumns(), res.rows());

                if (!header)
                    header = res.cloneEmpty();
            }

            disk_entry->bytes_on_disk += disk->getFileSize(result_path);
        }

        auto totals_path = key_path / "totals.bin";
        if (auto in = disk->readFileIfExists(totals_path))
        {
            auto compress_in = std::make_shared<CompressedReadBuffer>(*in);
            NativeReader reader(*compress_in, 0);
            Block res = reader.read();

            entry->totals.emplace(res.getColumns(), res.rows());
            disk_entry->bytes_on_disk += disk->getFileSize(totals_path);
        }

        auto extremes_path = key_path / "extremes.bin";
        if (auto in = disk->readFileIfExists(extremes_path))
        {
            auto compress_in = std::make_shared<CompressedReadBuffer>(*in);
            NativeReader reader(*compress_in, 0);
            Block res = reader.read();

            entry->extremes.emplace(res.getColumns(), res.rows());
            disk_entry->bytes_on_disk += disk->getFileSize(extremes_path);
        }

        return std::make_tuple(*header, entry, disk_entry);
    }
    catch (...)
    {
    }
    return {};
}

void QueryResultCache::compressEntry(const QueryResultCache::Cache::MappedPtr & entry)
{
    /// Compress result chunks. Reduces the space consumption of the cache but means reading from it will be slower due to decompression.
    Chunks compressed_chunks;

    for (const auto & chunk : entry->chunks)
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
    entry->chunks = std::move(compressed_chunks);

}

void QueryResultCache::loadEntrysFromDisk()
{
    if (!disk)
        return;

    std::vector<String> expired_entrys;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        for (auto entry_it = disk->iterateDirectory(it->path()); entry_it->isValid(); entry_it->next())
        {
            auto entry_path = fs::path(entry_it->path());
            try
            {
                String ast_hash_str = entry_path.parent_path().filename();
                size_t separator_pos = ast_hash_str.find('_');
                chassert(separator_pos != String::npos);
                String low64_str = ast_hash_str.substr(0, separator_pos);
                String high64_str = ast_hash_str.substr(separator_pos + 1, ast_hash_str.size());
                IASTHash ast_hash(std::stoull(low64_str), std::stoull(high64_str));

                Key key(ast_hash);
                {
                    auto entry_file = disk->readFile(entry_path / "key_metadata.txt", {});
                    key.deserialize(*entry_file);

                    if (key.expires_at < std::chrono::system_clock::now())
                    {
                        expired_entrys.push_back(entry_path);
                        continue;
                    }
                }

                /// When the EntryOnDisk is released, the disk file is also deleted.
                auto disk_entry= std::shared_ptr<DiskEntry>(
                    new DiskEntry,
                    [this, entry_path](DiskEntry * e)
                    {
                        try
                        {
                            if (!shutdown)
                                disk->removeRecursive(path / entry_path);
                        }
                        catch (...)
                        {
                        }
                        delete e;
                    });

                auto [header, entry, disk_entry_] = deserializeEntry(key);
                key.header = header;

                if (key.is_compressed)
                    compressEntry(entry);

                memory_cache.set(key, entry);

                disk_entry->bytes_on_disk = disk_entry_->bytes_on_disk;
                disk_cache.set(key, disk_entry);
            }
            catch (...)
            {
                expired_entrys.push_back(entry_path);
            }
        }
    }

    for (const auto & entry_path : expired_entrys)
        disk->removeRecursive(path / entry_path);

    LOG_TRACE(logger, "Loading entries from disk, {} succeeded and {} failed.", disk_cache.count(), expired_entrys.size());
}

bool QueryResultCache::checkAccess(const Key & entry_key, const Key & key) const
{
    const bool is_same_user_id = ((!entry_key.user_id.has_value() && !key.user_id.has_value()) || (entry_key.user_id.has_value() && key.user_id.has_value() && *entry_key.user_id == *key.user_id));
    const bool is_same_current_user_roles = (entry_key.current_user_roles == key.current_user_roles);
    if (!entry_key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
    {
        LOG_TRACE(logger, "Inaccessible query result found for query {}", doubleQuoteString(key.query_string));
        return false;
    }

    if (QueryResultCache::IsStale()(entry_key))
    {
        LOG_TRACE(logger, "Stale query result found for query {}", doubleQuoteString(key.query_string));
        return false;
    }
    return true;
}

void QueryResultCache::clear(const std::optional<String> & tag)
{
    if (tag)
    {
        auto predicate = [tag](const Key & key, const Cache::MappedPtr &) { return key.tag == tag.value(); };
        memory_cache.remove(predicate);
    }
    else
    {
        memory_cache.clear();
    }

    std::lock_guard lock(mutex);
    times_executed.clear();
}

size_t QueryResultCache::sizeInBytes() const
{
    return memory_cache.sizeInBytes();
}

size_t QueryResultCache::count() const
{
    return memory_cache.count();
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

std::vector<QueryResultCache::Cache::KeyMapped> QueryResultCache::dumpMemoryCache() const
{
    return memory_cache.dump();
}

std::vector<QueryResultCache::DiskCache::KeyMapped> QueryResultCache::dumpDiskCache() const
{
    return disk_cache.dump();
}

}
