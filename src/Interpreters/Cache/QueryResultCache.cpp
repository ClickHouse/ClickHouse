#include <Interpreters/Cache/QueryResultCache.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Cache/QueryResultCacheFactory.h>
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
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <base/defines.h> /// chassert
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Processors/Chunk.h>


namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryCacheBytes;
    extern const Metric QueryCacheEntries;
}

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

QueryResultCache::QueryResultCache()
{
}

QueryResultCache::~QueryResultCache()
{
}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    SharedHeader header_,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    const uint32_t ttl_seconds_,
    bool is_compressed_)
    : ast_hash(calculateAstHash(ast_, current_database, settings))
    , header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , expires_at(std::chrono::system_clock::now() + std::chrono::seconds(ttl_seconds))
    , is_compressed(is_compressed_)
    , query_string(queryStringFromAST(ast_))
    , query_id(query_id_)
    , tag(settings[Setting::query_cache_tag])
    , ttl_seconds(ttl_seconds_)
{
}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_)
    : QueryResultCache::Key(ast_, current_database, settings, std::make_shared<const Block>(Block{}), query_id_, user_id_, current_user_roles_, false, std::chrono::system_clock::from_time_t(1), false)
    /// ^^ dummy values for everything except AST, current database, query_id, user name/roles
{
}

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast_hash == other.ast_hash;
}

String QueryResultCache::Key::encodeTo() const
{
    // Tag-prefixed keys enable atomic namespace cleanup via prefix matching.
    WriteBufferFromOwnString buffer;
    writeString(tag, buffer);
    writeIntText(ast_hash.high64, buffer);
    writeIntText(ast_hash.low64, buffer);
    return buffer.str();
}

namespace
{
static size_t timePointToSizeT(const std::chrono::time_point<std::chrono::system_clock>& tp)
{
    auto duration = tp.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
}

std::chrono::time_point<std::chrono::system_clock> sizeTToTimePoint(size_t timestamp)
{
    using namespace std::chrono;
    return system_clock::from_time_t(0) + seconds(timestamp);
}

}
void QueryResultCache::Key::serialize(WriteBuffer & buffer) const
{
    NativeWriter writer { buffer, 0, header};

    writeBinary(ast_hash, buffer);
    writer.write(header);
    if (user_id)
    {
        writeVarInt(1, buffer);
        writeUUIDBinary(*user_id, buffer);
    }
    else
    {
        writeVarInt(0, buffer);
    }
    if (!current_user_roles.empty())
    {
        writeVarInt(1, buffer);
        writeVarInt(current_user_roles.size(), buffer);
        for (auto & role : current_user_roles)
            writeUUIDBinary(role, buffer);
    }
    else
    {
        writeVarInt(0, buffer);
    }
    writeVarInt(is_shared ? 1 : 0, buffer);
    writeVarInt(is_compressed ? 1 : 0, buffer);
    writeVarUInt(timePointToSizeT(expires_at), buffer);
    writeBinary(query_string, buffer);
    writeBinary(query_id, buffer);
    writeBinary(tag, buffer);
}

void QueryResultCache::Key::deserialize(DB::ReadBuffer & buffer)
{
    readBinary(ast_hash, buffer);
    NativeReader reader {buffer, 0};
    if (auto block = reader.read())
        header = block;
    Int64 flag = -1;
    readVarInt(flag, buffer);
    if (flag)
    {
        UUID user_id_;
        readUUIDBinary(user_id_, buffer);
        user_id.emplace(user_id_);
    }
    readVarInt(flag, buffer);
    if (flag)
    {
        Int64 count = 0;
        readVarInt(count, buffer);
        for (Int64 i = 0; i < count; i++)
        {
            UUID role;
            readUUIDBinary(role, buffer);
            current_user_roles.emplace_back(role);
        }
    }
    readVarInt(flag, buffer);
    is_shared = flag == 1;
    readVarInt(flag, buffer);
    is_compressed = flag == 1;
    size_t seconds = 0;
    readVarUInt(seconds, buffer);
    expires_at = sizeTToTimePoint(seconds);
    readBinary(query_string, buffer);
    readBinary(query_id, buffer);
    readBinary(tag, buffer);
}

size_t QueryResultCache::KeyHasher::operator()(const Key & key) const
{
    return key.ast_hash.low64;
}

size_t QueryResultCache::EntryWeight::operator()(const Entry & entry) const
{
    size_t res = 0;
    for (const auto & chunk : entry.chunks)
        res += chunk.allocatedBytes();
    res += entry.totals.has_value() ? entry.totals->allocatedBytes() : 0;
    res += entry.extremes.has_value() ? entry.extremes->allocatedBytes() : 0;
    return res;
}

void QueryResultCache::Entry::serializeWithKey(const QueryResultCache::Key & key, WriteBuffer & buffer)
{
    key.serialize(buffer);
    writeVarInt(chunks.size(), buffer);
    NativeWriter writer { buffer, 0, key.header };
    for (auto & chunk : chunks)
    {
        auto block = key.header.cloneWithColumns(chunk.detachColumns());
        writer.write(block);
        writer.flush();
    }
    auto write_optional_chunk = [&] (std::optional<Chunk> & chunk)
    {
        if (chunk)
        {
            writeVarInt(1, buffer);
            auto block = key.header.cloneWithColumns(chunk->detachColumns());
            writer.write(block);
        }
        else
            writeVarInt(0, buffer);
    };
    write_optional_chunk(totals);
    write_optional_chunk(extremes);
}

void QueryResultCache::Entry::deserializeWithKey(QueryResultCache::Key & key, ReadBuffer & buffer)
{
    key.deserialize(buffer);
    NativeReader reader {buffer, key.header, 0};
    Int64 count = 0;
    readVarInt(count, buffer);
    for (Int64 i = 0; i < count; i++)
    {
        auto block = reader.read();
        chunks.emplace_back(block.getColumns(), block.rows());
    }
    auto read_optional_chunk = [&] (std::optional<Chunk> & chunk)
    {
        Int64 flag = -1;
        readVarInt(flag, buffer);
        assert(flag == 0 || flag == 1);
        if (flag == 1)
        {
            auto block = reader.read();
            chunk.emplace(block.getColumns(), block.rows());
        }
    };
    read_optional_chunk(totals);
    read_optional_chunk(extremes);
}

QueryResultCacheWriter::QueryResultCacheWriter(const QueryResultCache::Key & key_, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_, std::chrono::milliseconds min_query_runtime_, bool squash_partial_results_, size_t max_block_size_)
    : key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
    , squash_partial_results(squash_partial_results_)
    , max_block_size(max_block_size_)
{
}

QueryResultCacheWriter::QueryResultCacheWriter(const DB::QueryResultCacheWriter & other)
    : key(other.key)
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

/// Creates a source processor which serves result chunks stored in the query result cache, and separate sources for optional totals/extremes.
void QueryResultCacheReader::buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes)
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

}
