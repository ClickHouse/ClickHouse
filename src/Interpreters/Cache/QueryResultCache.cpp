#include <Interpreters/Cache/QueryResultCache.h>

#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
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


namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
    extern const Event QueryCacheAgeSeconds;
    extern const Event QueryCacheReadRows;
    extern const Event QueryCacheReadBytes;
    extern const Event QueryCacheWrittenRows;
    extern const Event QueryCacheWrittenBytes;
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
    extern const SettingsBool enable_writes_to_query_cache;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsQueryResultCacheNondeterministicFunctionHandling query_cache_nondeterministic_function_handling;
    extern const SettingsQueryResultCacheSystemTableHandling query_cache_system_table_handling;
    extern const SettingsString query_cache_tag;
}

namespace ErrorCodes
{
    extern const int QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS;
    extern const int QUERY_CACHE_USED_WITH_SYSTEM_TABLE;
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
                                database_table = fieldToString(value);
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

/// Does AST contain non-deterministic functions like rand() and now()?
bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

/// Does AST contain system tables like "system.processes"?
bool astContainsSystemTables(ASTPtr ast, ContextPtr context)
{
    HasSystemTablesMatcher::Data finder_data{context};
    HasSystemTablesVisitor(finder_data).visit(ast);
    return finder_data.has_system_tables;
}

bool checkCanWriteQueryResultCache(ASTPtr ast, ContextPtr context, bool skip_context_check)
{
    const Settings & settings = context->getSettingsRef();

    if ((skip_context_check || context->getCanUseQueryResultCache()) && settings[Setting::enable_writes_to_query_cache])
    {
        const bool ast_contains_nondeterministic_functions = astContainsNonDeterministicFunctions(ast, context);
        const bool ast_contains_system_tables = astContainsSystemTables(ast, context);

        const QueryResultCacheNondeterministicFunctionHandling nondeterministic_function_handling
            = settings[Setting::query_cache_nondeterministic_function_handling];
        const QueryResultCacheSystemTableHandling system_table_handling = settings[Setting::query_cache_system_table_handling];

        if (ast_contains_nondeterministic_functions && nondeterministic_function_handling == QueryResultCacheNondeterministicFunctionHandling::Throw)
            throw Exception(ErrorCodes::QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS,
                "The query result was not cached because the query contains a non-deterministic function."
                " Use setting `query_cache_nondeterministic_function_handling = 'save'` or `= 'ignore'` to cache the query result regardless, or omit caching");

        if (ast_contains_system_tables && system_table_handling == QueryResultCacheSystemTableHandling::Throw)
            throw Exception(ErrorCodes::QUERY_CACHE_USED_WITH_SYSTEM_TABLE,
                "The query result was not cached because the query contains a system table."
                " Use setting `query_cache_system_table_handling = 'save'` or `= 'ignore'` to cache the query result regardless, or omit caching");

        if ((!ast_contains_nondeterministic_functions || nondeterministic_function_handling == QueryResultCacheNondeterministicFunctionHandling::Save)
            && (!ast_contains_system_tables || system_table_handling == QueryResultCacheSystemTableHandling::Save))
            return true;
    }

    return false;
}

namespace
{

bool isQueryResultCacheRelatedSetting(const String & setting_name)
{
    return (setting_name.starts_with("query_cache_") || setting_name.ends_with("_query_cache")) && setting_name != "query_cache_tag";
}

/// Some additional settings are set for subqueries, they don't affect the result of SELECT queries,
/// however with them similar subqueries can sometimes mismatch, so ignore this settings.
bool isSubquerySpecificSetting(const String & setting_name)
{
    return setting_name == "use_structure_from_insertion_table_in_table_functions";
}

bool settingDoesNotAffectQueryResultCache(const String & setting_name)
{
    return setting_name == "log_comment"
        /// As of today, the output format settings only affect the final output.
        /// However, it should be taken with caution - we should not use these settings in deterministic SQL functions.
        || setting_name.starts_with("output_format_")
        /// This setting is used to tune the server response, but does not affect the query behavior.
        /// An example why it should not affect query caching:
        /// - if you run a query as usual, and then run the same query with asking the server
        /// for Content-Disposition: attachment to download the result.
        || setting_name == "http_response_headers";
}

bool isSettingIgnoredInQueryResultCache(const String & setting_name)
{
    return isQueryResultCacheRelatedSetting(setting_name) || settingDoesNotAffectQueryResultCache(setting_name) || isSubquerySpecificSetting(setting_name);
}

class RemoveQueryResultCacheSettingsMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        auto remove_query_cache_settings = [](ASTSetQuery * set_clause)
        {
            chassert(!set_clause->is_standalone);

            auto is_query_cache_related_setting = [](const auto & change)
            {
                return isSettingIgnoredInQueryResultCache(change.name);
            };

            std::erase_if(set_clause->changes, is_query_cache_related_setting);
        };

        if (auto * select_clause = ast->as<ASTSelectQuery>())
        {
            if (auto select_settings = select_clause->settings())
            {
                auto* set_clause = select_settings->as<ASTSetQuery>();

                remove_query_cache_settings(set_clause);

                /// Remove SETTINGS clause completely if it is empty
                /// E.g. SELECT 1 SETTINGS use_query_cache = true
                /// and SET use_query_cache = true; SELECT 1;
                /// will match.
                if (set_clause->changes.empty())
                    select_clause->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
            }
        }
        else
        {
            /// Output options don't affect the cached data, as we do caching on a result block level.
            ASTQueryWithOutput::resetOutputASTIfExist(*ast);
        }
    }


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
    RemoveQueryResultCacheSettingsMatcher::Data visitor_data;
    RemoveQueryResultCacheSettingsVisitor(visitor_data).visit(ast);

    return ast;
}

/// Strips ORDER BY, LIMIT and OFFSET clauses from ASTSelectQuery nodes so that queries differing only in these clauses
/// produce the same cache key.
void removeOrderByAndLimit(ASTPtr & ast)
{
    if (auto * select = ast->as<ASTSelectQuery>())
    {
        select->setExpression(ASTSelectQuery::Expression::ORDER_BY, nullptr);
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, nullptr);
        select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, nullptr);
    }
    else if (auto * union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        if (union_query->list_of_selects)
        {
            for (auto & child : union_query->list_of_selects->children)
                removeOrderByAndLimit(child);
        }
    }
}

bool isSettingIgnoredForBeforeLimitCache(const String & setting_name)
{
    return setting_name == "limit" || setting_name == "offset";
}

/// The Analyzer/Planner generates synthetic table aliases of the exact form `__table<N>`
/// (see `createUniqueAliasesIfNecessary.cpp`), where `N` is a non-empty sequence of digits.
/// Only strip aliases that match this exact pattern, otherwise user-visible identifiers
/// like `__table_prod` could be confused with a planner-generated alias and rewritten,
/// which would let unrelated queries collide in the subquery cache.
bool isPlannerGeneratedTableAlias(std::string_view name)
{
    constexpr std::string_view prefix = "__table";
    if (!name.starts_with(prefix))
        return false;
    auto suffix = name.substr(prefix.size());
    if (suffix.empty())
        return false;
    for (char c : suffix)
    {
        if (c < '0' || c > '9')
            return false;
    }
    return true;
}

class RemoveTableAliasMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * table_identifier = ast->as<ASTTableIdentifier>())
        {
            if (isPlannerGeneratedTableAlias(table_identifier->alias))
                table_identifier->setAlias("");
        }
        else if (auto * identifier = ast->as<ASTIdentifier>())
        {
            if (identifier->compound() && isPlannerGeneratedTableAlias(identifier->name_parts[0]))
            {
                /// Preserve every component after the planner alias, so identifiers like
                /// `__table1.nested.field` are normalized to `nested.field`, not just `nested`.
                std::vector<String> trimmed_parts(identifier->name_parts.begin() + 1, identifier->name_parts.end());
                auto new_identifier = make_intrusive<ASTIdentifier>(std::move(trimmed_parts));
                new_identifier->setAlias(identifier->tryGetAlias());
                ast = std::move(new_identifier);
            }
        }
        else if (auto * function = ast->as<ASTFunction>())
        {
            if (isPlannerGeneratedTableAlias(function->alias))
                function->setAlias("");
        }
    }
};

using RemoveTableAliasVisitor = InDepthNodeVisitor<RemoveTableAliasMatcher, true>;

/// QueryTree is used for caching subqueries, therefore ast has unnecessary aliases (__table1, __table2, ...)
/// Remove these aliases from ast before using it for caching.
ASTPtr removeTableAliases(ASTPtr ast)
{
    RemoveTableAliasVisitor::Data visitor_data;
    RemoveTableAliasVisitor(visitor_data).visit(ast);

    return ast;
}

/// When `pre_cleaned` is true, the caller has already cloned the AST and stripped planner table
/// aliases (`removeTableAliases`). Skip both steps to avoid mutating the original AST or running
/// `removeTableAliases` twice (which would over-strip chains like `__table1.__table2.x` -> `x`).
IASTHash calculateASTHash(
    ASTPtr ast,
    const String & current_database,
    const Settings & settings,
    bool is_subquery,
    bool pre_cleaned = false,
    bool strip_order_by_and_limit = false,
    const Block * pre_sort_header = nullptr)
{
    if (!pre_cleaned)
    {
        ast = ast->clone();
        if (is_subquery)
            ast = removeTableAliases(ast);
    }
    ast = removeQueryResultCacheSettings(ast);

    if (strip_order_by_and_limit)
        removeOrderByAndLimit(ast);

    /// Hash the AST, we must consider aliases (issue #56258)
    SipHash hash;
    ast->updateTreeHash(hash, /*ignore_aliases=*/ false);

    /// Also hash the database specified via SQL `USE db`, otherwise identifiers in same query (AST) may mean different columns in different
    /// tables (issue #64136)
    hash.update(current_database);

    /// Finally, hash the (changed) settings as they might affect the query result (e.g. think of settings `additional_table_filters` and `limit`).
    /// Note: allChanged() returns the settings in random order. Also, update()-s of the composite hash must be done in deterministic order.
    /// Therefore, collect and sort the settings first, then hash them.
    auto changed_settings = settings.changes();
    std::vector<std::pair<String, String>> changed_settings_sorted; /// (name, value)
    for (const auto & change : changed_settings)
    {
        const String & name = change.name;
        if (!isSettingIgnoredInQueryResultCache(name))
            if (!(strip_order_by_and_limit && isSettingIgnoredForBeforeLimitCache(name)))
                changed_settings_sorted.push_back({name, Settings::valueToStringUtil(change.name, change.value)});
    }

    /// The Planner forcibly sets `extremes`, `max_result_bytes`, `max_result_rows` for subqueries, which makes them
    /// appear in `settings.changes()`. Non-subquery executions of the same AST typically don't have these in their
    /// changed settings. To ensure cache hits between subquery writes and subquery reads, normalize by always
    /// including the default values of these settings for subqueries.
    if (is_subquery)
    {
        if (!changed_settings.tryGet("extremes"))
            changed_settings_sorted.push_back({"extremes", settings[Setting::extremes].toString()});

        if (!changed_settings.tryGet("max_result_bytes"))
            changed_settings_sorted.push_back({"max_result_bytes", settings[Setting::max_result_bytes].toString()});

        if (!changed_settings.tryGet("max_result_rows"))
            changed_settings_sorted.push_back({"max_result_rows", settings[Setting::max_result_rows].toString()});
    }

    std::sort(changed_settings_sorted.begin(), changed_settings_sorted.end(), [](auto & lhs, auto & rhs) { return lhs.first < rhs.first; });
    for (const auto & setting : changed_settings_sorted)
    {
        hash.update(setting.first);
        hash.update(setting.second);
    }

    /// When caching before LIMIT/ORDER BY, also hash the pre-sort header to ensure queries needing different column sets
    /// (due to different ORDER BY columns) get separate cache entries.
    if (strip_order_by_and_limit && pre_sort_header)
    {
        for (const auto & col : *pre_sort_header)
        {
            hash.update(col.name);
            hash.update(col.type->getName());
        }
    }

    return getSipHash128AsPair(hash);
}

String queryStringFromAST(ASTPtr ast)
{
    return ast->formatForLogging();
}

/// For subqueries, clones the AST once, strips aliases, and returns both hash and query string from the cleaned AST.
/// For non-subqueries, computes hash (which clones internally) and query string from the original AST.
std::pair<IASTHash, String> calculateASTHashAndQueryString(
    ASTPtr ast, const String & current_database, const Settings & settings, bool is_subquery)
{
    if (is_subquery)
    {
        auto cleaned = removeTableAliases(ast->clone());
        /// Get the query string before `calculateASTHash` mutates the AST (it strips cache-related SETTINGS).
        auto qs = queryStringFromAST(cleaned);
        auto hash = calculateASTHash(cleaned, current_database, settings, is_subquery, /*pre_cleaned=*/ true);
        return {hash, std::move(qs)};
    }
    auto hash = calculateASTHash(ast, current_database, settings, is_subquery);
    auto qs = queryStringFromAST(ast);
    return {hash, std::move(qs)};
}

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
    std::chrono::time_point<std::chrono::system_clock> created_at_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_,
    bool is_subquery_,
    bool strip_order_by_and_limit_,
    const Block * pre_sort_header_)
    : header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , created_at(created_at_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_id(query_id_)
    , tag(settings[Setting::query_cache_tag])
    , is_subquery(is_subquery_)
{
    if (strip_order_by_and_limit_)
    {
        ast_hash = calculateASTHash(ast_, current_database, settings, is_subquery_, false, true, pre_sort_header_);
        query_string = queryStringFromAST(ast_);
    }
    else
    {
        auto [hash, qs] = calculateASTHashAndQueryString(ast_, current_database, settings, is_subquery_);
        ast_hash = hash;
        query_string = std::move(qs);
    }
}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_subquery_,
    bool strip_order_by_and_limit_,
    const Block * pre_sort_header_)
    : QueryResultCache::Key(
            ast_,
            current_database,
            settings,
            std::make_shared<const Block>(Block{}),
            query_id_,
            user_id_,
            current_user_roles_,
            false,
            std::chrono::system_clock::from_time_t(1),
            std::chrono::system_clock::from_time_t(1),
            false,
            is_subquery_,
            strip_order_by_and_limit_,
            pre_sort_header_)
    /// ^^ dummy values for everything except AST, current database, query_id, user name/roles
{
}

QueryResultCache::Key::Key(
    IASTHash precomputed_hash,
    SharedHeader header_,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> created_at_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_,
    const String & tag_)
    : ast_hash(precomputed_hash)
    , header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , created_at(created_at_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_string("partial_result")
    , query_id(query_id_)
    , tag(tag_)
    , is_subquery(false)
{
}

QueryResultCache::Key::Key(
    IASTHash precomputed_hash,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    const String & tag_)
    : QueryResultCache::Key(
            precomputed_hash,
            std::make_shared<const Block>(Block{}),
            query_id_,
            user_id_,
            current_user_roles_,
            false,
            std::chrono::system_clock::from_time_t(1),
            std::chrono::system_clock::from_time_t(1),
            false,
            tag_)
{
}

QueryResultCache::Key::Key(
    IASTHash precomputed_hash,
    SharedHeader header_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> created_at_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_,
    const String & tag_,
    const String & query_string_,
    bool is_subquery_)
    : ast_hash(precomputed_hash)
    , header(header_)
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , created_at(created_at_)
    , expires_at(expires_at_)
    , is_compressed(is_compressed_)
    , query_string(query_string_)
    , query_id{}
    , tag(tag_)
    , is_subquery(is_subquery_)
{
}

bool QueryResultCache::Key::operator==(const Key & other) const
{
    return ast_hash == other.ast_hash && is_subquery == other.is_subquery;
}

size_t QueryResultCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.ast_hash.low64);
    hash.update(key.is_subquery);
    return hash.get64();
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

bool QueryResultCache::IsStale::operator()(const Key & key) const
{
    return (key.expires_at < std::chrono::system_clock::now());
};

QueryResultCacheWriter::QueryResultCacheWriter(
    Cache & cache_,
    QueryResultCache & result_cache_,
    const QueryResultCache::Key & key_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_query_runtime_,
    bool squash_partial_results_,
    size_t max_block_size_,
    size_t recompute_cost_)
    : cache(cache_)
    , key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , min_query_runtime(min_query_runtime_)
    , squash_partial_results(squash_partial_results_)
    , max_block_size(max_block_size_)
    , result_cache(result_cache_)
    , recompute_cost(recompute_cost_)
{
    if (auto entry = cache.getWithKey(key); entry.has_value() && !QueryResultCache::IsStale()(entry->key))
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
    , result_cache(other.result_cache)
    , recompute_cost(other.recompute_cost)
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

    ProfileEvents::increment(ProfileEvents::QueryCacheWrittenRows, chunk.getNumRows());
    ProfileEvents::increment(ProfileEvents::QueryCacheWrittenBytes, chunk.bytes());

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

            removeSpecialColumnRepresentations(chunk);
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

    /// Multiple StreamInQueryResultCacheTransform instances (for Main/Totals/Extremes streams) share
    /// the same writer. The first call finalizes; subsequent calls are no-ops. This is correct because
    /// all transforms buffer into the same query_result before any of them calls finalizeWrite.
    /// Early-exit paths below (min_query_runtime, duplicate key, max size) are intentional rejections
    /// that should not be retried by another transform.
    if (was_finalized.exchange(true))
        return;

    std::lock_guard lock(mutex);

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

    if (squash_partial_results)
    {
        /// Squash partial result chunks to chunks of size 'max_block_size' each. This costs some performance but provides a more natural
        /// compression of neither too small nor big blocks. Also, it will look like 'max_block_size' is respected when the query result is
        /// served later on from the query result cache.

        Chunks squashed_chunks;
        size_t rows_remaining_in_squashed = 0; /// how many further rows can the last squashed chunk consume until it reaches max_block_size

        for (auto & chunk : query_result->chunks)
        {
            removeSpecialColumnRepresentations(chunk);
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

    size_t new_entry_size_in_bytes = QueryResultCache::EntryWeight()(*query_result);
    size_t new_entry_size_in_rows = count_rows_in_chunks(*query_result);

    if ((new_entry_size_in_bytes > max_entry_size_in_bytes) || (new_entry_size_in_rows > max_entry_size_in_rows))
    {
        LOG_TRACE(logger, "Skipped insert because the query result is too big, query result size: {} (maximum size: {}), query result size in rows: {} (maximum size: {}), query: {}",
                formatReadableSizeWithBinarySuffix(new_entry_size_in_bytes, 0), formatReadableSizeWithBinarySuffix(max_entry_size_in_bytes, 0), new_entry_size_in_rows, max_entry_size_in_rows, doubleQuoteString(key.query_string));
        return;
    }

    if (recompute_cost > 0)
        result_cache.adaptiveEvict(new_entry_size_in_bytes);

    cache.set(key, query_result);

    if (recompute_cost > 0)
        result_cache.registerEvictionMetadata(key, recompute_cost, new_entry_size_in_bytes);

    LOG_TRACE(logger, "Stored query result of query {}", doubleQuoteString(key.query_string));
}

/// Creates a source processor which serves result chunks stored in the query result cache, and separate sources for optional totals/extremes.
void QueryResultCacheReader::buildSourceFromChunks(SharedHeader header, Chunks && chunks, const std::optional<Chunk> & totals, const std::optional<Chunk> & extremes)
{
    /// Some bookkeeping for profile events
    size_t total_rows = 0;
    size_t total_bytes = 0;
    for (const auto & chunk : chunks)
    {
        total_rows += chunk.getNumRows();
        total_bytes += chunk.bytes();
    }
    ProfileEvents::increment(ProfileEvents::QueryCacheReadRows, total_rows);
    ProfileEvents::increment(ProfileEvents::QueryCacheReadBytes, total_bytes);

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

QueryResultCacheReader::QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &)
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

    if (QueryResultCache::IsStale()(entry_key))
    {
        LOG_TRACE(logger, "Stale query result found for query {}", doubleQuoteString(key.query_string));
        return;
    }

    auto age = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - entry_key.created_at).count();
    ProfileEvents::increment(ProfileEvents::QueryCacheAgeSeconds, age);

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

    created_at = entry_key.created_at;
    expires_at = entry_key.expires_at;

    LOG_TRACE(logger, "Query result found for query {}", doubleQuoteString(key.query_string));
}

bool QueryResultCacheReader::hasCacheEntryForKey(bool update_profile_events) const
{
    bool has_entry = (source_from_chunks != nullptr);

    if (update_profile_events)
    {
        if (has_entry)
            ProfileEvents::increment(ProfileEvents::QueryCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::QueryCacheMisses);
    }

    return has_entry;
}


std::chrono::time_point<std::chrono::system_clock> QueryResultCacheReader::entryCreatedAt()
{
    chassert(hasCacheEntryForKey(false));
    return created_at;
}

std::chrono::time_point<std::chrono::system_clock> QueryResultCacheReader::entryExpiresAt()
{
    chassert(hasCacheEntryForKey(false));
    return expires_at;
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

QueryResultCache::QueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, EntryWeight, IsStale>>(
            CurrentMetrics::QueryCacheBytes, CurrentMetrics::QueryCacheEntries, std::make_unique<PerUserTTLCachePolicyUserQuota>()))
{
    updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_);
}

void QueryResultCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache.setMaxSizeInBytes(max_size_in_bytes);
    cache.setMaxCount(max_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
}

QueryResultCacheReader QueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return QueryResultCacheReader(cache, key, lock);
}

QueryResultCacheWriter QueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t max_query_result_cache_size_in_bytes_quota,
    size_t max_query_result_cache_entries_quota,
    size_t per_query_max_entry_size_in_bytes,
    size_t recompute_cost)
{
    if (key.user_id.has_value())
        cache.setQuotaForUser(*key.user_id, max_query_result_cache_size_in_bytes_quota, max_query_result_cache_entries_quota);

    std::lock_guard lock(mutex);
    size_t effective_max = max_entry_size_in_bytes;
    if (per_query_max_entry_size_in_bytes > 0 && per_query_max_entry_size_in_bytes < effective_max)
        effective_max = per_query_max_entry_size_in_bytes;
    return QueryResultCacheWriter(cache, *this, key, effective_max, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size, recompute_cost);
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
    eviction_metadata.clear();
}

void QueryResultCache::adaptiveEvict(size_t needed_bytes)
{
    std::lock_guard lock(mutex);
    if (!adaptive_eviction_enabled)
        return;

    while (cache.sizeInBytes() + needed_bytes > cache.maxSizeInBytes()
           || cache.count() + 1 > cache.maxCount())
    {
        if (eviction_metadata.empty())
            break;

        const Key * victim_key = nullptr;
        double lowest_priority = std::numeric_limits<double>::max();

        for (const auto & [k, meta] : eviction_metadata)
        {
            double p = meta.priority();
            if (p < lowest_priority)
            {
                lowest_priority = p;
                victim_key = &k;
            }
        }

        if (!victim_key)
            break;

        Key victim_copy = *victim_key;
        eviction_metadata.erase(victim_copy);
        cache.remove(victim_copy);
    }
}

void QueryResultCache::registerEvictionMetadata(const Key & key, size_t cost, size_t size_bytes)
{
    std::lock_guard lock(mutex);
    auto & meta = eviction_metadata[key];
    meta.recompute_cost = cost;
    meta.size_bytes = size_bytes;
    meta.hit_count = 0;
}

void QueryResultCache::recordCacheHit(const Key & key)
{
    std::lock_guard lock(mutex);
    auto it = eviction_metadata.find(key);
    if (it != eviction_metadata.end())
        ++it->second.hit_count;
}

void QueryResultCache::setAdaptiveEviction(bool enabled)
{
    std::lock_guard lock(mutex);
    adaptive_eviction_enabled = enabled;
}

size_t QueryResultCache::maxSizeInBytes() const
{
    return cache.maxSizeInBytes();
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

static constexpr std::string_view SNAPSHOT_MAGIC = "CHQCSNP";
static constexpr UInt32 SNAPSHOT_VERSION = 1;

void QueryResultCache::saveSnapshot(const std::string & path) const
{
    auto entries = cache.dump();
    if (entries.empty())
        return;

    auto logger = getLogger("QueryResultCache");

    std::string tmp_path = path + ".tmp";
    WriteBufferFromFile buf(tmp_path);

    writeString(SNAPSHOT_MAGIC, buf);
    writeBinaryLittleEndian(SNAPSHOT_VERSION, buf);
    writeBinaryLittleEndian(static_cast<UInt64>(entries.size()), buf);

    size_t written = 0;
    for (const auto & [entry_key, entry_mapped] : entries)
    {
        if (IsStale()(entry_key))
            continue;

        /// Key fields
        writeBinaryLittleEndian(entry_key.ast_hash.low64, buf);
        writeBinaryLittleEndian(entry_key.ast_hash.high64, buf);
        writeBinary(static_cast<UInt8>(entry_key.is_subquery), buf);
        writeBinary(static_cast<UInt8>(entry_key.is_shared), buf);
        writeBinary(static_cast<UInt8>(entry_key.is_compressed), buf);

        UInt8 has_user_id = entry_key.user_id.has_value() ? 1 : 0;
        writeBinary(has_user_id, buf);
        if (entry_key.user_id.has_value())
            writeBinaryLittleEndian(*entry_key.user_id, buf);

        writeVarUInt(entry_key.current_user_roles.size(), buf);
        for (const auto & role : entry_key.current_user_roles)
            writeBinaryLittleEndian(role, buf);

        auto created_ms = std::chrono::duration_cast<std::chrono::milliseconds>(entry_key.created_at.time_since_epoch()).count();
        auto expires_ms = std::chrono::duration_cast<std::chrono::milliseconds>(entry_key.expires_at.time_since_epoch()).count();
        writeBinaryLittleEndian(static_cast<Int64>(created_ms), buf);
        writeBinaryLittleEndian(static_cast<Int64>(expires_ms), buf);

        writeStringBinary(entry_key.query_string, buf);
        writeStringBinary(entry_key.tag, buf);

        /// Header as zero-row block
        Block header_block = *entry_key.header;
        header_block.clear();
        {
            auto header_shared = std::make_shared<const Block>(header_block);
            NativeWriter native(buf, 0, header_shared);
            native.write(*entry_key.header);
        }

        /// Entry chunks
        writeVarUInt(entry_mapped->chunks.size(), buf);
        for (const auto & chunk : entry_mapped->chunks)
        {
            Block data_block = entry_key.header->cloneWithColumns(chunk.getColumns());
            auto header_shared = std::make_shared<const Block>(*entry_key.header);
            NativeWriter native(buf, 0, header_shared);
            native.write(data_block);
        }

        UInt8 has_totals = entry_mapped->totals.has_value() ? 1 : 0;
        writeBinary(has_totals, buf);
        if (entry_mapped->totals.has_value())
        {
            Block totals_block = entry_key.header->cloneWithColumns(entry_mapped->totals->getColumns());
            auto header_shared = std::make_shared<const Block>(*entry_key.header);
            NativeWriter native(buf, 0, header_shared);
            native.write(totals_block);
        }

        UInt8 has_extremes = entry_mapped->extremes.has_value() ? 1 : 0;
        writeBinary(has_extremes, buf);
        if (entry_mapped->extremes.has_value())
        {
            Block extremes_block = entry_key.header->cloneWithColumns(entry_mapped->extremes->getColumns());
            auto header_shared = std::make_shared<const Block>(*entry_key.header);
            NativeWriter native(buf, 0, header_shared);
            native.write(extremes_block);
        }

        /// Eviction metadata
        {
            std::lock_guard lock(mutex);
            auto it = eviction_metadata.find(entry_key);
            if (it != eviction_metadata.end())
            {
                writeBinary(static_cast<UInt8>(1), buf);
                writeVarUInt(it->second.hit_count, buf);
                writeVarUInt(it->second.recompute_cost, buf);
                writeVarUInt(it->second.size_bytes, buf);
            }
            else
            {
                writeBinary(static_cast<UInt8>(0), buf);
            }
        }

        ++written;
    }

    buf.finalize();
    std::filesystem::rename(tmp_path, path);

    LOG_INFO(logger, "Saved query result cache snapshot: {} entries to {}", written, path);
}

void QueryResultCache::loadSnapshot(const std::string & path)
{
    auto logger = getLogger("QueryResultCache");

    if (!std::filesystem::exists(path))
    {
        LOG_DEBUG(logger, "No query result cache snapshot found at {}", path);
        return;
    }

    ReadBufferFromFile buf(path);

    String magic;
    magic.resize(SNAPSHOT_MAGIC.size());
    buf.readStrict(magic.data(), magic.size());
    if (magic != SNAPSHOT_MAGIC)
    {
        LOG_WARNING(logger, "Invalid snapshot magic in {}, skipping load", path);
        return;
    }

    UInt32 version = 0;
    readBinaryLittleEndian(version, buf);
    if (version > SNAPSHOT_VERSION)
    {
        LOG_WARNING(logger, "Snapshot version {} is newer than supported ({}), skipping load", version, SNAPSHOT_VERSION);
        return;
    }

    UInt64 entry_count = 0;
    readBinaryLittleEndian(entry_count, buf);

    auto now = std::chrono::system_clock::now();
    size_t loaded = 0;
    size_t skipped_stale = 0;

    for (UInt64 i = 0; i < entry_count && !buf.eof(); ++i)
    {
        try
        {
            /// Key fields
            UInt64 hash_low = 0, hash_high = 0;
            readBinaryLittleEndian(hash_low, buf);
            readBinaryLittleEndian(hash_high, buf);
            IASTHash ast_hash{hash_low, hash_high};

            UInt8 is_subquery_u8 = 0, is_shared_u8 = 0, is_compressed_u8 = 0;
            readBinary(is_subquery_u8, buf);
            readBinary(is_shared_u8, buf);
            readBinary(is_compressed_u8, buf);

            UInt8 has_user_id = 0;
            readBinary(has_user_id, buf);
            std::optional<UUID> user_id;
            if (has_user_id)
            {
                UUID uid{};
                readBinaryLittleEndian(uid, buf);
                user_id = uid;
            }

            UInt64 role_count = 0;
            readVarUInt(role_count, buf);
            std::vector<UUID> roles(role_count);
            for (auto & role : roles)
                readBinaryLittleEndian(role, buf);

            Int64 created_ms = 0, expires_ms = 0;
            readBinaryLittleEndian(created_ms, buf);
            readBinaryLittleEndian(expires_ms, buf);

            auto created_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(created_ms));
            auto expires_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(expires_ms));

            if (expires_at <= now)
            {
                /// Skip the rest of this entry — need to read through it to advance the buffer
                /// For simplicity, just skip stale entries by breaking early and noting it.
                /// Since the format has variable-length fields, we can't easily skip; read and discard.
                ++skipped_stale;
            }

            String query_string, tag;
            readStringBinary(query_string, buf);
            readStringBinary(tag, buf);

            /// Header
            NativeReader header_reader(buf, 0);
            Block header_block = header_reader.read();
            auto shared_header = std::make_shared<const Block>(std::move(header_block));

            /// Entry chunks
            UInt64 chunk_count = 0;
            readVarUInt(chunk_count, buf);

            Chunks chunks;
            for (UInt64 c = 0; c < chunk_count; ++c)
            {
                NativeReader data_reader(buf, 0);
                Block data_block = data_reader.read();
                chunks.emplace_back(data_block.getColumns(), data_block.rows());
            }

            /// Totals
            UInt8 has_totals = 0;
            readBinary(has_totals, buf);
            std::optional<Chunk> totals;
            if (has_totals)
            {
                NativeReader totals_reader(buf, 0);
                Block totals_block = totals_reader.read();
                totals = Chunk(totals_block.getColumns(), totals_block.rows());
            }

            /// Extremes
            UInt8 has_extremes = 0;
            readBinary(has_extremes, buf);
            std::optional<Chunk> extremes;
            if (has_extremes)
            {
                NativeReader extremes_reader(buf, 0);
                Block extremes_block = extremes_reader.read();
                extremes = Chunk(extremes_block.getColumns(), extremes_block.rows());
            }

            /// Eviction metadata
            UInt8 has_meta = 0;
            readBinary(has_meta, buf);
            size_t hit_count = 0, recompute_cost = 0, size_bytes = 0;
            if (has_meta)
            {
                readVarUInt(hit_count, buf);
                readVarUInt(recompute_cost, buf);
                readVarUInt(size_bytes, buf);
            }

            if (expires_at <= now)
                continue;

            /// Re-compress columns if the original entry was compressed
            if (is_compressed_u8)
            {
                for (auto & chunk : chunks)
                {
                    const Columns & columns = chunk.getColumns();
                    Columns compressed;
                    for (const auto & col : columns)
                        compressed.push_back(col->compress(false));
                    chunk = Chunk(std::move(compressed), chunk.getNumRows());
                }
            }

            Key key(ast_hash, shared_header, user_id, roles,
                    is_shared_u8 != 0, created_at, expires_at, is_compressed_u8 != 0,
                    tag, query_string, is_subquery_u8 != 0);

            auto entry = std::make_shared<Entry>();
            entry->chunks = std::move(chunks);
            entry->totals = std::move(totals);
            entry->extremes = std::move(extremes);

            cache.set(key, entry);

            if (has_meta && recompute_cost > 0)
            {
                std::lock_guard lock(mutex);
                auto & meta = eviction_metadata[key];
                meta.hit_count = hit_count;
                meta.recompute_cost = recompute_cost;
                meta.size_bytes = size_bytes;
            }

            ++loaded;
        }
        catch (...)
        {
            LOG_WARNING(logger, "Error reading snapshot entry {}/{}, stopping load: {}", i, entry_count, getCurrentExceptionMessage(false));
            break;
        }
    }

    LOG_INFO(logger, "Loaded query result cache snapshot: {} entries from {} ({} stale entries skipped)", loaded, path, skipped_stale);
}

}
