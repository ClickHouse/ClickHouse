#include <Interpreters/Cache/QueryResultCache.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>
#include <base/defines.h> /// chassert
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>

#include <new>


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

bool hasSameUDTStorageDependencyProof(const QueryResultCache::Key & lhs, const QueryResultCache::Key & rhs)
{
    return lhs.udt_storage_dependency_proof == rhs.udt_storage_dependency_proof;
}

bool validateUDTStorageDependencyProof(
    const UDT::QueryResultCacheStorageDependencyProof & proof,
    UDT::QueryResultCacheContextualSinkCandidateMask current_contextual_sink_candidates,
    const ContextPtr & context)
{
    if (proof.contextual_sink_candidates != current_contextual_sink_candidates
        || current_contextual_sink_candidates & ~UDT::all_query_result_cache_contextual_sink_candidates)
        return false;

    constexpr auto contextual_activation_capabilities
        = UDT::semanticCapabilityBit(UDT::SemanticCapability::Input) | UDT::semanticCapabilityBit(UDT::SemanticCapability::ValueChecks);

    for (const auto & dependency : proof.dependencies)
    {
        StoragePtr storage;
        try
        {
            storage = DatabaseCatalog::instance().tryGetTable(dependency.storage_id, context);
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & error)
        {
            if (UDT::isUDTResourceOrControlExceptionCode(error.code()))
                throw;
            return false;
        }
        catch (...)
        {
            return false;
        }
        if (!storage)
            return false;

        const auto current_id = storage->getStorageID();
        if (std::tie(current_id.database_name, current_id.table_name, current_id.uuid)
            != std::tie(dependency.storage_id.database_name, dependency.storage_id.table_name, dependency.storage_id.uuid))
            return false;

        const auto current_kind = storage->isView() ? UDT::QueryResultCacheStorageKind::View : UDT::QueryResultCacheStorageKind::Storage;
        if (current_kind != dependency.kind || storage->getName() != dependency.engine_name)
            return false;

        try
        {
            const auto metadata = storage->getInMemoryMetadataPtr(context, false);
            if (!metadata)
                return false;
            /// Cache-hit validation must not reuse a query-context-pinned data
            /// snapshot: it needs a fresh lightweight storage boundary whose
            /// continuation evidence names the exact current authority root.
            const auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
            storage_snapshot->metadata->validateBoundUDTReferences();
            const auto & current_bound = storage_snapshot->metadata->getBoundUDTReferences();

            const auto database = DatabaseCatalog::instance().tryGetDatabase(current_id.database_name);
            const auto atomic_database = std::dynamic_pointer_cast<DatabaseAtomic>(database);
            if (!dependency.udt_binding)
            {
                if (current_bound)
                    return false;
                /// The authority/root is published before a mapped ALTER
                /// replaces storage-local metadata.  Close that exact seam for
                /// a proof which was collected from a physical-only image.
                if (atomic_database && atomic_database->hasDatabaseOwnedUDTObjectForQueryCache(current_id.uuid))
                    return false;
                continue;
            }

            if (!current_bound || !atomic_database)
                return false;
            UDT::QueryResultCacheUDTBindingIdentity current_binding{
                .object = current_bound->getObject(),
                .object_schema_revision = current_bound->getObjectSchemaRevision(),
                .sidecar_hash = current_bound->getSidecarHash(),
                .physical_schema_fingerprint = current_bound->getPhysicalSchemaFingerprint(),
                .semantic_capabilities = current_bound->getSemanticCapabilities(),
                .authority_root = {},
            };
            const auto unknown_capabilities = static_cast<UDT::SemanticCapabilityMask>(
                current_binding.semantic_capabilities & static_cast<UDT::SemanticCapabilityMask>(~UDT::all_semantic_capabilities));
            if (unknown_capabilities)
                return false;

            /// A contextual candidate with any object-level activation remains
            /// analyzer-owned: only the selected immutable use can prove it is
            /// physical.  Aggregate-negative mapped objects are a safe fast
            /// negative and retain ordinary physical query-cache hits.
            if (current_contextual_sink_candidates && (current_binding.semantic_capabilities & contextual_activation_capabilities) != 0)
                return false;

            const auto & continuation = storage_snapshot->udt_read_continuation_evidence;
            if (!continuation)
                return false;
            current_binding.authority_root = continuation->getPinnedRoot();
            const UDT::AuthorityObjectImageIdentity expected_image{
                .object = current_binding.object,
                .object_schema_revision = current_binding.object_schema_revision,
                .sidecar_hash = current_binding.sidecar_hash,
                .physical_schema_fingerprint = current_binding.physical_schema_fingerprint,
            };
            const auto & verification_stamp = continuation->getVerificationStamp();
            if (continuation->getObjectImage() != expected_image || !verification_stamp
                || verification_stamp->getVerifiedObject() != expected_image
                || verification_stamp->getVerifiedRoot() != continuation->getPinnedRoot().authority_root
                || current_binding != *dependency.udt_binding)
                return false;
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & error)
        {
            if (UDT::isUDTResourceOrControlExceptionCode(error.code()))
                throw;
            return false;
        }
        catch (...)
        {
            return false;
        }
    }
    return true;
}

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
            /// The `eval` table function hides its real query inside an opaque string argument, so the
            /// generated query cannot be inspected here. Treat it as non-deterministic (and, in
            /// HasSystemTablesMatcher, as touching a system table) to keep such queries out of the cache,
            /// e.g. `eval('SELECT now()')` must not be cached as if it were deterministic.
            if (function->name == "eval")
            {
                data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto func = FunctionFactory::instance().tryGet(function->name, data.context))
            {
                if (!func->isDeterministic())
                    data.has_non_deterministic_functions = true;
                return;
            }
            if (const auto udf_sql = UserDefinedSQLFunctionFactory::instance().tryGet(function->name))
            {
                /// Driver-created executable functions are also persisted in the SQL-object storage,
                /// but their determinism is described by the generated executable UDF configuration checked below.
                if (!udf_sql->as<ASTCreateFunctionWithDriverQuery>())
                {
                    /// ClickHouse currently doesn't know if SQL-based UDFs are deterministic or not. We must assume they are non-deterministic.
                    data.has_non_deterministic_functions = true;
                    return;
                }
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
            /// See HasNonDeterministicFunctionsMatcher: the query behind `eval` is opaque here, so
            /// conservatively assume it may read a system table (e.g. `eval('SELECT * FROM system.processes')`).
            if (function->name == "eval")
            {
                data.has_system_tables = true;
                return;
            }
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
static bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context)
{
    HasNonDeterministicFunctionsMatcher::Data finder_data{context};
    HasNonDeterministicFunctionsVisitor(finder_data).visit(ast);
    return finder_data.has_non_deterministic_functions;
}

/// Does AST contain system tables like "system.processes"?
static bool astContainsSystemTables(ASTPtr ast, ContextPtr context)
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
        {
            if (context->isQueryResultCacheBlockedByUDT())
                return false;
            if (const auto collector = context->getUDTQueryResultCacheStorageDependencyCollector();
                collector && !collector->snapshotIfComplete())
                return false;
            return true;
        }
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
IASTHash calculateASTHash(ASTPtr ast, const String & current_database, const Settings & settings, const bool is_subquery, const bool pre_cleaned = false)
{
    if (!pre_cleaned)
    {
        ast = ast->clone();
        if (is_subquery)
            ast = removeTableAliases(ast);
    }
    ast = removeQueryResultCacheSettings(ast);

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
        if (!isSettingIgnoredInQueryResultCache(name)) /// see removeQueryResultCacheSettings() and isSubquerySpecificSetting() for why this is a good idea
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
    std::optional<UDT::QueryResultCacheStorageDependencyProof> udt_storage_dependency_proof_)
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
    , udt_storage_dependency_proof(std::move(udt_storage_dependency_proof_))
{
    /// For subqueries, both hashing and display need a cloned AST with table aliases stripped.
    /// Compute both from a single clone via `calculateASTHashAndQueryString`.
    auto [hash, qs] = calculateASTHashAndQueryString(ast_, current_database, settings, is_subquery_);
    ast_hash = hash;
    query_string = std::move(qs);
}

QueryResultCache::Key::Key(
    ASTPtr ast_,
    const String & current_database,
    const Settings & settings,
    const String & query_id_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_subquery_)
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
            is_subquery_)
    /// ^^ dummy values for everything except AST, current database, query_id, user name/roles
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
    const QueryResultCache::Key & key_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
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
    if (auto entry = cache.getWithKey(key);
        entry.has_value() && !QueryResultCache::IsStale()(entry->key) && hasSameUDTStorageDependencyProof(entry->key, key))
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

    if (auto entry = cache.getWithKey(key);
        entry.has_value() && !QueryResultCache::IsStale()(entry->key) && hasSameUDTStorageDependencyProof(entry->key, key))
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

    /// CacheBase keeps the original non-hashed key object when set() replaces
    /// an equal key. Remove first so the new dependency payload is retained.
    cache.remove(key);
    cache.set(key, query_result);

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

QueryResultCacheReader::QueryResultCacheReader(Cache & cache_, const Cache::Key & key, const ContextPtr & context)
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

    const auto dependency_collector = context->getUDTQueryResultCacheStorageDependencyCollector();
    const bool current_query_requires_udt_storage_dependency_proof = dependency_collector != nullptr;
    if ((entry_key.udt_storage_dependency_proof && !current_query_requires_udt_storage_dependency_proof)
        || (entry_key.udt_storage_dependency_proof
            && !validateUDTStorageDependencyProof(
                *entry_key.udt_storage_dependency_proof, dependency_collector->getContextualSinkCandidates(), context))
        || (!entry_key.udt_storage_dependency_proof && current_query_requires_udt_storage_dependency_proof))
    {
        LOG_TRACE(logger, "Query result has no current exact UDT storage dependency proof");
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

QueryResultCacheReader QueryResultCache::createReader(const Key & key, const ContextPtr & context)
{
    /// Cache has its own synchronization and getWithKey() returns an owning
    /// snapshot.  Do not hold the configuration/times-executed mutex while
    /// validating catalog metadata or cloning/decompressing result chunks.
    return QueryResultCacheReader(cache, key, context);
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
    return QueryResultCacheWriter(cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
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

}
