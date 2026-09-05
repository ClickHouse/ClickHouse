#include <Client/SchemaDumper.h>

#include <Databases/enableAllExperimentalSettings.h>
#include <Client/IServerConnection.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Core/Protocol.h>
#include <Core/QualifiedTableName.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Databases/TablesDependencyGraph.h>
#include <Functions/FunctionFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <base/EnumReflection.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <string_view>
#include <tuple>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_WRITE_TO_FILE;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Sends `query`, calling `handle_block` for each received `Data` packet, in order.
void executeQuery(
    IServerConnection & connection,
    const ConnectionTimeouts & timeouts,
    const ClientInfo & client_info,
    const String & query,
    const std::function<void(const Block &)> & handle_block,
    const Settings & base_settings,
    bool show_datalake_catalogs = false,
    bool show_remote_databases = false)
{
    /// `system.tables` hides catalog/remote databases without these; in the query text because
    /// LocalConnection drops the settings argument.
    String query_to_send = query;
    /// Only when needed: constraints reject even a no-op change, so a redundant clause
    /// fails under readonly=1 and under a profile pinning the setting CONST on.
    std::string settings_clause;
    if (show_datalake_catalogs)
        settings_clause += "show_data_lake_catalogs_in_system_tables = 1";
    if (show_remote_databases)
    {
        if (!settings_clause.empty())
            settings_clause += ", ";
        settings_clause += "show_remote_databases_in_system_tables = 1";
    }
    if (!settings_clause.empty())
        query_to_send += " SETTINGS " + settings_clause;

    connection.sendQuery(
        timeouts, query_to_send, {} /* query_parameters */, "" /* query_id */, QueryProcessingStage::Complete,
        &base_settings, &client_info, false, {} /* external_roles*/, {});

    while (true)
    {
        Packet packet = connection.receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::Data:
                handle_block(packet.block);
                continue;

            case Protocol::Server::TimezoneUpdate:
            case Protocol::Server::Progress:
            case Protocol::Server::ProfileInfo:
            case Protocol::Server::Totals:
            case Protocol::Server::Extremes:
            case Protocol::Server::Log:
            case Protocol::Server::ProfileEvents:
                continue;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                return;

            case Protocol::Server::EndOfStream:
                return;

            default:
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}",
                    packet.type, connection.getDescription());
        }
    }
}

/// Splits a comma-separated list, supporting whitespace and doubled-backquote escaping.
std::vector<String> splitDatabaseList(const String & list)
{
    std::vector<String> result;
    size_t pos = 0;

    while (pos <= list.size())
    {
        while (pos < list.size() && isWhitespaceASCII(list[pos]))
            ++pos;

        if (pos < list.size() && list[pos] == '`')
        {
            String name;
            ++pos;
            while (true)
            {
                if (pos >= list.size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unterminated backquoted database name in list: {}", list);
                if (list[pos] == '`')
                {
                    /// A doubled backquote is an escaped one.
                    if (pos + 1 < list.size() && list[pos + 1] == '`')
                    {
                        name += '`';
                        pos += 2;
                        continue;
                    }
                    ++pos;
                    break;
                }
                name += list[pos++];
            }

            while (pos < list.size() && isWhitespaceASCII(list[pos]))
                ++pos;
            if (pos < list.size() && list[pos] != ',')
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unexpected text after a backquoted database name in list: {}", list);

            if (!name.empty())
                result.push_back(std::move(name));
            if (pos >= list.size())
                break;
            ++pos;
            continue;
        }

        size_t comma = list.find(',', pos);
        size_t end = (comma == String::npos) ? list.size() : comma;

        size_t begin = pos;
        size_t trimmed_end = end;
        while (trimmed_end > begin && isWhitespaceASCII(list[trimmed_end - 1]))
            --trimmed_end;

        if (trimmed_end > begin)
            result.emplace_back(list, begin, trimmed_end - begin);

        if (comma == String::npos)
            break;
        pos = comma + 1;
    }
    return result;
}

/// Fetches all values of a single-column `String` query result, in row order.
std::vector<String> fetchStringColumn(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info, const String & query,
    const Settings & base_settings)
{
    std::vector<String> result;
    executeQuery(connection, timeouts, client_info, query, [&](const Block & block)
    {
        if (block.empty())
            return;

        const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);
        for (size_t i = 0; i < column.size(); ++i)
            result.emplace_back(column[i].safeGet<String>());
    }, base_settings);
    return result;
}

/// Reads two parallel `Array(String)` fields as (first, second) pairs.
std::vector<std::pair<String, String>> readPairArray(const Field & firsts_field, const Field & seconds_field)
{
    const Array & firsts = firsts_field.safeGet<Array>();
    const Array & seconds = seconds_field.safeGet<Array>();
    if (firsts.size() != seconds.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatched dependency array sizes");

    std::vector<std::pair<String, String>> result;
    for (size_t i = 0; i < firsts.size(); ++i)
        result.emplace_back(firsts[i].safeGet<String>(), seconds[i].safeGet<String>());
    return result;
}

/// One dumped table, with enough dependency information to order it for replay.
struct TableInfo
{
    String database;
    String name;
    String create_query;
    std::vector<std::pair<String, String>> dependencies; /// (database, name) pairs this table must be created after
    /// Database-less references no dumped database contains. They resolve against `database` on
    /// replay, which does not contain them either, so the dump cannot create them.
    std::vector<String> unresolved_references;
};

/// One `system.tables` row as fetched, before implicit storage tables are filtered out and the
/// final dependency list per table is assembled.
struct RawTableRow
{
    String database;
    String name;
    String engine;
    String create_query;
    String as_select;
    UUID uuid;
    std::vector<std::pair<String, String>> loading_dependencies;
    std::vector<std::pair<String, String>> dependents; /// views/dictionaries that read from this table
    std::vector<String> unresolved_references;
    String target_database; /// materialized view only: its `TO` target, explicit or implicit
    String target_table;
};

/// Detects whether `system.tables` queries for `database_list` need external-database visibility
/// settings appended. `system.databases` always lists DataLakeCatalog and remote (MySQL /
/// PostgreSQL) engines, but `system.tables` hides their tables unless the corresponding setting
/// is enabled. Each setting is only appended when the server does not already provide it, to
/// avoid a redundant SETTINGS clause that constraints or CONST profiles could reject.
struct ExternalTableVisibility
{
    bool show_datalake_catalogs = false;
    bool show_remote_databases = false;
};

ExternalTableVisibility detectExternalTableVisibility(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info,
    const String & database_list, const Settings & base_settings)
{
    auto datalake_databases = fetchStringColumn(connection, timeouts, client_info,
        "SELECT name FROM system.databases WHERE engine = 'DataLakeCatalog' AND name IN (" + database_list + ")", base_settings);

    auto remote_databases = fetchStringColumn(connection, timeouts, client_info,
        "SELECT name FROM system.databases WHERE engine IN ('MySQL', 'PostgreSQL') AND name IN (" + database_list + ")", base_settings);

    bool session_shows_catalogs = false;
    if (!datalake_databases.empty())
        session_shows_catalogs = fetchStringColumn(connection, timeouts, client_info,
            "SELECT toString(getSetting('show_data_lake_catalogs_in_system_tables') = 1)", base_settings).at(0) == "1";

    bool session_shows_remote = false;
    if (!remote_databases.empty())
        session_shows_remote = fetchStringColumn(connection, timeouts, client_info,
            "SELECT toString(getSetting('show_remote_databases_in_system_tables') = 1)", base_settings).at(0) == "1";

    return { !datalake_databases.empty() && !session_shows_catalogs, !remote_databases.empty() && !session_shows_remote };
}

/// Fetches every non-temporary table in `databases`, minus orphaned mid-refresh leftovers of a
/// materialized view's implicit storage (`.tmp.inner_id.*`/`.tmp.inner.*`), filtered by name.
std::vector<RawTableRow> fetchRawRows(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info, const std::vector<String> & databases,
    const Settings & base_settings)
{
    String database_list;
    for (const auto & database : databases)
    {
        if (!database_list.empty())
            database_list += ", ";
        database_list += quoteString(database);
    }

    String query = "SELECT database, name, engine, create_table_query, as_select, uuid, "
        "loading_dependencies_database, loading_dependencies_table, "
        "dependencies_database, dependencies_table, target_database, target_table "
        "FROM system.tables WHERE database IN (" + database_list + ") AND NOT is_temporary "
        "ORDER BY database, name";

    auto visibility = detectExternalTableVisibility(connection, timeouts, client_info, database_list, base_settings);

    std::vector<RawTableRow> rows;
    executeQuery(connection, timeouts, client_info, query, [&](const Block & block)
    {
        if (block.empty())
            return;

        const ColumnString & database_column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);
        const ColumnString & name_column = typeid_cast<const ColumnString &>(*block.getByPosition(1).column);
        const ColumnString & engine_column = typeid_cast<const ColumnString &>(*block.getByPosition(2).column);
        const ColumnString & create_query_column = typeid_cast<const ColumnString &>(*block.getByPosition(3).column);
        const ColumnString & as_select_column = typeid_cast<const ColumnString &>(*block.getByPosition(4).column);
        const ColumnUUID & uuid_column = typeid_cast<const ColumnUUID &>(*block.getByPosition(5).column);
        const auto & loading_deps_database_column = *block.getByPosition(6).column;
        const auto & loading_deps_table_column = *block.getByPosition(7).column;
        const auto & dependents_database_column = *block.getByPosition(8).column;
        const auto & dependents_table_column = *block.getByPosition(9).column;
        const ColumnString & target_database_column = typeid_cast<const ColumnString &>(*block.getByPosition(10).column);
        const ColumnString & target_table_column = typeid_cast<const ColumnString &>(*block.getByPosition(11).column);

        for (size_t i = 0; i < block.rows(); ++i)
        {
            RawTableRow row;
            row.database = database_column[i].safeGet<String>();
            row.name = name_column[i].safeGet<String>();
            row.engine = engine_column[i].safeGet<String>();
            row.create_query = create_query_column[i].safeGet<String>();
            row.as_select = as_select_column[i].safeGet<String>();
            row.uuid = uuid_column[i].safeGet<UUID>();
            row.loading_dependencies = readPairArray(loading_deps_database_column[i], loading_deps_table_column[i]);
            row.dependents = readPairArray(dependents_database_column[i], dependents_table_column[i]);
            row.target_database = target_database_column[i].safeGet<String>();
            row.target_table = target_table_column[i].safeGet<String>();
            rows.push_back(std::move(row));
        }
    }, base_settings, visibility.show_datalake_catalogs, visibility.show_remote_databases);
    return rows;
}

/// True for a materialized view's own auto-generated storage table name (with no explicit `TO`).
bool looksLikeGeneratedInnerTableName(const String & name)
{
    return name.starts_with(".inner_id.") || name.starts_with(".inner.");
}

struct CreateTargets
{
    bool external_materialized_view = false;
    std::vector<StorageID> explicit_tables;
    std::set<ViewTarget::Kind> explicit_kinds;
    std::set<ViewTarget::Kind> mentioned_kinds;
};

/// Parses all target metadata once for a materialized view or TimeSeries table.
CreateTargets parseCreateTargets(const RawTableRow & row)
{
    ASTPtr ast;
    try
    {
        ParserCreateQuery create_parser;
        ast = parseQuery(create_parser, row.create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot parse the stored CREATE for {}.{} to resolve its targets for --dump-schema: {}",
            row.database, row.name, e.message());
    }

    const auto * create = ast->as<ASTCreateQuery>();
    if (!create)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stored CREATE for {}.{} is not an ASTCreateQuery", row.database, row.name);

    CreateTargets result;
    result.external_materialized_view = create->is_materialized_view_with_external_target();
    if (create->targets)
        for (const auto & target : create->targets->targets)
        {
            result.mentioned_kinds.insert(target.kind);
            if (!target.table_id.table_name.empty())
            {
                result.explicit_tables.push_back(target.table_id);
                result.explicit_kinds.insert(target.kind);
            }
        }
    return result;
}

/// Which clusters the connected server defines and which of those have local replicas: the gate
/// `DDLDependencyVisitor::visitRemoteFunction` applies to `cluster`/`clusterAllReplicas` needs both.
struct ClusterLocality
{
    std::set<String> known;
    std::set<String> local;
    /// `Context::tryGetCluster` expands macros before looking a cluster up, but a stored definition
    /// keeps the placeholder text, so the same expansion has to happen before the lookups here.
    std::map<String, String> macros;
    /// For mirroring the server's constant folding of `cluster*` name/table arguments.
    ContextPtr context;
};

/// Expands server macros to the same fixed point and depth cap as `Macros::expand`.
String expandClusterMacros(const String & name, const std::map<String, String> & macros)
{
    String current = name;
    for (size_t level = 0; level < 10 && current.contains('{'); ++level)
    {
        String result;
        result.reserve(current.size());
        bool substituted = false;
        for (size_t i = 0; i < current.size();)
        {
            if (current[i] == '{')
            {
                if (size_t close = current.find('}', i + 1); close != String::npos)
                {
                    if (auto it = macros.find(current.substr(i + 1, close - i - 1)); it != macros.end())
                    {
                        result += it->second;
                        i = close + 1;
                        substituted = true;
                        continue;
                    }
                }
            }
            result += current[i++];
        }
        current = std::move(result);
        if (!substituted)
            break;
    }
    return current;
}

bool isClusterTableFunctionName(const String & name)
{
    return name == "cluster" || name == "clusterAllReplicas";
}

/// Rejects expressions that would fold against the dump session or machine.
/// Function flags cover aliases and nested session/server constants without a name list.
bool dependsOnUnstoredContext(const IAST & node, const ContextPtr & context)
{
    if (const auto * function = node.as<ASTFunction>())
    {
        /// A nested table function - `cluster(c, merge(db, re))` - is dispatched, never scalar-folded,
        /// so only its arguments can carry unstored context; the child walk below covers those.
        if (!TableFunctionFactory::instance().isTableFunctionName(function->name))
        {
            auto resolver = FunctionFactory::instance().tryGet(function->name, context);
            if (!resolver || !resolver->isDeterministic() || resolver->isServerConstant())
                return true;
        }
    }
    for (const auto & child : node.children)
        if (child && dependsOnUnstoredContext(*child, context))
            return true;
    return false;
}

/// The cluster a `cluster*` call names, read the way the server's `tryGetClusterNameFromArgument`
/// does; fails loudly when the name is computed or the connected server does not define the cluster.
String resolveClusterOfFunction(const ASTFunction & function, const ClusterLocality & clusters)
{
    std::optional<String> cluster_name;
    const auto & args = function.arguments->children;
    if (!args.empty())
    {
        cluster_name = tryGetClusterName(*args[0]);
        if (!cluster_name)
            if (const auto * literal = args[0]->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                cluster_name = literal->value.safeGet<String>();
        if (!cluster_name)
        {
            /// The server also accepts a constant expression here (`evaluateConstantExpressionOrIdentifierAsLiteral`),
            /// but only one that folds the same way outside its session: the refusal below reports the rest.
            if (!dependsOnUnstoredContext(*args[0], clusters.context))
            {
                try
                {
                    auto evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(args[0]->clone(), clusters.context);
                    if (const auto * literal = evaluated->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                        cluster_name = literal->value.safeGet<String>();
                }
                catch (Exception &) // NOLINT(bugprone-empty-catch)
                {
                    /// Not a constant: the refusal below reports it.
                }
            }
        }
    }
    if (!cluster_name)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot statically resolve the cluster name of {} for --dump-schema: "
            "whether its table reference is a local dependency depends on the cluster's local replicas",
            function.formatForErrorMessage());
    if (cluster_name->contains('{'))
        cluster_name = expandClusterMacros(*cluster_name, clusters.macros);
    if (!clusters.known.contains(*cluster_name))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot resolve cluster {} of {} on the connected server for --dump-schema: "
            "whether its table reference is a local dependency depends on the cluster's local replicas",
            backQuoteIfNeed(*cluster_name), function.formatForErrorMessage());
    return *cluster_name;
}

/// Returns the argument subtree that cannot contain local dependencies for `remote` or non-local `cluster`.
const IAST * remoteFunctionArgumentsToSkip(const IAST & node, const ClusterLocality & clusters)
{
    const auto * function = node.as<ASTFunction>();
    if (!function || !function->arguments)
        return nullptr;
    if (function->name == "remote" || function->name == "remoteSecure")
        return function->arguments.get();
    if (isClusterTableFunctionName(function->name))
    {
        /// Nothing a non-local cluster names is read on this instance, so the whole argument list goes,
        /// like `remote`: callers compare against direct children, and an argument node is not one.
        if (function->arguments->children.size() >= 2
            && !clusters.local.contains(resolveClusterOfFunction(*function, clusters)))
            return function->arguments.get();
    }
    return nullptr;
}

template <typename Visitor>
void visitLocalAST(const IAST * node, const ClusterLocality & clusters, Visitor && visitor)
{
    if (!node)
        return;
    visitor(*node);
    const IAST * skip = remoteFunctionArgumentsToSkip(*node, clusters);
    for (const auto & child : node->children)
        if (child.get() != skip)
            visitLocalAST(child.get(), clusters, visitor);
}

/// Reads a table identifier or `db.name` string; an empty database is resolved by the caller.
std::optional<std::pair<String, String>> tryGetQualifiedNameFromFunctionArgument(const ASTFunction & function, size_t arg_idx)
{
    if (!function.arguments || arg_idx >= function.arguments->children.size())
        return std::nullopt;

    const ASTPtr & arg = function.arguments->children[arg_idx];

    if (const auto * identifier = arg->as<ASTIdentifier>())
    {
        auto table_id = identifier->createTable();
        if (!table_id)
            return std::nullopt;
        /// An empty database means "unqualified": the consumer resolves it against the owning
        /// object's database, and refuses only when that cannot satisfy it within the dump set.
        return std::pair(table_id->getDatabaseName(), table_id->shortName());
    }

    if (const auto * literal = arg->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
    {
        auto qualified = QualifiedTableName::tryParseFromString(literal->value.safeGet<String>());
        if (!qualified)
            return std::nullopt;
        return std::pair(qualified->database, qualified->table);
    }

    return std::nullopt;
}

/// Reads a `merge`/`loop` argument as a string literal, or unwraps a `REGEXP('...')` wrapper (the
/// syntax `TableFunctionMerge` accepts for a database-name regexp) into its own literal.
std::optional<String> tryGetStringLiteralOrRegexpWrapper(const ASTPtr & arg, bool & is_regexp)
{
    is_regexp = false;
    if (const auto * literal = arg->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        return literal->value.safeGet<String>();
    if (const auto * function = arg->as<ASTFunction>(); function && function->name == "REGEXP" && function->arguments
        && function->arguments->children.size() == 1)
    {
        if (const auto * inner = function->arguments->children[0]->as<ASTLiteral>(); inner && inner->value.getType() == Field::Types::String)
        {
            is_regexp = true;
            return inner->value.safeGet<String>();
        }
    }
    return std::nullopt;
}

/// Collects local `merge` and `loop` references, resolving empty `merge` databases to the owner.
void collectMergeAndLoopReferences(
    const IAST & node,
    const std::map<String, std::set<String>> & table_names_by_db,
    const std::set<String> & undumped_databases,
    const std::map<String, std::set<String>> & undumped_table_names_by_db,
    const String & owning_database,
    std::vector<std::pair<String, String>> & out)
{
    if (const auto * function = node.as<ASTFunction>(); function && function->arguments)
    {
        const auto & args = function->arguments->children;
        if (function->name == "merge" && args.size() == 2)
        {
            bool database_is_regexp = false;
            std::optional<String> database_pattern = tryGetStringLiteralOrRegexpWrapper(args[0], database_is_regexp);
            bool table_is_regexp = false;
            std::optional<String> table_pattern = tryGetStringLiteralOrRegexpWrapper(args[1], table_is_regexp);

            if (database_pattern && table_pattern)
            {
                String merge_ambiguous_dbs;
                bool merge_owning_matches = true;
                try
                {
                    std::vector<String> matched_databases;
                    if (database_is_regexp)
                    {
                        OptimizedRegularExpression database_regexp(*database_pattern);
                        for (const auto & [db, tables] : table_names_by_db)
                            if (database_regexp.match(db))
                                matched_databases.push_back(db);
                        /// Include omitted databases so their matches are reported as external dependencies.
                        for (const auto & db : undumped_databases)
                            if (database_regexp.match(db))
                                matched_databases.push_back(db);
                    }
                    else
                    {
                        /// An empty database resolves to the owner because replay emits `USE` before `CREATE`.
                        if (database_pattern->empty())
                            matched_databases.push_back(owning_database);
                        else
                            matched_databases.push_back(*database_pattern);
                    }

                    OptimizedRegularExpression table_regexp(*table_pattern);
                    /// Refuse empty-database references that the owning database cannot resolve uniquely.
                    if (!database_is_regexp && database_pattern->empty())
                    {
                        for (const auto & [db, tables] : table_names_by_db)
                        {
                            if (db == owning_database)
                                continue;
                            for (const auto & table : tables)
                                if (table_regexp.match(table))
                                {
                                    if (!merge_ambiguous_dbs.empty())
                                        merge_ambiguous_dbs += ", ";
                                    merge_ambiguous_dbs += backQuoteIfNeed(db);
                                    break;
                                }
                        }
                        merge_owning_matches = false;
                        if (auto own = table_names_by_db.find(owning_database); own != table_names_by_db.end())
                            for (const auto & table : own->second)
                                if (table_regexp.match(table))
                                {
                                    merge_owning_matches = true;
                                    break;
                                }
                        /// Also check undumped databases: if the owning database matches but an
                        /// omitted database also has matching tables, the create-time session could
                        /// have been the omitted one, and replaying under USE <own db> would rebind.
                        if (merge_owning_matches)
                            for (const auto & [db, tables] : undumped_table_names_by_db)
                                if (db != owning_database)
                                    for (const auto & table : tables)
                                        if (table_regexp.match(table))
                                        {
                                            if (!merge_ambiguous_dbs.empty())
                                                merge_ambiguous_dbs += ", ";
                                            merge_ambiguous_dbs += backQuoteIfNeed(db) + " (omitted)";
                                            break;
                                        }
                    }
                    for (const auto & db : matched_databases)
                    {
                        auto it = table_names_by_db.find(db);
                        if (it == table_names_by_db.end())
                        {
                            /// Preserve external matches so the caller can warn about them.
                            out.emplace_back(db, *table_pattern);
                            continue;
                        }
                        for (const auto & table : it->second)
                            if (table_regexp.match(table))
                                out.emplace_back(db, table);
                    }
                }
                catch (const Exception &) // Ok: malformed regexp leaves this reference unresolved, not the whole dump // NOLINT(bugprone-empty-catch)
                {
                }
                /// Thrown outside the catch above, which exists only for malformed regexps.
                if (!merge_ambiguous_dbs.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database-less merge('', {}) reference in database {} for --dump-schema: "
                        "tables matching it exist in more than one database ({} besides {}), and the session database "
                        "it was created under is not stored with the object",
                        quoteString(*table_pattern), backQuoteIfNeed(owning_database),
                        merge_ambiguous_dbs, backQuoteIfNeed(owning_database));
                if (!merge_owning_matches)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database-less merge('', {}) reference in database {} for --dump-schema: "
                        "no table there matches it, so it was created under a session database outside this dump set, "
                        "which is not stored with the object",
                        quoteString(*table_pattern), backQuoteIfNeed(owning_database));
            }
            else
            {
                /// The server evaluates arbitrary constant expressions here (e.g. concat('d', '')); treating
                /// what this walker can't recognize as dependency-free risks an unreplayable ordering.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                    "only string literals and REGEXP('...') are supported, not arbitrary expressions",
                    function->formatForErrorMessage());
            }
        }
        else if (function->name == "loop" && args.size() == 1)
        {
            if (args[0]->as<ASTFunction>())
            {
                /// loop(other_table_function(...)): an inner table function (e.g. loop(numbers(10))), not a table reference.
                /// Its child expressions will be visited by the recursive traversal below.
            }
            else if (auto candidate = tryGetQualifiedNameFromFunctionArgument(*function, 0))
                out.push_back(std::move(*candidate));
            else
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the table argument of {} for --dump-schema: "
                    "only table identifiers, string literals, and inner table functions are supported",
                    function->formatForErrorMessage());
            }
        }
        else if (function->name == "loop" && args.size() == 2)
        {
            /// loop(database, table): two separate plain arguments, not one qualified "db.table" name.
            auto read_plain_name = [](const ASTPtr & arg) -> std::optional<String>
            {
                if (const auto * literal = arg->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                    return literal->value.safeGet<String>();
                if (const auto * identifier = arg->as<ASTIdentifier>())
                    return identifier->name();
                return std::nullopt;
            };
            auto database = read_plain_name(args[0]);
            auto table = read_plain_name(args[1]);
            if (database && table)
                out.emplace_back(*database, *table);
            else
                /// Same reasoning as the merge() case above: a computed database/table name here is
                /// resolvable in principle but not by this walker, so refuse rather than mis-order.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                    "only string literals and identifiers are supported, not arbitrary expressions",
                    function->formatForErrorMessage());
        }
    }
}

/// Collects table references from dictionary, join, `IN`, and local `cluster` function arguments.
void collectFunctionArgumentReferences(
    const IAST & node, const ClusterLocality & clusters,
    std::vector<std::pair<String, String>> & out)
{
    if (const auto * function = node.as<ASTFunction>())
    {
        std::optional<std::pair<String, String>> candidate;
        if (isClusterTableFunctionName(function->name) && function->arguments)
        {
            const auto & args = function->arguments->children;
            /// A cluster with local replicas reads the named table locally; no current-database
            /// fallback here, a database-less first argument names the database for argument 2.
            if (args.size() >= 2
                && clusters.local.contains(resolveClusterOfFunction(*function, clusters)))
            {
                /// The server folded these at CREATE time against its own session and machine, neither
                /// of which the dump shares; rebinding them here would invent or miss a local edge.
                for (size_t i = 1; i < std::min<size_t>(args.size(), 3); ++i)
                    if (dependsOnUnstoredContext(*args[i], clusters.context))
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                            "they read the session database or user, or a session setting, or a server "
                            "constant such as getMacro()/hostName(), none of which is stored with the object",
                            function->formatForErrorMessage());

                /// The server runs evaluateConstantExpressionOrIdentifierAsLiteral on these
                /// positions: identifiers, string literals and constant expressions all name tables.
                auto read_name = [&clusters](const ASTPtr & arg) -> std::optional<String>
                {
                    if (const auto * literal = arg->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                        return literal->value.safeGet<String>();
                    if (const auto * identifier = arg->as<ASTIdentifier>())
                        return identifier->name();
                    try
                    {
                        auto evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(arg->clone(), clusters.context);
                        if (const auto * literal = evaluated->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                            return literal->value.safeGet<String>();
                    }
                    catch (Exception &) // NOLINT(bugprone-empty-catch)
                    {
                        /// Not a constant name; the caller decides what that means for its position.
                    }
                    return std::nullopt;
                };
                std::optional<std::pair<String, String>> dependency;
                String database_only;
                if (const auto * identifier = args[1]->as<ASTIdentifier>())
                {
                    if (auto table_id = identifier->createTable())
                    {
                        if (!table_id->getDatabaseName().empty())
                            dependency = std::pair(table_id->getDatabaseName(), table_id->shortName());
                        else
                            database_only = table_id->shortName();
                    }
                }
                else if (auto text = read_name(args[1]))
                {
                    if (auto qualified = QualifiedTableName::tryParseFromString(*text))
                    {
                        if (!qualified->database.empty())
                            dependency = std::pair(qualified->database, qualified->table);
                        else
                            database_only = qualified->table;
                    }
                }
                else if (!args[1]->as<ASTFunction>())
                    /// The server evaluates constant expressions in this position; treating what
                    /// this walker can't recognize as dependency-free risks an unreplayable ordering.
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                        "only table identifiers, string literals and constant expressions are supported",
                        function->formatForErrorMessage());
                /// A function that is not a constant name is an inner table function (e.g. `merge`);
                /// the recursive walk below visits it instead of reading it as a table reference.

                if (!dependency && !database_only.empty() && args.size() >= 3)
                {
                    if (auto table = read_name(args[2]))
                        dependency = std::pair(database_only, *table);
                    else
                        /// Same reasoning: a computed table name here is a real local dependency
                        /// the server would resolve, so refuse rather than dump in the wrong order.
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                            "only table identifiers, string literals and constant expressions are supported",
                            function->formatForErrorMessage());
                }
                if (dependency)
                    out.push_back(std::move(*dependency));
            }
        }
        else if (functionIsDictGet(function->name) || functionIsJoinGet(function->name) || function->name == "dictionary")
        {
            candidate = tryGetQualifiedNameFromFunctionArgument(*function, 0);
            /// The server evaluates constant expressions in this position (DDLDependencyVisitor's
            /// tryGetStringFromArgument), so an unrecognized argument is not dependency-free.
            if (!candidate && function->arguments && !function->arguments->children.empty()
                && !function->arguments->children[0]->as<ASTIdentifier>())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the dictionary/join argument of {} for --dump-schema: "
                    "only table identifiers and string literals are supported, not arbitrary expressions",
                    function->formatForErrorMessage());
        }
        else if (functionIsInOrGlobalInOperator(function->name))
        {
            /// In a stored CREATE the qualifier visitor has already qualified real tables, so a
            /// bare identifier here is a CTE/alias and a literal is a value set - only db.table counts.
            if (function->arguments && function->arguments->children.size() > 1)
                if (const auto * identifier = function->arguments->children[1]->as<ASTIdentifier>())
                    if (auto table_id = identifier->createTable(); table_id && !table_id->getDatabaseName().empty())
                        candidate = std::pair(table_id->getDatabaseName(), table_id->shortName());
        }
        if (candidate)
            out.push_back(std::move(*candidate));
    }
}

/// Combines server dependency columns with parsed view references and drops implicit storage.
std::vector<TableInfo> resolveTables(
    std::vector<RawTableRow> rows, const ClusterLocality & clusters, const std::set<String> & undumped_databases,
    const std::map<String, std::set<String>> & undumped_table_names_by_db)
{
    std::map<std::pair<String, String>, CreateTargets> targets_by_table;
    for (const auto & row : rows)
        if (row.engine == "TimeSeries"
            || (row.engine == "MaterializedView" && !row.target_database.empty() && looksLikeGeneratedInnerTableName(row.target_table)))
            targets_by_table.emplace(std::pair(row.database, row.name), parseCreateTargets(row));

    /// Maps a generated helper table to its owner; UUID-named helpers cannot survive replay by name.
    struct InnerOwner
    {
        std::pair<String, String> owner;
        bool uuid_named = false;
    };
    std::map<std::pair<String, String>, InnerOwner> inner_owner;

    std::set<std::pair<String, String>> implicit_inner;
    for (const auto & row : rows)
        if (row.engine == "MaterializedView" && !row.target_database.empty() && looksLikeGeneratedInnerTableName(row.target_table)
            && !targets_by_table.at({row.database, row.name}).external_materialized_view)
        {
            implicit_inner.emplace(row.target_database, row.target_table);
            inner_owner[{row.target_database, row.target_table}]
                = {{row.database, row.name}, row.target_table.starts_with(".inner_id.")};
        }

    /// Only UUID-backed `.tmp.inner_id.*` names prove ownership; name-based matches can be user tables.
    std::map<String, std::map<String, String>> mv_names_by_uuid_by_db;
    for (const auto & row : rows)
        if (row.engine == "MaterializedView" && row.uuid != UUIDHelpers::Nil)
            mv_names_by_uuid_by_db[row.database][toString(row.uuid)] = row.name;
    const String tmp_inner_id_prefix = ".tmp.inner_id.";
    for (const auto & row : rows)
    {
        if (!row.name.starts_with(tmp_inner_id_prefix))
            continue;
        const auto & mvs = mv_names_by_uuid_by_db[row.database];
        if (auto it = mvs.find(row.name.substr(tmp_inner_id_prefix.size())); it != mvs.end())
        {
            implicit_inner.emplace(row.database, row.name);
            inner_owner[{row.database, row.name}] = {{row.database, it->second}, true};
        }
    }

    /// TimeSeries helper tables are recreated by the owner; exact generated names avoid false matches.
    for (const auto & row : rows)
    {
        const StorageID owner_id{row.database, row.name, row.uuid};
        const bool uuid_named = owner_id.hasUUID();
        if (row.engine == "TimeSeries")
        {
            const auto & target_info = targets_by_table.at({row.database, row.name});
            for (auto kind : magic_enum::enum_values<ViewTarget::Kind>())
            {
                if (kind == ViewTarget::To || kind == ViewTarget::Inner || target_info.explicit_kinds.contains(kind))
                    continue;
                /// `buildTargets` builds the optional kinds only when the CREATE declares them.
                if (kind == ViewTarget::RecentSamples && !target_info.mentioned_kinds.contains(kind))
                    continue;
                inner_owner[{row.database, getTimeSeriesInnerTableName(kind, owner_id)}] = {{row.database, row.name}, uuid_named};
            }
            /// The legacy `data` helper is implicit only when the modern `samples` helper is absent.
            const String legacy_samples_name = getTimeSeriesInnerTableName("data", owner_id);
            const String modern_samples_name = getTimeSeriesInnerTableName(ViewTarget::Samples, owner_id);
            const bool modern_samples_exists = std::any_of(rows.begin(), rows.end(), [&](const auto & other)
            {
                return other.database == row.database && other.name == modern_samples_name;
            });
            if (!target_info.explicit_kinds.contains(ViewTarget::Samples) && !modern_samples_exists)
                inner_owner[{row.database, legacy_samples_name}] = {{row.database, row.name}, uuid_named};
        }
    }
    for (const auto & row : rows)
        if (inner_owner.contains({row.database, row.name}))
            implicit_inner.emplace(row.database, row.name);

    std::map<std::pair<String, String>, RawTableRow *> rows_by_name;
    for (auto & row : rows)
        rows_by_name[{row.database, row.name}] = &row;
    for (const auto & row : rows)
        for (const auto & dependent : row.dependents)
            if (auto it = rows_by_name.find(dependent); it != rows_by_name.end())
                it->second->loading_dependencies.emplace_back(row.database, row.name);

    for (auto & row : rows)
        if (row.engine == "MaterializedView" && !row.target_database.empty()
            && !implicit_inner.contains({row.target_database, row.target_table}))
            row.loading_dependencies.emplace_back(row.target_database, row.target_table);

    /// `target_*` above covers only materialized views; a TimeSeries table names its target tables
    /// in the CREATE itself and resolves them at CREATE time just the same.
    for (auto & row : rows)
        if (row.engine == "TimeSeries")
            for (const auto & target_id : targets_by_table.at({row.database, row.name}).explicit_tables)
            {
                /// `InterpreterCreateQuery` stamps the creator's session database onto every target
                /// (`ASTViewTargets::setCurrentDatabase`) before storing, so the stored name is qualified.
                if (!implicit_inner.contains({target_id.database_name, target_id.table_name}))
                    row.loading_dependencies.emplace_back(target_id.database_name, target_id.table_name);
            }

    std::set<std::pair<String, String>> known_tables;
    std::map<String, std::set<String>> table_names_by_db;
    /// Match `merge` against all tables, then remap omitted helpers to their owners.
    std::map<String, std::set<String>> all_table_names_by_db;
    for (const auto & row : rows)
    {
        all_table_names_by_db[row.database].insert(row.name);
        if (!implicit_inner.contains({row.database, row.name}))
        {
            known_tables.emplace(row.database, row.name);
            table_names_by_db[row.database].insert(row.name);
        }
    }

    for (auto & row : rows)
    {
        /// A view's select sources are deliberately not in `loading_dependencies_*`, so they are
        /// only discoverable by parsing the stored `SELECT`.
        if (row.engine != "View" && row.engine != "MaterializedView")
            continue;

        ASTPtr select_ast;
        try
        {
            /// `as_select` is server-produced SQL already known to be valid, not untrusted input;
            /// max_query_size=0 disables the size cap so a legitimately large SELECT still parses.
            ParserSelectWithUnionQuery select_parser;
            select_ast = parseQuery(select_parser, row.as_select, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        catch (const Exception & e)
        {
            /// A dependency this parse would have found stays undiscovered otherwise, and the dump
            /// can come out unreplayable without any indication why; fail loudly instead.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot parse the stored SELECT for {}.{} to resolve its dependencies for --dump-schema: {}. "
                "The dump would be incomplete without this view/materialized view's dependency edges",
                row.database, row.name, e.message());
        }

        auto add_dependency = [&](const std::pair<String, String> & candidate)
        {
            std::pair<String, String> resolved = candidate;
            if (resolved.first.empty())
            {
                /// Replay resolves unqualified names under `USE <owner database>`; reject ambiguous rebinding.
                String other_databases;
                for (const auto & [db, names] : table_names_by_db)
                    if (db != row.database && names.contains(resolved.second))
                    {
                        if (!other_databases.empty())
                            other_databases += ", ";
                        other_databases += backQuoteIfNeed(db);
                    }
                /// Present in the owning database AND elsewhere in the dump: the CREATE-time session
                /// could have bound either one, so replaying under `USE <own db>` may silently rebind.
                if (!other_databases.empty()
                    && (known_tables.contains({row.database, resolved.second})
                        || implicit_inner.contains({row.database, resolved.second})))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database-less reference {} in {}.{} for --dump-schema: "
                        "it exists in more than one dumped database ({} besides {}), and the session database "
                        "it was created under is not stored with the object",
                        backQuoteIfNeed(resolved.second), backQuoteIfNeed(row.database), backQuoteIfNeed(row.name),
                        other_databases, backQuoteIfNeed(row.database));
                if (!known_tables.contains({row.database, resolved.second})
                    && !implicit_inner.contains({row.database, resolved.second}))
                {
                    /// A name absent everywhere is external; a match in another dumped database is ambiguous.
                    if (other_databases.empty())
                    {
                        /// Record unresolved function references so qualified and unqualified forms warn equally.
                        row.unresolved_references.push_back(resolved.second);
                        return;
                    }
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database-less reference {} in {}.{} for --dump-schema: "
                        "it resolves against the session database, which is not stored with the object; "
                        "database {} does not contain it, but {} in the dump does",
                        backQuoteIfNeed(resolved.second), backQuoteIfNeed(row.database), backQuoteIfNeed(row.name),
                        backQuoteIfNeed(row.database), other_databases);
                }
                /// The name is in the owning database; also check undumped databases for ambiguity.
                /// If an omitted database has the same table, the create-time session could have
                /// been that database, and replaying under `USE <own db>` would silently rebind.
                String omitted_databases;
                for (const auto & [db, names] : undumped_table_names_by_db)
                    if (db != row.database && names.contains(resolved.second))
                    {
                        if (!omitted_databases.empty())
                            omitted_databases += ", ";
                        omitted_databases += backQuoteIfNeed(db);
                    }
                if (!omitted_databases.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot statically resolve the database-less reference {} in {}.{} for --dump-schema: "
                        "it exists in the owning database {} but also in omitted database(s) {}; "
                        "the session database it was created under is not stored with the object, "
                        "so replaying under USE {} could silently rebind it",
                        backQuoteIfNeed(resolved.second), backQuoteIfNeed(row.database), backQuoteIfNeed(row.name),
                        backQuoteIfNeed(row.database), omitted_databases, backQuoteIfNeed(row.database));
                resolved.first = row.database;
            }
            if (resolved != std::pair(row.database, row.name))
                row.loading_dependencies.emplace_back(resolved);
        };

        std::vector<std::pair<String, String>> references;
        visitLocalAST(select_ast.get(), clusters, [&](const IAST & node)
        {
            if (const auto * table_id = node.as<ASTTableIdentifier>(); table_id && !table_id->getDatabaseName().empty())
                references.emplace_back(table_id->getDatabaseName(), table_id->shortName());
            collectFunctionArgumentReferences(node, clusters, references);
            collectMergeAndLoopReferences(node, all_table_names_by_db, undumped_databases, undumped_table_names_by_db, row.database, references);
        });
        for (const auto & candidate : references)
            add_dependency(candidate);
    }

    std::vector<TableInfo> tables;
    for (auto & row : rows)
    {
        if (implicit_inner.contains({row.database, row.name}))
            continue;

        /// Empty CREATE text means concurrent deletion or unreadable catalog metadata; never omit it silently.
        if (row.create_query.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot dump {}.{} for --dump-schema: the server returned an empty CREATE for it, "
                "which happens when the table is being dropped concurrently or its metadata cannot "
                "be read. Re-run the dump",
                backQuoteIfNeed(row.database), backQuoteIfNeed(row.name));

        TableInfo table;
        table.database = row.database;
        table.name = row.name;
        table.create_query = std::move(row.create_query);
        table.dependencies = std::move(row.loading_dependencies);
        table.unresolved_references = std::move(row.unresolved_references);
        /// A dependency on an omitted helper table is remapped onto the owning object - which is what
        /// creates the helper on replay - so the edge survives instead of dangling on a skipped row.
        for (auto & dependency : table.dependencies)
        {
            auto it = inner_owner.find(dependency);
            if (it == inner_owner.end())
                continue;
            if (it->second.uuid_named && it->second.owner != std::pair(row.database, row.name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot dump {}.{} for --dump-schema: it references {}.{}, the generated inner storage of {}.{}, "
                    "and that name embeds a UUID which will not match the object recreated on replay. "
                    "Reference the owning object instead",
                    backQuoteIfNeed(row.database), backQuoteIfNeed(row.name),
                    backQuoteIfNeed(dependency.first), backQuoteIfNeed(dependency.second),
                    backQuoteIfNeed(it->second.owner.first), backQuoteIfNeed(it->second.owner.second));
            dependency = it->second.owner;
        }
        tables.push_back(std::move(table));
    }
    return tables;
}

/// Fetches and resolves every dumpable table in `databases`; see `resolveTables`. `undumped_databases`
/// are the server's other databases, which a `merge(REGEXP(...), ...)` can still reach.
std::vector<TableInfo> fetchTables(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info, ContextPtr context,
    const std::vector<String> & databases, const std::set<String> & undumped_databases)
{
    /// Resolved once per dump: the walkers gate `cluster*` calls on the server's cluster locality.
    ClusterLocality clusters;
    clusters.context = context;
    for (auto & name : fetchStringColumn(connection, timeouts, client_info, "SELECT DISTINCT cluster FROM system.clusters", context->getSettingsRef()))
        clusters.known.insert(std::move(name));
    for (auto & name : fetchStringColumn(connection, timeouts, client_info, "SELECT DISTINCT cluster FROM system.clusters WHERE is_local", context->getSettingsRef()))
        clusters.local.insert(std::move(name));
    /// Both ordered by `macro`, so the two columns line up.
    auto macro_names = fetchStringColumn(connection, timeouts, client_info, "SELECT macro FROM system.macros ORDER BY macro", context->getSettingsRef());
    auto macro_values = fetchStringColumn(connection, timeouts, client_info, "SELECT substitution FROM system.macros ORDER BY macro", context->getSettingsRef());
    if (macro_names.size() == macro_values.size())
        for (size_t i = 0; i < macro_names.size(); ++i)
            clusters.macros.emplace(std::move(macro_names[i]), std::move(macro_values[i]));

    /// Fetch table names from undumped databases so unqualified references and empty-database
    /// merge() calls can be checked for ambiguity against them, not just against dumped databases.
    std::map<String, std::set<String>> undumped_table_names_by_db;
    if (!undumped_databases.empty())
    {
        String undumped_list;
        for (const auto & db : undumped_databases)
        {
            if (!undumped_list.empty())
                undumped_list += ", ";
            undumped_list += quoteString(db);
        }
        auto undumped_visibility = detectExternalTableVisibility(connection, timeouts, client_info, undumped_list, context->getSettingsRef());
        executeQuery(connection, timeouts, client_info,
            "SELECT database, name FROM system.tables WHERE database IN (" + undumped_list + ") AND NOT is_temporary",
            [&](const Block & block)
            {
                if (block.empty())
                    return;
                const auto & db_col = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);
                const auto & name_col = typeid_cast<const ColumnString &>(*block.getByPosition(1).column);
                for (size_t i = 0; i < db_col.size(); ++i)
                    undumped_table_names_by_db[db_col[i].safeGet<String>()].insert(name_col[i].safeGet<String>());
            }, context->getSettingsRef(), undumped_visibility.show_datalake_catalogs, undumped_visibility.show_remote_databases);
    }

    return resolveTables(fetchRawRows(connection, timeouts, client_info, databases, context->getSettingsRef()), clusters, undumped_databases, undumped_table_names_by_db);
}

/// Warns when stored CREATE statements contain masked credentials.
void reportMaskedSecrets(
    const std::vector<TableInfo> & tables, const std::map<String, String> & database_queries, std::ostream & err)
{
    std::set<std::pair<String, String>> masked;
    for (const auto & table : tables)
        if (table.create_query.contains("[HIDDEN]"))
            masked.emplace(table.database, table.name);

    /// `SHOW CREATE DATABASE` is masked by the same path, and a credential lives on the database
    /// itself for the engines that carry one (`PostgreSQL`, `MySQL`, a data lake catalog, ...).
    std::set<String> masked_databases;
    for (const auto & [database, create_query] : database_queries)
        if (create_query.contains("[HIDDEN]"))
            masked_databases.insert(database);

    for (const auto & database : masked_databases)
        err << "Warning: database " << backQuoteIfNeed(database)
            << " has credentials masked as [HIDDEN] in its stored CREATE; replaying this dump would "
               "create it with that literal instead of the real value.\n";

    for (const auto & [database, name] : masked)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name)
            << " has credentials masked as [HIDDEN] in its stored CREATE; replaying this dump would "
               "create it with that literal instead of the real value.\n";

    if (!masked.empty() || !masked_databases.empty())
        err << "Warning: re-run with a session allowed to see secrets to dump those objects faithfully.\n";
}

void reportDependenciesOutsideDumpSet(
    const std::vector<TableInfo> & tables, const std::vector<String> & target_databases, std::ostream & err)
{
    std::set<String> dumped_databases(target_databases.begin(), target_databases.end());

    /// A set so the same missing dependency is reported once, in a deterministic order. Predefined
    /// databases are skipped: they always exist wherever the dump is replayed.
    std::set<std::tuple<String, String, String, String>> missing;
    for (const auto & table : tables)
        for (const auto & dependency : table.dependencies)
            if (!dumped_databases.contains(dependency.first) && !DatabaseCatalog::isPredefinedDatabase(dependency.first))
                missing.emplace(table.database, table.name, dependency.first, dependency.second);

    /// The original session database is unknown; replay binds these names to the owner database.
    std::set<std::tuple<String, String, String>> unresolved;
    for (const auto & table : tables)
        for (const auto & reference : table.unresolved_references)
            unresolved.emplace(table.database, table.name, reference);

    for (const auto & [database, name, dependency_database, dependency_name] : missing)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " depends on "
            << backQuoteIfNeed(dependency_database) << "." << backQuoteIfNeed(dependency_name)
            << ", which is outside the dumped database(s) and will not be created by this dump.\n";

    for (const auto & [database, name, reference] : unresolved)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " references "
            << backQuoteIfNeed(reference) << " without a database; no dumped database contains it, and on replay it "
            << "will resolve against " << backQuoteIfNeed(database) << ", which does not contain it either.\n";

    if (!missing.empty() || !unresolved.empty())
        err << "Warning: replaying this dump into a fresh instance requires those objects to already exist.\n";
}

/// Orders tables with `TablesDependencyGraph`; ties are broken by `(database, name)`.
std::vector<size_t> orderTablesByDependencies(const std::vector<TableInfo> & tables)
{
    std::map<std::pair<String, String>, size_t> index_by_key;
    for (size_t i = 0; i < tables.size(); ++i)
        index_by_key[{tables[i].database, tables[i].name}] = i;

    TablesDependencyGraph graph("--dump-schema");
    for (const auto & table : tables)
    {
        StorageID table_id(table.database, table.name);
        std::vector<StorageID> dependency_ids;
        for (const auto & dependency : table.dependencies)
            if (dependency != std::pair(table.database, table.name) && index_by_key.contains(dependency))
                dependency_ids.emplace_back(dependency.first, dependency.second);

        graph.addDependencies(table_id, dependency_ids);
    }

    graph.checkNoCyclicDependencies();

    std::vector<size_t> order;
    order.reserve(tables.size());
    for (auto & level : graph.getTablesSplitByDependencyLevel())
    {
        std::sort(level.begin(), level.end(), [](const StorageID & a, const StorageID & b)
        {
            return std::tie(a.database_name, a.table_name) < std::tie(b.database_name, b.table_name);
        });
        for (const auto & storage_id : level)
            order.push_back(index_by_key.at({storage_id.database_name, storage_id.table_name}));
    }

    return order;
}

/// Orders `target_databases` by cross-database table dependency, for `--dump-schema-dir` file
/// replay order. Returns `std::nullopt` on a database-level cycle (distinct from a table-level one).
std::optional<std::vector<String>> orderDatabasesByDependencies(const std::vector<String> & target_databases, const std::vector<TableInfo> & tables)
{
    std::set<String> db_set(target_databases.begin(), target_databases.end());

    std::map<String, std::set<String>> depends_on;
    for (const auto & db : target_databases)
        depends_on[db]; /// every database gets an entry, even with no dependencies

    for (const auto & table : tables)
        for (const auto & dependency : table.dependencies)
            if (dependency.first != table.database && db_set.contains(dependency.first))
                depends_on[table.database].insert(dependency.first);

    std::map<String, size_t> remaining_dependencies;
    std::map<String, std::vector<String>> dependents;
    for (const auto & [db, deps] : depends_on)
    {
        remaining_dependencies[db] = deps.size();
        for (const auto & dep : deps)
            dependents[dep].push_back(db);
    }

    std::set<String> ready;
    for (const auto & [db, count] : remaining_dependencies)
        if (count == 0)
            ready.insert(db);

    std::vector<String> order;
    order.reserve(target_databases.size());
    while (!ready.empty())
    {
        String db = *ready.begin();
        ready.erase(ready.begin());
        order.push_back(db);

        for (const auto & dependent : dependents[db])
            if (--remaining_dependencies.at(dependent) == 0)
                ready.insert(dependent);
    }

    if (order.size() != target_databases.size())
        return std::nullopt;
    return order;
}

/// Replay gates detected from parsed CREATE statements rather than text substrings.
struct ReplayGateNeeds
{
    bool explicit_uuid = false;
    bool replicated_engine_arguments = false;
    bool materialized_view = false;
    bool analyzable_query_text = false;
    bool ordinary_database = false;
    bool replicated_database = false;
    bool materialized_postgresql_database = false;
    bool materialized_mysql_database = false;
    bool materialized_postgresql_table = false;
    bool time_series_table = false;
    bool kafka_engine = false;
    bool nullable_tuple_type = false;
};

ReplayGateNeeds collectReplayGateNeeds(const std::vector<String> & create_queries)
{
    ReplayGateNeeds needs;
    for (const auto & create_query : create_queries)
    {
        ASTPtr create_ast;
        try
        {
            ParserCreateQuery create_parser;
            create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        catch (const Exception &)
        {
            /// Cannot rule any gate out for a statement that does not parse, so keep them all.
            return {.explicit_uuid = true, .replicated_engine_arguments = true, .materialized_view = true,
                    .analyzable_query_text = true, .ordinary_database = true, .replicated_database = true,
                    .materialized_postgresql_database = true, .materialized_mysql_database = true,
                    .materialized_postgresql_table = true, .time_series_table = true,
                    .kafka_engine = true, .nullable_tuple_type = true};
        }

        const auto * create = create_ast->as<ASTCreateQuery>();
        if (!create)
            continue;

        /// What `assertOrSetUUID` re-enters on when the dump is replayed into a `Replicated` database.
        if (create->has_uuid || create->has_uuid_clause)
            needs.explicit_uuid = true;

        /// All three `allow_materialized_view_with_bad_select` checks sit inside
        /// `InterpreterCreateQuery`'s materialized-view branch, so none can fire without one.
        if (create->is_materialized_view)
            needs.materialized_view = true;

        /// The analyzer-side gates fire only where stored query text is re-analysed at replay: a
        /// view's AS SELECT, or a projection (`ProjectionsDescription` runs `runOnlyResolve` on it).
        if (create->select
            || (create->columns_list && create->columns_list->projections
                && !create->columns_list->projections->children.empty()))
            needs.analyzable_query_text = true;

        /// `registerStorageMergeTree` re-enters its check only for an engine that kept its arguments.
        if (create->storage && create->storage->engine)
        {
            const auto & engine = *create->storage->engine;

            if (create->getTable().empty())
            {
                /// CREATE DATABASE: gate on the database engine name.
                if (engine.name == "Ordinary")
                    needs.ordinary_database = true;
                else if (engine.name == "Replicated")
                    needs.replicated_database = true;
                else if (engine.name == "MaterializedPostgreSQL")
                    needs.materialized_postgresql_database = true;
                else if (engine.name == "MaterializedMySQL")
                    needs.materialized_mysql_database = true;
            }
            else
            {
                /// CREATE TABLE: gate on the table engine name.
                if (engine.name.starts_with("Replicated") && engine.arguments && !engine.arguments->children.empty())
                    needs.replicated_engine_arguments = true;
                if (engine.name == "MaterializedPostgreSQL")
                    needs.materialized_postgresql_table = true;
                if (engine.name == "TimeSeries")
                    needs.time_series_table = true;
                if (engine.name == "Kafka")
                    needs.kafka_engine = true;
            }
        }

        /// `enable_nullable_tuple_type` gates `Nullable(Tuple(...))` column types.
        if (create->columns_list && create->columns_list->columns)
            for (const auto & child : create->columns_list->columns->children)
                if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getType())
                    if (column->getType()->formatWithSecretsOneLine().contains("Nullable(Tuple"))
                        needs.nullable_tuple_type = true;
    }
    return needs;
}

String replaySettingsPrelude(const std::set<String> & settings_known_to_server, const std::vector<String> & create_queries)
{
    /// Nothing to replay means no gate can fire. Reachable whenever every database is predefined
    /// or excluded, which leaves the dump empty.
    if (create_queries.empty())
        return {};

    /// Additional gates required when replay revalidates stored metadata.
    static const std::vector<std::pair<String, String>> dump_specific =
    {
        {"allow_deprecated_database_ordinary", "1"},
        {"allow_experimental_database_replicated", "1"},
        {"allow_experimental_database_materialized_postgresql", "1"},
        {"allow_experimental_database_materialized_mysql", "1"},
        {"allow_experimental_materialized_postgresql_table", "1"},
        {"allow_experimental_time_series_table", "1"},
        {"allow_experimental_kafka_offsets_storage_in_keeper", "1"},
        {"enable_nullable_tuple_type", "1"},
        /// Materialized-view target compatibility is revalidated on replay.
        {"allow_materialized_view_with_bad_select", "1"},
        /// Value 3 preserves explicit engine arguments without per-table warnings.
        {"database_replicated_allow_replicated_engine_arguments", "3"},
        /// Value 3 preserves explicit UUIDs; value 2 would replace them.
        {"database_replicated_allow_explicit_uuid", "3"},
    };
    /// Emit only dump-specific gates known by the source server and required by these statements.
    const ReplayGateNeeds needs = collectReplayGateNeeds(create_queries);
    auto is_needed = [&needs](const String & name)
    {
        if (name == "database_replicated_allow_explicit_uuid")
            return needs.explicit_uuid;
        if (name == "database_replicated_allow_replicated_engine_arguments")
            return needs.replicated_engine_arguments;
        if (name == "allow_materialized_view_with_bad_select")
            return needs.materialized_view;
        if (name == "allow_deprecated_database_ordinary")
            return needs.ordinary_database;
        if (name == "allow_experimental_database_replicated")
            return needs.replicated_database;
        if (name == "allow_experimental_database_materialized_postgresql")
            return needs.materialized_postgresql_database;
        if (name == "allow_experimental_database_materialized_mysql")
            return needs.materialized_mysql_database;
        if (name == "allow_experimental_materialized_postgresql_table")
            return needs.materialized_postgresql_table;
        if (name == "allow_experimental_time_series_table")
            return needs.time_series_table;
        if (name == "allow_experimental_kafka_offsets_storage_in_keeper")
            return needs.kafka_engine;
        if (name == "enable_nullable_tuple_type")
            return needs.nullable_tuple_type;
        return false;
    };

    String res;
    /// Emit all shared experimental settings unconditionally — CREATE replay re-enters
    /// many of them (view/projection analysis, default-expression validation, suspicious-type
    /// checks, etc.) and narrowing the list risks making the dump non-self-contained.
    /// Exclude three known-dead settings that cannot gate a replay on any schema.
    static const std::set<std::string_view> dead_settings = {
        "allow_experimental_window_functions",
        "allow_experimental_hash_functions",
        "allow_simdjson",
    };
    for (const auto & name : allExperimentalSettingNames())
        if (settings_known_to_server.contains(name) && !dead_settings.contains(name))
            res += "SET " + name + " = 1;\n";
    /// Emit dump-specific gates only when the dumped AST proves they are needed.
    for (const auto & [name, value] : dump_specific)
        if (settings_known_to_server.contains(name) && is_needed(name))
            res += "SET " + name + " = " + value + ";\n";
    res += "\n";
    return res;
}

}

void dumpDatabaseSchema(
    IServerConnection & connection,
    const ConnectionTimeouts & timeouts,
    const ClientInfo & client_info,
    ContextPtr context,
    const String & databases,
    const String & exclude_databases,
    const String & output_dir,
    std::ostream & out,
    std::ostream & err)
{
    std::vector<String> database_list = splitDatabaseList(databases);
    std::vector<String> exclude_list = splitDatabaseList(exclude_databases);

    /// A selector that was given but names nothing must not broaden the dump: this surface decides
    /// which schemas leave the server, so malformed input fails closed instead of meaning "all".
    if (!databases.empty() && database_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The `--dump-schema` database list {} contains no database names", quoteString(databases));
    if (!exclude_databases.empty() && exclude_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The `--dump-schema-exclude` database list {} contains no database names", quoteString(exclude_databases));

    if (!database_list.empty() && !exclude_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`--dump-schema` with an explicit database list cannot be combined with `--dump-schema-exclude`");

    /// Materialized-view target columns are required to hide inner storage and order explicit targets.
    std::vector<String> system_tables_columns = fetchStringColumn(connection, timeouts, client_info,
        "SELECT name FROM system.columns WHERE database = 'system' AND table = 'tables'", context->getSettingsRef());
    std::set<String> system_tables_column_set(system_tables_columns.begin(), system_tables_columns.end());
    if (!system_tables_column_set.contains("target_database") || !system_tables_column_set.contains("target_table"))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "`--dump-schema` requires a server whose `system.tables` has `target_database` and `target_table` "
            "(added in 26.6); this server does not have them, and a dump taken without them would not replay");

    std::vector<String> all_databases = fetchStringColumn(connection, timeouts, client_info, "SELECT name FROM system.databases ORDER BY name", context->getSettingsRef());

    std::vector<String> target_databases;
    if (!database_list.empty())
    {
        String missing;
        String predefined;
        for (const auto & db : database_list)
        {
            if (std::find(all_databases.begin(), all_databases.end(), db) == all_databases.end())
            {
                if (!missing.empty())
                    missing += ", ";
                missing += backQuoteIfNeed(db);
            }
            else if (DatabaseCatalog::isPredefinedDatabase(db))
            {
                if (!predefined.empty())
                    predefined += ", ";
                predefined += backQuoteIfNeed(db);
            }
            else
                target_databases.push_back(db);
        }
        if (!missing.empty())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database(s) {} do not exist", missing);
        /// The all-databases path skips these; naming one explicitly would emit a CREATE DATABASE
        /// that fails to replay, because the database already exists on every server.
        if (!predefined.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database(s) {} are predefined and cannot be dumped", predefined);

        std::sort(target_databases.begin(), target_databases.end());
        target_databases.erase(std::unique(target_databases.begin(), target_databases.end()), target_databases.end());
    }
    else
    {
        /// A typo here would silently include the database the user meant to leave out, so an
        /// exclude name is validated the way the explicit include list is: it must exist.
        String missing_excludes;
        for (const auto & db : exclude_list)
        {
            if (std::find(all_databases.begin(), all_databases.end(), db) == all_databases.end())
            {
                if (!missing_excludes.empty())
                    missing_excludes += ", ";
                missing_excludes += backQuoteIfNeed(db);
            }
        }
        if (!missing_excludes.empty())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE,
                "Database(s) {} in `--dump-schema-exclude` do not exist", missing_excludes);

        std::set<String> exclude_set(exclude_list.begin(), exclude_list.end());
        /// `all_databases` is sorted, so filtering it keeps `target_databases` sorted too.
        for (const auto & db : all_databases)
            if (!DatabaseCatalog::isPredefinedDatabase(db) && !exclude_set.contains(db))
                target_databases.push_back(db);
    }

    std::map<String, String> create_database_query_by_db;
    for (const auto & db : target_databases)
    {
        std::vector<String> create_database_query = fetchStringColumn(
            connection, timeouts, client_info, "SHOW CREATE DATABASE " + backQuoteIfNeed(db), context->getSettingsRef());
        if (create_database_query.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected one row from SHOW CREATE DATABASE {}, got {}", backQuoteIfNeed(db), create_database_query.size());

        /// Every server is born with `default`, so its CREATE must tolerate the existing one:
        /// a bare replay would otherwise stop on DATABASE_ALREADY_EXISTS before any user object.
        if (db == "default" && create_database_query.front().starts_with("CREATE DATABASE "))
            create_database_query.front() = "CREATE DATABASE IF NOT EXISTS "
                + create_database_query.front().substr(std::string_view("CREATE DATABASE ").size());
        create_database_query_by_db.emplace(db, std::move(create_database_query.front()));
    }

    /// Names the source server actually has, used to filter the replay prelude below.
    std::vector<String> server_setting_names
        = fetchStringColumn(connection, timeouts, client_info, "SELECT name FROM system.settings", context->getSettingsRef());
    std::set<String> settings_known_to_server(server_setting_names.begin(), server_setting_names.end());

    std::vector<TableInfo> tables;
    std::vector<size_t> order;
    if (!target_databases.empty())
    {
        /// Databases the dump leaves out but a `merge(REGEXP(...), ...)` in it can still refer to.
        std::set<String> undumped_databases(all_databases.begin(), all_databases.end());
        for (const auto & db : target_databases)
            undumped_databases.erase(db);

        tables = fetchTables(connection, timeouts, client_info, context, target_databases, undumped_databases);
        reportDependenciesOutsideDumpSet(tables, target_databases, err);
        reportMaskedSecrets(tables, create_database_query_by_db, err);
        order = orderTablesByDependencies(tables);
    }

    if (output_dir.empty())
    {
        std::vector<String> dumped_creates;
        for (const auto & db : target_databases)
            dumped_creates.push_back(create_database_query_by_db.at(db));
        for (size_t i : order)
            dumped_creates.push_back(tables[i].create_query);

        out << replaySettingsPrelude(settings_known_to_server, dumped_creates);

        for (const auto & db : target_databases)
            out << create_database_query_by_db.at(db) << ";\n\n";

        /// Stored queries can keep names unqualified and resolve them against the session's current
        /// database, so restore that context whenever the effective database changes.
        String current_database;
        for (size_t i : order)
        {
            if (tables[i].database != current_database)
            {
                current_database = tables[i].database;
                out << "USE " << backQuoteIfNeed(current_database) << ";\n\n";
            }
            out << tables[i].create_query + ";\n\n";
        }
        return;
    }

    /// Each database's tables land in their own file, so the files must be replayed in order too.
    std::optional<std::vector<String>> database_order = orderDatabasesByDependencies(target_databases, tables);
    if (!database_order)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot dump to `--dump-schema-dir`: these databases have circular cross-database table "
            "dependencies, so no file order would replay correctly; use `--dump-schema` without "
            "`--dump-schema-dir` instead, which orders individual tables directly");
    bool has_cross_database_dependency = std::any_of(tables.begin(), tables.end(), [](const TableInfo & table)
    {
        return std::any_of(table.dependencies.begin(), table.dependencies.end(), [&](const auto & dependency)
        {
            return dependency.first != table.database;
        });
    });

    auto file_path = [&](const String & db)
    {
        return std::filesystem::path(output_dir) / (escapeForFileName(db) + ".sql");
    };
    /// Escaped database names must remain unique on case-insensitive filesystems.
    std::map<String, String> database_by_lowercase_filename;
    for (const auto & db : target_databases)
    {
        String lowercase_filename = file_path(db).filename().string();
        std::transform(lowercase_filename.begin(), lowercase_filename.end(), lowercase_filename.begin(),
            [](unsigned char c) { return std::tolower(c); });
        if (auto [it, inserted] = database_by_lowercase_filename.emplace(lowercase_filename, db); !inserted)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot dump to `--dump-schema-dir`: databases {} and {} would be written to the same file "
                "on a case-insensitive filesystem", backQuoteIfNeed(it->second), backQuoteIfNeed(db));
    }

    std::filesystem::create_directories(output_dir);

    for (const auto & db : *database_order)
    {
        const auto path = file_path(db);
        std::ofstream file(path);
        if (!file)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open {} for writing", path.string());

        std::vector<String> dumped_creates = {create_database_query_by_db.at(db)};
        for (size_t i : order)
            if (tables[i].database == db)
                dumped_creates.push_back(tables[i].create_query);

        file << replaySettingsPrelude(settings_known_to_server, dumped_creates);
        file << create_database_query_by_db.at(db) << ";\n\nUSE " << backQuoteIfNeed(db) << ";\n\n";
        for (size_t i : order)
            if (tables[i].database == db)
                file << tables[i].create_query << ";\n\n";
        file.flush();
        if (file.fail())
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE, "Failed writing {}", path.string());
    }

    for (const auto & db : *database_order)
        out << "Dumped database " << backQuoteIfNeed(db) << " schema to " << file_path(db).string() << '\n';
    if (has_cross_database_dependency)
        out << "Note: some tables depend on tables in another dumped database; replay these files in the order printed above.\n";
}

}
