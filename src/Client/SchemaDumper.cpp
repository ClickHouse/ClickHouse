#include <Client/SchemaDumper.h>

#include <Client/IServerConnection.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Core/Protocol.h>
#include <Core/QualifiedTableName.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <Databases/TablesDependencyGraph.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <Functions/FunctionFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Interpreters/misc.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageURL.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <base/EnumReflection.h>
#include <Poco/Net/IPAddress.h>
#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/isLocalAddress.h>
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

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

namespace Setting
{
    extern const SettingsUInt64 table_function_remote_max_addresses;
}

namespace ErrorCodes
{
    extern const int CLUSTER_DOESNT_EXIST;
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

bool startsWithCaseInsensitive(std::string_view s, std::string_view prefix)
{
    return prefix.size() <= s.size() && equalsCaseInsensitive(s.substr(0, prefix.size()), prefix);
}

bool endsWithCaseInsensitive(std::string_view s, std::string_view suffix)
{
    return suffix.size() <= s.size() && equalsCaseInsensitive(s.substr(s.size() - suffix.size()), suffix);
}

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
/// A result column can arrive wrapped in an internal representation (`Const` over a local connection,
/// `Replicated`, `Sparse`); every cast below reads the unwrapped block instead.
Block unwrapColumns(const Block & block)
{
    Block full = block;
    for (auto & column : full)
        column.column = column.column->convertToFullIfWrapped();
    return full;
}

std::vector<String> fetchStringColumn(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info, const String & query,
    const Settings & base_settings)
{
    std::vector<String> result;
    executeQuery(connection, timeouts, client_info, query, [&](const Block & block)
    {
        if (block.empty())
            return;

        const Block full = unwrapColumns(block);
        const ColumnString & column = typeid_cast<const ColumnString &>(*full.getByPosition(0).column);
        for (size_t i = 0; i < column.size(); ++i)
            result.emplace_back(column[i].safeGet<String>());
    }, base_settings);
    return result;
}

struct DatabaseInfo
{
    String engine;
    bool is_external = false;
};

std::map<String, DatabaseInfo> fetchDatabaseInfo(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info, const Settings & base_settings)
{
    std::map<String, DatabaseInfo> result;
    executeQuery(
        connection,
        timeouts,
        client_info,
        "SELECT name, engine, is_external FROM system.databases ORDER BY name",
        [&](const Block & block)
        {
            if (block.empty())
                return;
            const Block full = unwrapColumns(block);
            const auto & name_col = typeid_cast<const ColumnString &>(*full.getByPosition(0).column);
            const auto & engine_col = typeid_cast<const ColumnString &>(*full.getByPosition(1).column);
            const auto & external_col = typeid_cast<const ColumnUInt8 &>(*full.getByPosition(2).column);
            for (size_t i = 0; i < name_col.size(); ++i)
                result.emplace(
                    name_col[i].safeGet<String>(), DatabaseInfo{engine_col[i].safeGet<String>(), external_col.getData()[i] != 0});
        },
        base_settings);
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

struct NamedCollectionDependencies
{
    bool unscannable = false;
    std::vector<String> confirmed;
    std::vector<String> unconfirmed;
};

/// One dumped table, with enough dependency information to order it for replay.
struct TableInfo
{
    String database;
    String name;
    String create_query;
    bool emit = true;
    std::vector<std::pair<String, String>> dependencies; /// (database, name) pairs this table must be created after
    /// Database-less references no dumped database contains. They resolve against `database` on
    /// replay, which does not contain them either, so the dump cannot create them.
    std::vector<String> unresolved_references;
    NamedCollectionDependencies named_collections;
    /// Materialized view only: replay may reach a check that `allow_materialized_view_with_bad_select` relaxes.
    bool needs_bad_select_gate = false;
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
    NamedCollectionDependencies named_collections;
    String target_database; /// materialized view only: its `TO` target, explicit or implicit
    String target_table;
};

/// Detects when system.tables visibility settings must be enabled for selected external databases.
/// Redundant SETTINGS clauses are avoided because constrained profiles can reject them.
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
        "SELECT name FROM system.databases WHERE engine IN ('MySQL', 'PostgreSQL', 'Remote', 'RemoteSecure', 'Cluster') AND name IN ("
            + database_list + ")",
        base_settings);

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

        const Block full = unwrapColumns(block);
        const ColumnString & database_column = typeid_cast<const ColumnString &>(*full.getByPosition(0).column);
        const ColumnString & name_column = typeid_cast<const ColumnString &>(*full.getByPosition(1).column);
        const ColumnString & engine_column = typeid_cast<const ColumnString &>(*full.getByPosition(2).column);
        const ColumnString & create_query_column = typeid_cast<const ColumnString &>(*full.getByPosition(3).column);
        const ColumnString & as_select_column = typeid_cast<const ColumnString &>(*full.getByPosition(4).column);
        const ColumnUUID & uuid_column = typeid_cast<const ColumnUUID &>(*full.getByPosition(5).column);
        const auto & loading_deps_database_column = *full.getByPosition(6).column;
        const auto & loading_deps_table_column = *full.getByPosition(7).column;
        const auto & dependents_database_column = *full.getByPosition(8).column;
        const auto & dependents_table_column = *full.getByPosition(9).column;
        const ColumnString & target_database_column = typeid_cast<const ColumnString &>(*full.getByPosition(10).column);
        const ColumnString & target_table_column = typeid_cast<const ColumnString &>(*full.getByPosition(11).column);

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

struct ClusterNames
{
    std::set<String> known;
    std::set<String> local;
};

/// Cluster and server metadata used to classify distributed references as local dependencies.
struct ClusterLocality
{
    std::function<const ClusterNames &()> names;
    /// What `parseRemoteFunctionArguments` compares a spelled-out port against; the secure port is
    /// fetched on first use because asking a server without one raises an error.
    UInt16 tcp_port = 0;
    std::function<UInt16()> tcp_port_secure;
    /// clickhouse-local listens on no port, so it treats any address with an explicit port as remote.
    bool treat_local_port_as_remote = false;
    /// `Context::tryGetCluster` expands macros before looking a cluster up, but a stored definition
    /// keeps the placeholder text, so the same expansion has to happen before the lookups here.
    std::function<const std::map<String, String> &()> macros;
    /// Named collections the dump session can read, by name and then key. `remote*` resolves an
    /// identifier first argument against these before the clusters, so one can name a local address.
    /// Fetched on first use because most dumps contain no such call.
    std::function<const std::map<String, std::map<String, String>> &()> named_collections;
    /// Server hostnames and local cluster replica addresses considered local dependencies.
    std::function<const std::set<String> &()> local_hostnames;
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
    return equalsCaseInsensitive(name, "cluster") || equalsCaseInsensitive(name, "clusterAllReplicas");
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
        cluster_name = expandClusterMacros(*cluster_name, clusters.macros());
    if (!clusters.names().known.contains(*cluster_name))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot resolve cluster {} of {} on the connected server for --dump-schema: "
            "whether its table reference is a local dependency depends on the cluster's local replicas",
            backQuoteIfNeed(*cluster_name), function.formatForErrorMessage());
    return *cluster_name;
}

bool isRemoteFunctionName(const String & name)
{
    return equalsCaseInsensitive(name, "remote") || equalsCaseInsensitive(name, "remoteSecure");
}

/// Whether the server reads one replica of a `remote*` pattern locally: only a loopback address or a name the server
/// reports as its own, on its port, counts, because the dump cannot resolve other hosts the way the server does.
bool remoteAddressIsLocal(const String & address, bool secure, const ClusterLocality & clusters)
{
    bool has_explicit_port = address.starts_with('[') ? address.contains("]:") : address.contains(':');
    if (has_explicit_port && clusters.treat_local_port_as_remote)
        return false;
    String host = address;
    if (has_explicit_port)
    {
        auto [parsed_host, port] = parseAddress(address, 0);
        if (port != (secure ? clusters.tcp_port_secure() : clusters.tcp_port))
            return false;
        host = parsed_host;
    }
    if (host.starts_with('[') && host.ends_with(']'))
        host = host.substr(1, host.size() - 2);
    if (equalsCaseInsensitive(host, "localhost"))
        return true;
    /// `isLocalAddress` decides a loopback address by its value alone, so the client answers it the way the server does.
    if (Poco::Net::IPAddress ip; Poco::Net::IPAddress::tryParse(host, ip) && ip.isLoopback())
        return isLocalAddress(ip);
    if (clusters.local_hostnames)
    {
        for (const auto & local_host : clusters.local_hostnames())
            if (equalsCaseInsensitive(host, local_host))
                return true;
    }
    return false;
}

/// Whether any replica of a `remote*` address pattern is read without a connection.
bool remoteDescriptionHasLocalReplica(const String & pattern, bool secure, const ClusterLocality & clusters)
{
    size_t max_addresses = clusters.context->getSettingsRef()[Setting::table_function_remote_max_addresses];
    for (const auto & shard : parseRemoteDescription(pattern, 0, pattern.size(), ',', max_addresses))
        for (const auto & replica : parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses))
            if (remoteAddressIsLocal(replica, secure, clusters))
                return true;
    return false;
}

/// What a `remote*` named-collection call names, once the call's overrides are applied.
struct RemoteCollectionTarget
{
    String addresses;
    String database;
    String table;
    /// `remote(nc, database = mysql(...))`: the target is a table function, so there is no table edge.
    bool target_is_table_function = false;
};

/// The collection a `remote*` identifier first argument names: `parseRemoteFunctionArguments` tries
/// named collections before configured clusters. Returns null when the name is a cluster and the call
/// cannot be in the collection form, and refuses otherwise - a collection the dump session cannot see
/// would otherwise pass for a remote address and lose the dependency edge.
const std::map<String, String> * tryGetRemoteNamedCollection(
    const ASTFunction & function, const String & name, const ClusterLocality & clusters)
{
    const auto & collections = clusters.named_collections();
    if (auto it = collections.find(name); it != collections.end())
        return &it->second;
    /// A named collection takes only `key = value` or table-function arguments after its name, so any other one proves a cluster.
    const auto & arguments = function.arguments->children;
    const bool may_be_collection = std::all_of(
        std::next(arguments.begin()), arguments.end(), [](const ASTPtr & argument) { return argument->as<ASTFunction>() != nullptr; });
    const bool is_cluster = clusters.names().known.contains(name);
    if (is_cluster && !may_be_collection)
        return nullptr;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Cannot resolve {} of {} on the connected server for --dump-schema: {}, and a named collection may point at a local address",
        backQuoteIfNeed(name), function.formatForErrorMessage(),
        is_cluster ? "a named collection of that name, which this session cannot read, would be read before the cluster"
                   : "it names neither a cluster nor a named collection this session can read");
}

/// Reads a named-collection override value as text, the way `getKeyValueFromAST` does.
std::optional<String> tryReadNamedCollectionValue(const ASTPtr & arg, const ClusterLocality & clusters)
{
    ASTPtr evaluated = arg;
    if (!arg->as<ASTLiteral>())
    {
        if (dependsOnUnstoredContext(*arg, clusters.context))
            return std::nullopt;
        try
        {
            evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(arg->clone(), clusters.context);
        }
        catch (Exception &) // NOLINT(bugprone-empty-catch)
        {
            /// Not a constant: the caller refuses the dump.
            return std::nullopt;
        }
    }
    const auto * literal = evaluated->as<ASTLiteral>();
    if (!literal)
        return std::nullopt;
    if (literal->value.getType() == Field::Types::String)
        return literal->value.safeGet<String>();
    if (literal->value.getType() == Field::Types::UInt64)
        return toString(literal->value.safeGet<UInt64>());
    return std::nullopt;
}

/// Mirrors the named-collection branch of `parseRemoteFunctionArguments`: the collection's own keys
/// with the call's `key = value` overrides applied on top.
RemoteCollectionTarget resolveRemoteNamedCollection(
    const ASTFunction & function, const String & name, const std::map<String, String> & collection,
    const ClusterLocality & clusters)
{
    auto refuse = [&](std::string_view reason)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot statically resolve named collection {} of {} for --dump-schema: {}, so whether its table "
            "reference is a local dependency is unknown",
            backQuoteIfNeed(name), function.formatForErrorMessage(), reason);
    };

    std::map<String, String> values = collection;
    RemoteCollectionTarget target;
    const auto & args = function.arguments->children;
    for (size_t i = 1; i < args.size(); ++i)
    {
        const auto * equals = args[i]->as<ASTFunction>();
        if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
            continue;
        String key;
        if (!tryGetIdentifierNameInto(equals->arguments->children[0], key))
            continue;
        /// Credentials and the sharding key name no table and reach no address.
        if (key == "user" || key == "username" || key == "password" || key == "sharding_key")
            continue;
        const ASTPtr & value = equals->arguments->children[1];
        if (const auto * value_function = value->as<ASTFunction>();
            value_function && TableFunctionFactory::instance().isTableFunctionName(value_function->name))
        {
            target.target_is_table_function = true;
            continue;
        }
        auto text = tryReadNamedCollectionValue(value, clusters);
        if (!text)
            refuse("override " + backQuoteIfNeed(key) + " is not a constant the dump can read");
        else
            values[key] = *text;
    }

    /// Without SHOW NAMED COLLECTIONS SECRETS every value reads back as [HIDDEN].
    for (const auto & key : {"addresses_expr", "host", "hostname", "port", "db", "database", "table"})
        if (auto it = values.find(key); it != values.end() && it->second == "[HIDDEN]")
            refuse("its values are masked for this session");

    auto get = [&](const String & key) -> String
    {
        auto it = values.find(key);
        return it == values.end() ? String{} : it->second;
    };

    target.addresses = get("addresses_expr");
    if (target.addresses.empty())
    {
        String host = values.contains("host") ? get("host") : get("hostname");
        if (host.empty())
            refuse("it carries no address");
        String port = get("port");
        target.addresses = port.empty() ? host : host + ':' + port;
    }
    /// `db` wins over `database`, and neither present means the server's own default.
    if (values.contains("db"))
        target.database = get("db");
    else if (values.contains("database"))
        target.database = get("database");
    else
        target.database = "default";
    target.table = get("table");
    return target;
}

/// Whether a `remote*` call has a replica the server reads without a connection, the way
/// `parseRemoteFunctionArguments` builds its ad-hoc cluster from the first argument.
bool remoteFunctionHasLocalReplica(const ASTFunction & function, const ClusterLocality & clusters)
{
    const auto & first = function.arguments->children.at(0);
    bool secure = equalsCaseInsensitive(function.name, "remoteSecure");
    String name;
    if (tryGetIdentifierNameInto(first, name))
    {
        if (const auto * collection = tryGetRemoteNamedCollection(function, name, clusters))
            return remoteDescriptionHasLocalReplica(
                resolveRemoteNamedCollection(function, name, *collection, clusters).addresses, secure, clusters);
        return clusters.names().local.contains(name);
    }
    const auto * literal = first->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return false;
    return remoteDescriptionHasLocalReplica(literal->value.safeGet<String>(), secure, clusters);
}

/// Whether a `cluster*`/`remote*` call reads its table argument on this instance.
bool distributedFunctionReadsLocally(const ASTFunction & function, const ClusterLocality & clusters)
{
    if (!function.arguments || function.arguments->children.empty())
        return false;
    if (isClusterTableFunctionName(function.name))
        return function.arguments->children.size() >= 2
            && clusters.names().local.contains(resolveClusterOfFunction(function, clusters));
    /// A `remote*` call needs no second argument: a named collection can carry the table itself.
    return remoteFunctionHasLocalReplica(function, clusters);
}

/// Returns the argument subtree that cannot contain local dependencies for non-local `remote`/`cluster`.
const IAST * remoteFunctionArgumentsToSkip(const IAST & node, const ClusterLocality & clusters)
{
    const auto * function = node.as<ASTFunction>();
    if (!function || !function->arguments)
        return nullptr;
    if (isRemoteFunctionName(function->name) || isClusterTableFunctionName(function->name))
    {
        /// Nothing a non-local call names is read on this instance, so the whole argument list goes:
        /// callers compare against direct children, and an argument node is not one.
        if (function->arguments->children.size() >= 2 && !distributedFunctionReadsLocally(*function, clusters))
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

/// Folds a `merge`/`loop` name argument like their parsers do, except one that reads the session or the server,
/// which the dump does not share.
std::optional<String> tryFoldNameArgument(const ASTPtr & arg, const ContextPtr & context)
{
    if (const auto * identifier = arg->as<ASTIdentifier>())
        return identifier->name();
    if (dependsOnUnstoredContext(*arg, context))
        return std::nullopt;
    try
    {
        auto evaluated = evaluateConstantExpressionAsLiteral(arg->clone(), context);
        if (const auto * literal = evaluated->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
            return literal->value.safeGet<String>();
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
        /// Not a constant name: the caller refuses it or treats it as unknown.
    }
    return std::nullopt;
}

/// Reads a `merge` argument like `tryFoldNameArgument`, or unwraps a `REGEXP('...')` wrapper (the
/// syntax `TableFunctionMerge` accepts for a database-name regexp) into its own literal.
std::optional<String> tryFoldMergeArgument(const ASTPtr & arg, bool & is_regexp, const ContextPtr & context)
{
    is_regexp = false;
    if (const auto * function = arg->as<ASTFunction>(); function && equalsCaseInsensitive(function->name, "REGEXP") && function->arguments
        && function->arguments->children.size() == 1)
    {
        if (const auto * inner = function->arguments->children[0]->as<ASTLiteral>(); inner && inner->value.getType() == Field::Types::String)
        {
            is_regexp = true;
            return inner->value.safeGet<String>();
        }
    }
    return tryFoldNameArgument(arg, context);
}

/// What a reference can bind to: `dictGet`/`dictionary()` name a dictionary and `joinGet` a Join
/// table, so a same-named object of any other engine is not a candidate binding for them.
enum class ReferenceKind
{
    Any,
    Dictionary,
    Join,
};

struct TableReference
{
    String database;
    String table;
    ReferenceKind kind = ReferenceKind::Any;
};

/// Whether an object with this `system.tables.engine` can be what such a reference resolved to.
bool engineCanBind(ReferenceKind kind, const String & engine)
{
    switch (kind)
    {
        case ReferenceKind::Dictionary:
            return engine == "Dictionary";
        case ReferenceKind::Join:
            return engine == "Join";
        case ReferenceKind::Any:
            return true;
    }
}

/// Collects local `merge` and `loop` references, resolving empty `merge` databases to the owner.
void collectMergeAndLoopReferences(
    const IAST & node,
    const std::map<String, std::set<String>> & table_names_by_db,
    const std::set<String> & undumped_databases,
    const std::map<String, std::map<String, String>> & undumped_tables_by_db,
    const String & owning_database,
    const ContextPtr & context,
    std::vector<TableReference> & out)
{
    if (const auto * function = node.as<ASTFunction>(); function && function->arguments)
    {
        const auto & args = function->arguments->children;
        if (equalsCaseInsensitive(function->name, "merge") && (args.size() == 1 || args.size() == 2))
        {
            /// Older servers store `merge('re')` as written; it reads the current database, like `merge('', 're')`.
            bool database_is_regexp = false;
            std::optional<String> database_pattern
                = args.size() == 1 ? std::optional<String>(String{}) : tryFoldMergeArgument(args[0], database_is_regexp, context);
            bool table_is_regexp = false;
            std::optional<String> table_pattern = tryFoldMergeArgument(args.back(), table_is_regexp, context);

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
                            for (const auto & [db, tables] : undumped_tables_by_db)
                                if (db != owning_database)
                                    for (const auto & table_and_engine : tables)
                                        if (table_regexp.match(table_and_engine.first))
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
                            out.push_back({db, *table_pattern});
                            continue;
                        }
                        for (const auto & table : it->second)
                            if (table_regexp.match(table))
                                out.push_back({db, table});
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
                /// Treating an argument this walker cannot fold as dependency-free risks an unreplayable ordering.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                    "only constant expressions that read neither the session nor the server are supported",
                    function->formatForErrorMessage());
            }
        }
        else if (equalsCaseInsensitive(function->name, "loop") && args.size() == 1)
        {
            if (args[0]->as<ASTFunction>())
            {
                /// loop(other_table_function(...)): an inner table function (e.g. loop(numbers(10))), not a table reference.
                /// Its child expressions will be visited by the recursive traversal below.
            }
            else if (auto candidate = tryGetQualifiedNameFromFunctionArgument(*function, 0))
                out.push_back({candidate->first, candidate->second});
            else
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the table argument of {} for --dump-schema: "
                    "only table identifiers, string literals, and inner table functions are supported",
                    function->formatForErrorMessage());
            }
        }
        else if (equalsCaseInsensitive(function->name, "loop") && args.size() == 2)
        {
            /// loop(database, table): two separate plain arguments, not one qualified "db.table" name.
            auto database = tryFoldNameArgument(args[0], context);
            auto table = tryFoldNameArgument(args[1], context);
            if (database && table)
                out.push_back({*database, *table});
            else
                /// Same reasoning as the merge() case above.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot statically resolve the database/table arguments of {} for --dump-schema: "
                    "only constant expressions that read neither the session nor the server are supported",
                    function->formatForErrorMessage());
        }
    }
}

/// Collects table references from dictionary, join, `IN`, and local `cluster`/`remote` function arguments.
void collectFunctionArgumentReferences(
    const IAST & node, const ClusterLocality & clusters,
    std::vector<TableReference> & out)
{
    if (const auto * function = node.as<ASTFunction>())
    {
        std::optional<std::pair<String, String>> candidate;
        ReferenceKind candidate_kind = ReferenceKind::Any;
        if (isClusterTableFunctionName(function->name) || isRemoteFunctionName(function->name))
        {
            /// A call with local replicas reads the named table locally; no current-database
            /// fallback here, a database-less first argument names the database for argument 2.
            if (distributedFunctionReadsLocally(*function, clusters))
            {
                const auto & args = function->arguments->children;
                /// A named-collection call carries no positional database/table arguments: the edge
                /// comes from the collection's own keys, with the call's overrides applied.
                if (isRemoteFunctionName(function->name))
                {
                    String collection_name;
                    if (tryGetIdentifierNameInto(args[0], collection_name))
                    {
                        if (const auto * collection = tryGetRemoteNamedCollection(*function, collection_name, clusters))
                        {
                            auto target = resolveRemoteNamedCollection(
                                *function, collection_name, *collection, clusters);
                            if (!target.target_is_table_function && !target.table.empty())
                                out.push_back({target.database, target.table});
                            return;
                        }
                    }
                }
                /// `remote('addr')` alone reads `system.one`, and `cluster('name')` cannot get here.
                if (args.size() < 2)
                    return;
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
                    out.push_back({dependency->first, dependency->second});
            }
        }
        else if (functionIsDictGet(function->name) || functionIsJoinGet(function->name) || equalsCaseInsensitive(function->name, "dictionary"))
        {
            candidate = tryGetQualifiedNameFromFunctionArgument(*function, 0);
            candidate_kind = functionIsJoinGet(function->name) ? ReferenceKind::Join : ReferenceKind::Dictionary;
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
            out.push_back({candidate->first, candidate->second, candidate_kind});
    }
}

void addNamedCollectionDependency(const String & name, const ClusterLocality & clusters, NamedCollectionDependencies & dependencies)
{
    if (clusters.named_collections().contains(name))
        dependencies.confirmed.push_back(name);
    else
        dependencies.unconfirmed.push_back(name);
}

bool tableFunctionUsesNamedCollections(std::string_view name)
{
    static constexpr std::string_view names[] = {
        "remote",
        "remoteSecure",
        "url",
        "urlCluster",
        "mysql",
        "postgresql",
        "mongodb",
        "jdbc",
        "odbc",
        "arrowFlight",
        "arrowflight",
        "bigquery",
        "fuzzJSON",
        "ytsaurus",
        "s3",
        "s3Cluster",
        "gcs",
        "cosn",
        "oss",
        "azureBlobStorage",
        "azureBlobStorageCluster",
        "hdfs",
        "hdfsCluster",
        "iceberg",
        "icebergCluster",
        "icebergS3",
        "icebergS3Cluster",
        "icebergAzure",
        "icebergAzureCluster",
        "icebergHDFS",
        "icebergHDFSCluster",
        "icebergLocal",
        "icebergLocalCluster",
        "deltaLake",
        "deltaLakeCluster",
        "deltaLakeS3",
        "deltaLakeS3Cluster",
        "deltaLakeAzure",
        "deltaLakeAzureCluster",
        "deltaLakeLocal",
        "hudi",
        "hudiCluster",
        "paimon",
        "paimonCluster",
        "paimonS3",
        "paimonS3Cluster",
        "paimonAzure",
        "paimonAzureCluster",
        "paimonHDFS",
        "paimonHDFSCluster",
        "paimonLocal",
    };
    return std::ranges::any_of(names, [&](std::string_view n) { return equalsCaseInsensitive(n, name); });
}

bool tableEngineUsesNamedCollections(std::string_view name)
{
    static constexpr std::string_view names[] = {
        "URL",
        "MySQL",
        "PostgreSQL",
        "MaterializedPostgreSQL",
        "MongoDB",
        "JDBC",
        "ODBC",
        "ArrowFlight",
        "BigQuery",
        "FuzzJSON",
        "YTsaurus",
        "Redis",
        "Kafka",
        "NATS",
        "RabbitMQ",
        "Remote",
        "RemoteSecure",
        "S3",
        "S3Queue",
        "GCS",
        "COSN",
        "OSS",
        "AzureBlobStorage",
        "AzureQueue",
        "HDFS",
        "Iceberg",
        "IcebergS3",
        "IcebergAzure",
        "IcebergHDFS",
        "IcebergLocal",
        "DeltaLake",
        "DeltaLakeS3",
        "DeltaLakeAzure",
        "DeltaLakeLocal",
        "Hudi",
        "Paimon",
        "PaimonS3",
        "PaimonAzure",
        "PaimonHDFS",
        "PaimonLocal",
    };
    return std::ranges::any_of(names, [&](std::string_view n) { return equalsCaseInsensitive(n, name); });
}

bool databaseEngineUsesNamedCollections(std::string_view name)
{
    static constexpr std::string_view names[] = {
        "S3",
        "MySQL",
        "PostgreSQL",
        "MaterializedPostgreSQL",
        "Remote",
        "RemoteSecure",
    };
    return std::ranges::any_of(names, [&](std::string_view n) { return equalsCaseInsensitive(n, name); });
}

void collectNamedCollectionFromFunction(
    const ASTFunction & function,
    size_t slot,
    bool classify_remote,
    const ClusterLocality & clusters,
    NamedCollectionDependencies & dependencies)
{
    if (!function.arguments || slot >= function.arguments->children.size())
        return;

    String name;
    if (!tryGetIdentifierNameInto(function.arguments->children[slot], name))
        return;

    if (classify_remote)
    {
        if (tryGetRemoteNamedCollection(function, name, clusters))
            dependencies.confirmed.push_back(name);
    }
    else
        addNamedCollectionDependency(name, clusters, dependencies);
}

void collectNamedCollectionsFromTableExpressions(
    const IAST & node, const ClusterLocality & clusters, NamedCollectionDependencies & dependencies);

void collectNamedCollectionsFromTableFunction(
    const ASTFunction & function, const ClusterLocality & clusters, NamedCollectionDependencies & dependencies)
{
    const bool is_remote = isRemoteFunctionName(function.name);
    if (tableFunctionUsesNamedCollections(function.name))
    {
        const size_t slot = endsWithCaseInsensitive(function.name, "Cluster") ? 1 : 0;
        collectNamedCollectionFromFunction(function, slot, is_remote, clusters, dependencies);
    }

    if (!function.arguments)
        return;

    for (const auto & argument : function.arguments->children)
        collectNamedCollectionsFromTableExpressions(*argument, clusters, dependencies);

    const auto collect_nested = [&](const ASTPtr & argument)
    {
        const auto * nested = argument ? argument->as<ASTFunction>() : nullptr;
        if (nested && TableFunctionFactory::instance().isTableFunctionName(nested->name))
            collectNamedCollectionsFromTableFunction(*nested, clusters, dependencies);
    };

    const auto & arguments = function.arguments->children;
    if ((is_remote || isClusterTableFunctionName(function.name)) && arguments.size() >= 2)
        collect_nested(arguments[1]);
    else if (equalsCaseInsensitive(function.name, "loop") && arguments.size() == 1)
        collect_nested(arguments[0]);
    else if (equalsCaseInsensitive(function.name, "viewIfPermitted") && arguments.size() == 2)
        collect_nested(arguments[1]);

    if (is_remote)
    {
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * equals = arguments[i]->as<ASTFunction>();
            if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
                continue;
            String key;
            if (tryGetIdentifierNameInto(equals->arguments->children[0], key) && (key == "database" || key == "db"))
                collect_nested(equals->arguments->children[1]);
        }
    }
}

void collectNamedCollectionsFromTableExpressions(
    const IAST & node, const ClusterLocality & clusters, NamedCollectionDependencies & dependencies)
{
    if (const auto * table_expression = node.as<ASTTableExpression>())
    {
        if (const auto * function = table_expression->table_function ? table_expression->table_function->as<ASTFunction>() : nullptr)
            collectNamedCollectionsFromTableFunction(*function, clusters, dependencies);
        if (table_expression->subquery)
            collectNamedCollectionsFromTableExpressions(*table_expression->subquery, clusters, dependencies);
        return;
    }

    for (const auto & child : node.children)
        collectNamedCollectionsFromTableExpressions(*child, clusters, dependencies);
}

void collectDictionaryNamedCollection(
    const ASTFunctionWithKeyValueArguments * source, const ClusterLocality & clusters, NamedCollectionDependencies & dependencies)
{
    static constexpr std::string_view source_names[] = {
        "clickhouse",
        "http",
        "mongodb",
        "mysql",
        "postgresql",
        "ytsaurus",
    };
    if (!source || !source->elements
        || !std::ranges::any_of(source_names, [&](std::string_view n) { return equalsCaseInsensitive(n, source->name); }))
        return;

    for (const auto & element : source->elements->children)
    {
        const auto * pair = element->as<ASTPair>();
        if (!pair || pair->first != "name" || !pair->second)
            continue;
        String name;
        if (const auto * literal = pair->second->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
            name = literal->value.safeGet<String>();
        else
            tryGetIdentifierNameInto(pair->second, name);
        if (!name.empty())
            addNamedCollectionDependency(name, clusters, dependencies);
    }
}

/// Parses one stored CREATE and records confirmed, hidden, or unscannable collection dependencies.
NamedCollectionDependencies namedCollectionsOfCreate(const String & create_query, const ClusterLocality & clusters)
{
    NamedCollectionDependencies dependencies;
    ASTPtr create_ast;
    try
    {
        ParserCreateQuery create_parser;
        create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        dependencies.unscannable = true;
        return dependencies;
    }

    const auto * create = create_ast->as<ASTCreateQuery>();
    if (!create)
        return dependencies;

    std::vector<const ASTFunction *> engines;
    if (create->storage && create->storage->engine)
        engines.push_back(create->storage->engine);
    /// A view or TimeSeries keeps its inner table engines in `targets`.
    if (create->targets && !create->getTable().empty())
        for (const auto * inner : create->targets->getInnerEngines())
            if (inner->engine)
                engines.push_back(inner->engine);

    for (const auto * engine : engines)
    {
        if (!(create->getTable().empty() ? databaseEngineUsesNamedCollections(engine->name) : tableEngineUsesNamedCollections(engine->name)))
            continue;
        const bool classify_remote = !create->getTable().empty() && isRemoteFunctionName(engine->name);
        collectNamedCollectionFromFunction(*engine, 0, classify_remote, clusters, dependencies);
        if (classify_remote)
            collectNamedCollectionsFromTableFunction(*engine, clusters, dependencies);
    }
    if (const auto * function = create->as_table_function ? create->as_table_function->as<ASTFunction>() : nullptr)
        collectNamedCollectionsFromTableFunction(*function, clusters, dependencies);
    if (create->select)
        collectNamedCollectionsFromTableExpressions(*create->select, clusters, dependencies);
    if (create->dictionary)
        collectDictionaryNamedCollection(create->dictionary->source, clusters, dependencies);
    return dependencies;
}

/// Combines server dependency columns with parsed view references and drops implicit storage.
std::vector<TableInfo> resolveTables(
    std::vector<RawTableRow> rows,
    const ClusterLocality & clusters,
    const std::set<String> & undumped_databases,
    const std::map<String, std::map<String, String>> & undumped_tables_by_db,
    const std::map<String, String> & database_queries,
    const std::map<String, DatabaseInfo> & database_info)
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
                inner_owner[{row.database, getTimeSeriesInnerTableName(kind, owner_id, TimeSeriesVersion::LATEST)}]
                    = {{row.database, row.name}, uuid_named};
            }
            auto table_exists = [&](const String & name)
            {
                return std::any_of(rows.begin(), rows.end(), [&](const auto & other)
                {
                    return other.database == row.database && other.name == name;
                });
            };
            /// An older table names a helper differently. Each legacy name is implicit only while the
            /// modern one is absent, so a user table carrying it stays in the dump.
            for (auto [kind, legacy_name] : {std::pair{ViewTarget::Samples, "data"}, std::pair{ViewTarget::MetricFamilies, "metrics"}})
            {
                const String modern_name = getTimeSeriesInnerTableName(kind, owner_id, TimeSeriesVersion::LATEST);
                if (!target_info.explicit_kinds.contains(kind) && !table_exists(modern_name))
                    inner_owner[{row.database, getTimeSeriesInnerTableName(legacy_name, owner_id)}]
                        = {{row.database, row.name}, uuid_named};
            }
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
    std::map<String, std::map<String, String>> table_engines_by_db;
    /// Match `merge` against all tables, then remap omitted helpers to their owners.
    std::map<String, std::set<String>> all_table_names_by_db;
    for (const auto & row : rows)
    {
        all_table_names_by_db[row.database].insert(row.name);
        if (!implicit_inner.contains({row.database, row.name}))
        {
            known_tables.emplace(row.database, row.name);
            table_engines_by_db[row.database].emplace(row.name, row.engine);
        }
    }

    /// Remote proxy rows are graph-only, but a local proxy must still follow its effective source.
    for (auto & row : rows)
    {
        const auto & database_engine = database_info.at(row.database).engine;
        const bool cluster_database = database_engine == "Cluster";
        if (database_engine != "Remote" && database_engine != "RemoteSecure" && !cluster_database)
            continue;

        /// A `Cluster` proxy is always resolved through its database's `Cluster(...)` arguments.
        const bool use_database_create = row.create_query.empty() || cluster_database;
        const String & create_query = use_database_create ? database_queries.at(row.database) : row.create_query;
        ASTPtr create_ast;
        try
        {
            ParserCreateQuery create_parser;
            create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        catch (const Exception & e)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot parse the stored CREATE for external proxy {}.{} to resolve its local source for --dump-schema: {}",
                backQuoteIfNeed(row.database),
                backQuoteIfNeed(row.name),
                e.message());
        }

        const auto * create = create_ast->as<ASTCreateQuery>();
        const auto * stored_engine = create && create->storage ? create->storage->engine : nullptr;
        const ASTFunction * engine = stored_engine;
        ASTPtr effective_engine;
        if (use_database_create && stored_engine && (stored_engine->name == "Remote" || stored_engine->name == "RemoteSecure"))
        {
            effective_engine = stored_engine->clone();
            auto * proxy_engine = effective_engine->as<ASTFunction>();
            auto & arguments = proxy_engine->arguments->children;
            String collection_name;
            if (!arguments.empty() && tryGetIdentifierNameInto(arguments.front(), collection_name))
                arguments.push_back(
                    makeASTOperator("equals", make_intrusive<ASTIdentifier>("table"), make_intrusive<ASTLiteral>(row.name)));
            else if (arguments.size() >= 2)
                arguments.insert(arguments.begin() + 2, make_intrusive<ASTLiteral>(row.name));
            else
                continue;
            engine = proxy_engine;
        }
        else if (use_database_create && stored_engine && stored_engine->name == "Cluster")
        {
            /// `Cluster('name', 'db')` serves each table the way `cluster('name', 'db', 'table')` reads it.
            if (!stored_engine->arguments || stored_engine->arguments->children.size() != 2)
                continue;
            effective_engine = makeASTFunction(
                "cluster",
                stored_engine->arguments->children[0]->clone(),
                stored_engine->arguments->children[1]->clone(),
                make_intrusive<ASTLiteral>(row.name));
            engine = effective_engine->as<ASTFunction>();
        }
        if (!engine || (engine->name != "Remote" && engine->name != "RemoteSecure" && engine->name != "cluster"))
        {
            if (use_database_create)
                continue;
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot resolve the stored CREATE for external proxy {}.{} for --dump-schema: expected a Remote engine",
                backQuoteIfNeed(row.database),
                backQuoteIfNeed(row.name));
        }

        std::vector<TableReference> references;
        collectFunctionArgumentReferences(*engine, clusters, references);
        for (const auto & reference : references)
            if (std::pair(reference.database, reference.table) != std::pair(row.database, row.name))
                row.loading_dependencies.emplace_back(reference.database, reference.table);
    }

    /// Scan only CREATE slots whose runtime parsers accept named collections.
    for (auto & row : rows)
        if (!database_info.at(row.database).is_external && !row.create_query.empty())
            row.named_collections = namedCollectionsOfCreate(row.create_query, clusters);

    for (auto & row : rows)
    {
        /// A view's select sources are deliberately not in `loading_dependencies_*`, so they are
        /// only discoverable by parsing the stored `SELECT`.
        ASTPtr select_ast;
        if (row.engine == "Distributed" && !row.create_query.empty() && !database_info.at(row.database).is_external)
        {
            /// A table with `ENGINE = Remote(...)` also reads its local shard's table-function target at CREATE.
            ASTPtr create_ast;
            try
            {
                ParserCreateQuery create_parser;
                create_ast = parseQuery(
                    create_parser, row.create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            }
            catch (const Exception & e)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot parse the stored CREATE for {}.{} to resolve its dependencies for --dump-schema: {}",
                    row.database, row.name, e.message());
            }
            auto * create = create_ast->as<ASTCreateQuery>();
            ASTFunction * engine = create && create->storage ? create->storage->engine : nullptr;
            if (!engine || (engine->name != "Remote" && engine->name != "RemoteSecure"))
                continue;
            select_ast = engine->ptr();
        }
        else if (row.engine != "View" && row.engine != "MaterializedView")
            continue;
        else
        {
            try
            {
                /// `as_select` is server-produced SQL already known to be valid, not untrusted input;
                /// max_query_size=0 disables the size cap so a legitimately large SELECT still parses.
                ParserSelectWithUnionQuery select_parser;
                select_ast = parseQuery(
                    select_parser, row.as_select, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
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
        }

        auto add_dependency = [&](const TableReference & candidate)
        {
            std::pair<String, String> resolved{candidate.database, candidate.table};
            if (resolved.first.empty())
            {
                /// Replay resolves unqualified names under `USE <owner database>`; reject ambiguous rebinding.
                /// Only an object the reference can actually bind to competes for the name.
                String other_databases;
                for (const auto & [db, engines] : table_engines_by_db)
                {
                    auto engine_it = engines.find(resolved.second);
                    if (db == row.database || engine_it == engines.end() || !engineCanBind(candidate.kind, engine_it->second))
                        continue;
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
                for (const auto & [db, engines] : undumped_tables_by_db)
                {
                    auto engine_it = engines.find(resolved.second);
                    if (db == row.database || engine_it == engines.end() || !engineCanBind(candidate.kind, engine_it->second))
                        continue;
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

        std::vector<TableReference> references;
        visitLocalAST(select_ast.get(), clusters, [&](const IAST & node)
        {
            if (const auto * table_id = node.as<ASTTableIdentifier>(); table_id && !table_id->getDatabaseName().empty())
                references.push_back({table_id->getDatabaseName(), table_id->shortName()});
            collectFunctionArgumentReferences(node, clusters, references);
            collectMergeAndLoopReferences(
                node, all_table_names_by_db, undumped_databases, undumped_tables_by_db, row.database, clusters.context, references);
        });
        for (const auto & candidate : references)
            add_dependency(candidate);
    }

    std::vector<TableInfo> tables;
    for (auto & row : rows)
    {
        if (implicit_inner.contains({row.database, row.name}))
            continue;

        const bool emit = !database_info.at(row.database).is_external;
        /// Empty emitted CREATE text means concurrent deletion or unreadable catalog metadata.
        if (emit && row.create_query.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot dump {}.{} for --dump-schema: the server returned an empty CREATE for it, "
                "which happens when the table is being dropped concurrently or its metadata cannot "
                "be read. Re-run the dump",
                backQuoteIfNeed(row.database), backQuoteIfNeed(row.name));

        TableInfo table;
        table.database = row.database;
        table.name = row.name;
        table.create_query = std::move(row.create_query);
        table.emit = emit;
        table.dependencies = std::move(row.loading_dependencies);
        table.unresolved_references = std::move(row.unresolved_references);
        table.named_collections = std::move(row.named_collections);
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
    IServerConnection & connection,
    const ConnectionTimeouts & timeouts,
    const ClientInfo & client_info,
    ContextPtr context,
    const std::vector<String> & databases,
    const std::set<String> & undumped_databases,
    const std::map<String, String> & database_queries,
    const std::map<String, DatabaseInfo> & database_info,
    std::map<String, NamedCollectionDependencies> & database_named_collections)
{
    ClusterLocality clusters;
    clusters.context = context;
    /// Most schemas need no cluster metadata, and reading system.clusters requires a separate grant.
    clusters.names = [&, cached = std::optional<ClusterNames>{}]() mutable -> const ClusterNames &
    {
        if (!cached)
        {
            cached.emplace();
            for (auto & name : fetchStringColumn(
                     connection, timeouts, client_info, "SELECT DISTINCT cluster FROM system.clusters", context->getSettingsRef()))
                cached->known.insert(std::move(name));
            for (auto & name : fetchStringColumn(
                     connection, timeouts, client_info, "SELECT DISTINCT cluster FROM system.clusters WHERE is_local", context->getSettingsRef()))
                cached->local.insert(std::move(name));
        }
        return *cached;
    };
    clusters.tcp_port = parse<UInt16>(
        fetchStringColumn(connection, timeouts, client_info, "SELECT toString(tcpPort())", context->getSettingsRef()).at(0));
    clusters.tcp_port_secure = [&, cached = std::optional<UInt16>{}]() mutable
    {
        if (!cached)
        {
            try
            {
                cached = parse<UInt16>(fetchStringColumn(connection, timeouts, client_info,
                    "SELECT toString(getServerPort('tcp_port_secure'))", context->getSettingsRef()).at(0));
            }
            catch (const Exception & e)
            {
                /// No secure port configured: `remoteSecure` then defaults to the well-known one.
                if (e.code() != ErrorCodes::CLUSTER_DOESNT_EXIST)
                    throw;
                cached = DBMS_DEFAULT_SECURE_PORT;
            }
        }
        return *cached;
    };
    clusters.treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;
    using NamedCollectionMap = std::map<String, std::map<String, String>>;
    clusters.named_collections
        = [&, cached = std::optional<NamedCollectionMap>{}]() mutable -> const NamedCollectionMap &
    {
        if (!cached)
        {
            cached.emplace();
            executeQuery(connection, timeouts, client_info,
                "SELECT name, tupleElement(kv, 1), tupleElement(kv, 2) "
                "FROM system.named_collections ARRAY JOIN collection AS kv",
                [&](const Block & block)
                {
                    if (block.empty())
                        return;
                    const Block full = unwrapColumns(block);
                    const auto & name_col = typeid_cast<const ColumnString &>(*full.getByPosition(0).column);
                    const auto & key_col = typeid_cast<const ColumnString &>(*full.getByPosition(1).column);
                    const auto & value_col = typeid_cast<const ColumnString &>(*full.getByPosition(2).column);
                    for (size_t i = 0; i < name_col.size(); ++i)
                        (*cached)[name_col[i].safeGet<String>()].emplace(
                            key_col[i].safeGet<String>(), value_col[i].safeGet<String>());
                }, context->getSettingsRef());
        }
        return *cached;
    };
    /// Macro metadata is needed only when a cluster name actually contains a placeholder.
    clusters.macros = [&, cached = std::optional<std::map<String, String>>{}]() mutable -> const std::map<String, String> &
    {
        if (!cached)
        {
            cached.emplace();
            auto names = fetchStringColumn(
                connection, timeouts, client_info, "SELECT macro FROM system.macros ORDER BY macro", context->getSettingsRef());
            auto values = fetchStringColumn(
                connection, timeouts, client_info, "SELECT substitution FROM system.macros ORDER BY macro", context->getSettingsRef());
            if (names.size() == values.size())
                for (size_t i = 0; i < names.size(); ++i)
                    cached->emplace(std::move(names[i]), std::move(values[i]));
        }
        return *cached;
    };
    /// Local hostnames and cluster replica addresses, queried lazily on first remote* check.
    /// A failure refuses the dump: without these names a same-server address would pass for a remote one.
    clusters.local_hostnames = [&, cached = std::optional<std::set<String>>{}]() mutable -> const std::set<String> &
    {
        if (!cached)
        {
            cached.emplace();
            for (auto & host : fetchStringColumn(
                     connection,
                     timeouts,
                     client_info,
                     "SELECT hostName() UNION DISTINCT SELECT fqdn() UNION DISTINCT SELECT host_name FROM system.clusters WHERE is_local UNION DISTINCT SELECT host_address FROM system.clusters WHERE is_local",
                     context->getSettingsRef()))
                if (!host.empty())
                    cached->insert(std::move(host));
        }
        return *cached;
    };

    /// Fetch table names from undumped databases so unqualified references and empty-database
    /// merge() calls can be checked for ambiguity against them, not just against dumped databases.
    /// A predefined database is never dumped and exists wherever the dump is replayed, so a
    /// namesake there never competes for a database-less reference's binding.
    std::set<String> undumped_databases_to_scan;
    for (const auto & db : undumped_databases)
        if (!DatabaseCatalog::isPredefinedDatabase(db))
            undumped_databases_to_scan.insert(db);

    std::map<String, std::map<String, String>> undumped_tables_by_db;
    if (!undumped_databases_to_scan.empty())
    {
        String undumped_list;
        for (const auto & db : undumped_databases_to_scan)
        {
            if (!undumped_list.empty())
                undumped_list += ", ";
            undumped_list += quoteString(db);
        }
        auto undumped_visibility = detectExternalTableVisibility(connection, timeouts, client_info, undumped_list, context->getSettingsRef());
        executeQuery(connection, timeouts, client_info,
            "SELECT database, name, engine FROM system.tables WHERE database IN (" + undumped_list + ") AND NOT is_temporary",
            [&](const Block & block)
            {
                if (block.empty())
                    return;
                const Block full = unwrapColumns(block);
                const auto & db_col = typeid_cast<const ColumnString &>(*full.getByPosition(0).column);
                const auto & name_col = typeid_cast<const ColumnString &>(*full.getByPosition(1).column);
                const auto & engine_col = typeid_cast<const ColumnString &>(*full.getByPosition(2).column);
                for (size_t i = 0; i < db_col.size(); ++i)
                    undumped_tables_by_db[db_col[i].safeGet<String>()].emplace(
                        name_col[i].safeGet<String>(), engine_col[i].safeGet<String>());
            }, context->getSettingsRef(), undumped_visibility.show_datalake_catalogs, undumped_visibility.show_remote_databases);
    }

    /// A database engine carries a collection the same way a table engine does.
    for (const auto & [db, create_query] : database_queries)
        database_named_collections.emplace(db, namedCollectionsOfCreate(create_query, clusters));

    return resolveTables(
        fetchRawRows(connection, timeouts, client_info, databases, context->getSettingsRef()),
        clusters,
        undumped_databases,
        undumped_tables_by_db,
        database_queries,
        database_info);
}

/// Warns when stored CREATE statements contain masked credentials.
void reportMaskedSecrets(
    const std::vector<TableInfo> & tables, const std::map<String, String> & database_queries, std::ostream & err)
{
    std::set<std::pair<String, String>> masked;
    for (const auto & table : tables)
        if (table.emit && table.create_query.contains("[HIDDEN]"))
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
    const std::vector<TableInfo> & tables,
    const std::map<String, NamedCollectionDependencies> & database_named_collections,
    const std::vector<String> & target_databases,
    std::ostream & err)
{
    std::set<String> dumped_databases(target_databases.begin(), target_databases.end());

    /// A set so the same missing dependency is reported once, in a deterministic order. Predefined
    /// databases are skipped: they always exist wherever the dump is replayed.
    std::set<std::tuple<String, String, String, String>> missing;
    for (const auto & table : tables)
        for (const auto & dependency : table.dependencies)
            if (!dumped_databases.contains(dependency.first) && !DatabaseCatalog::isPredefinedDatabase(dependency.first))
                missing.emplace(table.database, table.name, dependency.first, dependency.second);

    /// A named collection is not in any database, so it is outside every dump set. Its values can
    /// include credentials, which a schema dump must not print, so it is reported rather than emitted.
    std::set<std::tuple<String, String, String>> collections;
    for (const auto & table : tables)
        if (table.emit)
            for (const auto & collection : table.named_collections.confirmed)
                collections.emplace(table.database, table.name, collection);
    std::set<std::tuple<String, String, String>> unconfirmed_collections;
    for (const auto & table : tables)
        if (table.emit)
            for (const auto & collection : table.named_collections.unconfirmed)
                unconfirmed_collections.emplace(table.database, table.name, collection);
    std::set<std::pair<String, String>> database_collections;
    std::set<std::pair<String, String>> unconfirmed_database_collections;
    for (const auto & [database, dependencies] : database_named_collections)
    {
        for (const auto & collection : dependencies.confirmed)
            database_collections.emplace(database, collection);
        for (const auto & collection : dependencies.unconfirmed)
            unconfirmed_database_collections.emplace(database, collection);
    }

    /// The original session database is unknown; replay binds these names to the owner database.
    std::set<std::tuple<String, String, String>> unresolved;
    for (const auto & table : tables)
        if (table.emit)
            for (const auto & reference : table.unresolved_references)
                unresolved.emplace(table.database, table.name, reference);

    for (const auto & [database, name, dependency_database, dependency_name] : missing)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " depends on "
            << backQuoteIfNeed(dependency_database) << "." << backQuoteIfNeed(dependency_name)
            << ", which is outside the dumped database(s) and will not be created by this dump.\n";

    for (const auto & [database, name, collection] : collections)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " depends on named collection "
            << backQuoteIfNeed(collection) << ", which lives on the server rather than in a database and will not be "
            << "created by this dump; its values can include credentials, so the dump does not carry them.\n";

    for (const auto & table : tables)
        if (table.emit && table.named_collections.unscannable)
            err << "Warning: the stored CREATE of " << backQuoteIfNeed(table.database) << "." << backQuoteIfNeed(table.name)
                << " could not be parsed here, so it was not checked for named collections; it is dumped as it is stored.\n";

    for (const auto & [database, name, collection] : unconfirmed_collections)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " may depend on named collection "
            << backQuoteIfNeed(collection) << ", but this session cannot see that name in system.named_collections; "
            << "the collection is not included in this dump.\n";

    for (const auto & [database, collection] : database_collections)
        err << "Warning: database " << backQuoteIfNeed(database) << " depends on named collection "
            << backQuoteIfNeed(collection) << ", which lives on the server rather than in a database and will not be "
            << "created by this dump; its values can include credentials, so the dump does not carry them.\n";

    for (const auto & [database, dependencies] : database_named_collections)
        if (dependencies.unscannable)
            err << "Warning: the stored CREATE of database " << backQuoteIfNeed(database)
                << " could not be parsed here, so it was not checked for named collections; it is dumped as it is stored.\n";

    for (const auto & [database, collection] : unconfirmed_database_collections)
        err << "Warning: database " << backQuoteIfNeed(database) << " may depend on named collection " << backQuoteIfNeed(collection)
            << ", but this session cannot see that name in system.named_collections; "
            << "the collection is not included in this dump.\n";

    for (const auto & [database, name, reference] : unresolved)
        err << "Warning: " << backQuoteIfNeed(database) << "." << backQuoteIfNeed(name) << " references "
            << backQuoteIfNeed(reference) << " without a database; no dumped database contains it, and on replay it "
            << "will resolve against " << backQuoteIfNeed(database) << ", which does not contain it either.\n";

    if (!missing.empty() || !unresolved.empty() || !collections.empty() || !database_collections.empty() || !unconfirmed_collections.empty()
        || !unconfirmed_database_collections.empty())
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

    /// A proxy row is not emitted, so its readers must also wait for the source it reads.
    std::map<std::pair<String, String>, const TableInfo *> proxy_rows;
    for (const auto & table : tables)
        if (!table.emit)
            proxy_rows.emplace(std::pair(table.database, table.name), &table);

    for (const auto & table : tables)
    {
        if (!table.emit)
            continue;
        auto add_dependency = [&](const String & database)
        {
            if (database != table.database && db_set.contains(database))
                depends_on[table.database].insert(database);
        };
        /// A proxy can read through another proxy, so follow the chain to the real source.
        std::set<std::pair<String, String>> visited;
        auto pending = table.dependencies;
        while (!pending.empty())
        {
            auto dependency = pending.back();
            pending.pop_back();
            if (!visited.insert(dependency).second)
                continue;
            add_dependency(dependency.first);
            if (auto it = proxy_rows.find(dependency); it != proxy_rows.end())
                pending.insert(pending.end(), it->second->dependencies.begin(), it->second->dependencies.end());
        }
    }

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
    bool parse_failed = false; /// a statement did not parse, so the gates it might need are unknown
    bool analyzer_group_by = false; /// a GROUP BY or window PARTITION BY in analyzed text
    bool analyzer_order_by = false; /// an ORDER BY or window ORDER BY in analyzed text
    bool analyzer_subquery = false; /// a subquery in analyzed text, which may be correlated
    bool ordinary_database = false;
    bool materialized_postgresql_database = false;
    bool materialized_postgresql_table = false;
    bool time_series_table = false;
    bool kafka_keeper_offsets = false;
    bool nullable_tuple_type = false;
    bool unique_key = false;
    bool data_lake_catalog_database = false;
    bool ytsaurus_table = false;
    bool paimon_table = false;
    bool delta_lake_table = false;

    /// Carriers of the shared gates, `parse_failed` keeps all of them.
    bool funnel_functions = false;
    bool nlp_functions = false;
    bool fuzz_query_functions = false;
    bool error_prone_window_functions = false;
    bool hyperscan_functions = false;
    bool time_series_aggregate_functions = false;
    bool ytsaurus_table_function = false;
    bool eval_table_function = false;
    bool ytsaurus_dictionary_source = false;
    bool low_cardinality_type = false;
    bool fixed_string_type = false;
    bool variant_type = false;
    bool time_type = false;
    bool suspicious_codecs = false;
    bool deprecated_merge_tree_syntax = false;
    bool suspicious_primary_key = false;
    bool suspicious_ttl_expressions = false;
    bool full_text_index = false;
    bool queue_hive_partitioning = false;
    bool url_wildcard = false;
    std::set<String> codec_gates; /// `enable_<family>_codec` of the codecs the statements name
    std::set<String> data_lake_catalog_gates; /// gates of the known `catalog_type`s; `data_lake_catalog_database` keeps all
};

/// Kafka reads its Keeper-offsets gate only when `kafka_keeper_path` or `kafka_replica_name` is set,
/// in SETTINGS or by a named collection, so any non-literal engine argument keeps the gate.
bool kafkaMayStoreOffsetsInKeeper(const ASTStorage & storage)
{
    if (storage.engine->arguments)
        for (const auto & argument : storage.engine->arguments->children)
            if (!argument->as<ASTLiteral>())
                return true;
    if (storage.settings)
        for (const auto & change : storage.settings->changes)
            if (change.name == "kafka_keeper_path" || change.name == "kafka_replica_name")
                return true;
    return false;
}

/// Whether the lowercase `text` has `token` starting at an identifier boundary, and ending at one unless `prefix`.
bool hasToken(std::string_view text, std::string_view token, bool prefix = false)
{
    for (size_t pos = text.find(token); pos != std::string_view::npos; pos = text.find(token, pos + 1))
    {
        if (pos > 0 && isWordCharASCII(text[pos - 1]))
            continue;
        const size_t end = pos + token.size();
        if (prefix || end == text.size() || !isWordCharASCII(text[end]))
            return true;
    }
    return false;
}

/// A `timeSeries*` aggregate function; the bare `TimeSeries` engine name does not match.
bool hasTimeSeriesFunction(std::string_view text)
{
    constexpr std::string_view token = "timeseries";
    for (size_t pos = text.find(token); pos != std::string_view::npos; pos = text.find(token, pos + 1))
    {
        const size_t end = pos + token.size();
        if ((pos == 0 || !isWordCharASCII(text[pos - 1])) && end < text.size() && isAlphaASCII(text[end]))
            return true;
    }
    return false;
}

/// The queue engines read `use_hive_partitioning` from SETTINGS, or from a named collection.
bool queueMayUseHivePartitioning(const ASTStorage & storage)
{
    if (storage.engine->arguments && !storage.engine->arguments->children.empty()
        && !storage.engine->arguments->children.front()->as<ASTLiteral>())
        return true;
    if (storage.settings)
        for (const auto & change : storage.settings->changes)
        {
            std::string_view name = change.name;
            if (name.starts_with("s3queue_"))
                name.remove_prefix(std::string_view("s3queue_").size());
            if (name == "use_hive_partitioning")
                return true;
        }
    return false;
}

/// `url`, `urlCluster` and `ENGINE = URL` list index pages only for a URL the server's own predicate accepts;
/// a URL that is not a string literal (a named collection) may be one.
bool urlMayHaveWildcard(const ASTFunction & function_or_engine)
{
    const size_t url_position = equalsCaseInsensitive(function_or_engine.name, "urlCluster") ? 1 : 0;
    if (!function_or_engine.arguments || function_or_engine.arguments->children.size() <= url_position)
        return true;
    const auto * literal = function_or_engine.arguments->children[url_position]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return true;
    return urlPathHasListableGlobs(literal->value.safeGet<String>());
}

/// The `DataLakeCatalog` creator reads only the gate of its `catalog_type`; nullopt when the type is not known here.
std::optional<std::vector<String>> dataLakeCatalogGates(const ASTStorage & storage)
{
    /// A named collection can carry the settings, so only literal arguments can be trusted.
    if (storage.engine->arguments)
        for (const auto & argument : storage.engine->arguments->children)
            if (!argument->as<ASTLiteral>())
                return std::nullopt;
    const Field * type = storage.settings ? storage.settings->changes.tryGet("catalog_type") : nullptr;
    if (!type || type->getType() != Field::Types::String)
        return std::nullopt;
    String name = type->safeGet<String>();
    std::ranges::transform(name, name.begin(), [](unsigned char c) { return std::tolower(c); });
    if (name == "rest" || name == "onelake" || name == "biglake" || name == "horizon" || name == "s3tables" || name == "delta_sharing")
        return std::vector<String>{"allow_database_iceberg", "allow_experimental_database_iceberg"};
    if (name == "glue")
        return std::vector<String>{"allow_database_glue_catalog", "allow_experimental_database_glue_catalog"};
    if (name == "unity")
        return std::vector<String>{"allow_database_unity_catalog", "allow_experimental_database_unity_catalog"};
    if (name == "hive")
        return std::vector<String>{"allow_experimental_database_hms_catalog"};
    if (name == "paimon_rest")
        return std::vector<String>{"allow_experimental_database_paimon_rest_catalog"};
    return std::nullopt;
}

/// Visits `node` and its subtree, except the `skip` subtree.
void forEachNode(const IAST & node, const std::function<void(const IAST &)> & visit, const IAST * skip = nullptr)
{
    if (&node == skip)
        return;
    visit(node);
    for (const auto & child : node.children)
        forEachNode(*child, visit, skip);
}

/// Lowercase function names and type names of `ast`, without the column, table and database names, so that
/// a gate matched on them is never carried by an object's name.
String nameTokens(const IAST & ast, const IAST * skip, bool with_types, std::vector<ASTPtr> * validated_types = nullptr)
{
    String names;
    std::set<const IAST *> table_functions;
    /// `validateDataType` reads strings only as a table-function structure or a CAST target type; other strings are data.
    const auto add_types_in_string = [&](const IAST & node, bool is_structure)
    {
        const auto * literal = node.as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String)
            return;
        try
        {
            ParserColumnDeclarationList structure_parser;
            ParserDataType type_parser;
            IParser & parser = is_structure ? static_cast<IParser &>(structure_parser) : type_parser;
            const ASTPtr parsed = parseQuery(
                parser, literal->value.safeGet<String>(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            names += nameTokens(*parsed, nullptr, true, validated_types);
            if (validated_types && !is_structure)
                validated_types->push_back(parsed);
            else if (validated_types)
                for (const auto & child : parsed->children)
                    if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getType())
                        validated_types->push_back(column->getType());
        }
        catch (const Exception &) // NOLINT(bugprone-empty-catch)
        {
        }
    };
    forEachNode(ast, [&](const IAST & node)
    {
        if (const auto * table_expression = node.as<ASTTableExpression>(); table_expression && table_expression->table_function)
            table_functions.insert(table_expression->table_function.get());
        else if (const auto * function = node.as<ASTFunction>())
        {
            /// A table function such as `fuzzQuery` or `timeSeriesData` reads none of these gates.
            if (table_functions.contains(&node))
            {
                if (function->arguments)
                    forEachNode(*function->arguments, [&](const IAST & argument) { add_types_in_string(argument, true); });
            }
            else
            {
                names += function->name + ' ';
                const bool is_cast = equalsCaseInsensitive(function->name, "CAST") || equalsCaseInsensitive(function->name, "_CAST")
                    || function->name == "accurateCast" || function->name == "accurateCastOrNull";
                if (is_cast && function->arguments && !function->arguments->children.empty())
                    add_types_in_string(*function->arguments->children.back(), false);
            }
        }
        else if (const auto * data_type = node.as<ASTDataType>(); data_type && with_types)
        {
            names += data_type->name + ' ';
            /// `AggregateFunction(sum, UInt64)` names its function with a bare identifier.
            if (const auto arguments = data_type->getArguments())
                for (const auto & argument : arguments->children)
                    if (const auto * identifier = argument->as<ASTIdentifier>())
                        names += identifier->name() + ' ';
        }
    }, skip);
    std::ranges::transform(names, names.begin(), [](unsigned char c) { return std::tolower(c); });
    return names;
}

/// Which type gates `validateDataType` reads for a type: each is switched off alone, and is needed when that throws.
/// A type this client cannot build needs all of them.
struct TypeGateNeeds
{
    bool low_cardinality = true;
    bool fixed_string = true;
    bool variant = true;
    bool time = true;
    bool nullable_tuple = true;
};

TypeGateNeeds typeGateNeeds(const ASTPtr & type_ast)
{
    DataTypePtr type;
    try
    {
        type = DataTypeFactory::instance().get(type_ast);
    }
    catch (const Exception &)
    {
        return {};
    }
    const auto fails_without = [&type](bool DataTypeValidationSettings::*gate)
    {
        DataTypeValidationSettings settings;
        settings.*gate = false;
        try
        {
            validateDataType(type, settings);
            return false;
        }
        catch (const Exception &)
        {
            return true;
        }
    };
    return {
        .low_cardinality = fails_without(&DataTypeValidationSettings::allow_suspicious_low_cardinality_types),
        .fixed_string = fails_without(&DataTypeValidationSettings::allow_suspicious_fixed_string_types),
        .variant = fails_without(&DataTypeValidationSettings::allow_suspicious_variant_types),
        .time = fails_without(&DataTypeValidationSettings::enable_time_time64_type),
        .nullable_tuple = fails_without(&DataTypeValidationSettings::enable_nullable_tuple_type),
    };
}

/// The codec gates the codec factory reads for a column's CODEC: each is switched off alone, and is needed when that
/// throws. A column type this client cannot build needs all of them.
std::vector<String> codecGateNeeds(const ASTPtr & codec, const ASTPtr & type_ast, const std::vector<String> & gates)
{
    DataTypePtr type;
    try
    {
        if (type_ast)
            type = DataTypeFactory::instance().get(type_ast);
    }
    catch (const Exception &)
    {
        return gates;
    }
    if (!type)
        return gates;
    std::vector<String> needed;
    for (const auto & gate : gates)
    {
        Settings settings;
        for (const auto & other : gates)
            settings.set(other, Field(other != gate));
        try
        {
            CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, type, CodecValidationSettings(settings));
        }
        catch (const Exception &)
        {
            needed.push_back(gate);
        }
    }
    return needed;
}

/// `verifySortingKey` rejects only a `SimpleAggregateFunction` column in the sorting key, so only a key naming one needs
/// `allow_suspicious_primary_key`. The old `MergeTree(date, key, ...)` form keeps its key in the engine arguments.
bool sortingKeyMayUseSimpleAggregateFunction(const ASTStorage & storage, const ASTCreateQuery & create)
{
    if (!create.columns_list || !create.columns_list->columns)
        return true;
    std::set<String> columns;
    for (const auto & child : create.columns_list->columns->children)
        if (const auto * column = child->as<ASTColumnDeclaration>())
            if (const auto * type = column->getType() ? column->getType()->as<ASTDataType>() : nullptr;
                type && equalsCaseInsensitive(type->name, "SimpleAggregateFunction"))
                columns.insert(column->name);
    bool found = false;
    const auto visit = [&](const IAST & node)
    {
        if (const auto * identifier = node.as<ASTIdentifier>(); identifier && columns.contains(identifier->shortName()))
            found = true;
    };
    for (const IAST * key : {storage.order_by, storage.primary_key, storage.engine->arguments.get()})
        if (key)
            forEachNode(*key, visit);
    return found;
}

/// `allow_suspicious_ttl_expressions` only skips the TTL checks: the expression reads a column and calls only
/// deterministic functions. A TTL that passes them on its AST alone does not need the gate.
bool ttlMayNeedSuspiciousGate(const IAST & expression, const std::set<String> & columns, const ContextPtr & context)
{
    bool reads_column = false;
    bool suspicious = false;
    forEachNode(expression, [&](const IAST & node)
    {
        if (const auto * identifier = node.as<ASTIdentifier>())
            reads_column |= columns.contains(identifier->shortName());
        else if (const auto * function = node.as<ASTFunction>())
        {
            /// An aggregate function or a lambda is not a regular function, and keeps the gate.
            const auto resolver = FunctionFactory::instance().tryGet(function->name, context);
            suspicious |= !resolver || !resolver->isDeterministic();
        }
        else if (!node.as<ASTLiteral>() && !node.as<ASTExpressionList>())
            suspicious = true;
    });
    return suspicious || !reads_column;
}

/// Runs a SELECT through the source server's analyzer under setting changes, and says whether it analyzes.
using AnalyzesOnSource = std::function<bool(const String & select_query, const SettingsChanges & changes)>;

bool tableFunctionHasStaticStructure(const ASTFunction & function, const ContextPtr & context);

/// Whether a SELECT analyzes the same on the source server as at replay: every table function in it is a local generator,
/// reads a literal structure, or names its database.
bool analyzesOnlyLocally(const IAST & select, const ContextPtr & context)
{
    static const std::set<std::string_view> generators = {"numbers", "numbers_mt", "zeros", "zeros_mt", "values", "generateRandom",
        "generateSeries", "generate_series", "primes", "null"};
    bool local = true;
    forEachNode(select, [&](const IAST & node)
    {
        const auto * table_expression = node.as<ASTTableExpression>();
        const auto * function = table_expression && table_expression->table_function
            ? table_expression->table_function->as<ASTFunction>() : nullptr;
        if (!function)
            return;
        const auto is = [&](std::string_view name) { return equalsCaseInsensitive(function->name, name); };
        if (std::ranges::any_of(generators, is) || tableFunctionHasStaticStructure(*function, context))
            return;
        /// `merge` and `loop` read local tables; a database-less one would resolve in the dump session's database.
        const auto & arguments = function->arguments ? function->arguments->children : ASTs{};
        if (is("merge") && arguments.size() == 2)
            if (const auto * database = arguments[0]->as<ASTLiteral>();
                database && database->value.getType() == Field::Types::String && !database->value.safeGet<String>().empty())
                return;
        if (is("loop") && arguments.size() == 1)
            if (const auto * table = arguments[0]->as<ASTIdentifier>(); table && table->compound())
                return;
        local = false;
    });
    return local;
}

ReplayGateNeeds collectReplayGateNeeds(
    const std::vector<String> & create_queries, const ContextPtr & context, const AnalyzesOnSource & analyzes_on_source)
{
    ReplayGateNeeds needs;
    std::vector<String> codec_gates_to_check = {"allow_suspicious_codecs"};
    for (const auto & name : allExperimentalSettingNames())
        if (name.starts_with("enable_") && name.ends_with("_codec"))
            codec_gates_to_check.push_back(name);

    /// Whether a dumped database is, or may be, `Replicated` at replay.
    std::map<String, bool> database_may_be_replicated;
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
                    .parse_failed = true, .analyzer_group_by = true, .analyzer_order_by = true,
                    .analyzer_subquery = true, .ordinary_database = true,
                    .materialized_postgresql_database = true,
                    .materialized_postgresql_table = true, .time_series_table = true,
                    .kafka_keeper_offsets = true, .nullable_tuple_type = true, .unique_key = true,
                    .data_lake_catalog_database = true, .ytsaurus_table = true, .paimon_table = true,
                    .delta_lake_table = true, .codec_gates = {}, .data_lake_catalog_gates = {}};
        }

        const auto * create = create_ast->as<ASTCreateQuery>();
        if (!create)
            continue;

        /// The explicit-UUID and engine-argument checks run only for an object created in a `Replicated` database.
        /// The dump creates its databases before their objects; `IF NOT EXISTS` (`default`) keeps whatever engine exists.
        bool in_replicated_database = true;
        if (create->getTable().empty())
            database_may_be_replicated[create->getDatabase()] = create->if_not_exists
                || (create->storage && create->storage->engine && equalsCaseInsensitive(create->storage->engine->name, "Replicated"));
        else if (auto it = database_may_be_replicated.find(create->getDatabase()); it != database_may_be_replicated.end())
            in_replicated_database = it->second;

        /// What `assertOrSetUUID` re-enters on when the dump is replayed into a `Replicated` database.
        if (!create->getTable().empty() && in_replicated_database && (create->has_uuid || create->has_uuid_clause))
            needs.explicit_uuid = true;

        /// All three `allow_materialized_view_with_bad_select` checks sit inside
        /// `InterpreterCreateQuery`'s materialized-view branch, so none can fire without one;
        /// whether one can fire for a given view is decided by `needs_bad_select_gate`.
        if (create->is_materialized_view)
            needs.materialized_view = true;

        /// A plain view stores its columns, so replay neither analyzes its SELECT nor validates its column types.
        const bool plain_view = create->is_ordinary_view
            && (create->isParameterizedView()
                || (create->columns_list && create->columns_list->columns && !create->columns_list->columns->children.empty()));
        const IAST * unread_select = plain_view ? create->select : nullptr;

        /// The analyzer-side gates fire only where stored query text is re-analysed at replay: a
        /// view's AS SELECT, or a projection (`ProjectionsDescription` runs `runOnlyResolve` on it).
        struct AnalyzerCarriers
        {
            bool group_by = false;
            bool order_by = false;
            bool subquery = false;
        };
        const auto scan_analyzed = [](const IAST & query)
        {
            /// Each is read for its own clause: GROUP BY or PARTITION BY keys, ORDER BY keys, a correlated subquery.
            AnalyzerCarriers carriers;
            forEachNode(query, [&carriers](const IAST & node)
            {
                if (const auto * select = node.as<ASTSelectQuery>())
                {
                    carriers.group_by |= select->groupBy() || select->group_by_all;
                    carriers.order_by |= select->orderBy() != nullptr;
                }
                else if (const auto * projection = node.as<ASTProjectionSelectQuery>())
                {
                    carriers.group_by |= projection->groupBy() != nullptr;
                    carriers.order_by |= projection->orderBy() != nullptr;
                }
                else if (const auto * window = node.as<ASTWindowDefinition>())
                {
                    carriers.group_by |= window->partition_by != nullptr;
                    carriers.order_by |= window->order_by != nullptr;
                }
                else if (node.as<ASTSubquery>())
                    carriers.subquery = true;
            });
            return carriers;
        };
        const auto add_carriers = [&needs](const AnalyzerCarriers & carriers)
        {
            needs.analyzer_group_by |= carriers.group_by;
            needs.analyzer_order_by |= carriers.order_by;
            needs.analyzer_subquery |= carriers.subquery;
        };
        /// Asks the source server which gates `select_query` really reads; without a query or an answer, the clauses decide.
        const auto add_gates_read = [&](const AnalyzerCarriers & carriers, const String & select_query)
        {
            const SettingsChanges all_on = {{"allow_suspicious_types_in_group_by", Field(true)},
                {"allow_suspicious_types_in_order_by", Field(true)}, {"allow_experimental_correlated_subqueries", Field(true)}};
            if (select_query.empty() || !analyzes_on_source(select_query, all_on))
            {
                add_carriers(carriers);
                return;
            }
            const auto fails_without = [&](const String & gate)
            {
                SettingsChanges changes = all_on;
                changes.setSetting(gate, Field(false));
                return !analyzes_on_source(select_query, changes);
            };
            needs.analyzer_group_by |= carriers.group_by && fails_without("allow_suspicious_types_in_group_by");
            needs.analyzer_order_by |= carriers.order_by && fails_without("allow_suspicious_types_in_order_by");
            needs.analyzer_subquery |= carriers.subquery && fails_without("allow_experimental_correlated_subqueries");
        };
        const auto has_carrier = [](const AnalyzerCarriers & carriers)
        {
            return carriers.group_by || carriers.order_by || carriers.subquery;
        };
        if (create->select && !plain_view)
        {
            const AnalyzerCarriers carriers = scan_analyzed(*create->select);
            /// A SELECT that may analyze differently on the source (current database, remote data) is not asked.
            const bool ask_source = create->is_materialized_view && analyzes_on_source && has_carrier(carriers)
                && analyzesOnlyLocally(*create->select, context);
            add_gates_read(carriers, ask_source ? create->select->formatWithSecretsOneLine() : "");
        }
        if (create->columns_list && create->columns_list->projections)
            for (const auto & child : create->columns_list->projections->children)
            {
                const auto * declaration = child->as<ASTProjectionDeclaration>();
                const auto * projection = declaration && declaration->query ? declaration->query->as<ASTProjectionSelectQuery>() : nullptr;
                const AnalyzerCarriers carriers = scan_analyzed(*child);
                /// A projection is analyzed as this SELECT over its table's columns, which the source table has too.
                String select_query;
                if (projection && analyzes_on_source && has_carrier(carriers) && !create->getDatabase().empty()
                    && !create->getTable().empty())
                {
                    ASTPtr select = projection->cloneToASTSelect();
                    select->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::SETTINGS, nullptr);
                    select->as<ASTSelectQuery &>().replaceDatabaseAndTable(create->getDatabase(), create->getTable());
                    select_query = select->formatWithSecretsOneLine();
                }
                add_gates_read(carriers, select_query);
            }

        /// `registerStorageMergeTree` re-enters its check only for an engine that kept its arguments.
        if (create->getTable().empty())
        {
            if (create->storage && create->storage->engine)
            {
                const auto & engine = *create->storage->engine;

                /// CREATE DATABASE: gate on the database engine name.
                if (equalsCaseInsensitive(engine.name, "Ordinary"))
                    needs.ordinary_database = true;
                else if (equalsCaseInsensitive(engine.name, "MaterializedPostgreSQL"))
                    needs.materialized_postgresql_database = true;
                else if (equalsCaseInsensitive(engine.name, "DataLakeCatalog"))
                {
                    if (auto gates = dataLakeCatalogGates(*create->storage))
                        needs.data_lake_catalog_gates.insert(gates->begin(), gates->end());
                    else
                        needs.data_lake_catalog_database = true;
                }
            }
        }
        else
        {
            std::vector<const ASTStorage *> table_storages;
            if (create->storage && create->storage->engine)
                table_storages.push_back(create->storage);
            /// A view or TimeSeries keeps its inner table engines in `targets`.
            if (create->targets)
                for (const auto * inner : create->targets->getInnerEngines())
                    if (inner->engine)
                        table_storages.push_back(inner);

            /// CREATE TABLE: gate on the table engine name.
            for (const auto * storage : table_storages)
            {
                const auto * engine = storage->engine;
                if (in_replicated_database && startsWithCaseInsensitive(engine->name, "Replicated") && engine->arguments
                    && !engine->arguments->children.empty())
                    needs.replicated_engine_arguments = true;
                if (equalsCaseInsensitive(engine->name, "MaterializedPostgreSQL"))
                    needs.materialized_postgresql_table = true;
                if (equalsCaseInsensitive(engine->name, "TimeSeries"))
                    needs.time_series_table = true;
                if (equalsCaseInsensitive(engine->name, "Kafka") && kafkaMayStoreOffsetsInKeeper(*storage))
                    needs.kafka_keeper_offsets = true;
                if (equalsCaseInsensitive(engine->name, "YTsaurus"))
                    needs.ytsaurus_table = true;
                if (startsWithCaseInsensitive(engine->name, "Paimon"))
                    needs.paimon_table = true;
                if (startsWithCaseInsensitive(engine->name, "DeltaLake"))
                    needs.delta_lake_table = true;
            }
        }

        /// `enable_unique_key` is checked only for a UNIQUE KEY; a view or TimeSeries keeps its engines in `targets`.
        if (create->storage && create->storage->unique_key)
            needs.unique_key = true;
        if (create->targets)
            for (const auto * inner_engine : create->targets->getInnerEngines())
                if (inner_engine->unique_key)
                    needs.unique_key = true;

        /// Shared gates: each carrier is matched on the statement's names or AST, over-approximated
        /// where the exact check site is not worth mirroring.
        std::vector<ASTPtr> validated_types;
        const String names = nameTokens(*create_ast, unread_select, /* with_types= */ !plain_view, &validated_types);

        needs.funnel_functions |= hasToken(names, "sequencenextnode", true);
        needs.nlp_functions |= hasToken(names, "synonyms") || hasToken(names, "lemmatize") || hasToken(names, "detectlanguage", true)
            || hasToken(names, "detectcharset") || hasToken(names, "detecttonality");
        needs.fuzz_query_functions |= hasToken(names, "fuzzquery");
        needs.error_prone_window_functions |= hasToken(names, "runningaccumulate") || hasToken(names, "runningdifference", true)
            || hasToken(names, "neighbor");
        needs.hyperscan_functions |= hasToken(names, "multimatch", true) || hasToken(names, "multifuzzymatch", true);
        needs.time_series_aggregate_functions |= hasTimeSeriesFunction(names);

        /// Type gates come from `validateDataType` itself. `InterpreterCreateQuery` validates the stored columns except a
        /// view's; a materialized view's inner table validates them on its own CREATE.
        const bool columns_validated = !plain_view && !(create->is_materialized_view && create->hasTargetTableID(ViewTarget::To));
        if (columns_validated && create->columns_list && create->columns_list->columns)
            for (const auto & child : create->columns_list->columns->children)
                if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getType())
                    validated_types.push_back(column->getType());
        for (const auto & type : validated_types)
        {
            const TypeGateNeeds type_needs = typeGateNeeds(type);
            needs.low_cardinality_type |= type_needs.low_cardinality;
            needs.fixed_string_type |= type_needs.fixed_string;
            needs.variant_type |= type_needs.variant;
            needs.time_type |= type_needs.time;
            needs.nullable_tuple_type |= type_needs.nullable_tuple;
        }

        /// Codec gates come from the codec factory itself, for every column CODEC (`getColumnsDescription`).
        if (create->columns_list && create->columns_list->columns)
            for (const auto & child : create->columns_list->columns->children)
                if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getCodec())
                    for (const auto & gate : codecGateNeeds(column->getCodec(), column->getType(), codec_gates_to_check))
                    {
                        if (gate == "allow_suspicious_codecs")
                            needs.suspicious_codecs = true;
                        else
                            needs.codec_gates.insert(gate);
                    }

        if (create->dictionary && create->dictionary->source && equalsCaseInsensitive(create->dictionary->source->name, "ytsaurus"))
            needs.ytsaurus_dictionary_source = true;

        const IAST * main_engine = create->storage ? create->storage->engine : nullptr;
        forEachNode(*create_ast, [&](const IAST & node)
        {
            const auto * function = node.as<ASTFunction>();
            if (!function)
                return;
            if (equalsCaseInsensitive(function->name, "ytsaurus") && &node != main_engine)
                needs.ytsaurus_table_function = true;
            else if (equalsCaseInsensitive(function->name, "eval"))
                needs.eval_table_function = true;
            else if ((equalsCaseInsensitive(function->name, "url") || equalsCaseInsensitive(function->name, "urlCluster"))
                     && urlMayHaveWildcard(*function))
                needs.url_wildcard = true;
        }, unread_select);

        const bool has_indices = create->columns_list && create->columns_list->indices && !create->columns_list->indices->children.empty();
        const bool has_projections = create->columns_list && create->columns_list->projections
            && !create->columns_list->projections->children.empty();
        if (has_indices)
            for (const auto & child : create->columns_list->indices->children)
                if (const auto * index = child->as<ASTIndexDeclaration>())
                    if (const auto type = index->getType(); type && equalsCaseInsensitive(type->name, "text"))
                        needs.full_text_index = true;

        std::vector<const ASTStorage *> storages;
        if (create->storage && create->storage->engine)
            storages.push_back(create->storage);
        if (create->targets)
            for (const auto * inner : create->targets->getInnerEngines())
                if (inner->engine)
                    storages.push_back(inner);

        bool has_merge_tree = false;
        for (const auto * storage : storages)
        {
            const auto & engine = *storage->engine;
            if (endsWithCaseInsensitive(engine.name, "MergeTree"))
            {
                has_merge_tree = true;
                /// The old `MergeTree(date, key, granularity)` form: arguments and no extended clause.
                if (engine.arguments && !engine.arguments->children.empty() && !storage->isExtendedStorageDefinition()
                    && !has_indices && !has_projections)
                    needs.deprecated_merge_tree_syntax = true;
            }
            else if ((equalsCaseInsensitive(engine.name, "S3Queue") || equalsCaseInsensitive(engine.name, "AzureQueue"))
                     && queueMayUseHivePartitioning(*storage))
                needs.queue_hive_partitioning = true;
        }

        if (has_merge_tree)
        {
            std::set<String> columns;
            /// The TTL build is stricter for `Variant`/`Dynamic` values without the gate, so a TTL reading one keeps it.
            std::set<String> variant_or_dynamic_columns;
            if (create->columns_list && create->columns_list->columns)
                for (const auto & child : create->columns_list->columns->children)
                    if (const auto * column = child->as<ASTColumnDeclaration>())
                    {
                        columns.insert(column->name);
                        const String type = column->getType() ? column->getType()->formatWithSecretsOneLine() : "";
                        if (type.empty() || type.contains("Variant") || type.contains("Dynamic") || type.contains("JSON")
                            || type.contains("Object"))
                            variant_or_dynamic_columns.insert(column->name);
                    }
            const auto ttl_needs_gate = [&](const IAST & expression)
            {
                bool reads_variant_or_dynamic = false;
                forEachNode(expression, [&](const IAST & node)
                {
                    if (const auto * identifier = node.as<ASTIdentifier>())
                        reads_variant_or_dynamic |= variant_or_dynamic_columns.contains(identifier->shortName());
                });
                return reads_variant_or_dynamic || ttlMayNeedSuspiciousGate(expression, columns, context);
            };
            for (const auto * storage : storages)
            {
                if (!endsWithCaseInsensitive(storage->engine->name, "MergeTree"))
                    continue;
                needs.suspicious_primary_key |= sortingKeyMayUseSimpleAggregateFunction(*storage, *create);
                if (storage->ttl_table)
                    for (const auto & child : storage->ttl_table->children)
                    {
                        /// A WHERE, GROUP BY, SET or RECOMPRESS part goes through more checks; keep the gate, which skips them all.
                        const auto * element = child->as<ASTTTLElement>();
                        needs.suspicious_ttl_expressions |= !element || !element->ttl() || element->where()
                            || !element->group_by_key.empty() || !element->group_by_assignments.empty() || element->recompression_codec
                            || ttl_needs_gate(*element->ttl());
                    }
            }
            if (create->columns_list && create->columns_list->columns)
                for (const auto & child : create->columns_list->columns->children)
                    if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getTTL())
                        needs.suspicious_ttl_expressions |= ttl_needs_gate(*column->getTTL());
        }
    }
    return needs;
}

String replaySettingsPrelude(
    const std::set<String> & settings_known_to_server,
    const std::vector<String> & create_queries,
    bool materialized_view_may_need_bad_select,
    const ContextPtr & context,
    const AnalyzesOnSource & analyzes_on_source)
{
    /// Nothing to replay means no gate can fire. Reachable whenever every database is predefined
    /// or excluded, which leaves the dump empty.
    if (create_queries.empty())
        return {};

    /// Additional gates required when replay revalidates stored metadata.
    static const std::vector<std::pair<String, String>> dump_specific =
    {
        {"allow_deprecated_database_ordinary", "1"},
        {"allow_experimental_database_materialized_postgresql", "1"},
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
    const ReplayGateNeeds needs = collectReplayGateNeeds(create_queries, context, analyzes_on_source);
    auto is_needed = [&needs, materialized_view_may_need_bad_select](const String & name)
    {
        if (name == "database_replicated_allow_explicit_uuid")
            return needs.explicit_uuid;
        if (name == "database_replicated_allow_replicated_engine_arguments")
            return needs.replicated_engine_arguments;
        if (name == "allow_materialized_view_with_bad_select")
            return needs.materialized_view && (needs.parse_failed || materialized_view_may_need_bad_select);
        if (name == "allow_deprecated_database_ordinary")
            return needs.ordinary_database;
        if (name == "allow_experimental_database_materialized_postgresql")
            return needs.materialized_postgresql_database;
        if (name == "allow_experimental_materialized_postgresql_table")
            return needs.materialized_postgresql_table;
        if (name == "allow_experimental_time_series_table")
            return needs.time_series_table;
        if (name == "allow_experimental_kafka_offsets_storage_in_keeper")
            return needs.kafka_keeper_offsets;
        if (name == "enable_nullable_tuple_type")
            return needs.nullable_tuple_type;
        return false;
    };

    String res;
    /// Gates no replayed CREATE reads; a statement that does not parse cannot need them either.
    static const std::set<std::string_view> dead_settings = {
        "allow_experimental_window_functions",
        "allow_experimental_hash_functions",
        "allow_simdjson",
        /// Read only by `CREATE INDEX`, `UPDATE` and `ALTER`, never by a replayed `CREATE`.
        "allow_create_index_without_type",
        "allow_experimental_lightweight_update",
        "allow_experimental_json_lazy_type_hints",
        /// Read only when a query plans a JOIN; neither a view nor a `Join` table plans one at CREATE.
        "allow_dynamic_type_in_join_keys",
        /// Read only by ALTER; CREATE checks the table's own MergeTree setting of the same name, kept in its SETTINGS.
        "allow_minmax_index_for_json",
        "allow_suspicious_indices",
        /// Read only by Iceberg INSERT, ALTER and EXECUTE.
        "allow_insert_into_iceberg",
        "allow_iceberg_remove_orphan_files",
        "allow_experimental_expire_snapshots",
    };
    /// Read only when a `DataLakeCatalog` database is created, each by its own `catalog_type`.
    static const std::set<std::string_view> data_lake_catalog_settings = {
        "allow_experimental_database_iceberg",
        "allow_experimental_database_hms_catalog",
        "allow_experimental_database_unity_catalog",
        "allow_experimental_database_glue_catalog",
        "allow_database_unity_catalog",
        "allow_database_glue_catalog",
        "allow_database_iceberg",
        "allow_experimental_database_paimon_rest_catalog",
    };
    /// Read only when a `DeltaLake*` table is created; a `DataLakeCatalog` database's tables are not replayed.
    static const std::set<std::string_view> delta_lake_settings = {
        "allow_delta_lake_create_table",
        "allow_delta_kernel_rs",
        "allow_experimental_delta_lake_writes",
    };
    /// The CREATE-statement feature that reads each shared gate. A gate with no carrier here is never emitted,
    /// except a per-codec `enable_<family>_codec`, which follows the codecs the columns use.
    static const std::map<std::string_view, bool ReplayGateNeeds::*> carriers = {
        {"allow_experimental_funnel_functions", &ReplayGateNeeds::funnel_functions},
        {"allow_experimental_nlp_functions", &ReplayGateNeeds::nlp_functions},
        {"allow_fuzz_query_functions", &ReplayGateNeeds::fuzz_query_functions},
        {"allow_deprecated_error_prone_window_functions", &ReplayGateNeeds::error_prone_window_functions},
        {"allow_hyperscan", &ReplayGateNeeds::hyperscan_functions},
        {"allow_experimental_time_series_aggregate_functions", &ReplayGateNeeds::time_series_aggregate_functions},
        {"allow_experimental_ytsaurus_table_function", &ReplayGateNeeds::ytsaurus_table_function},
        {"allow_experimental_eval_table_function", &ReplayGateNeeds::eval_table_function},
        {"allow_experimental_ytsaurus_dictionary_source", &ReplayGateNeeds::ytsaurus_dictionary_source},
        {"allow_suspicious_low_cardinality_types", &ReplayGateNeeds::low_cardinality_type},
        {"allow_suspicious_fixed_string_types", &ReplayGateNeeds::fixed_string_type},
        {"allow_suspicious_variant_types", &ReplayGateNeeds::variant_type},
        {"allow_experimental_time_time64_type", &ReplayGateNeeds::time_type},
        {"allow_experimental_nullable_tuple_type", &ReplayGateNeeds::nullable_tuple_type},
        {"allow_suspicious_codecs", &ReplayGateNeeds::suspicious_codecs},
        {"allow_deprecated_syntax_for_merge_tree", &ReplayGateNeeds::deprecated_merge_tree_syntax},
        {"allow_suspicious_primary_key", &ReplayGateNeeds::suspicious_primary_key},
        {"allow_suspicious_ttl_expressions", &ReplayGateNeeds::suspicious_ttl_expressions},
        {"allow_experimental_full_text_index", &ReplayGateNeeds::full_text_index},
        {"allow_experimental_object_storage_queue_hive_partitioning", &ReplayGateNeeds::queue_hive_partitioning},
        {"allow_experimental_url_wildcard_from_index_pages", &ReplayGateNeeds::url_wildcard},
        {"allow_suspicious_types_in_group_by", &ReplayGateNeeds::analyzer_group_by},
        {"allow_suspicious_types_in_order_by", &ReplayGateNeeds::analyzer_order_by},
        {"allow_experimental_correlated_subqueries", &ReplayGateNeeds::analyzer_subquery},
        {"allow_experimental_unique_key", &ReplayGateNeeds::unique_key},
        {"allow_experimental_ytsaurus_table_engine", &ReplayGateNeeds::ytsaurus_table},
        {"allow_experimental_paimon_storage_engine", &ReplayGateNeeds::paimon_table},
    };
    auto shared_needed = [&needs](const String & name)
    {
        if (dead_settings.contains(name))
            return false;
        if (needs.parse_failed)
            return true;
        if (auto it = carriers.find(name); it != carriers.end())
            return needs.*(it->second);
        if (data_lake_catalog_settings.contains(name))
            return needs.data_lake_catalog_database || needs.data_lake_catalog_gates.contains(name);
        if (delta_lake_settings.contains(name))
            return needs.delta_lake_table;
        return needs.codec_gates.contains(name);
    };
    /// A dump-specific gate is emitted by the loop below, with its own value and condition.
    std::set<String> dump_specific_names;
    for (const auto & [name, value] : dump_specific)
        dump_specific_names.insert(name);
    /// The name the source server knows the gate by: a renamed gate such as `allow_delta_kernel_rs` keeps its old name.
    /// Only non-obsolete names are known, so an obsolete gate is never emitted, even when a statement does not parse.
    auto server_spelling = [&settings_known_to_server](const String & name) -> std::optional<String>
    {
        if (settings_known_to_server.contains(name))
            return name;
        const std::string_view canonical = Settings::resolveName(name);
        for (const auto & known : settings_known_to_server)
            if (Settings::resolveName(known) == canonical)
                return known;
        return std::nullopt;
    };
    for (const auto & name : allExperimentalSettingNames())
        if (!dump_specific_names.contains(name) && shared_needed(name))
            if (const auto spelling = server_spelling(name))
                res += "SET " + *spelling + " = 1;\n";
    /// Emit dump-specific gates only when the dumped AST proves they are needed.
    for (const auto & [name, value] : dump_specific)
        if (const auto spelling = server_spelling(name); spelling && is_needed(name))
            res += "SET " + *spelling + " = " + value + ";\n";
    res += "\n";
    return res;
}

ASTPtr tryParseCreate(const String & create_query)
{
    try
    {
        ParserCreateQuery create_parser;
        return parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return nullptr;
    }
}

/// The columns `InterpreterCreateQuery::validateMaterializedViewColumnsAndEngine` treats as insertable.
std::set<String> insertableColumnNames(const ASTCreateQuery & create)
{
    std::set<String> names;
    if (create.columns_list && create.columns_list->columns)
        for (const auto & child : create.columns_list->columns->children)
            if (const auto * column = child->as<ASTColumnDeclaration>();
                column && column->default_specifier != ColumnDefaultSpecifier::Materialized
                && column->default_specifier != ColumnDefaultSpecifier::Alias)
                names.insert(column->name);
    return names;
}

/// A table function the server builds from a literal or fixed structure without reading the source, by its own `hasStaticStructure`.
/// Data lakes read their metadata even then, and `file`/`url` must not reach a per-server `user_files_path` check.
bool tableFunctionHasStaticStructure(const ASTFunction & function, const ContextPtr & context)
{
    static const std::set<std::string_view> names
        = {"url", "file", "s3", "gcs", "oss", "cosn", "azureBlobStorage", "hdfs", "input", "executable", "hive", "filesystem", "traceView"};
    /// A stored definition keeps the spelling it was written with, such as `URL(...)`.
    const bool known = std::ranges::any_of(names, [&](std::string_view name) { return equalsCaseInsensitive(function.name, name); });
    if (!known || !function.arguments)
        return false;
    const bool is_url = equalsCaseInsensitive(function.name, "url");
    const bool is_file = equalsCaseInsensitive(function.name, "file");
    const auto & arguments = function.arguments->children;
    for (const auto & argument : arguments)
    {
        /// An `auto` format or structure is resolved from the data.
        const auto * literal = argument->as<ASTLiteral>();
        const auto * identifier = argument->as<ASTIdentifier>();
        const bool is_auto_literal = literal && literal->value.getType() == Field::Types::String
            && equalsCaseInsensitive(literal->value.safeGet<String>(), "auto");
        if (is_auto_literal || (identifier && equalsCaseInsensitive(identifier->name(), "auto")))
            return false;
    }
    if (is_url || is_file)
    {
        const auto * location = arguments.empty() ? nullptr : arguments.front()->as<ASTLiteral>();
        if (!location || location->value.getType() != Field::Types::String)
            return false;
        const String & path = location->value.safeGet<String>();
        if (is_url ? !(startsWithCaseInsensitive(path, "http://") || startsWithCaseInsensitive(path, "https://"))
                   : path.starts_with('/') || path.contains(".."))
            return false;
    }
    try
    {
        /// Parsing these reads only their arguments.
        return TableFunctionFactory::instance().get(function.clone(), context)->hasStaticStructure();
    }
    catch (const Poco::Exception &) /// `DB::Exception` derives from it, so every parse error answers false here.
    {
        return false;
    }
}

/// Always analyzes on replay: `numbers`/`zeros` with counts, `generateRandom`/`values` with constant arguments, a source with
/// a static structure, and a `merge` or `loop` reading another emitted table, which replay creates before `owner`.
bool tableFunctionAlwaysAnalyzes(
    const IAST & node, const TableInfo & owner, const std::map<std::pair<String, String>, const TableInfo *> & emitted_tables,
    const ContextPtr & context)
{
    const auto * function = node.as<ASTFunction>();
    if (!function || !function->arguments)
        return false;
    ASTs arguments = function->arguments->children;
    /// These local generators fold their arguments as constant expressions, so `numbers(1 + 1)` is checked as `numbers(2)`.
    static const std::set<std::string_view> generators = {"numbers", "numbers_mt", "zeros", "zeros_mt", "values", "generateRandom",
        "generateSeries", "generate_series", "primes", "null", "fuzzQuery"};
    const bool is_generator
        = std::ranges::any_of(generators, [&](std::string_view name) { return equalsCaseInsensitive(function->name, name); });
    if (is_generator)
    {
        for (auto & argument : arguments)
        {
            /// A trailing `SETTINGS` argument (`generateRandom`) is not folded.
            if (argument->as<ASTLiteral>() || argument->as<ASTSetQuery>())
                continue;
            if (dependsOnUnstoredContext(*argument, context))
                return false;
            try
            {
                argument = evaluateConstantExpressionAsLiteral(argument->clone(), context);
            }
            catch (const Exception &)
            {
                return false;
            }
        }
    }
    const auto is_count = [](const ASTPtr & argument)
    {
        const auto * literal = argument->as<ASTLiteral>();
        return literal && literal->value.getType() == Field::Types::UInt64;
    };
    /// `numbers`, `generateSeries` and `primes` convert any non-negative number to `UInt64`; a third argument is a step,
    /// which must not be zero.
    const auto counts_with_step = [&](size_t min_arguments)
    {
        const auto non_negative = [](const ASTPtr & argument) -> std::optional<UInt64>
        {
            const auto * literal = argument->as<ASTLiteral>();
            if (literal && literal->value.getType() == Field::Types::UInt64)
                return literal->value.safeGet<UInt64>();
            if (literal && literal->value.getType() == Field::Types::Int64 && literal->value.safeGet<Int64>() >= 0)
                return static_cast<UInt64>(literal->value.safeGet<Int64>());
            return std::nullopt;
        };
        return arguments.size() >= min_arguments && arguments.size() <= 3
            && std::ranges::all_of(arguments, [&](const ASTPtr & argument) { return non_negative(argument).has_value(); })
            && (arguments.size() < 3 || *non_negative(arguments[2]) != 0);
    };
    if (equalsCaseInsensitive(function->name, "numbers") || equalsCaseInsensitive(function->name, "numbers_mt"))
        return counts_with_step(0);
    if (equalsCaseInsensitive(function->name, "generateSeries") || equalsCaseInsensitive(function->name, "generate_series"))
        return counts_with_step(2);
    if (equalsCaseInsensitive(function->name, "primes"))
        return counts_with_step(1);
    /// `null` takes a structure, `fuzzQuery` a query and its limits.
    const auto is_literal = [](const ASTPtr & argument) { return argument->as<ASTLiteral>() != nullptr; };
    if (equalsCaseInsensitive(function->name, "null"))
    {
        const auto * structure = arguments.size() == 1 ? arguments[0]->as<ASTLiteral>() : nullptr;
        return structure && structure->value.getType() == Field::Types::String
            && !equalsCaseInsensitive(structure->value.safeGet<String>(), "auto");
    }
    if (equalsCaseInsensitive(function->name, "fuzzQuery"))
        return !arguments.empty() && std::ranges::all_of(arguments, is_literal);
    if (equalsCaseInsensitive(function->name, "zeros") || equalsCaseInsensitive(function->name, "zeros_mt"))
        return arguments.size() <= 1 && std::ranges::all_of(arguments, is_count);
    if (equalsCaseInsensitive(function->name, "generateRandom") && !arguments.empty() && arguments.back()->as<ASTSetQuery>())
        arguments.pop_back();
    const bool literal_arguments = !arguments.empty()
        && std::ranges::all_of(arguments, [](const ASTPtr & argument) { return argument->as<ASTLiteral>() != nullptr; });
    if (equalsCaseInsensitive(function->name, "values"))
        return literal_arguments;
    if (equalsCaseInsensitive(function->name, "generateRandom"))
        return literal_arguments && arguments[0]->as<ASTLiteral>()->value.getType() == Field::Types::String;
    /// `loop` analyzes over an emitted table, which replay creates before `owner`, or over a table function that does.
    if (equalsCaseInsensitive(function->name, "loop") && arguments.size() == 1 && arguments[0]->as<ASTFunction>())
        return tableFunctionAlwaysAnalyzes(*arguments[0], owner, emitted_tables, context);
    if (equalsCaseInsensitive(function->name, "loop") && (arguments.size() == 1 || arguments.size() == 2))
    {
        std::optional<std::pair<String, String>> table;
        if (arguments.size() == 1)
            table = tryGetQualifiedNameFromFunctionArgument(*function, 0);
        else if (auto database = tryFoldNameArgument(arguments[0], context), name = tryFoldNameArgument(arguments[1], context);
                 database && name)
            table = std::pair(*database, *name);
        if (!table)
            return false;
        if (table->first.empty())
            table->first = owner.database;
        return *table != std::pair(owner.database, owner.name) && emitted_tables.contains(*table);
    }
    if (equalsCaseInsensitive(function->name, "merge") && (arguments.size() == 1 || arguments.size() == 2))
    {
        bool database_is_regexp = false;
        bool table_is_regexp = false;
        const auto database = arguments.size() == 1 ? std::optional<String>(String{})
                                                    : tryFoldMergeArgument(arguments[0], database_is_regexp, context);
        const auto table = tryFoldMergeArgument(arguments.back(), table_is_regexp, context);
        if (!database || !table)
            return false;
        try
        {
            std::optional<OptimizedRegularExpression> database_regexp;
            if (database_is_regexp)
                database_regexp.emplace(*database);
            const OptimizedRegularExpression table_regexp(*table);
            const String & database_name = database->empty() ? owner.database : *database;
            return std::ranges::any_of(emitted_tables, [&](const auto & entry)
            {
                const auto & [db, name] = entry.first;
                return entry.first != std::pair(owner.database, owner.name)
                    && (database_regexp ? database_regexp->match(db) : db == database_name) && table_regexp.match(name);
            });
        }
        catch (const Exception &)
        {
            return false;
        }
    }
    return tableFunctionHasStaticStructure(*function, context);
}

bool containsTableFunction(
    const IAST & node, const TableInfo & owner, const std::map<std::pair<String, String>, const TableInfo *> & emitted_tables,
    const ContextPtr & context)
{
    if (const auto * table_expression = node.as<ASTTableExpression>(); table_expression && table_expression->table_function
        && !tableFunctionAlwaysAnalyzes(*table_expression->table_function, owner, emitted_tables, context))
        return true;
    return std::any_of(node.children.begin(), node.children.end(),
        [&](const auto & child) { return containsTableFunction(*child, owner, emitted_tables, context); });
}

/// True when the leftmost SELECT may output a column whose name is not in `target_columns`: the
/// output names of a UNION come from its first branch, and a name that cannot be read off the AST counts as unknown.
bool selectMayOutputUnknownColumn(const IAST & select, const std::set<String> & target_columns)
{
    const IAST * node = &select;
    while (!node->as<ASTSelectQuery>())
    {
        ASTs branches;
        if (const auto * union_query = node->as<ASTSelectWithUnionQuery>())
        {
            if (union_query->list_of_selects)
                branches = union_query->list_of_selects->children;
        }
        else if (const auto * intersect_except = node->as<ASTSelectIntersectExceptQuery>())
            branches = intersect_except->getListOfSelects();
        if (branches.empty())
            return true;
        node = branches.front().get();
    }

    const ASTPtr select_list = node->as<ASTSelectQuery>()->select();
    if (!select_list)
        return true;
    for (const auto & column : select_list->children)
    {
        if (column->as<ASTAsterisk>() || column->as<ASTQualifiedAsterisk>() || column->as<ASTColumnsRegexpMatcher>()
            || column->as<ASTColumnsListMatcher>() || column->as<ASTQualifiedColumnsRegexpMatcher>()
            || column->as<ASTQualifiedColumnsListMatcher>())
            return true;

        String name = column->tryGetAlias();
        if (name.empty())
        {
            const auto * identifier = column->as<ASTIdentifier>();
            if (!identifier || identifier->compound())
                return true;
            name = identifier->shortName();
        }
        if (!target_columns.contains(name))
            return true;
    }
    return false;
}

/// Whether replaying this materialized view can reach a check that `allow_materialized_view_with_bad_select`
/// relaxes in `validateMaterializedViewColumnsAndEngine`: a `TO` target that may not exist yet, a SELECT
/// that may not analyze, or an output column the target does not have. Every unknown answers true, so
/// the gate is only ever over-emitted. The stored `CREATE` always carries a column list, so the column
/// check runs at replay for every view. SQL UDFs the SELECT calls are not tracked.
bool materializedViewMayNeedBadSelectGate(
    const TableInfo & table, const std::map<std::pair<String, String>, const TableInfo *> & emitted_tables, const ContextPtr & context)
{
    const ASTPtr ast = tryParseCreate(table.create_query);
    if (!ast)
        return true;
    const auto * create = ast->as<ASTCreateQuery>();
    if (!create || !create->is_materialized_view)
        return false;
    if (!create->select)
        return true;

    /// A target that is not an emitted table (generated helpers included) is not ordered before the view.
    std::set<String> target_columns;
    if (create->hasTargetTableID(ViewTarget::To))
    {
        const StorageID target_id = create->getTargetTableID(ViewTarget::To);
        auto it = emitted_tables.find({target_id.database_name, target_id.table_name});
        if (it == emitted_tables.end())
            return true;
        const ASTPtr target_ast = tryParseCreate(it->second->create_query);
        const auto * target_create = target_ast ? target_ast->as<ASTCreateQuery>() : nullptr;
        if (!target_create)
            return true;
        target_columns = insertableColumnNames(*target_create);
    }
    else
        target_columns = insertableColumnNames(*create);

    if (!table.unresolved_references.empty())
        return true;
    for (const auto & dependency : table.dependencies)
        if (!DatabaseCatalog::isPredefinedDatabase(dependency.first) && !emitted_tables.contains(dependency))
            return true;
    if (containsTableFunction(*create->select, table, emitted_tables, context))
        return true;

    return selectMayOutputUnknownColumn(*create->select, target_columns);
}

void markMaterializedViewsNeedingBadSelectGate(
    std::vector<TableInfo> & tables, const ContextPtr & context, const std::set<String> & user_defined_functions, std::ostream & err)
{
    std::map<std::pair<String, String>, const TableInfo *> emitted_tables;
    for (const auto & table : tables)
        if (table.emit)
            emitted_tables.emplace(std::pair(table.database, table.name), &table);
    for (auto & table : tables)
    {
        if (!table.emit)
            continue;
        table.needs_bad_select_gate = materializedViewMayNeedBadSelectGate(table, emitted_tables, context);

        /// The dump does not create user-defined functions; a stored CREATE still calls one only if it predates the function.
        const ASTPtr ast = user_defined_functions.empty() ? nullptr : tryParseCreate(table.create_query);
        std::set<String> called;
        if (ast)
            forEachNode(*ast, [&](const IAST & node)
            {
                if (const auto * function = node.as<ASTFunction>(); function && user_defined_functions.contains(function->name))
                    called.insert(function->name);
            });
        for (const auto & name : called)
            err << "Warning: " << backQuoteIfNeed(table.database) << "." << backQuoteIfNeed(table.name)
                << " calls user-defined function " << backQuoteIfNeed(name) << ", which this dump does not create.\n";
        /// Such a materialized view's SELECT does not analyze at replay, so it needs the gate.
        if (!called.empty() && ast->as<ASTCreateQuery>() && ast->as<ASTCreateQuery>()->is_materialized_view)
            table.needs_bad_select_gate = true;
    }
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

    const auto database_info = fetchDatabaseInfo(connection, timeouts, client_info, context->getSettingsRef());
    std::vector<String> all_databases;
    all_databases.reserve(database_info.size());
    for (const auto & entry : database_info)
        all_databases.push_back(entry.first);

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
        /// Backup serializes its locator as a quoted string that its CREATE path rejects.
        if (equalsCaseInsensitive(database_info.at(db).engine, "Backup"))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot dump database {} for --dump-schema: SHOW CREATE DATABASE for the Backup engine is not replayable",
                backQuoteIfNeed(db));

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

    /// Names the source server actually has and still acts on, used to filter the replay prelude below.
    std::vector<String> server_setting_names = fetchStringColumn(
        connection, timeouts, client_info, "SELECT name FROM system.settings WHERE NOT is_obsolete", context->getSettingsRef());
    std::set<String> settings_known_to_server(server_setting_names.begin(), server_setting_names.end());

    /// `EXPLAIN QUERY TREE` runs the analyzer only, so it reads nothing and creates nothing on the source. The settings go
    /// in the query text, because `LocalConnection` drops the settings argument.
    const AnalyzesOnSource analyzes_on_source = [&](const String & select_query, const SettingsChanges & changes)
    {
        String query = "EXPLAIN QUERY TREE " + select_query + " SETTINGS ";
        for (size_t i = 0; i < changes.size(); ++i)
            query += (i ? ", " : "") + changes[i].name + (changes[i].value.safeGet<bool>() ? " = 1" : " = 0");
        try
        {
            fetchStringColumn(connection, timeouts, client_info, query, context->getSettingsRef());
            return true;
        }
        catch (const Exception &)
        {
            return false;
        }
    };

    std::vector<TableInfo> tables;
    std::vector<size_t> order;
    if (!target_databases.empty())
    {
        /// Databases the dump leaves out but a `merge(REGEXP(...), ...)` in it can still refer to.
        std::set<String> undumped_databases(all_databases.begin(), all_databases.end());
        for (const auto & db : target_databases)
            undumped_databases.erase(db);

        std::map<String, NamedCollectionDependencies> database_named_collections;
        tables = fetchTables(
            connection,
            timeouts,
            client_info,
            context,
            target_databases,
            undumped_databases,
            create_database_query_by_db,
            database_info,
            database_named_collections);
        reportDependenciesOutsideDumpSet(tables, database_named_collections, target_databases, err);
        reportMaskedSecrets(tables, create_database_query_by_db, err);
        order = orderTablesByDependencies(tables);
        const std::vector<String> user_defined_functions = fetchStringColumn(connection, timeouts, client_info,
            "SELECT name FROM system.functions WHERE origin != 'System'", context->getSettingsRef());
        markMaterializedViewsNeedingBadSelectGate(
            tables, context, std::set<String>(user_defined_functions.begin(), user_defined_functions.end()), err);
    }

    if (output_dir.empty())
    {
        std::vector<String> dumped_creates;
        for (const auto & db : target_databases)
            dumped_creates.push_back(create_database_query_by_db.at(db));
        for (size_t i : order)
            if (tables[i].emit)
                dumped_creates.push_back(tables[i].create_query);

        out << replaySettingsPrelude(
            settings_known_to_server,
            dumped_creates,
            std::any_of(tables.begin(), tables.end(), [](const TableInfo & table) { return table.emit && table.needs_bad_select_gate; }),
            context,
            analyzes_on_source);

        for (const auto & db : target_databases)
            out << create_database_query_by_db.at(db) << ";\n\n";

        /// Stored queries can keep names unqualified and resolve them against the session's current
        /// database, so restore that context whenever the effective database changes.
        String current_database;
        for (size_t i : order)
        {
            if (!tables[i].emit)
                continue;
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
        if (!table.emit)
            return false;
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
            if (tables[i].emit && tables[i].database == db)
                dumped_creates.push_back(tables[i].create_query);

        file << replaySettingsPrelude(
            settings_known_to_server,
            dumped_creates,
            std::any_of(
                tables.begin(),
                tables.end(),
                [&](const TableInfo & table) { return table.database == db && table.emit && table.needs_bad_select_gate; }),
            context,
            analyzes_on_source);
        file << create_database_query_by_db.at(db) << ";\n\nUSE " << backQuoteIfNeed(db) << ";\n\n";
        for (size_t i : order)
            if (tables[i].emit && tables[i].database == db)
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
