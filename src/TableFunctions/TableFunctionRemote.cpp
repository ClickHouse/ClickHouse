#include "TableFunctionRemote.h"

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Common/typeid_cast.h>
#include <Common/parseRemoteDescription.h>
#include <Common/Macros.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <base/range.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


void TableFunctionRemote::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    String cluster_name;
    String cluster_description;
    String database = "system";
    String table = "one"; /// The table containing one row is used by default for queries without explicit table specification.
    String username = "default";
    String password;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    /**
     * Number of arguments for remote function is 4.
     * Number of arguments for cluster function is 6.
     * For now named collection can be used only for remote as cluster does not require credentials.
     */
    size_t max_args = is_cluster_function ? 4 : 6;
    NamedCollectionPtr named_collection;
    std::vector<std::pair<std::string, ASTPtr>> complex_args;
    if (!is_cluster_function && (named_collection = tryGetNamedCollectionWithOverrides(args, context, false, &complex_args)))
    {
        validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(
            *named_collection,
            {"addresses_expr", "host", "hostname", "table"},
            {"username", "user", "password", "sharding_key", "port", "database", "db"});

        if (!complex_args.empty())
        {
            for (const auto & [arg_name, arg_ast] : complex_args)
            {
                if (arg_name == "database" || arg_name == "db")
                    remote_table_function_ptr = arg_ast;
                else if (arg_name == "sharding_key")
                    sharding_key = arg_ast;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected argument representation for {}", arg_name);
            }
        }
        else
            database = named_collection->getAnyOrDefault<String>({"db", "database"}, "default");

        cluster_description = named_collection->getOrDefault<String>("addresses_expr", "");
        if (cluster_description.empty() && named_collection->hasAny({"host", "hostname"}))
            cluster_description = named_collection->has("port")
                ? named_collection->getAny<String>({"host", "hostname"}) + ':' + toString(named_collection->get<UInt64>("port"))
                : named_collection->getAny<String>({"host", "hostname"});
        table = named_collection->get<String>("table");
        username = named_collection->getAnyOrDefault<String>({"username", "user"}, "default");
        password = named_collection->getOrDefault<String>("password", "");
    }
    else
    {
        /// Supported signatures:
        /// remote('addresses_expr')
        /// remote('addresses_expr', db.table)
        /// remote('addresses_expr', 'db', 'table')
        /// remote('addresses_expr', db.table, 'user')
        /// remote('addresses_expr', 'db', 'table', 'user')
        /// remote('addresses_expr', db.table, 'user', 'password')
        /// remote('addresses_expr', 'db', 'table', 'user', 'password')
        /// remote('addresses_expr', db.table, sharding_key)
        /// remote('addresses_expr', 'db', 'table', sharding_key)
        /// remote('addresses_expr', db.table, 'user', sharding_key)
        /// remote('addresses_expr', 'db', 'table', 'user', sharding_key)
        /// remote('addresses_expr', db.table, 'user', 'password', sharding_key)
        /// remote('addresses_expr', 'db', 'table', 'user', 'password', sharding_key)
        ///
        /// remoteSecure() - same as remote()
        ///
        /// cluster()
        /// cluster('cluster_name')
        /// cluster('cluster_name', db.table)
        /// cluster('cluster_name', 'db', 'table')
        /// cluster('cluster_name', db.table, sharding_key)
        /// cluster('cluster_name', 'db', 'table', sharding_key)
        ///
        /// clusterAllReplicas() - same as cluster()

        if ((!is_cluster_function && args.empty()) || args.size() > max_args)
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        size_t arg_num = 0;
        auto get_string_literal = [](const IAST & node, String & res)
        {
            const auto * lit = node.as<ASTLiteral>();
            if (!lit)
                return false;

            if (lit->value.getType() != Field::Types::String)
                return false;

            res = lit->value.safeGet<const String &>();
            return true;
        };

        if (is_cluster_function)
        {
            if (!args.empty())
            {
                args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                cluster_name = checkAndGetLiteralArgument<String>(args[arg_num], "cluster_name");
            }
            else
            {
                cluster_name = "default";
            }
        }
        else
        {
            if (!tryGetIdentifierNameInto(args[arg_num], cluster_name))
            {
                if (!get_string_literal(*args[arg_num], cluster_description))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hosts pattern must be string literal (in single quotes).");
            }
        }

        ++arg_num;

        /// Names of database and table is not necessary.
        if (arg_num < args.size())
        {
            const auto * function = args[arg_num]->as<ASTFunction>();
            if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
            {
                remote_table_function_ptr = args[arg_num];
                ++arg_num;
            }
            else
            {
                args[arg_num] = evaluateConstantExpressionForDatabaseName(args[arg_num], context);
                database = checkAndGetLiteralArgument<String>(args[arg_num], "database");

                ++arg_num;

                auto qualified_name = QualifiedTableName::parseFromString(database);
                if (qualified_name.database.empty())
                {
                    if (arg_num >= args.size())
                    {
                        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table name was not found in function arguments. {}", static_cast<const std::string>(help_message));
                    }
                    else
                    {
                        std::swap(qualified_name.database, qualified_name.table);
                        args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                        qualified_name.table = checkAndGetLiteralArgument<String>(args[arg_num], "table");
                        ++arg_num;
                    }
                }

                database = std::move(qualified_name.database);
                table = std::move(qualified_name.table);

                /// Cluster function may have sharding key for insert
                if (is_cluster_function && arg_num < args.size())
                {
                    sharding_key = args[arg_num];
                    ++arg_num;
                }
            }
        }

        /// Username and password parameters are prohibited in cluster version of the function
        if (!is_cluster_function)
        {
            if (arg_num < args.size())
            {
                if (!get_string_literal(*args[arg_num], username))
                {
                    username = "default";
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size() && !sharding_key)
            {
                if (!get_string_literal(*args[arg_num], password))
                {
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size())
            {
                if (sharding_key)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arguments `user` and `password` should be string literals (in single quotes)");
                sharding_key = args[arg_num];
                ++arg_num;
            }
        }

        if (arg_num < args.size())
        {
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }

    if (!cluster_name.empty())
    {
        /// Use an existing cluster from the main config
        if (name != "clusterAllReplicas")
            cluster = context->getCluster(cluster_name);
        else
            cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());
    }
    else
    {
        /// Create new cluster from the scratch
        size_t max_addresses = context->getSettingsRef().table_function_remote_max_addresses;
        std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        std::vector<std::vector<String>> names;
        names.reserve(shards.size());
        for (const auto & shard : shards)
            names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses));

        if (names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard list is empty after parsing first argument");

        auto maybe_secure_port = context->getTCPPortSecure();

        /// Check host and port on affiliation allowed hosts.
        for (const auto & hosts : names)
        {
            for (const auto & host : hosts)
            {
                size_t colon = host.find(':');
                if (colon == String::npos)
                    context->getRemoteHostFilter().checkHostAndPort(
                        host,
                        toString((secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort())));
                else
                    context->getRemoteHostFilter().checkHostAndPort(host.substr(0, colon), host.substr(colon + 1));
            }
        }

        bool treat_local_as_remote = false;
        bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;
        ClusterConnectionParameters params{
            username,
            password,
            static_cast<UInt16>(secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
            treat_local_as_remote,
            treat_local_port_as_remote,
            secure,
            /* priority= */ Priority{1},
            /* cluster_name= */ "",
            /* cluster_secret= */ ""
        };
        cluster = std::make_shared<Cluster>(context->getSettingsRef(), names, params);
    }

    if (!remote_table_function_ptr && table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The name of remote table cannot be empty");

    remote_table_id.database_name = database;
    remote_table_id.table_name = table;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context, is_insert_query);

    assert(cluster);
    StoragePtr res = std::make_shared<StorageDistributed>(
            StorageID(getDatabaseName(), table_name),
            cached_columns,
            ConstraintsDescription{},
            String{},
            remote_table_id.database_name,
            remote_table_id.table_name,
            String{},
            context,
            sharding_key,
            String{},
            String{},
            DistributedSettings{},
            LoadingStrictnessLevel::CREATE,
            cluster,
            remote_table_function_ptr,
            !is_cluster_function);

    res->startup();
    return res;
}

ColumnsDescription TableFunctionRemote::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    assert(cluster);
    return getStructureOfRemoteTable(*cluster, remote_table_id, context, remote_table_function_ptr);
}

TableFunctionRemote::TableFunctionRemote(const std::string & name_, bool secure_)
    : name{name_}, secure{secure_}
{
    is_cluster_function = (name == "cluster" || name == "clusterAllReplicas");
    help_message = PreformattedMessage::create(
        "Table function '{}' requires from {} to {} parameters: "
        "{}",
        name,
        is_cluster_function ? 0 : 1,
        is_cluster_function ? 4 : 6,
        is_cluster_function ? "[<cluster name or default if not specify>, <name of remote database>, <name of remote table>] [, sharding_key]"
                            : "<addresses pattern> [, <name of remote database>, <name of remote table>] [, username[, password], sharding_key]");
}

void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); });
    factory.registerFunction("remoteSecure", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); });
    factory.registerFunction("cluster", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); }, {.documentation = {}, .allow_readonly = true}});
    factory.registerFunction("clusterAllReplicas", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("clusterAllReplicas"); }, {.documentation = {}, .allow_readonly = true}});
}

}
