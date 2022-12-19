#include "TableFunctionRemote.h"

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
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
#include <Storages/ExternalDataSourceConfiguration.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Core/Defines.h>
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
    ExternalDataSourceConfiguration configuration;

    String cluster_name;
    String cluster_description;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    /**
     * Number of arguments for remote function is 4.
     * Number of arguments for cluster function is 6.
     * For now named collection can be used only for remote as cluster does not require credentials.
     */
    size_t max_args = is_cluster_function ? 4 : 6;
    auto named_collection = getExternalDataSourceConfiguration(args, context, false, false);
    if (named_collection)
    {
        if (is_cluster_function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named collection cannot be used for table function cluster");

        /**
         * Common arguments: database, table, username, password, addresses_expr.
         * Specific args (remote): sharding_key, or database (in case it is not ASTLiteral).
         * None of the common arguments is empty at this point, it is checked in getExternalDataSourceConfiguration.
         */
        auto [common_configuration, storage_specific_args, _] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "sharding_key")
            {
                sharding_key = arg_value;
            }
            else if (arg_name == "database")
            {
                const auto * function = arg_value->as<ASTFunction>();
                if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
                {
                    remote_table_function_ptr = arg_value;
                }
                else
                {
                    auto database_literal = evaluateConstantExpressionOrIdentifierAsLiteral(arg_value, context);
                    configuration.database = database_literal->as<ASTLiteral>()->value.safeGet<String>();
                }
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key-value argument."
                        "Got: {}, but expected: sharding_key", arg_name);
        }
        cluster_description = configuration.addresses_expr;
        if (cluster_description.empty())
            cluster_description = configuration.port ? configuration.host + ':' + toString(configuration.port) : configuration.host;
    }
    else
    {
        if (args.size() < 2 || args.size() > max_args)
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        size_t arg_num = 0;
        auto get_string_literal = [](const IAST & node, String & res)
        {
            const auto * lit = node.as<ASTLiteral>();
            if (!lit)
                return false;

            if (lit->value.getType() != Field::Types::String)
                return false;

            res = safeGet<const String &>(lit->value);
            return true;
        };

        if (is_cluster_function)
        {
            args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
            cluster_name = args[arg_num]->as<ASTLiteral &>().value.safeGet<const String &>();
        }
        else
        {
            if (!tryGetIdentifierNameInto(args[arg_num], cluster_name))
            {
                if (!get_string_literal(*args[arg_num], cluster_description))
                    throw Exception("Hosts pattern must be string literal (in single quotes).", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        ++arg_num;
        const auto * function = args[arg_num]->as<ASTFunction>();
        if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
        {
            remote_table_function_ptr = args[arg_num];
            ++arg_num;
        }
        else
        {
            args[arg_num] = evaluateConstantExpressionForDatabaseName(args[arg_num], context);
            configuration.database = args[arg_num]->as<ASTLiteral &>().value.safeGet<String>();

            ++arg_num;

            auto qualified_name = QualifiedTableName::parseFromString(configuration.database);
            if (qualified_name.database.empty())
            {
                if (arg_num >= args.size())
                {
                    throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }
                else
                {
                    std::swap(qualified_name.database, qualified_name.table);
                    args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                    qualified_name.table = args[arg_num]->as<ASTLiteral &>().value.safeGet<String>();
                    ++arg_num;
                }
            }

            configuration.database = std::move(qualified_name.database);
            configuration.table = std::move(qualified_name.table);

            /// Cluster function may have sharding key for insert
            if (is_cluster_function && arg_num < args.size())
            {
                sharding_key = args[arg_num];
                ++arg_num;
            }
        }

        /// Username and password parameters are prohibited in cluster version of the function
        if (!is_cluster_function)
        {
            if (arg_num < args.size())
            {
                if (!get_string_literal(*args[arg_num], configuration.username))
                {
                    configuration.username = "default";
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size() && !sharding_key)
            {
                if (!get_string_literal(*args[arg_num], configuration.password))
                {
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size() && !sharding_key)
            {
                sharding_key = args[arg_num];
                ++arg_num;
            }
        }

        if (arg_num < args.size())
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (!cluster_name.empty())
    {
        /// Use an existing cluster from the main config
        String cluster_name_expanded = context->getMacros()->expand(cluster_name);
        if (name != "clusterAllReplicas")
            cluster = context->getCluster(cluster_name_expanded);
        else
            cluster = context->getCluster(cluster_name_expanded)->getClusterWithReplicasAsShards(context->getSettingsRef());
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
            throw Exception("Shard list is empty after parsing first argument", ErrorCodes::BAD_ARGUMENTS);

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
        cluster = std::make_shared<Cluster>(
            context->getSettingsRef(),
            names,
            configuration.username,
            configuration.password,
            (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
            treat_local_as_remote,
            treat_local_port_as_remote,
            secure);
    }

    if (!remote_table_function_ptr && configuration.table.empty())
        throw Exception("The name of remote table cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    remote_table_id.database_name = configuration.database;
    remote_table_id.table_name = configuration.table;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const
{
    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context);

    assert(cluster);
    StoragePtr res = remote_table_function_ptr
        ? StorageDistributed::create(
            StorageID(getDatabaseName(), table_name),
            cached_columns,
            ConstraintsDescription{},
            remote_table_function_ptr,
            String{},
            context,
            sharding_key,
            String{},
            String{},
            DistributedSettings{},
            false,
            cluster)
        : StorageDistributed::create(
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
            false,
            cluster);

    res->startup();
    return res;
}

ColumnsDescription TableFunctionRemote::getActualTableStructure(ContextPtr context) const
{
    assert(cluster);
    return getStructureOfRemoteTable(*cluster, remote_table_id, context, remote_table_function_ptr);
}

TableFunctionRemote::TableFunctionRemote(const std::string & name_, bool secure_)
    : name{name_}, secure{secure_}
{
    is_cluster_function = (name == "cluster" || name == "clusterAllReplicas");
    help_message = fmt::format(
        "Table function '{}' requires from 2 to {} parameters: "
        "<addresses pattern or cluster name>, <name of remote database>, <name of remote table>{}",
        name,
        is_cluster_function ? 4 : 6,
        is_cluster_function ? " [, sharding_key]" : " [, username[, password], sharding_key]");
}


void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); });
    factory.registerFunction("remoteSecure", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); });
    factory.registerFunction("cluster", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); });
    factory.registerFunction("clusterAllReplicas", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("clusterAllReplicas"); });
}

}
