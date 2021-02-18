#include "TableFunctionRemote.h"

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Common/typeid_cast.h>
#include <Common/parseRemoteDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Core/Defines.h>
#include <ext/range.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    const size_t max_args = is_cluster_function ? 3 : 5;
    if (args.size() < 2 || args.size() > max_args)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String cluster_name;
    String cluster_description;
    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;
    String username;
    String password;

    size_t arg_num = 0;

    auto get_string_literal = [](const IAST & node, const char * description)
    {
        const auto * lit = node.as<ASTLiteral>();
        if (!lit)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        if (lit->value.getType() != Field::Types::String)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        return safeGet<const String &>(lit->value);
    };

    if (is_cluster_function)
    {
        args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
        cluster_name = args[arg_num]->as<ASTLiteral &>().value.safeGet<const String &>();
    }
    else
    {
        if (!tryGetIdentifierNameInto(args[arg_num], cluster_name))
            cluster_description = get_string_literal(*args[arg_num], "Hosts pattern");
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
        remote_database = args[arg_num]->as<ASTLiteral &>().value.safeGet<String>();

        ++arg_num;

        size_t dot = remote_database.find('.');
        if (dot != String::npos)
        {
            /// NOTE Bad - do not support identifiers in backquotes.
            remote_table = remote_database.substr(dot + 1);
            remote_database = remote_database.substr(0, dot);
        }
        else
        {
            if (arg_num >= args.size())
            {
                throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            else
            {
                args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                remote_table = args[arg_num]->as<ASTLiteral &>().value.safeGet<String>();
                ++arg_num;
            }
        }
    }

    /// Username and password parameters are prohibited in cluster version of the function
    if (!is_cluster_function)
    {
        if (arg_num < args.size())
        {
            username = get_string_literal(*args[arg_num], "Username");
            ++arg_num;
        }
        else
            username = "default";

        if (arg_num < args.size())
        {
            password = get_string_literal(*args[arg_num], "Password");
            ++arg_num;
        }
    }

    if (arg_num < args.size())
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// ExpressionAnalyzer will be created in InterpreterSelectQuery that will meet these `Identifier` when processing the request.
    /// We need to mark them as the name of the database or table, because the default value is column.
    for (auto ast : args)
        setIdentifierSpecial(ast);

    ClusterPtr cluster;
    if (!cluster_name.empty())
    {
        /// Use an existing cluster from the main config
        if (name != "clusterAllReplicas")
            cluster = context.getCluster(cluster_name);
        else
            cluster = context.getCluster(cluster_name)->getClusterWithReplicasAsShards(context.getSettings());
    }
    else
    {
        /// Create new cluster from the scratch
        size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;
        std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        std::vector<std::vector<String>> names;
        names.reserve(shards.size());
        for (const auto & shard : shards)
            names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses));

        if (names.empty())
            throw Exception("Shard list is empty after parsing first argument", ErrorCodes::BAD_ARGUMENTS);

        auto maybe_secure_port = context.getTCPPortSecure();

        /// Check host and port on affiliation allowed hosts.
        for (const auto & hosts : names)
        {
            for (const auto & host : hosts)
            {
                size_t colon = host.find(':');
                if (colon == String::npos)
                    context.getRemoteHostFilter().checkHostAndPort(
                        host,
                        toString((secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context.getTCPPort())));
                else
                    context.getRemoteHostFilter().checkHostAndPort(host.substr(0, colon), host.substr(colon + 1));
            }
        }

        cluster = std::make_shared<Cluster>(
            context.getSettings(),
            names,
            username,
            password,
            (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context.getTCPPort()),
            false,
            secure);
    }

    if (!remote_table_function_ptr && remote_table.empty())
        throw Exception("The name of remote table cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    auto remote_table_id = StorageID::createEmpty();
    remote_table_id.database_name = remote_database;
    remote_table_id.table_name = remote_table;
    auto structure_remote_table = getStructureOfRemoteTable(*cluster, remote_table_id, context, remote_table_function_ptr);

    StoragePtr res = remote_table_function_ptr
        ? StorageDistributed::createWithOwnCluster(
            StorageID(getDatabaseName(), table_name),
            structure_remote_table,
            remote_table_function_ptr,
            cluster,
            context)
        : StorageDistributed::createWithOwnCluster(
            StorageID(getDatabaseName(), table_name),
            structure_remote_table,
            remote_database,
            remote_table,
            cluster,
            context);

    res->startup();
    return res;
}


TableFunctionRemote::TableFunctionRemote(const std::string & name_, bool secure_)
    : name{name_}, secure{secure_}
{
    is_cluster_function = (name == "cluster" || name == "clusterAllReplicas");

    std::stringstream ss;
    ss << "Table function '" << name + "' requires from 2 to " << (is_cluster_function ? 3 : 5) << " parameters"
       << ": <addresses pattern or cluster name>, <name of remote database>, <name of remote table>"
       << (is_cluster_function ? "" : ", [username, [password]].");
    help_message = ss.str();
}


void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); });
    factory.registerFunction("remoteSecure", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); });
    factory.registerFunction("cluster", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); });
    factory.registerFunction("clusterAllReplicas", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("clusterAllReplicas"); });
}

}
