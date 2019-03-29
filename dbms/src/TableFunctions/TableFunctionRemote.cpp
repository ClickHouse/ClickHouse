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


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & ast_function, const Context & context) const
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

    auto getStringLiteral = [](const IAST & node, const char * description)
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
        ASTPtr ast_name = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
        cluster_name = ast_name->as<ASTLiteral &>().value.safeGet<const String &>();
    }
    else
    {
        if (!getIdentifierName(args[arg_num], cluster_name))
            cluster_description = getStringLiteral(*args[arg_num], "Hosts pattern");
    }
    ++arg_num;

    args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);

    const auto * function = args[arg_num]->as<ASTFunction>();

    if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
    {
        remote_table_function_ptr = args[arg_num];
        ++arg_num;
    }
    else
    {
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
            username = getStringLiteral(*args[arg_num], "Username");
            ++arg_num;
        }
        else
            username = "default";

        if (arg_num < args.size())
        {
            password = getStringLiteral(*args[arg_num], "Password");
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
        cluster = context.getCluster(cluster_name);
    }
    else
    {
        /// Create new cluster from the scratch
        size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;
        std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        std::vector<std::vector<String>> names;
        for (size_t i = 0; i < shards.size(); ++i)
            names.push_back(parseRemoteDescription(shards[i], 0, shards[i].size(), '|', max_addresses));

        if (names.empty())
            throw Exception("Shard list is empty after parsing first argument", ErrorCodes::BAD_ARGUMENTS);

        auto maybe_secure_port = context.getTCPPortSecure();
        cluster = std::make_shared<Cluster>(context.getSettings(), names, username, password, (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context.getTCPPort()), false, secure);
    }

    auto structure_remote_table = getStructureOfRemoteTable(*cluster, remote_database, remote_table, context, remote_table_function_ptr);

    StoragePtr res = remote_table_function_ptr
        ? StorageDistributed::createWithOwnCluster(
            getName(),
            structure_remote_table,
            remote_table_function_ptr,
            cluster,
            context)
        : StorageDistributed::createWithOwnCluster(
            getName(),
            structure_remote_table,
            remote_database,
            remote_table,
            cluster,
            context);

    res->startup();
    return res;
}


TableFunctionRemote::TableFunctionRemote(const std::string & name, bool secure)
    : name{name}, secure{secure}
{
    is_cluster_function = name == "cluster";

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
}

}
