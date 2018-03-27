#include <TableFunctions/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/getClusterName.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/TableFunctionShardByHash.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionShardByHash::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

    const char * err = "Table function 'shardByHash' requires 4 parameters: "
        "cluster name, key string to hash, name of remote database, name of remote table.";

    if (args_func.size() != 1)
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

    if (args.size() != 4)
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String cluster_name;
    String key;
    String remote_database;
    String remote_table;

    auto getStringLiteral = [](const IAST & node, const char * description)
    {
        const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(&node);
        if (!lit)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        if (lit->value.getType() != Field::Types::String)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        return safeGet<const String &>(lit->value);
    };

    cluster_name = getClusterName(*args[0]);
    key = getStringLiteral(*args[1], "Key to hash");

    args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
    args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);

    remote_database = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();
    remote_table = static_cast<const ASTLiteral &>(*args[3]).value.safeGet<String>();

    /// Similar to other TableFunctions.
    for (auto & arg : args)
        if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(arg.get()))
            id->kind = ASTIdentifier::Table;

    auto cluster = context.getCluster(cluster_name);
    size_t shard_index = sipHash64(key) % cluster->getShardCount();

    std::shared_ptr<Cluster> shard(cluster->getClusterWithSingleShard(shard_index).release());

    auto res = StorageDistributed::createWithOwnCluster(
        getName(),
        getStructureOfRemoteTable(*shard, remote_database, remote_table, context),
        remote_database,
        remote_table,
        shard,
        context);
    res->startup();
    return res;
}


void registerTableFunctionShardByHash(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionShardByHash>();
}

}
