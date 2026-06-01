#include <TableFunctions/TableFunctionRemote.h>

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Access/Common/AccessFlags.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void TableFunctionRemote::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    auto parsed = parseRemoteFunctionArguments(args, context, name, is_cluster_function, secure, help_message);
    cluster = std::move(parsed.cluster);
    remote_table_id = std::move(parsed.remote_table_id);
    sharding_key = std::move(parsed.sharding_key);
    remote_table_function_ptr = std::move(parsed.remote_table_function_ptr);
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context, is_insert_query);

    chassert(cluster);

    bool has_local_shard = false;
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (shard_info.isLocal())
        {
            has_local_shard = true;
            break;
        }
    }

    if (has_local_shard && !is_insert_query)
        context->checkAccess(AccessType::SELECT, remote_table_id);
    else if (has_local_shard)
        context->checkAccess(AccessType::INSERT, remote_table_id);

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
    chassert(cluster);
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
        is_cluster_function ? "[<cluster name or default if not specified>, [<database.table> | [<name of remote database>, <name of remote table>] | <table function>]] [, sharding_key]"
                            : "<addresses pattern> [, <name of remote database>, <name of remote table>] [, username[, password], sharding_key]");
}

void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); }, {}});
    factory.registerFunction("remoteSecure", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); }, {}});
    factory.registerFunction("cluster", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); }, {}, {.allow_readonly = true}});
    factory.registerFunction("clusterAllReplicas", {[] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("clusterAllReplicas"); }, {}, {.allow_readonly = true}});
}

}
