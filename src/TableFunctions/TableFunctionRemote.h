#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/// The result of parsing the arguments of the `remote`/`remoteSecure`/`cluster`/`clusterAllReplicas`
/// table functions or the `Remote`/`RemoteSecure` storage engines. It is everything needed to
/// construct a `StorageDistributed`.
struct ParsedRemoteFunctionArguments
{
    ClusterPtr cluster;
    StorageID remote_table_id = StorageID::createEmpty();
    ASTPtr sharding_key;
    ASTPtr remote_table_function_ptr;
};

/// Parses the arguments shared by the `remote` family of table functions and the `Remote`/`RemoteSecure`
/// storage engines. `args` is the list of argument expressions (may be modified in place during evaluation).
ParsedRemoteFunctionArguments parseRemoteFunctionArguments(
    ASTs & args,
    ContextPtr context,
    const std::string & name,
    bool is_cluster_function,
    bool secure,
    const PreformattedMessage & help_message);

/* remote ('address', db, table) - creates a temporary StorageDistributed.
 * To get the table structure, a DESC TABLE request is made to the remote server.
 * For example:
 * SELECT count() FROM remote('example01-01-1', merge, hits) - go to `example01-01-1`, in the merge database, the hits table.
 * An expression that generates a set of shards and replicas can also be specified as the host name - see below.
 * Also, there is a cluster version of the function: cluster('existing_cluster_name', 'db', 'table').
 */
class TableFunctionRemote : public ITableFunction
{
public:
    explicit TableFunctionRemote(const std::string & name_, bool secure_ = false);

    std::string getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    bool needStructureConversion() const override { return false; }

private:

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "Distributed"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::string name;
    bool is_cluster_function;
    PreformattedMessage help_message;
    bool secure;

    ClusterPtr cluster;
    StorageID remote_table_id = StorageID::createEmpty();
    ASTPtr remote_table_function_ptr;
    ASTPtr sharding_key = nullptr;
};

}
