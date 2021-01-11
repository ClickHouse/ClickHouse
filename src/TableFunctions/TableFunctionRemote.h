#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/* remote ('address', db, table) - creates a temporary StorageDistributed.
 * To get the table structure, a DESC TABLE request is made to the remote server.
 * For example
 * SELECT count() FROM remote('example01-01-1', merge, hits) - go to `example01-01-1`, in the merge database, the hits table.
 * An expression that generates a set of shards and replicas can also be specified as the host name - see below.
 * Also, there is a cluster version of the function: cluster('existing_cluster_name', 'db', 'table')
 */
class TableFunctionRemote : public ITableFunction
{
public:
    TableFunctionRemote(const std::string & name_, bool secure_ = false);

    std::string getName() const override { return name; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;

    bool needStructureConversion() const override { return false; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Distributed"; }

    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    std::string name;
    bool is_cluster_function;
    std::string help_message;
    bool secure;

    ClusterPtr cluster;
    StorageID remote_table_id = StorageID::createEmpty();
    ASTPtr remote_table_function_ptr;
};

}
