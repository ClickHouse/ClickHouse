#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/*
 * remote ('address', db, table) - creates a temporary StorageDistributed.
 * To get the table structure, a DESC TABLE request is made to the remote server.
 * For example
 * SELECT count() FROM remote('example01-01-1', merge, hits) - go to `example01-01-1`, in the merge database, the hits table.
 * An expression that generates a lot of shards and replicas can also be specified as the host name - see below.
 */
class TableFunctionRemote : public ITableFunction
{
public:
    std::string getName() const override { return "remote"; }
    StoragePtr execute(ASTPtr ast_function, Context & context) const override;
};

}
