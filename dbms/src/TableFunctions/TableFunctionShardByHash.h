#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* shardByHash(cluster, 'key', db, table) - creates a temporary StorageDistributed,
 *  using the cluster `cluster`, and selecting from it only one shard by hashing the string key.
 *
 * Similarly to the `remote` function, to get the table structure, a DESC TABLE request is made to the remote server.
 */
class TableFunctionShardByHash : public ITableFunction
{
public:
    static constexpr auto name = "shardByHash";
    std::string getName() const override { return name; }
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const override;
};

}
