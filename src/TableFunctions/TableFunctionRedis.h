#pragma once

#include <Storages/StorageRedis.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace DB
{

/* Implements Redis table function.
 * Use redis(host:port, key, structure[, db_index[, password[, pool_size]]]);
 */
class TableFunctionRedis : public ITableFunction
{
public:
    static constexpr auto name = "redis";
    String getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context,
        const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Redis"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    RedisConfiguration configuration;
    String structure;
    String primary_key;
};

}
