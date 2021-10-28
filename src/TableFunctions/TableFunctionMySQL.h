#pragma once
#include "config_core.h"

#if USE_MYSQL
#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <mysqlxx/Pool.h>


namespace DB
{

/* mysql ('host:port', database, table, user, password) - creates a temporary StorageMySQL.
 * The structure of the table is taken from the mysql query DESCRIBE table.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMySQL : public ITableFunction
{
public:
    static constexpr auto name = "mysql";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "MySQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable std::optional<mysqlxx::PoolWithFailover> pool;
    std::optional<StorageMySQLConfiguration> configuration;
};

}

#endif
