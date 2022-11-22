#pragma once
#include "config_core.h"

#if USE_LIBPQXX
#include <TableFunctions/ITableFunction.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Storages/ExternalDataSourceConfiguration.h>


namespace DB
{

class TableFunctionPostgreSQL : public ITableFunction
{
public:
    static constexpr auto name = "postgresql";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "PostgreSQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    postgres::PoolWithFailoverPtr connection_pool;
    std::optional<StoragePostgreSQLConfiguration> configuration;
};

}

#endif
