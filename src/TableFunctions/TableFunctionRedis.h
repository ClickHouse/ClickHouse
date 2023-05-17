#pragma once

#include <Storages/StorageRedis.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace DB
{

class TableFunctionRedis : public ITableFunction
{
public:
    static constexpr auto name = "redis";
    String getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context,
        const String & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "Redis"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<StorageRedis::Configuration> configuration;
};

}
