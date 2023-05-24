#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageMongoDB.h>

namespace DB
{

class TableFunctionMongoDB : public ITableFunction
{
public:
    static constexpr auto name = "mongodb";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "MongoDB"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<StorageMongoDBConfiguration> configuration;
    String structure;
};

}
