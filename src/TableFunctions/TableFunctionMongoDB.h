#pragma once

#include <Storages/StorageMongoDB.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>

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
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "MongoDB"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<StorageMongoDB::Configuration> configuration;
    String structure;
};

}
