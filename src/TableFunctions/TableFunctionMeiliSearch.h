#pragma once
#include <TableFunctions/ITableFunction.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{

class TableFunctionMeiliSearch : public ITableFunction
{
public:
    static constexpr auto name = "MeiliSearch";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "MeiliSearch"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::optional<MeiliSearchConfiguration> configuration;
};

}

