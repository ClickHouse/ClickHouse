#pragma once

#include <Storages/StorageArrowFlight.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

class TableFunctionArrowFlight : public ITableFunction
{
public:
    static constexpr auto name = "arrowflight";
    String getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context,
        const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "ArrowFlight"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    StorageArrowFlight::Configuration configuration;
};

}
