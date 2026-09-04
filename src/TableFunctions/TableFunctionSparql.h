#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

class TableFunctionSparql : public ITableFunction
{
public:
    static constexpr auto name = "sparql";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "View"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    ASTCreateQuery create;
};

}
