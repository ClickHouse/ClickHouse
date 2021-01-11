#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/* input(structure) - allows to make INSERT SELECT from incoming stream of data
 */
class TableFunctionInput : public ITableFunction
{
public:
    static constexpr auto name = "input";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Input"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    String structure;
};

}
