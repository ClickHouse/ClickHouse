#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/* format(format_name, data) - ...
 */
class TableFunctionFormat : public ITableFunction
{
public:
    static constexpr auto name = "format";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return false; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Values"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    Block parseData(ColumnsDescription columns, ContextPtr context) const;

    String format;
    String data;
};

}
