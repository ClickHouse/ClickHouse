#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTExplainQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <base/types.h>


namespace DB
{

/* Invoked via `SELECT * FROM (EXPLAIN <query>)`
 * Return result of EXPLAIN in a single string column.
 * Can be used to further processing of the result of EXPLAIN using SQL (e.g. in tests).
 */
class TableFunctionExplain : public ITableFunction
{
public:
    static constexpr auto name = "viewExplain";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Explain"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    InterpreterExplainQuery getInterpreter(ContextPtr context) const;

    ASTPtr query = nullptr;
};


}
