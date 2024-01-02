#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTExplainQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <base/types.h>


namespace DB
{

class TableFunctionExplain : public ITableFunction
{
public:
    static constexpr auto name = "viewExplain";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Explain"; }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    InterpreterExplainQuery getInterpreter(ContextPtr context) const;

    ASTPtr query = nullptr;
};

}
