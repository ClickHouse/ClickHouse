#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_NOT_ALLOWED;
}

/* A wrapper for disabled table functions
 */
class TableFunctionDisabled : public ITableFunction
{
public:
    explicit TableFunctionDisabled(TableFunctionPtr func_)
        : func(func_)
    {
    }

    std::string getName() const final { return func->getName(); }
    bool hasStaticStructure() const final { return func->hasStaticStructure(); }
    bool needStructureConversion() const final { return func->needStructureConversion(); }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override
    {
        return func->skipAnalysisForArguments(query_node_table_function, context);
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) final { func->parseArguments(ast_function, context); }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const final
    {
        return func->getActualTableStructure(context, is_insert_query);
    }

    bool needStructureHint() const final { return func->needStructureHint(); }
    void setStructureHint(const ColumnsDescription & cd) final { func->setStructureHint(cd); }

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const final
    {
        return func->getVirtualsToCheckBeforeUsingStructureHint();
    }

    bool supportsReadingSubsetOfColumns(const ContextPtr & ctx) final { return func->supportsReadingSubsetOfColumns(ctx); }
    bool canBeUsedToCreateTable() const final { return func->canBeUsedToCreateTable(); }
    void setPartitionBy(const ASTPtr & ast) final { func->setPartitionBy(ast); }

private:
    TableFunctionPtr func;

    StoragePtr executeImpl(const ASTPtr &, ContextPtr, const std::string &, ColumnsDescription, bool) const final
    {
        throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Function '{}' is disabled in config in table_functions_deny_list", getName());
    }

    const char * getStorageEngineName() const final { return "Null"; }
};
}
