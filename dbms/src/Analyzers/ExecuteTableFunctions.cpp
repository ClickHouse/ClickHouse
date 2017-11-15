#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <Analyzers/ExecuteTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int LOGICAL_ERROR;
}

/// Allows to execute exactly same table functions only once.
using ASTTreeToTable = std::map<IAST::Hash, StoragePtr>;


static void processTableFunction(const ASTPtr & ast_table_function, const Context & context, ExecuteTableFunctions::Tables & result_map)
{
    const ASTFunction & function = typeid_cast<const ASTFunction &>(*ast_table_function);

    /// If already executed.
    IAST::Hash ast_hash = ast_table_function->getTreeHash();
    if (result_map.count(ast_hash))
        return;

    /// Obtain table function
    TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(function.name, context);
    /// Execute it and store result
    StoragePtr table = table_function_ptr->execute(ast_table_function, context);
    result_map[ast_hash] = table;
}


void ExecuteTableFunctions::process(ASTPtr & ast, const Context & context)
{
    const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get());
    if (!select)
        throw Exception("ExecuteTableFunctions::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    if (!select->tables)
        return;

    for (auto & child : select->tables->children)
    {
        ASTTablesInSelectQueryElement & element = static_cast<ASTTablesInSelectQueryElement &>(*child);

        if (!element.table_expression)        /// This is ARRAY JOIN
            continue;

        ASTTableExpression & table_expression = static_cast<ASTTableExpression &>(*element.table_expression);

        if (!table_expression.table_function)
            continue;

        processTableFunction(table_expression.table_function, context, tables);
    }
}


void ExecuteTableFunctions::dump(WriteBuffer & out) const
{
    for (const auto & table : tables)
    {
        writeString(table.second->getName(), out);
        writeCString("\n\n", out);
        writeString(table.second->getColumnsList().toString(), out);
        writeCString("\n", out);
    }
}

}
