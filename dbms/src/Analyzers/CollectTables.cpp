#include <Analyzers/CollectTables.h>
#include <Analyzers/CollectAliases.h>
#include <Analyzers/ExecuteTableFunctions.h>
#include <Analyzers/AnalyzeResultOfQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int LOGICAL_ERROR;
}


static CollectTables::TableInfo processOrdinaryTable(const ASTPtr & ast_database_and_table_name, const Context & context)
{
    const ASTIdentifier & identifier = static_cast<const ASTIdentifier &>(*ast_database_and_table_name);

    CollectTables::TableInfo res;
    res.node = ast_database_and_table_name;
    res.alias = identifier.tryGetAlias();

    if (ast_database_and_table_name->children.empty())
    {
        res.table_name = identifier.name;
    }
    else
    {
        if (ast_database_and_table_name->children.size() != 2)
            throw Exception("Logical error: number of components in table expression not equal to two", ErrorCodes::LOGICAL_ERROR);

        res.database_name = static_cast<const ASTIdentifier &>(*identifier.children[0]).name;
        res.table_name = static_cast<const ASTIdentifier &>(*identifier.children[1]).name;
    }

    res.storage = context.getTable(res.database_name, res.table_name);
    return res;
}


static CollectTables::TableInfo processTableFunction(const ASTPtr & ast_table_function, const ExecuteTableFunctions & table_functions)
{
    const ASTFunction & function = typeid_cast<const ASTFunction &>(*ast_table_function);

    IAST::Hash ast_hash = ast_table_function->getTreeHash();

    auto it = table_functions.tables.find(ast_hash);
    if (table_functions.tables.end() == it)
        throw Exception("Table function " + function.name + " was not executed in advance.", ErrorCodes::LOGICAL_ERROR);

    CollectTables::TableInfo res;
    res.node = ast_table_function;
    res.alias = function.tryGetAlias();
    res.storage = it->second;
    return res;
}


static CollectTables::TableInfo processNoTables(const Context & context)
{
    /// No FROM section. Interpret it as FROM system.one.
    CollectTables::TableInfo res;
    res.database_name = "system";
    res.table_name = "one";
    res.storage = context.getTable(res.database_name, res.table_name);
    return res;
}


static CollectTables::TableInfo processSubquery(ASTPtr & ast_subquery, const Context & context, ExecuteTableFunctions & table_functions)
{
    AnalyzeResultOfQuery analyzer;
    analyzer.process(typeid_cast<ASTSubquery &>(*ast_subquery).children.at(0), context, table_functions);

    CollectTables::TableInfo res;
    res.node = ast_subquery;
    res.alias = ast_subquery->tryGetAlias();
    res.structure_of_subquery = analyzer.result;
    return res;
}


void CollectTables::process(ASTPtr & ast, const Context & context, const CollectAliases & /*aliases*/, ExecuteTableFunctions & table_functions)
{
    const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get());
    if (!select)
        throw Exception("CollectTables::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    if (!select->tables)
    {
        tables.emplace_back(processNoTables(context));
        return;
    }

    for (auto & child : select->tables->children)
    {
        ASTTablesInSelectQueryElement & element = static_cast<ASTTablesInSelectQueryElement &>(*child);

        if (!element.table_expression)        /// This is ARRAY JOIN
            continue;

        ASTTableExpression & table_expression = static_cast<ASTTableExpression &>(*element.table_expression);

        if (table_expression.database_and_table_name)
        {
            tables.emplace_back(processOrdinaryTable(table_expression.database_and_table_name, context));

            /// TODO It could be alias to another table expression.
        }
        else if (table_expression.table_function)
        {
            tables.emplace_back(processTableFunction(table_expression.table_function, table_functions));
        }
        else if (table_expression.subquery)
        {
            tables.emplace_back(processSubquery(table_expression.subquery, context, table_functions));
        }
        else
            throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);
    }

    /// TODO Control that tables don't have conflicting names.
}


void CollectTables::dump(WriteBuffer & out) const
{
    for (const auto & table : tables)
    {
        writeCString("Database name: ", out);
        if (table.database_name.empty())
            writeCString("(none)", out);
        else
            writeProbablyBackQuotedString(table.database_name, out);

        writeCString(". Table name: ", out);
        if (table.table_name.empty())
            writeCString("(none)", out);
        else
            writeProbablyBackQuotedString(table.table_name, out);

        writeCString(". Alias: ", out);
        if (table.alias.empty())
            writeCString("(none)", out);
        else
            writeProbablyBackQuotedString(table.alias, out);

        writeCString(". Storage: ", out);
        if (!table.storage)
            writeCString("(none)", out);
        else
            writeProbablyBackQuotedString(table.storage->getName(), out);

        writeCString(". Structure of subquery: ", out);
        if (!table.structure_of_subquery)
            writeCString("(none)", out);
        else
            writeString(table.structure_of_subquery.dumpStructure(), out);

        writeCString(". AST: ", out);
        if (!table.node)
            writeCString("(none)", out);
        else
        {
            std::stringstream formatted_ast;
            formatAST(*table.node, formatted_ast, false, true);
            writeString(formatted_ast.str(), out);
        }

        writeChar('\n', out);
    }
}

}
