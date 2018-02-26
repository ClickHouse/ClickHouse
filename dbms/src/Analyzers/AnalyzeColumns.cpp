#include <vector>
#include <Analyzers/AnalyzeColumns.h>
#include <Analyzers/CollectAliases.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int AMBIGUOUS_TABLE_NAME;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int UNKNOWN_TABLE;
    extern const int THERE_IS_NO_COLUMN;
}


namespace
{

/// Find by fully qualified name, like db.table.column
const CollectTables::TableInfo * findTableByDatabaseAndTableName(
    const CollectTables & tables, const String & database_name, const String & table_name)
{
    for (const auto & table : tables.tables)
        if (table.database_name == database_name && table.table_name == table_name)
            return &table;

    return nullptr;
}


/** Find by single-qualified name, like table.column or alias.column.
  *
  * There are primary matches:
  *  when name is alias like
  *   SELECT name.column FROM (SELECT 1) AS name
  *  or name is table in current database like
  *   SELECT name.column FROM name
  *
  * And secondary matches:
  *  when name is name of table in explicitly specified database like
  *   SELECT name.column FROM db.name
  *
  * If there is only one primary match - return it.
  * If there is many primary matches - ambiguity.
  * If there is no primary matches and only one secondary match - return it.
  * If there is no primary matches and many secondary matches - ambiguity.
  * If there is no any matches - not found.
  */
const CollectTables::TableInfo * findTableByNameOrAlias(
    const CollectTables & tables, const String & name)
{
    const CollectTables::TableInfo * primary_match = nullptr;
    const CollectTables::TableInfo * secondary_match = nullptr;

    for (const auto & table : tables.tables)
    {
        if (table.alias == name
            || (table.database_name.empty() && table.table_name == name))
        {
            if (primary_match)
                throw Exception("Table name " + backQuoteIfNeed(name) + " is ambiguous", ErrorCodes::AMBIGUOUS_TABLE_NAME);
            primary_match = &table;
        }
        else if (!primary_match && table.table_name == name)
        {
            if (secondary_match)
                throw Exception("Table name " + backQuoteIfNeed(name) + " is ambiguous", ErrorCodes::AMBIGUOUS_TABLE_NAME);
            secondary_match = &table;
        }
    }

    if (primary_match)
        return primary_match;
    if (secondary_match)
        return secondary_match;
    return nullptr;
}


/** Find table in case when its name is not specified. Like just
  *  SELECT column FROM t1, t2
  * Select a table, where specified column exists.
  * If more than one such table - ambiguity.
  */
const CollectTables::TableInfo * findTableWithUnqualifiedName(const CollectTables & tables, const String & column_name)
{
    const CollectTables::TableInfo * res = nullptr;

    for (const auto & table : tables.tables)
    {
        if (table.structure_of_subquery)
        {
            if (table.structure_of_subquery.has(column_name))
            {
                if (res)
                    throw Exception("Ambiguous column name " + backQuoteIfNeed(column_name), ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                res = &table;
                break;
            }
        }
        else if (table.storage)
        {
            if (table.storage->hasColumn(column_name))
            {
                if (res)
                    throw Exception("Ambiguous column name " + backQuoteIfNeed(column_name), ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                res = &table;
            }
        }
        else
            throw Exception("Logical error: no storage and no structure of subquery is specified for table", ErrorCodes::LOGICAL_ERROR);
    }

    return res;
}


/// Create maximum-qualified identifier for column in table.
ASTPtr createASTIdentifierForColumnInTable(const String & column, const CollectTables::TableInfo & table)
{
    ASTPtr database_name_identifier_node;
    if (!table.database_name.empty())
        database_name_identifier_node =  std::make_shared<ASTIdentifier>(table.database_name, ASTIdentifier::Column);

    ASTPtr table_name_identifier_node;
    String table_name_or_alias;

    if (!table.table_name.empty())
        table_name_or_alias = table.table_name;
    else if (table.database_name.empty() && !table.alias.empty())
        table_name_or_alias = table.alias;

    if (!table_name_or_alias.empty())
        table_name_identifier_node = std::make_shared<ASTIdentifier>(table_name_or_alias, ASTIdentifier::Column);

    ASTPtr column_identifier_node = std::make_shared<ASTIdentifier>(column, ASTIdentifier::Column);

    String compound_name;
    if (database_name_identifier_node)
        compound_name += table.database_name + ".";
    if (table_name_identifier_node)
        compound_name += table_name_or_alias + ".";
    compound_name += column;

    auto elem = std::make_shared<ASTIdentifier>(compound_name, ASTIdentifier::Column);

    if (database_name_identifier_node)
        elem->children.emplace_back(std::move(database_name_identifier_node));
    if (table_name_identifier_node)
        elem->children.emplace_back(std::move(table_name_identifier_node));
    if (!elem->children.empty())
        elem->children.emplace_back(std::move(column_identifier_node));

    return elem;
}


void createASTsForAllColumnsInTable(const CollectTables::TableInfo & table, ASTs & res)
{
    if (table.storage)
        for (const auto & name : table.storage->getColumnNamesList())
            res.emplace_back(createASTIdentifierForColumnInTable(name, table));
    else
        for (size_t i = 0, size = table.structure_of_subquery.columns(); i < size; ++i)
            res.emplace_back(createASTIdentifierForColumnInTable(table.structure_of_subquery.getByPosition(i).name, table));
}


ASTs expandUnqualifiedAsterisk(const CollectTables & tables)
{
    ASTs res;
    for (const auto & table : tables.tables)
        createASTsForAllColumnsInTable(table, res);
    return res;
}


ASTs expandQualifiedAsterisk(
    const IAST & ast, const CollectTables & tables)
{
    if (ast.children.size() != 1)
        throw Exception("Logical error: AST node for qualified asterisk has number of children not equal to one", ErrorCodes::LOGICAL_ERROR);

    const ASTIdentifier & qualifier = static_cast<const ASTIdentifier &>(*ast.children[0]);

    const CollectTables::TableInfo * table = nullptr;

    if (qualifier.children.empty())
        table = findTableByNameOrAlias(tables, qualifier.name);
    else if (qualifier.children.size() == 2)
        table = findTableByDatabaseAndTableName(tables,
            static_cast<const ASTIdentifier &>(*qualifier.children[0]).name,
            static_cast<const ASTIdentifier &>(*qualifier.children[1]).name);
    else
        throw Exception("Unsupported number of components in asterisk qualifier", ErrorCodes::NOT_IMPLEMENTED);

    /// TODO Implement for case table.nested.* and database.table.nested.*

    if (!table)
        throw Exception("There is no table " + qualifier.name + " in query", ErrorCodes::UNKNOWN_TABLE);

    ASTs res;
    createASTsForAllColumnsInTable(*table, res);
    return res;
}


void processIdentifier(
    const ASTPtr & ast, AnalyzeColumns::Columns & columns, const CollectAliases & aliases, const CollectTables & tables)
{
    const ASTIdentifier & identifier = static_cast<const ASTIdentifier &>(*ast);

    if (aliases.aliases.count(identifier.name))
        return;

    if (columns.count(identifier.name))
        return;

    const CollectTables::TableInfo * table = nullptr;
    String column_name;

    if (identifier.children.empty())
    {
        /** Lambda parameters are not columns from table. Just skip them.
          * This step requires AnalyzeLambdas to be done on AST.
          */
        if (startsWith(identifier.name, "_lambda"))
            return;

        table = findTableWithUnqualifiedName(tables, identifier.name);
        if (table)
            column_name = identifier.name;
    }
    else if (identifier.children.size() == 2)
    {
        const String & first = static_cast<const ASTIdentifier &>(*identifier.children[0]).name;
        const String & second = static_cast<const ASTIdentifier &>(*identifier.children[1]).name;

        /// table.column
        table = findTableByNameOrAlias(tables, first);

        if (table)
        {
            column_name = second;
        }
        else
        {
            /// column.nested
            table = findTableWithUnqualifiedName(tables, identifier.name);
            if (table)
                column_name = identifier.name;
        }
    }
    else if (identifier.children.size() == 3)
    {
        const String & first = static_cast<const ASTIdentifier &>(*identifier.children[0]).name;
        const String & second = static_cast<const ASTIdentifier &>(*identifier.children[1]).name;
        const String & third = static_cast<const ASTIdentifier &>(*identifier.children[2]).name;

        /// database.table.column
        table = findTableByDatabaseAndTableName(tables, first, second);

        if (table)
        {
            column_name = third;
        }
        else
        {
            /// table.column.nested
            table = findTableByNameOrAlias(tables, first);

            if (table)
            {
                column_name = second + "." + third;
            }
            else
            {
                /// column.nested.nested
                table = findTableWithUnqualifiedName(tables, identifier.name);
                if (table)
                    column_name = identifier.name;
            }
        }
    }

    if (!table)
        throw Exception("Cannot find column " + identifier.name, ErrorCodes::THERE_IS_NO_COLUMN);

    AnalyzeColumns::ColumnInfo info;
    info.node = ast;
    info.table = *table;
    info.name_in_table = column_name;

    if (table->structure_of_subquery)
    {
        if (!table->structure_of_subquery.has(column_name))
            throw Exception("Cannot find column " + backQuoteIfNeed(column_name) + " in subquery", ErrorCodes::LOGICAL_ERROR);

        info.data_type = table->structure_of_subquery.getByName(column_name).type;
    }
    else if (table->storage)
    {
        info.data_type = table->storage->getDataTypeByName(column_name);
    }
    else
        throw Exception("Logical error: no storage and no structure of subquery is specified for table", ErrorCodes::LOGICAL_ERROR);

    columns[identifier.name] = info;
}


void processImpl(ASTPtr & ast, AnalyzeColumns::Columns & columns, const CollectAliases & aliases, const CollectTables & tables)
{
    /// Don't go into subqueries and table-like expressions.
    if (typeid_cast<const ASTSelectQuery *>(ast.get())
        || typeid_cast<const ASTTableExpression *>(ast.get()))
    {
        return;
    }
    else if (const ASTFunction * func = typeid_cast<const ASTFunction *>(ast.get()))
    {
        String func_name_lowercase = Poco::toLower(func->name);

        /// As special case, treat count(*) as count(), not as count(list of all columns).
        if (func_name_lowercase == "count" && func->arguments->children.size() == 1
            && typeid_cast<const ASTAsterisk *>(func->arguments->children[0].get()))
        {
            func->arguments->children.clear();
        }
    }
    else if (typeid_cast<ASTExpressionList *>(ast.get()))
    {
        /// Replace asterisks to list of columns.
        ASTs & asts = ast->children;
        for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
        {
            if (typeid_cast<ASTAsterisk *>(asts[i].get()))
            {
                ASTs expanded = expandUnqualifiedAsterisk(tables);
                asts.erase(asts.begin() + i);
                asts.insert(asts.begin() + i, expanded.begin(), expanded.end());
            }
            else if (ASTQualifiedAsterisk * asterisk = typeid_cast<ASTQualifiedAsterisk *>(asts[i].get()))
            {
                ASTs expanded = expandQualifiedAsterisk(*asterisk, tables);
                asts.erase(asts.begin() + i);
                asts.insert(asts.begin() + i, expanded.begin(), expanded.end());
            }
        }
    }
    else if (typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        processIdentifier(ast, columns, aliases, tables);
        return;
    }

    for (auto & child : ast->children)
        processImpl(child, columns, aliases, tables);
}

}


void AnalyzeColumns::process(ASTPtr & ast, const CollectAliases & aliases, const CollectTables & tables)
{
    /// If this is SELECT query, don't go into FORMAT and SETTINGS clauses
    /// - they contain identifiers that are not columns.
    const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get());

    for (auto & child : ast->children)
    {
        if (select && (child.get() == select->format.get() || child.get() == select->settings.get()))
            continue;

        processImpl(child, columns, aliases, tables);
    }
}


void AnalyzeColumns::dump(WriteBuffer & out) const
{
    /// For need of tests, we need to dump result in some fixed order.
    std::vector<Columns::const_iterator> vec;
    vec.reserve(columns.size());
    for (auto it = columns.begin(); it != columns.end(); ++it)
        vec.emplace_back(it);

    std::sort(vec.begin(), vec.end(), [](const auto & a, const auto & b) { return a->first < b->first; });

    for (const auto & it : vec)
    {
        writeString(it->first, out);
        writeCString(" -> ", out);

        writeProbablyBackQuotedString(it->second.name_in_table, out);
        writeCString(" ", out);
        writeProbablyBackQuotedString(it->second.data_type->getName(), out);

        const auto & table = it->second.table;

        writeCString(". Database name: ", out);
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

        writeCString(". AST: ", out);
        if (it->second.node)
        {
            std::stringstream formatted_ast;
            formatAST(*it->second.node, formatted_ast, false, true);
            writeString(formatted_ast.str(), out);
        }
        else
            writeCString("(none)", out);

        writeChar('\n', out);
    }
}


}
