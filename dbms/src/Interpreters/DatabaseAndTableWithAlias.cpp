#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

/// Checks that ast is ASTIdentifier and remove num_qualifiers_to_strip components from left.
/// Example: 'database.table.name' -> (num_qualifiers_to_strip = 2) -> 'name'.
void stripIdentifier(DB::ASTPtr & ast, size_t num_qualifiers_to_strip)
{
    ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(ast.get());

    if (!identifier)
        throw DB::Exception("ASTIdentifier expected for stripIdentifier", DB::ErrorCodes::LOGICAL_ERROR);

    if (num_qualifiers_to_strip)
    {
        size_t num_components = identifier->children.size();

        /// plain column
        if (num_components - num_qualifiers_to_strip == 1)
        {
            DB::String node_alias = identifier->tryGetAlias();
            ast = identifier->children.back();
            if (!node_alias.empty())
                ast->setAlias(node_alias);
        }
        else
            /// nested column
        {
            identifier->children.erase(identifier->children.begin(), identifier->children.begin() + num_qualifiers_to_strip);
            DB::String new_name;
            for (const auto & child : identifier->children)
            {
                if (!new_name.empty())
                    new_name += '.';
                new_name += static_cast<const ASTIdentifier &>(*child.get()).name;
            }
            identifier->name = new_name;
        }
    }
}

/// Get the number of components of identifier which are correspond to 'alias.', 'table.' or 'databas.table.' from names.
size_t getNumComponentsToStripInOrderToTranslateQualifiedName(const ASTIdentifier & identifier,
                                                              const DatabaseAndTableWithAlias & names)
{
    /// database.table.column
    if (doesIdentifierBelongTo(identifier, names.database, names.table))
        return 2;

    /// table.column or alias.column.
    if (doesIdentifierBelongTo(identifier, names.table) ||
        doesIdentifierBelongTo(identifier, names.alias))
        return 1;

    return 0;
}


DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database)
{
    database = current_database;
    table = identifier.name;
    alias = identifier.tryGetAlias();

    if (!identifier.children.empty())
    {
        if (identifier.children.size() != 2)
            throw Exception("Logical error: number of components in table expression not equal to two", ErrorCodes::LOGICAL_ERROR);

        getIdentifierName(identifier.children[0], database);
        getIdentifierName(identifier.children[1], table);
    }
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTPtr & node, const String & current_database)
{
    const auto * identifier = typeid_cast<const ASTIdentifier *>(node.get());
    if (!identifier)
        throw Exception("Logical error: identifier expected", ErrorCodes::LOGICAL_ERROR);

    *this = DatabaseAndTableWithAlias(*identifier, current_database);
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database)
{
    if (table_expression.database_and_table_name)
        *this = DatabaseAndTableWithAlias(table_expression.database_and_table_name, current_database);
    else if (table_expression.table_function)
        alias = table_expression.table_function->tryGetAlias();
    else if (table_expression.subquery)
        alias = table_expression.subquery->tryGetAlias();
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);
}

bool DatabaseAndTableWithAlias::satisfies(const DatabaseAndTableWithAlias & db_table, bool table_may_be_an_alias)
{
    /// table.*, alias.* or database.table.*

    if (database.empty())
    {
        if (!db_table.table.empty() && table == db_table.table)
            return true;

        if (!db_table.alias.empty())
            return (alias == db_table.alias) || (table_may_be_an_alias && table == db_table.alias);
    }

    return database == db_table.database && table == db_table.table;
}

String DatabaseAndTableWithAlias::getQualifiedNamePrefix() const
{
    if (alias.empty() && table.empty())
        return "";

    return (!alias.empty() ? alias : (database + '.' + table)) + '.';
}

void DatabaseAndTableWithAlias::makeQualifiedName(const ASTPtr & ast) const
{
    if (auto identifier = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        String prefix = getQualifiedNamePrefix();
        identifier->name.insert(identifier->name.begin(), prefix.begin(), prefix.end());

        addIdentifierQualifier(*identifier, database, table, alias);
    }
}

std::vector<const ASTTableExpression *> getSelectTablesExpression(const ASTSelectQuery & select_query)
{
    if (!select_query.tables)
        return {};

    std::vector<const ASTTableExpression *> tables_expression;

    for (const auto & child : select_query.tables->children)
    {
        ASTTablesInSelectQueryElement * tables_element = static_cast<ASTTablesInSelectQueryElement *>(child.get());

        if (tables_element->table_expression)
            tables_expression.emplace_back(static_cast<const ASTTableExpression *>(tables_element->table_expression.get()));
    }

    return tables_expression;
}

static const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (!select.tables)
        return {};

    ASTTablesInSelectQuery & tables_in_select_query = static_cast<ASTTablesInSelectQuery &>(*select.tables);
    if (tables_in_select_query.children.size() <= table_number)
        return {};

    ASTTablesInSelectQueryElement & tables_element =
        static_cast<ASTTablesInSelectQueryElement &>(*tables_in_select_query.children[table_number]);

    if (!tables_element.table_expression)
        return {};

    return static_cast<const ASTTableExpression *>(tables_element.table_expression.get());
}

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database)
{
    std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(select_query);

    std::vector<DatabaseAndTableWithAlias> database_and_table_with_aliases;
    database_and_table_with_aliases.reserve(tables_expression.size());

    for (const auto & table_expression : tables_expression)
        database_and_table_with_aliases.emplace_back(DatabaseAndTableWithAlias(*table_expression, current_database));

    return database_and_table_with_aliases;
}

std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number)
{
    const ASTTableExpression * table_expression = getTableExpression(select, table_number);
    if (!table_expression)
        return {};

    ASTPtr database_and_table_name = table_expression->database_and_table_name;
    if (!database_and_table_name || !isIdentifier(database_and_table_name))
        return {};

    return DatabaseAndTableWithAlias(database_and_table_name);
}

ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (const ASTTableExpression * table_expression = getTableExpression(select, table_number))
    {
        if (table_expression->database_and_table_name)
            return table_expression->database_and_table_name;

        if (table_expression->table_function)
            return table_expression->table_function;

        if (table_expression->subquery)
            return static_cast<const ASTSubquery *>(table_expression->subquery.get())->children[0];
    }

    return nullptr;
}

}
