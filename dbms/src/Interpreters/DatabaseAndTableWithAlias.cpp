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
    size_t num_qualifiers_to_strip = 0;

    auto get_identifier_name = [](const ASTPtr & ast) { return static_cast<const ASTIdentifier &>(*ast).name; };

    /// It is compound identifier
    if (!identifier.children.empty())
    {
        size_t num_components = identifier.children.size();

        /// database.table.column
        if (num_components >= 3
            && !names.database.empty()
            && get_identifier_name(identifier.children[0]) == names.database
            && get_identifier_name(identifier.children[1]) == names.table)
        {
            num_qualifiers_to_strip = 2;
        }

        /// table.column or alias.column. If num_components > 2, it is like table.nested.column.
        if (num_components >= 2
            && ((!names.table.empty() && get_identifier_name(identifier.children[0]) == names.table)
                || (!names.alias.empty() && get_identifier_name(identifier.children[0]) == names.alias)))
        {
            num_qualifiers_to_strip = 1;
        }
    }

    return num_qualifiers_to_strip;
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

        const ASTIdentifier * db_identifier = typeid_cast<const ASTIdentifier *>(identifier.children[0].get());
        const ASTIdentifier * table_identifier = typeid_cast<const ASTIdentifier *>(identifier.children[1].get());
        if (!db_identifier || !table_identifier)
            throw Exception("Logical error: identifiers expected", ErrorCodes::LOGICAL_ERROR);

        database = db_identifier->name;
        table = table_identifier->name;
    }
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database)
{
    if (table_expression.database_and_table_name)
    {
        const auto * identifier = typeid_cast<const ASTIdentifier *>(table_expression.database_and_table_name.get());
        if (!identifier)
            throw Exception("Logical error: identifier expected", ErrorCodes::LOGICAL_ERROR);

        *this = DatabaseAndTableWithAlias(*identifier, current_database);
    }
    else if (table_expression.table_function)
        alias = table_expression.table_function->tryGetAlias();
    else if (table_expression.subquery)
        alias = table_expression.subquery->tryGetAlias();
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);
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

        Names qualifiers;
        if (!alias.empty())
            qualifiers.push_back(alias);
        else
        {
            qualifiers.push_back(database);
            qualifiers.push_back(table);
        }

        for (const auto & qualifier : qualifiers)
            identifier->children.emplace_back(std::make_shared<ASTIdentifier>(qualifier));
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
    if (!database_and_table_name)
        return {};

    const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(database_and_table_name.get());
    if (!identifier)
        return {};

    return *identifier;
}

ASTPtr getTableFunctionOrSubquery(const ASTSelectQuery & select, size_t table_number)
{
    const ASTTableExpression * table_expression = getTableExpression(select, table_number);
    if (table_expression)
    {
#if 1   /// TODO: It hides some logical error in InterpreterSelectQuery & distributed tables
        if (table_expression->database_and_table_name)
        {
            if (table_expression->database_and_table_name->children.empty())
                return table_expression->database_and_table_name;

            if (table_expression->database_and_table_name->children.size() == 2)
                return table_expression->database_and_table_name->children[1];
        }
#endif
        if (table_expression->table_function)
            return table_expression->table_function;

        if (table_expression->subquery)
            return static_cast<const ASTSubquery *>(table_expression->subquery.get())->children[0];
    }

    return nullptr;
}

}
