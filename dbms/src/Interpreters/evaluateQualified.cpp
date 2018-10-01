#include <Interpreters/evaluateQualified.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

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


DatabaseAndTableWithAlias getTableNameWithAliasFromTableExpression(const ASTTableExpression & table_expression,
                                                                   const Context & context)
{
    DatabaseAndTableWithAlias database_and_table_with_alias;

    if (table_expression.database_and_table_name)
    {
        const auto & identifier = static_cast<const ASTIdentifier &>(*table_expression.database_and_table_name);

        database_and_table_with_alias.alias = identifier.tryGetAlias();

        if (table_expression.database_and_table_name->children.empty())
        {
            database_and_table_with_alias.database = context.getCurrentDatabase();
            database_and_table_with_alias.table = identifier.name;
        }
        else
        {
            if (table_expression.database_and_table_name->children.size() != 2)
                throw Exception("Logical error: number of components in table expression not equal to two", ErrorCodes::LOGICAL_ERROR);

            database_and_table_with_alias.database = static_cast<const ASTIdentifier &>(*identifier.children[0]).name;
            database_and_table_with_alias.table = static_cast<const ASTIdentifier &>(*identifier.children[1]).name;
        }
    }
    else if (table_expression.table_function)
    {
        database_and_table_with_alias.alias = table_expression.table_function->tryGetAlias();
    }
    else if (table_expression.subquery)
    {
        database_and_table_with_alias.alias = table_expression.subquery->tryGetAlias();
    }
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);

    return database_and_table_with_alias;
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

std::pair<String, String> getDatabaseAndTableNameFromIdentifier(const ASTIdentifier & identifier)
{
    std::pair<String, String> res;
    res.second = identifier.name;
    if (!identifier.children.empty())
    {
        if (identifier.children.size() != 2)
            throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

        res.first = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
        res.second = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
    }
    return res;
}

String DatabaseAndTableWithAlias::getQualifiedNamePrefix() const
{
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

}
