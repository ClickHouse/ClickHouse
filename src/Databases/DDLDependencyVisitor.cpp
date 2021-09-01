#include <Databases/DDLDependencyVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void DDLDependencyVisitor::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
}

bool DDLDependencyVisitor::needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/)
{
    return !node->as<ASTStorage>();
}

void DDLDependencyVisitor::visit(const ASTFunction & function, Data & data)
{
    if (function.name == "joinGet" ||
        function.name == "dictHas" ||
        function.name == "dictIsIn" ||
        function.name.starts_with("dictGet"))
    {
        extractTableNameFromArgument(function, data, 0);
    }
}

void DDLDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
{
    /// Just ignore incorrect arguments, proper exception will be thrown later
    if (!function.arguments || function.arguments->children.size() <= arg_idx)
        return;

    String database_name;
    String table_name;

    const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();
    if (const auto * literal = arg->as<ASTLiteral>())
    {
        if (literal->value.getType() != Field::Types::String)
            return;

        String maybe_qualified_name = literal->value.get<String>();
        auto pos = maybe_qualified_name.find('.');
        if (pos == 0 || pos == (maybe_qualified_name.size() - 1))
        {
            /// Most likely name is invalid
            return;
        }
        else if (pos == std::string::npos)
        {
            table_name = std::move(maybe_qualified_name);
        }
        else
        {
            database_name = maybe_qualified_name.substr(0, pos);
            table_name = maybe_qualified_name.substr(pos + 1);
        }
    }
    else if (const auto * identifier = arg->as<ASTIdentifier>())
    {
        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;

        database_name = table_identifier->getDatabaseName();
        table_name = table_identifier->shortName();
    }
    else
    {
        assert(false);
        return;
    }

    if (database_name.empty())
        database_name = data.default_database;
    data.dependencies.emplace(QualifiedTableName{std::move(database_name), std::move(table_name)});
}

}
