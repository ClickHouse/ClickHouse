#include <Interpreters/MarkTableIdentifiersVisitor.h>

#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

namespace
{
    void replaceArgumentWithTableIdentifierIfNotAlias(ASTFunction & func, size_t argument_pos, const Aliases & aliases)
    {
        if (!func.arguments || (func.arguments->children.size() <= argument_pos))
            return;
        auto arg = func.arguments->children[argument_pos];
        auto * identifier = arg->as<ASTIdentifier>();
        if (!identifier)
            return;
        if (aliases.contains(identifier->name()))
            return;
        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;
        func.arguments->children[argument_pos] = table_identifier;
    }
}


bool MarkTableIdentifiersMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    if (child->as<ASTSelectQuery>())
        return false;
    if (node->as<ASTTableExpression>())
        return false;
    return true;
}

void MarkTableIdentifiersMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * node_func = ast->as<ASTFunction>())
        visit(*node_func, data);
}

void MarkTableIdentifiersMatcher::visit(ASTFunction & func, const Data & data)
{
    /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        replaceArgumentWithTableIdentifierIfNotAlias(func, 1, data.aliases);
    }

    // First argument of joinGet can be a table name, perhaps with a database.
    // First argument of dictGet can be a dictionary name, perhaps with a database.
    else if (functionIsJoinGet(func.name) || functionIsDictGet(func.name))
    {
        replaceArgumentWithTableIdentifierIfNotAlias(func, 0, data.aliases);
    }
}

}
