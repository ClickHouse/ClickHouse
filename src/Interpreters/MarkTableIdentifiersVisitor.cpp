#include <Poco/String.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/misc.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

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
        visit(*node_func, ast, data);
}

void MarkTableIdentifiersMatcher::visit(const ASTFunction & func, ASTPtr & ptr, Data & data)
{
    /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        auto ast = func.arguments->children.at(1);
        auto opt_name = tryGetIdentifierName(ast);
        if (opt_name && !data.aliases.count(*opt_name) && ast->as<ASTIdentifier>())
        {
            ptr = func.clone();
            ptr->as<ASTFunction>()->arguments->children[1] = ast->as<ASTIdentifier>()->createTable();
            assert(ptr->as<ASTFunction>()->arguments->children[1]);
        }
    }

    // First argument of joinGet can be a table name, perhaps with a database.
    // First argument of dictGet can be a dictionary name, perhaps with a database.
    else if (functionIsJoinGet(func.name) || functionIsDictGet(func.name))
    {
        if (func.arguments->children.empty())
            return;
        auto ast = func.arguments->children.at(0);
        auto opt_name = tryGetIdentifierName(ast);
        if (opt_name && !data.aliases.count(*opt_name) && ast->as<ASTIdentifier>())
        {
            ptr = func.clone();
            ptr->as<ASTFunction>()->arguments->children[0] = ast->as<ASTIdentifier>()->createTable();
            assert(ptr->as<ASTFunction>()->arguments->children[0]);
        }
    }
}

}
