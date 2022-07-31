#include <Interpreters/RewriteCountVariantsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Poco/String.h>
#include <Common/typeid_cast.h>

namespace DB
{
void RewriteCountVariantsVisitor::visit(ASTPtr & node)
{
    if (node->as<ASTSubquery>() || node->as<ASTTableExpression>() || node->as<ASTArrayJoin>())
        return;

    for (auto & child : node->children)
        visit(child);

    if (auto * func = node->as<ASTFunction>())
        visit(*func);
}

void RewriteCountVariantsVisitor::visit(ASTFunction & func)
{
    if (!func.arguments || func.arguments->children.empty() || func.arguments->children.size() > 1 || !func.arguments->children.front())
        return;

    auto name = Poco::toLower(func.name);

    if (name != "sum" && name != "count")
        return;

    auto & func_arguments = func.arguments->children;

    const auto * first_arg_literal = func_arguments.front()->as<ASTLiteral>();
    if (!first_arg_literal)
        return;

    bool transform = false;
    if (name == "count")
    {
        if (first_arg_literal->value.getType() != Field::Types::Null)
            transform = true;
    }
    else if (name == "sum")
    {
        if (first_arg_literal->value.getType() == Field::Types::UInt64)
        {
            auto constant = first_arg_literal->value.get<UInt64>();
            if (constant == 1)
                transform = true;
        }
    }
    if (!transform)
        return;

    func.name = "count";
    func.arguments->children.clear();
}

}
