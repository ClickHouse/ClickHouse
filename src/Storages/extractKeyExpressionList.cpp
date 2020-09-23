#include <Storages/extractKeyExpressionList.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTOrderByElement.h>

#include <cassert>


namespace DB
{

ASTPtr extractKeyExpressionList(const ASTPtr & node)
{
    if (!node)
        return std::make_shared<ASTExpressionList>();

    const auto * expr_func = node->as<ASTFunction>();
    const auto * expr_list = node->as<ASTExpressionList>();

    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple, extract its arguments.
        return expr_func->arguments->clone();
    }
    else if (expr_list)
    {
        auto res = std::make_shared<ASTExpressionList>();
        for (const auto & child : expr_list->children)
        {
            // FIXME: do we always ignore all ORDER BY options like ASC/DESC ?
            const auto * element = child->as<ASTOrderByElement>();
            assert(!element->children.empty());
            res->children.push_back(element->children.front());
        }
        return res;
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node);
        return res;
    }
}

}
