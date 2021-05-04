#include "ConstraintMatcherVisitor.h"

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Poco/Logger.h>

namespace DB
{

bool ConstraintMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return node->as<ASTFunction>() || node->as<ASTExpressionList>();
}

std::optional<bool> ConstraintMatcher::getASTValue(const ASTPtr & node, Data & data)
{
    const auto it = data.constraints.find(node->getTreeHash().second);
    if (it != std::end(data.constraints))
    {
        for (const auto & ast : it->second)
        {
            if (node->getColumnName() == ast->getColumnName())
            {
                return true;
            }
        }
    }
    return std::nullopt;
}

void ConstraintMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto always_value = getASTValue(ast, data); always_value)
        ast = std::make_shared<ASTLiteral>(static_cast<UInt8>(*always_value));
}

}
