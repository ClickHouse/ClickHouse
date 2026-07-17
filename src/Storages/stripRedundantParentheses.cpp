#include <Storages/stripRedundantParentheses.h>

#include <Parsers/ASTWithAlias.h>
#include <Common/checkStackSize.h>

namespace DB
{

void stripRedundantParentheses(IAST & ast)
{
    checkStackSize();

    if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(&ast); !with_alias || with_alias->alias.empty())
        ast.setParenthesized(false);

    for (const auto & child : ast.children)
        if (child)
            stripRedundantParentheses(*child);
}

}
