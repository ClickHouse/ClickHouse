#include <Interpreters/MaterializedCTEUtils.h>

#include <Parsers/ASTWithElement.h>
#include <Parsers/IAST.h>

namespace DB
{

bool hasMaterializedCTE(const IAST & ast)
{
    if (const auto * with_element = ast.as<ASTWithElement>(); with_element && with_element->is_materialized)
        return true;

    for (const auto & child : ast.children)
        if (hasMaterializedCTE(*child))
            return true;

    return false;
}

void treatMaterializedCTEsAsPlain(IAST & ast)
{
    if (auto * with_element = ast.as<ASTWithElement>())
        with_element->is_materialized = false;

    for (const auto & child : ast.children)
        treatMaterializedCTEsAsPlain(*child);
}

}
